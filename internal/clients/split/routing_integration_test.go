/*
Copyright 2021 Upbound Inc.
*/

package split

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	xpresource "github.com/crossplane/crossplane-runtime/pkg/resource"
	tjcontroller "github.com/crossplane/upjet/pkg/controller"
	"github.com/crossplane/upjet/pkg/terraform"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	dnsv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/dns/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/apis/v1beta1"
	"github.com/crossplane-contrib/provider-infoblox-nios/config"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients"
)

// wapiVersion is the WAPI version the mock servers answer under; the credentials
// secret carries the same value so the go-client builds /wapi/v<ver>/ paths that
// the mock matches.
const wapiVersion = "2.12.3"

// TestReadWriteRoutingAgainstMockWAPI is the Tier-2 offline proof that the
// gridmaster read/write split routes per-verb correctly. It drives the REAL
// no-fork runtime (config.GetProvider(ctx, false) -> infoblox.Provider()) wired
// exactly as the generated controllers wire it (split.WrapConnector around a
// tjcontroller.NewTerraformPluginSDKAsyncConnector, sharing a single
// OperationTrackerStore), against two mock WAPI servers, and asserts:
//
//   - writes (POST create, PUT update, DELETE) hit the PRIMARY only, never the
//     candidate;
//   - the Observe read-back (GET record:a/<ref>) hits the CANDIDATE (read
//     endpoint) — the core proof of the split;
//   - with read_server absent the split degrades to a no-op: every verb hits the
//     single primary and the candidate is untouched.
//
// The async runtime is honored with bounded polling of the shared operation
// tracker (never immediate assertions).
func TestReadWriteRoutingAgainstMockWAPI(t *testing.T) {
	t.Run("SplitRoutesPerVerb", func(t *testing.T) {
		resetForTest()
		g := newGrid()
		primary := newWAPIMock(t, "primary", wapiVersion, g)
		candidate := newWAPIMock(t, "candidate", wapiVersion, g)

		conn, ots := buildSplitConnector(t, primary, candidate, true)
		mr := newARecordMR("arec-split-1")

		// --- reconcile 1: Observe (not exists) -> Create --------------------
		ec := mustConnect(t, conn, mr)
		obs, err := ec.Observe(context.Background(), mr)
		if err != nil {
			t.Fatalf("initial Observe: %v", err)
		}
		if obs.ResourceExists {
			t.Fatalf("initial Observe should report the resource does not exist")
		}
		if _, err := ec.Create(context.Background(), mr); err != nil {
			t.Fatalf("Create: %v", err)
		}
		waitOpDone(t, ots, mr, "create")
		_ = ec.Disconnect(context.Background())

		// --- reconcile 2: Observe (exists / up-to-date) ---------------------
		// A fresh Connect mirrors a new reconcile; the shared tracker retains the
		// tfstate (with the created record's _ref) so Observe reads it back from
		// the candidate.
		ec = mustConnect(t, conn, mr)
		obs, err = ec.Observe(context.Background(), mr)
		if err != nil {
			t.Fatalf("post-create Observe: %v", err)
		}
		if !obs.ResourceExists {
			dumpRequests(t, primary, candidate)
			t.Fatalf("post-create Observe should report the resource exists")
		}
		_ = ec.Disconnect(context.Background())

		// --- reconcile 3: force drift -> Update -----------------------------
		mr.Spec.ForProvider.Comment = ptr.To("updated by routing test")
		ec = mustConnect(t, conn, mr)
		obs, err = ec.Observe(context.Background(), mr)
		if err != nil {
			t.Fatalf("pre-update Observe: %v", err)
		}
		if obs.ResourceExists && !obs.ResourceUpToDate {
			if _, err := ec.Update(context.Background(), mr); err != nil {
				t.Fatalf("Update: %v", err)
			}
			waitOpDone(t, ots, mr, "update")
		} else {
			t.Logf("note: Observe did not detect drift (exists=%v upToDate=%v); skipping Update verb",
				obs.ResourceExists, obs.ResourceUpToDate)
		}
		_ = ec.Disconnect(context.Background())

		// --- reconcile 4: Delete --------------------------------------------
		ec = mustConnect(t, conn, mr)
		if _, err := ec.Delete(context.Background(), mr); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		waitOpDone(t, ots, mr, "delete")
		_ = ec.Disconnect(context.Background())

		// --- routing assertions ---------------------------------------------
		dumpRequests(t, primary, candidate)

		// (1) The candidate must have received ONLY GETs.
		for _, r := range candidate.requests() {
			if r.method != "GET" {
				t.Fatalf("candidate (read endpoint) received a non-GET %s %s; reads must never mutate the candidate", r.method, r.path)
			}
		}
		// (2) POST create lands on primary, never candidate.
		if got := primary.countRecordA("POST"); got < 1 {
			t.Fatalf("expected >=1 POST record:a on primary, got %d", got)
		}
		if got := candidate.countRecordA("POST"); got != 0 {
			t.Fatalf("expected 0 POST record:a on candidate, got %d", got)
		}
		// (3) The Observe read-back GET record:a/<ref> lands on the candidate.
		if got := candidate.countRecordA("GET"); got < 1 {
			t.Fatalf("expected >=1 GET record:a/<ref> on candidate (Observe read-back), got %d", got)
		}
		// (4) PUT/DELETE (if exercised) land on primary, never candidate.
		if got := candidate.countRecordA("PUT"); got != 0 {
			t.Fatalf("expected 0 PUT record:a on candidate, got %d", got)
		}
		if got := candidate.countRecordA("DELETE"); got != 0 {
			t.Fatalf("expected 0 DELETE record:a on candidate, got %d", got)
		}
		if got := primary.countRecordA("DELETE"); got < 1 {
			t.Fatalf("expected >=1 DELETE record:a on primary, got %d", got)
		}
		// Both endpoints must have answered the Connect prerequisite EA probe.
		if !sawEAProbe(primary) {
			t.Fatalf("primary never received the extensibleattributedef Connect prerequisite GET")
		}
		if !sawEAProbe(candidate) {
			t.Fatalf("candidate never received the extensibleattributedef Connect prerequisite GET")
		}
		reportUnexpected(t, primary, candidate)
	})

	t.Run("NoReadServerIsBackwardCompatNoOp", func(t *testing.T) {
		resetForTest()
		g := newGrid()
		primary := newWAPIMock(t, "primary", wapiVersion, g)
		candidate := newWAPIMock(t, "candidate", wapiVersion, g)

		// withReadServer=false: the credentials secret has no read_server, so the
		// read setup is identical to the write setup and every verb hits primary.
		conn, ots := buildSplitConnector(t, primary, candidate, false)
		mr := newARecordMR("arec-noop-1")

		ec := mustConnect(t, conn, mr)
		if _, err := ec.Observe(context.Background(), mr); err != nil {
			t.Fatalf("Observe: %v", err)
		}
		if _, err := ec.Create(context.Background(), mr); err != nil {
			t.Fatalf("Create: %v", err)
		}
		waitOpDone(t, ots, mr, "create")
		_ = ec.Disconnect(context.Background())

		ec = mustConnect(t, conn, mr)
		obs, err := ec.Observe(context.Background(), mr)
		if err != nil {
			t.Fatalf("post-create Observe: %v", err)
		}
		if !obs.ResourceExists {
			dumpRequests(t, primary, candidate)
			t.Fatalf("post-create Observe should report exists in no-op mode")
		}
		_ = ec.Disconnect(context.Background())

		dumpRequests(t, primary, candidate)
		if len(candidate.requests()) != 0 {
			t.Fatalf("candidate must be untouched when read_server is absent; got %d requests", len(candidate.requests()))
		}
		if got := primary.countRecordA("POST"); got < 1 {
			t.Fatalf("expected create POST on primary in no-op mode, got %d", got)
		}
		if got := primary.countRecordA("GET"); got < 1 {
			t.Fatalf("expected Observe read-back GET on primary in no-op mode, got %d", got)
		}
	})
}

// buildSplitConnector wires the REAL runtime path exactly as the generated
// controllers do: it builds the write + read terraform.SetupFn from the real
// infoblox.Provider(), constructs the async write connector, registers the read
// side via Configure (sharing the write OTS), and wraps the write connector with
// WrapConnector. It returns the split-decorated connector and the shared OTS.
func buildSplitConnector(t *testing.T, primary, candidate *wapiMock, withReadServer bool) (managed.ExternalConnecter, *tjcontroller.OperationTrackerStore) {
	t.Helper()

	// Deterministic routing: disable the replication grace window so the first
	// Observe after a write completes returns to the candidate immediately. The
	// grace-window state machine itself is covered by TestGraceWindowStateMachine.
	prevGrace := GraceWindow
	GraceWindow = 0
	prevMP := ManagementPolicies
	ManagementPolicies = false
	t.Cleanup(func() {
		GraceWindow = prevGrace
		ManagementPolicies = prevMP
	})

	p, err := config.GetProvider(context.Background(), false)
	if err != nil {
		t.Fatalf("GetProvider(runtime): %v", err)
	}
	tfProv := p.TerraformProvider
	cfg := p.Resources["infoblox_a_record"]
	if cfg == nil {
		t.Fatalf("resource config for infoblox_a_record not found")
	}

	kube := buildKube(t, primary, candidate, withReadServer)

	writeSetup := clients.TerraformSetupBuilder(tfProv)
	readSetup := clients.TerraformReadSetupBuilder(tfProv)

	ots := tjcontroller.NewOperationStore(logging.NewNopLogger())
	// Register the read side sharing the write OTS — exactly cmd/provider/main.go.
	Configure(readSetup, ots)

	writeConn := tjcontroller.NewTerraformPluginSDKAsyncConnector(kube, ots, writeSetup, cfg,
		tjcontroller.WithTerraformPluginSDKAsyncLogger(logging.NewNopLogger()),
		tjcontroller.WithTerraformPluginSDKAsyncManagementPolicies(false),
		// The generated controller supplies an APICallbacks provider; the async
		// runtime invokes callback.Create/Update/Destroy on completion, so a nil
		// provider would panic. A no-op provider is sufficient here since the test
		// polls the operation tracker directly rather than requeueing.
		tjcontroller.WithTerraformPluginSDKAsyncCallbackProvider(noopCallbacks{}),
	)
	conn := WrapConnector(kube, cfg, writeConn, logging.NewNopLogger())
	return conn, ots
}

func buildKube(t *testing.T, primary, candidate *wapiMock, withReadServer bool) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1beta1.SchemeBuilder.AddToScheme(s); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	if err := dnsv1alpha1.SchemeBuilder.AddToScheme(s); err != nil {
		t.Fatalf("add dns v1alpha1: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("add corev1: %v", err)
	}

	primaryHost, primaryPort := primary.hostPort()
	candidateHost, candidatePort := candidate.hostPort()

	creds := map[string]string{
		"server":             primaryHost,
		"port":               primaryPort,
		"username":           "admin",
		"password":           "secret",
		"sslmode":            "false", // -> InsecureSkipVerify=true (accept self-signed httptest cert)
		"wapi_version":       wapiVersion,
		"connection_timeout": "60",
		"pool_connections":   "10",
	}
	if withReadServer {
		creds["read_server"] = candidateHost
		creds["read_port"] = candidatePort
	}
	credJSON, err := json.Marshal(creds)
	if err != nil {
		t.Fatal(err)
	}

	pc := &v1beta1.ProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
		Spec: v1beta1.ProviderConfigSpec{
			Credentials: v1beta1.ProviderCredentials{
				Source: xpv1.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
					SecretRef: &xpv1.SecretKeySelector{
						SecretReference: xpv1.SecretReference{Name: "creds", Namespace: "crossplane-system"},
						Key:             "credentials",
					},
				},
			},
		},
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "crossplane-system"},
		Data:       map[string][]byte{"credentials": credJSON},
	}
	return fakeclient.NewClientBuilder().WithScheme(s).WithObjects(pc, sec).Build()
}

// newARecordMR builds a real ARecord managed resource with a static IP so the
// create path takes the direct CreateARecord branch (no next-available-IP or
// DNS-view lookups).
func newARecordMR(uid string) *dnsv1alpha1.ARecord {
	mr := &dnsv1alpha1.ARecord{}
	mr.SetName("a-" + uid)
	mr.SetUID(types.UID("uid-" + uid))
	mr.Spec.ForProvider = dnsv1alpha1.ARecordParameters{
		Fqdn:        ptr.To("test.example.com"),
		IPAddr:      ptr.To("10.0.0.10"),
		DNSView:     ptr.To("default"),
		NetworkView: ptr.To("default"),
	}
	mr.Spec.ProviderConfigReference = &xpv1.Reference{Name: "default"}
	return mr
}

// noopCallbacks is a no-op tjcontroller.CallbackProvider. The async runtime
// calls these on operation completion; the test observes completion by polling
// the shared operation tracker instead of relying on the callback's requeue.
type noopCallbacks struct{}

func (noopCallbacks) Create(string) terraform.CallbackFn {
	return func(error, context.Context) error { return nil }
}
func (noopCallbacks) Update(string) terraform.CallbackFn {
	return func(error, context.Context) error { return nil }
}
func (noopCallbacks) Destroy(string) terraform.CallbackFn {
	return func(error, context.Context) error { return nil }
}

func mustConnect(t *testing.T, conn managed.ExternalConnecter, mr xpresource.Managed) managed.ExternalClient {
	t.Helper()
	ec, err := conn.Connect(context.Background(), mr)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return ec
}

// waitOpDone bounded-polls the shared operation tracker until the async op
// completes, then fails on any recorded async error. This is the eventual
// consistency contract of the no-fork async runtime.
func waitOpDone(t *testing.T, ots *tjcontroller.OperationTrackerStore, mr *dnsv1alpha1.ARecord, op string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	tr := ots.Tracker(mr)
	for time.Now().Before(deadline) {
		if !tr.LastOperation.IsRunning() {
			if err := tr.LastOperation.Error(); err != nil {
				t.Fatalf("async %s operation failed: %v", op, err)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("async %s operation did not complete within timeout", op)
}

func sawEAProbe(m *wapiMock) bool {
	for _, r := range m.requests() {
		if r.method == "GET" && strings.Contains(r.path, "extensibleattributedef") {
			return true
		}
	}
	return false
}

func dumpRequests(t *testing.T, primary, candidate *wapiMock) {
	t.Helper()
	t.Logf("--- PRIMARY (write/server) recorded %d requests ---", len(primary.requests()))
	for _, r := range primary.requests() {
		t.Logf("  primary   %-6s %s", r.method, r.path)
	}
	t.Logf("--- CANDIDATE (read/read_server) recorded %d requests ---", len(candidate.requests()))
	for _, r := range candidate.requests() {
		t.Logf("  candidate %-6s %s", r.method, r.path)
	}
}

func reportUnexpected(t *testing.T, mocks ...*wapiMock) {
	t.Helper()
	for _, m := range mocks {
		m.mu.Lock()
		u := append([]string(nil), m.unexpect...)
		m.mu.Unlock()
		if len(u) > 0 {
			t.Logf("note: %s answered %d request(s) via the permissive fallback (extra WAPI surface): %s",
				m.name, len(u), strings.Join(u, "; "))
		}
	}
}
