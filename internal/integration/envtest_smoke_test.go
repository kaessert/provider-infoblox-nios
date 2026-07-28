/*
Copyright 2021 Upbound Inc.
*/

// Package integration hosts Tier-2 offline validation tests that stand up the
// REAL provider runtime against mock WAPI servers.
//
// The envtest smoke test in this package validates the Infoblox gridmaster
// read/write split end-to-end under the REAL controller-runtime manager and the
// REAL upjet async reconciler (MaxConcurrentReconciles=1, shared
// OperationTrackerStore): a create routes to the primary gridmaster while the
// Observe read-back routes to the candidate. It requires envtest control-plane
// binaries; when those assets are not available it SKIPs (so `go test ./...`
// stays green without them). Fetch them with:
//
//	go install sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.20
//	setup-envtest use -p path
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	xpcontroller "github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/crossplane/crossplane-runtime/pkg/feature"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/ratelimiter"
	tjcontroller "github.com/crossplane/upjet/pkg/controller"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	provapis "github.com/crossplane-contrib/provider-infoblox-nios/apis"
	dnsv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/dns/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/apis/v1beta1"
	"github.com/crossplane-contrib/provider-infoblox-nios/config"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/split"
	arecordctrl "github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/dns/arecord"
)

const wapiVersion = "2.12.3"

// TestEnvtestSplitSmoke runs the ARecord controller under a real envtest API
// server and the real upjet async reconciler, wired for the read/write split
// exactly like cmd/provider/main.go, and asserts the split routes create ->
// primary and reads -> candidate under real reconcile timing. It also checks the
// resource reaches Synced=True (claim #4: the shared-tracker early-return keeps
// the async runtime consistent).
func TestEnvtestSplitSmoke(t *testing.T) {
	assets := envtestAssets()
	if assets == "" {
		t.Skip("envtest assets not found; set KUBEBUILDER_ASSETS or run: setup-envtest use -p path. This validates claim #4 under the real async reconciler and needs the control-plane binaries.")
	}

	// Deterministic read routing: the shared grid replicates instantly, so the
	// convergence gate returns reads to the candidate on the first post-write
	// Observe; no management-policy init merge.
	split.ManagementPolicies = false

	g := newGrid()
	primary := newMock(t, g)
	candidate := newMock(t, g)

	crdPath, err := filepath.Abs(filepath.Join("..", "..", "package", "crds"))
	if err != nil {
		t.Fatal(err)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{crdPath},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: assets,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := provapis.AddToScheme(mgr.GetScheme()); err != nil {
		t.Fatalf("add provider apis to scheme: %v", err)
	}

	ctx := context.Background()
	p, err := config.GetProvider(ctx, false)
	if err != nil {
		t.Fatalf("GetProvider: %v", err)
	}
	log := logging.NewNopLogger()
	o := tjcontroller.Options{
		Options: xpcontroller.Options{
			Logger:                  log,
			GlobalRateLimiter:       ratelimiter.NewGlobal(10),
			PollInterval:            5 * time.Second,
			MaxConcurrentReconciles: 1,
			Features:                &feature.Flags{},
		},
		Provider:              p,
		SetupFn:               clients.TerraformSetupBuilder(p.TerraformProvider),
		OperationTrackerStore: tjcontroller.NewOperationStore(log),
	}
	split.Configure(clients.TerraformReadSetupBuilder(p.TerraformProvider), o.OperationTrackerStore)

	if err := arecordctrl.Setup(mgr, o); err != nil {
		t.Fatalf("setup arecord controller: %v", err)
	}

	mgrCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() {
		if err := mgr.Start(mgrCtx); err != nil {
			// Start returns non-nil on context cancel during teardown; that's fine.
			t.Logf("manager stopped: %v", err)
		}
	}()

	k := mgr.GetClient()
	// Wait for the manager cache to sync before creating objects.
	if !mgr.GetCache().WaitForCacheSync(mgrCtx) {
		t.Fatal("cache did not sync")
	}

	primaryHost, primaryPort := hostPort(primary)
	candidateHost, candidatePort := hostPort(candidate)
	credJSON, _ := json.Marshal(map[string]string{
		"server":       primaryHost,
		"port":         primaryPort,
		"read_server":  candidateHost,
		"read_port":    candidatePort,
		"username":     "admin",
		"password":     "secret",
		"sslmode":      "false",
		"wapi_version": wapiVersion,
	})

	mustCreate(t, ctx, k, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "crossplane-system"}})
	mustCreate(t, ctx, k, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "crossplane-system"},
		Data:       map[string][]byte{"credentials": credJSON},
	})
	mustCreate(t, ctx, k, &v1beta1.ProviderConfig{
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
	})

	ar := &dnsv1alpha1.ARecord{
		ObjectMeta: metav1.ObjectMeta{Name: "smoke-a-record"},
		Spec: dnsv1alpha1.ARecordSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: dnsv1alpha1.ARecordParameters{
				Fqdn:        ptr.To("smoke.example.com"),
				IPAddr:      ptr.To("10.0.0.20"),
				DNSView:     ptr.To("default"),
				NetworkView: ptr.To("default"),
			},
		},
	}
	mustCreate(t, ctx, k, ar)

	// Poll for routing evidence: a create POST on the primary and an Observe
	// read-back GET on the candidate, under the real async reconciler.
	deadline := time.Now().Add(60 * time.Second)
	var synced bool
	for time.Now().Before(deadline) {
		got := &dnsv1alpha1.ARecord{}
		if err := k.Get(ctx, types.NamespacedName{Name: "smoke-a-record"}, got); err == nil {
			if c := got.GetCondition(xpv1.TypeSynced); c.Status == corev1.ConditionTrue {
				synced = true
			}
		}
		if primary.count("POST", "record:a") >= 1 && candidate.count("GET", "record:a") >= 1 && synced {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	dump(t, "primary", primary)
	dump(t, "candidate", candidate)

	if primary.count("POST", "record:a") < 1 {
		t.Fatalf("expected a create POST record:a on the primary under the real reconciler")
	}
	if candidate.count("GET", "record:a") < 1 {
		t.Fatalf("expected an Observe read-back GET record:a on the candidate under the real reconciler")
	}
	// The candidate must never receive a mutating verb.
	for _, r := range candidate.snapshot() {
		if r.method != "GET" {
			t.Fatalf("candidate received a non-GET %s %s under the real reconciler", r.method, r.path)
		}
	}
	if !synced {
		t.Fatalf("ARecord did not reach Synced=True within timeout (async reconcile / shared-tracker coordination)")
	}
}

func mustCreate(t *testing.T, ctx context.Context, k client.Client, obj client.Object) {
	t.Helper()
	if err := k.Create(ctx, obj); err != nil {
		t.Fatalf("create %T %s: %v", obj, obj.GetName(), err)
	}
}

// envtestAssets returns the directory holding the envtest control-plane binaries,
// or "" if none can be found.
func envtestAssets() string {
	if v := os.Getenv("KUBEBUILDER_ASSETS"); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	for _, glob := range []string{
		filepath.Join(home, "Library", "Application Support", "io.kubebuilder.envtest", "k8s", "*"),
		filepath.Join(home, ".local", "share", "kubebuilder-envtest", "k8s", "*"),
	} {
		matches, _ := filepath.Glob(glob)
		for _, m := range matches {
			if _, err := os.Stat(filepath.Join(m, "kube-apiserver")); err == nil {
				return m
			}
		}
	}
	return ""
}

// --- minimal WAPI mock (self-contained; mirrors the split package's mock) ------

type req struct{ method, path string }

type grid struct {
	mu      sync.Mutex
	records map[string]map[string]any
	seq     int64
}

func newGrid() *grid { return &grid{records: map[string]map[string]any{}} }

type mock struct {
	server *httptest.Server
	grid   *grid
	mu     sync.Mutex
	reqs   []req
}

func newMock(t *testing.T, g *grid) *mock {
	m := &mock{grid: g}
	m.server = httptest.NewTLSServer(http.HandlerFunc(m.handle))
	t.Cleanup(m.server.Close)
	return m
}

func hostPort(m *mock) (string, string) {
	hp := strings.TrimPrefix(m.server.URL, "https://")
	if i := strings.LastIndex(hp, ":"); i >= 0 {
		return hp[:i], hp[i+1:]
	}
	return hp, ""
}

func (m *mock) snapshot() []req {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]req, len(m.reqs))
	copy(out, m.reqs)
	return out
}

func (m *mock) count(method, contains string) int {
	n := 0
	for _, r := range m.snapshot() {
		if r.method == method && strings.Contains(r.path, contains) {
			n++
		}
	}
	return n
}

func (m *mock) handle(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.reqs = append(m.reqs, req{r.Method, r.URL.Path})
	m.mu.Unlock()

	rest := strings.TrimPrefix(r.URL.Path, "/wapi/v"+wapiVersion+"/")
	if r.Method == http.MethodGet && strings.HasPrefix(rest, "extensibleattributedef") {
		writeJSON(w, 200, []map[string]any{{"name": "Terraform Internal ID", "type": "STRING"}})
		return
	}
	switch {
	case strings.HasPrefix(rest, "record:a/"):
		ref := rest
		switch r.Method {
		case http.MethodGet:
			if rec, ok := m.grid.get(ref); ok {
				writeJSON(w, 200, rec)
			} else {
				writeJSON(w, 200, []any{})
			}
		case http.MethodPut:
			if rec, ok := m.grid.get(ref); ok {
				mergeInto(rec, readBody(r))
				m.grid.put(ref, rec)
			}
			writeQuoted(w, 200, ref)
		case http.MethodDelete:
			m.grid.del(ref)
			writeQuoted(w, 200, ref)
		default:
			writeJSON(w, 200, map[string]any{})
		}
	case rest == "record:a" && r.Method == http.MethodPost:
		ref := m.grid.next()
		m.grid.put(ref, recordFromBody(ref, readBody(r)))
		writeQuoted(w, 201, ref)
	default:
		if r.Method == http.MethodGet {
			writeJSON(w, 200, []any{})
		} else {
			writeJSON(w, 200, map[string]any{})
		}
	}
}

func (g *grid) put(ref string, rec map[string]any) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.records[ref] = rec
}
func (g *grid) get(ref string) (map[string]any, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	r, ok := g.records[ref]
	return r, ok
}
func (g *grid) del(ref string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.records, ref)
}
func (g *grid) next() string {
	n := atomic.AddInt64(&g.seq, 1)
	return fmt.Sprintf("record:a/ZG5zLmJpbmRfYSix%d:smoke%d.example.com/default", n, n)
}

func recordFromBody(ref string, body map[string]any) map[string]any {
	rec := map[string]any{"_ref": ref, "use_ttl": false, "comment": "", "extattrs": map[string]any{}}
	for _, k := range []string{"name", "ipv4addr", "view", "comment", "use_ttl", "ttl"} {
		if v, ok := body[k]; ok {
			rec[k] = v
		}
	}
	if ea, ok := body["extattrs"].(map[string]any); ok {
		rec["extattrs"] = ea
	}
	return rec
}

func mergeInto(rec, body map[string]any) {
	for _, k := range []string{"name", "ipv4addr", "view", "comment", "use_ttl", "ttl"} {
		if v, ok := body[k]; ok {
			rec[k] = v
		}
	}
	if ea, ok := body["extattrs"].(map[string]any); ok {
		rec["extattrs"] = ea
	}
}

func readBody(r *http.Request) map[string]any {
	b, _ := io.ReadAll(r.Body)
	m := map[string]any{}
	_ = json.Unmarshal(b, &m)
	return m
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeQuoted(w http.ResponseWriter, code int, ref string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	b, _ := json.Marshal(ref)
	_, _ = w.Write(b)
}

func dump(t *testing.T, name string, m *mock) {
	t.Helper()
	rs := m.snapshot()
	t.Logf("--- %s recorded %d requests ---", name, len(rs))
	for _, r := range rs {
		t.Logf("  %-9s %-6s %s", name, r.method, r.path)
	}
}
