/*
Copyright 2021 Upbound Inc.
*/

// Package integration — LIVE Tier-3 e2e against a REAL NIOS grid.
//
// TestNIOSLiveE2E stands up envtest + the REAL controller-runtime manager and
// registers ALL provider controllers exactly as cmd/provider/main.go does
// (same OperationTrackerStore, MaxConcurrentReconciles, split.Configure, and
// controller.Setup wiring), then drives real CRUD through the provider against
// a live Infoblox NIOS grid and independently verifies every mutation via
// direct WAPI calls.
//
// This is the FIRST test that talks to a real grid; everything else in this
// package is offline mock validation. It is ENVIRONMENT-GATED: it is a no-op
// (t.Skip) unless NIOS_E2E is set, so it never runs in normal CI. Required env
// (see the scratchpad env file, sourced before the run):
//
//	NIOS_E2E=1 NIOS_HOST=... NIOS_USER=... NIOS_PASS=...
//	NIOS_WAPI=2.12 NIOS_PORT=443 NIOS_SSLMODE=false
//
// The lab box is MASTER-ONLY (no gridmaster candidate), so a true two-endpoint
// read/write split cannot be exercised. That is fine: with no read_server the
// split degrades to a functional no-op, and the e2e proves the no-fork provider
// + controllers perform real CRUD against real NIOS. Note the split routing
// state machine (WrapConnector -> connector -> external, incl. primeWrite and
// the convergence gate) IS still exercised here, because Configure is called
// and the read connecter connects successfully against the same box
// (sameClient=false); the distinct read_server only flips ReadSplitEnabled,
// which routing does not consult. The optional stretch subtest additionally
// points read_server at a loopback TCP proxy so a distinct endpoint string is
// exercised.
//
// All objects use clearly-throwaway names and are cleaned up (WAPI-level) even
// on failure, including the out-of-band zone. The grid is left as found; no
// grid feature (e.g. Objects Changes Tracking) is enabled.
package integration

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	xpcontroller "github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/crossplane/crossplane-runtime/pkg/feature"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/ratelimiter"
	xpresource "github.com/crossplane/crossplane-runtime/pkg/resource"
	tjcontroller "github.com/crossplane/upjet/pkg/controller"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	provapis "github.com/crossplane-contrib/provider-infoblox-nios/apis"
	dnsv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/dns/v1alpha1"
	ipv4v1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/ipv4/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/apis/v1beta1"
	"github.com/crossplane-contrib/provider-infoblox-nios/config"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/split"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/features"
)

// Throwaway object names for the live e2e. The zzz- prefix and .local zone make
// them obviously disposable and sort to the end of any grid listing.
const (
	liveZoneFQDN = "zzz-e2e-throwaway.local"
	liveView     = "Internal"
	liveNetView  = "default"
	liveGridMbr  = "infoblox.localdomain"

	liveAFQDN     = "a-e2e." + liveZoneFQDN
	liveAIPInit   = "10.9.9.10"
	liveAIPUpdate = "10.9.9.11"

	liveTXTFQDN     = "txt-e2e." + liveZoneFQDN
	liveTXTInit     = "e2e-txt-initial"
	liveTXTUpdate   = "e2e-txt-updated"
	liveNetworkCIDR = "10.253.0.0/24"

	// Stretch (two-endpoint) A record, routed through a loopback proxy so a
	// distinct read_server string exercises the ReadSplitEnabled path.
	liveSplitAFQDN = "a-split-e2e." + liveZoneFQDN
	liveSplitAIP   = "10.9.9.20"

	condTimeout = 3 * time.Minute
	condPoll    = 2 * time.Second
)

// TestNIOSLiveE2E is the environment-gated live e2e. See the package doc.
func TestNIOSLiveE2E(t *testing.T) {
	if os.Getenv("NIOS_E2E") == "" {
		t.Skip("NIOS_E2E not set; skipping live NIOS e2e (never runs in normal CI). Source the scratchpad env file to enable.")
	}
	host := mustEnv(t, "NIOS_HOST")
	user := mustEnv(t, "NIOS_USER")
	pass := mustEnv(t, "NIOS_PASS")
	wapi := envOr("NIOS_WAPI", "2.12")
	port := envOr("NIOS_PORT", "443")
	sslmode := envOr("NIOS_SSLMODE", "false")

	assets := envtestAssets()
	if assets == "" {
		t.Skip("envtest assets not found; set KUBEBUILDER_ASSETS or run: setup-envtest use -p path.")
	}

	nios := newNIOSClient(host, port, wapi, user, pass)

	// --- envtest + manager, wired exactly like cmd/provider/main.go ----------
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

	// main.go defaults --enable-management-policies=true, so mirror that.
	const enableMP = true
	split.ManagementPolicies = enableMP

	o := tjcontroller.Options{
		Options: xpcontroller.Options{
			Logger:                  log,
			GlobalRateLimiter:       ratelimiter.NewGlobal(10),
			PollInterval:            5 * time.Second, // main.go uses 1m; shortened for test turnaround only.
			MaxConcurrentReconciles: 1,
			Features:                &feature.Flags{},
		},
		Provider:              p,
		SetupFn:               clients.TerraformSetupBuilder(p.TerraformProvider),
		OperationTrackerStore: tjcontroller.NewOperationStore(log),
	}
	if enableMP {
		o.Features.Enable(features.EnableBetaManagementPolicies)
	}
	// Register the read side of the split with the SHARED tracker store, exactly
	// like main.go. With no read_server in the secret the read setup is identical
	// to the write setup (functional no-op) but still connects (sameClient=false).
	split.Configure(clients.TerraformReadSetupBuilder(p.TerraformProvider), o.OperationTrackerStore)

	// Register ALL controllers, exactly like main.go.
	if err := controller.Setup(mgr, o); err != nil {
		t.Fatalf("controller.Setup: %v", err)
	}

	mgrCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() {
		if err := mgr.Start(mgrCtx); err != nil {
			t.Logf("manager stopped: %v", err)
		}
	}()

	k := mgr.GetClient()
	if !mgr.GetCache().WaitForCacheSync(mgrCtx) {
		t.Fatal("cache did not sync")
	}

	// --- out-of-band throwaway zone (records need a grid-primary zone) --------
	// Register teardown FIRST so it runs even if zone creation partially
	// succeeds or a later step fails. Teardown is WAPI-level and idempotent.
	t.Cleanup(func() { liveTeardown(t, nios) })

	if err := nios.deleteAllByQuery("zone_auth", url.Values{"fqdn": {liveZoneFQDN}, "view": {liveView}}); err != nil {
		t.Logf("pre-clean stale zone: %v", err)
	}
	zoneRef, err := nios.createZone(liveZoneFQDN, liveView, liveGridMbr)
	if err != nil {
		t.Fatalf("create throwaway zone via WAPI: %v", err)
	}
	t.Logf("created throwaway zone %s (ref=%s)", liveZoneFQDN, zoneRef)

	// --- k8s: namespace + credentials secret + ProviderConfig (NO read_server)
	credJSON, _ := json.Marshal(map[string]string{
		"server":       host,
		"port":         port,
		"username":     user,
		"password":     pass,
		"sslmode":      sslmode,
		"wapi_version": wapi,
	})
	mustCreate(t, ctx, k, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "crossplane-system"}})
	mustCreate(t, ctx, k, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "crossplane-system"},
		Data:       map[string][]byte{"credentials": credJSON},
	})
	mustCreate(t, ctx, k, providerConfig("default", "creds"))

	// ---------------------------------------------------------------- DNS: A
	t.Run("dns_arecord_crud", func(t *testing.T) {
		ar := &dnsv1alpha1.ARecord{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e-a-record"},
			Spec: dnsv1alpha1.ARecordSpec{
				ResourceSpec: xpv1.ResourceSpec{ProviderConfigReference: &xpv1.Reference{Name: "default"}},
				ForProvider: dnsv1alpha1.ARecordParameters{
					Fqdn:    ptr.To(liveAFQDN),
					IPAddr:  ptr.To(liveAIPInit),
					DNSView: ptr.To(liveView),
				},
			},
		}
		mustCreate(t, ctx, k, ar)
		waitSyncedReady(t, ctx, k, ar)

		// Independent WAPI verification of create.
		rec := nios.expectOne(t, "record:a", url.Values{
			"name": {liveAFQDN}, "view": {liveView}, "_return_fields": {"name,ipv4addr,view"},
		})
		if got := str(rec["ipv4addr"]); got != liveAIPInit {
			t.Fatalf("WAPI A record ip after create = %q, want %q", got, liveAIPInit)
		}
		t.Logf("WAPI verified A create: %s -> %s", liveAFQDN, liveAIPInit)

		// Update the IP through the provider.
		if err := k.Get(ctx, client.ObjectKeyFromObject(ar), ar); err != nil {
			t.Fatal(err)
		}
		ar.Spec.ForProvider.IPAddr = ptr.To(liveAIPUpdate)
		if err := k.Update(ctx, ar); err != nil {
			t.Fatalf("update ARecord CR: %v", err)
		}
		nios.waitField(t, "record:a", url.Values{"name": {liveAFQDN}, "view": {liveView}, "_return_fields": {"ipv4addr"}}, "ipv4addr", liveAIPUpdate)
		t.Logf("WAPI verified A update: %s -> %s", liveAFQDN, liveAIPUpdate)

		// Delete through the provider.
		deleteAndWaitGone(t, ctx, k, ar)
		nios.waitGone(t, "record:a", url.Values{"name": {liveAFQDN}, "view": {liveView}})
		t.Logf("WAPI verified A delete: %s gone", liveAFQDN)
	})

	// -------------------------------------------------------------- DNS: TXT
	t.Run("dns_txtrecord_crud", func(t *testing.T) {
		txt := &dnsv1alpha1.TXTRecord{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e-txt-record"},
			Spec: dnsv1alpha1.TXTRecordSpec{
				ResourceSpec: xpv1.ResourceSpec{ProviderConfigReference: &xpv1.Reference{Name: "default"}},
				ForProvider: dnsv1alpha1.TXTRecordParameters{
					Fqdn:    ptr.To(liveTXTFQDN),
					Text:    ptr.To(liveTXTInit),
					DNSView: ptr.To(liveView),
				},
			},
		}
		mustCreate(t, ctx, k, txt)
		waitSyncedReady(t, ctx, k, txt)

		rec := nios.expectOne(t, "record:txt", url.Values{
			"name": {liveTXTFQDN}, "view": {liveView}, "_return_fields": {"name,text,view"},
		})
		if got := unquote(str(rec["text"])); got != liveTXTInit {
			t.Fatalf("WAPI TXT text after create = %q, want %q", got, liveTXTInit)
		}
		t.Logf("WAPI verified TXT create: %s -> %q", liveTXTFQDN, liveTXTInit)

		if err := k.Get(ctx, client.ObjectKeyFromObject(txt), txt); err != nil {
			t.Fatal(err)
		}
		txt.Spec.ForProvider.Text = ptr.To(liveTXTUpdate)
		if err := k.Update(ctx, txt); err != nil {
			t.Fatalf("update TXTRecord CR: %v", err)
		}
		nios.waitField(t, "record:txt", url.Values{"name": {liveTXTFQDN}, "view": {liveView}, "_return_fields": {"text"}}, "text", liveTXTUpdate)
		t.Logf("WAPI verified TXT update: %s -> %q", liveTXTFQDN, liveTXTUpdate)

		deleteAndWaitGone(t, ctx, k, txt)
		nios.waitGone(t, "record:txt", url.Values{"name": {liveTXTFQDN}, "view": {liveView}})
		t.Logf("WAPI verified TXT delete: %s gone", liveTXTFQDN)
	})

	// -------------------------------------------------------------- IPAM: Network
	t.Run("ipam_network_crud", func(t *testing.T) {
		net := &ipv4v1alpha1.Network{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e-network"},
			Spec: ipv4v1alpha1.NetworkSpec{
				ResourceSpec: xpv1.ResourceSpec{ProviderConfigReference: &xpv1.Reference{Name: "default"}},
				ForProvider: ipv4v1alpha1.NetworkParameters{
					Cidr:        ptr.To(liveNetworkCIDR),
					NetworkView: ptr.To(liveNetView),
					Comment:     ptr.To("zzz e2e throwaway"),
				},
			},
		}
		mustCreate(t, ctx, k, net)
		waitSyncedReady(t, ctx, k, net)

		rec := nios.expectOne(t, "network", url.Values{
			"network": {liveNetworkCIDR}, "network_view": {liveNetView},
		})
		if got := str(rec["network"]); got != liveNetworkCIDR {
			t.Fatalf("WAPI network after create = %q, want %q", got, liveNetworkCIDR)
		}
		t.Logf("WAPI verified network create: %s (view=%s)", liveNetworkCIDR, liveNetView)

		deleteAndWaitGone(t, ctx, k, net)
		nios.waitGone(t, "network", url.Values{"network": {liveNetworkCIDR}, "network_view": {liveNetView}})
		t.Logf("WAPI verified network delete: %s gone", liveNetworkCIDR)
	})

	// ---------------------------------------------- STRETCH: two-endpoint split
	// Optional. Stands up a loopback TCP proxy to $NIOS_HOST:$port and points a
	// second ProviderConfig's read_server at 127.0.0.1 (a DISTINCT string from
	// server, so ReadSplitEnabled=true). A DNS create must still converge (it is
	// the same box). If proxy setup fights us we log and skip — never fail the
	// main e2e on the stretch.
	t.Run("stretch_two_endpoint_proxy", func(t *testing.T) {
		proxyHost, proxyPort, stop, err := startLoopbackProxy(host, port)
		if err != nil {
			t.Skipf("loopback proxy setup failed; skipping stretch: %v", err)
		}
		defer stop()
		if proxyHost == host {
			t.Skipf("proxy host %q equals server host; read_server would not be distinct", proxyHost)
		}

		splitCredJSON, _ := json.Marshal(map[string]string{
			"server":       host,
			"port":         port,
			"read_server":  proxyHost, // distinct string -> ReadSplitEnabled=true
			"read_port":    proxyPort,
			"username":     user,
			"password":     pass,
			"sslmode":      sslmode,
			"wapi_version": wapi,
		})
		mustCreate(t, ctx, k, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "creds-split", Namespace: "crossplane-system"},
			Data:       map[string][]byte{"credentials": splitCredJSON},
		})
		mustCreate(t, ctx, k, providerConfig("split", "creds-split"))

		ar := &dnsv1alpha1.ARecord{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e-a-split"},
			Spec: dnsv1alpha1.ARecordSpec{
				ResourceSpec: xpv1.ResourceSpec{ProviderConfigReference: &xpv1.Reference{Name: "split"}},
				ForProvider: dnsv1alpha1.ARecordParameters{
					Fqdn:    ptr.To(liveSplitAFQDN),
					IPAddr:  ptr.To(liveSplitAIP),
					DNSView: ptr.To(liveView),
				},
			},
		}
		mustCreate(t, ctx, k, ar)
		waitSyncedReady(t, ctx, k, ar)
		rec := nios.expectOne(t, "record:a", url.Values{"name": {liveSplitAFQDN}, "view": {liveView}, "_return_fields": {"ipv4addr"}})
		if got := str(rec["ipv4addr"]); got != liveSplitAIP {
			t.Fatalf("WAPI split A ip = %q, want %q", got, liveSplitAIP)
		}
		t.Logf("WAPI verified two-endpoint (read via proxy %s:%s) A create converged: %s -> %s", proxyHost, proxyPort, liveSplitAFQDN, liveSplitAIP)

		deleteAndWaitGone(t, ctx, k, ar)
		nios.waitGone(t, "record:a", url.Values{"name": {liveSplitAFQDN}, "view": {liveView}})
		t.Logf("WAPI verified split A delete: %s gone", liveSplitAFQDN)
	})
}

// providerConfig builds a ProviderConfig referencing a credentials secret.
func providerConfig(name, secret string) *v1beta1.ProviderConfig {
	return &v1beta1.ProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1beta1.ProviderConfigSpec{
			Credentials: v1beta1.ProviderCredentials{
				Source: xpv1.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
					SecretRef: &xpv1.SecretKeySelector{
						SecretReference: xpv1.SecretReference{Name: secret, Namespace: "crossplane-system"},
						Key:             "credentials",
					},
				},
			},
		},
	}
}

// waitSyncedReady polls until the MR reports Synced=True AND Ready=True, or fails.
func waitSyncedReady(t *testing.T, ctx context.Context, k client.Client, mg xpresource.Managed) {
	t.Helper()
	deadline := time.Now().Add(condTimeout)
	var lastSynced, lastReady xpv1.Condition
	for time.Now().Before(deadline) {
		if err := k.Get(ctx, client.ObjectKeyFromObject(mg), mg); err == nil {
			lastSynced = mg.GetCondition(xpv1.TypeSynced)
			lastReady = mg.GetCondition(xpv1.TypeReady)
			if lastSynced.Status == corev1.ConditionTrue && lastReady.Status == corev1.ConditionTrue {
				t.Logf("%T %s reached Synced=True Ready=True", mg, mg.GetName())
				return
			}
		}
		time.Sleep(condPoll)
	}
	t.Fatalf("%T %s did not reach Synced=True/Ready=True within %s; lastSynced=%s(%s:%s) lastReady=%s(%s:%s)",
		mg, mg.GetName(), condTimeout,
		lastSynced.Status, lastSynced.Reason, lastSynced.Message,
		lastReady.Status, lastReady.Reason, lastReady.Message)
}

// deleteAndWaitGone deletes the MR and waits for it to disappear from the API.
func deleteAndWaitGone(t *testing.T, ctx context.Context, k client.Client, mg xpresource.Managed) {
	t.Helper()
	if err := k.Delete(ctx, mg); err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("delete %T %s: %v", mg, mg.GetName(), err)
	}
	deadline := time.Now().Add(condTimeout)
	for time.Now().Before(deadline) {
		err := k.Get(ctx, client.ObjectKeyFromObject(mg), mg)
		if apierrors.IsNotFound(err) {
			return
		}
		time.Sleep(condPoll)
	}
	t.Fatalf("%T %s not removed from API within %s", mg, mg.GetName(), condTimeout)
}

// --- direct WAPI client (independent of the provider) ------------------------

type niosClient struct {
	base       string
	user, pass string
	hc         *http.Client
}

func newNIOSClient(host, port, wapi, user, pass string) *niosClient {
	return &niosClient{
		base: fmt.Sprintf("https://%s:%s/wapi/v%s", host, port, wapi),
		user: user, pass: pass,
		hc: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // customer self-signed lab, sslmode=false
			},
		},
	}
}

func (n *niosClient) do(method, path string, q url.Values, body any) ([]byte, int, error) {
	u := n.base + "/" + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = strings.NewReader(string(b))
	}
	req, err := http.NewRequest(method, u, rdr)
	if err != nil {
		return nil, 0, err
	}
	req.SetBasicAuth(n.user, n.pass)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := n.hc.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b, resp.StatusCode, nil
}

// get returns the array of objects for an object-type query.
func (n *niosClient) get(objtype string, q url.Values) ([]map[string]any, error) {
	b, code, err := n.do(http.MethodGet, objtype, q, nil)
	if err != nil {
		return nil, err
	}
	if code < 200 || code >= 300 {
		return nil, fmt.Errorf("GET %s: status %d: %s", objtype, code, string(b))
	}
	var out []map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("GET %s: decode %q: %w", objtype, string(b), err)
	}
	return out, nil
}

// createZone creates an authoritative zone with a grid_primary member so that
// records placed in it are actually served. Returns the object _ref.
func (n *niosClient) createZone(fqdn, view, member string) (string, error) {
	body := map[string]any{
		"fqdn":         fqdn,
		"view":         view,
		"grid_primary": []map[string]any{{"name": member}},
	}
	b, code, err := n.do(http.MethodPost, "zone_auth", nil, body)
	if err != nil {
		return "", err
	}
	if code < 200 || code >= 300 {
		return "", fmt.Errorf("create zone: status %d: %s", code, string(b))
	}
	var ref string
	_ = json.Unmarshal(b, &ref)
	return ref, nil
}

// deleteRef deletes an object by _ref (idempotent: 404 is not an error).
func (n *niosClient) deleteRef(ref string) error {
	b, code, err := n.do(http.MethodDelete, ref, nil, nil)
	if err != nil {
		return err
	}
	if code == 404 {
		return nil
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("delete %s: status %d: %s", ref, code, string(b))
	}
	return nil
}

// deleteAllByQuery deletes every object matching the query.
func (n *niosClient) deleteAllByQuery(objtype string, q url.Values) error {
	objs, err := n.get(objtype, q)
	if err != nil {
		return err
	}
	for _, o := range objs {
		ref := str(o["_ref"])
		if ref == "" {
			continue
		}
		if err := n.deleteRef(ref); err != nil {
			return err
		}
	}
	return nil
}

// expectOne asserts exactly one object matches and returns it.
func (n *niosClient) expectOne(t *testing.T, objtype string, q url.Values) map[string]any {
	t.Helper()
	objs, err := n.get(objtype, q)
	if err != nil {
		t.Fatalf("WAPI GET %s: %v", objtype, err)
	}
	if len(objs) != 1 {
		t.Fatalf("WAPI GET %s: expected exactly 1 object, got %d: %v", objtype, len(objs), objs)
	}
	return objs[0]
}

// waitField polls until a single matching object has field == want.
func (n *niosClient) waitField(t *testing.T, objtype string, q url.Values, field, want string) {
	t.Helper()
	deadline := time.Now().Add(condTimeout)
	var last string
	for time.Now().Before(deadline) {
		objs, err := n.get(objtype, q)
		if err == nil && len(objs) == 1 {
			last = unquote(str(objs[0][field]))
			if last == want {
				return
			}
		}
		time.Sleep(condPoll)
	}
	t.Fatalf("WAPI %s field %q did not become %q within %s (last=%q)", objtype, field, want, condTimeout, last)
}

// waitGone polls until no object matches the query.
func (n *niosClient) waitGone(t *testing.T, objtype string, q url.Values) {
	t.Helper()
	deadline := time.Now().Add(condTimeout)
	for time.Now().Before(deadline) {
		objs, err := n.get(objtype, q)
		if err == nil && len(objs) == 0 {
			return
		}
		time.Sleep(condPoll)
	}
	t.Fatalf("WAPI %s %v still present within %s", objtype, q, condTimeout)
}

// liveTeardown removes everything the test may have created, WAPI-level and
// idempotent, then the throwaway zone (which also cascades any child records).
// Runs even on failure. Leaves the grid as found; enables no grid feature.
func liveTeardown(t *testing.T, n *niosClient) {
	t.Helper()
	steps := []struct {
		objtype string
		q       url.Values
	}{
		{"record:a", url.Values{"name": {liveAFQDN}, "view": {liveView}}},
		{"record:a", url.Values{"name": {liveSplitAFQDN}, "view": {liveView}}},
		{"record:txt", url.Values{"name": {liveTXTFQDN}, "view": {liveView}}},
		{"network", url.Values{"network": {liveNetworkCIDR}, "network_view": {liveNetView}}},
		{"zone_auth", url.Values{"fqdn": {liveZoneFQDN}, "view": {liveView}}},
	}
	for _, s := range steps {
		if err := n.deleteAllByQuery(s.objtype, s.q); err != nil {
			t.Logf("teardown %s %v: %v", s.objtype, s.q, err)
		}
	}
	t.Logf("teardown complete: throwaway zone %s and all e2e objects removed", liveZoneFQDN)
}

// startLoopbackProxy forwards 127.0.0.1:<ephemeral> -> host:port as raw TCP
// (TLS passthrough; the infoblox client uses sslmode=false so no SNI/hostname
// verification). Returns the proxy host, port, and a stop func.
func startLoopbackProxy(host, port string) (string, string, func(), error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", "", nil, err
	}
	target := net.JoinHostPort(host, port)
	done := make(chan struct{})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
					return
				default:
					return
				}
			}
			go func(client net.Conn) {
				defer client.Close()
				up, err := net.DialTimeout("tcp", target, 10*time.Second)
				if err != nil {
					return
				}
				defer up.Close()
				go func() { _, _ = io.Copy(up, client) }()
				_, _ = io.Copy(client, up)
			}(c)
		}
	}()
	_, pport, _ := net.SplitHostPort(ln.Addr().String())
	stop := func() { close(done); _ = ln.Close() }
	return "127.0.0.1", pport, stop, nil
}

// --- tiny helpers ------------------------------------------------------------

func mustEnv(t *testing.T, k string) string {
	t.Helper()
	v := os.Getenv(k)
	if v == "" {
		t.Fatalf("%s must be set for the live e2e", k)
	}
	return v
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

// unquote trims a single pair of surrounding double quotes (NIOS may wrap TXT
// rdata in quotes).
func unquote(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}
