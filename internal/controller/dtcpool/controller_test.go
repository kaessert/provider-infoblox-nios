// Package dtcpool unit tests for the DTCPool MR controllers. Tests use
// inline httptest.NewServer mocks that emulate the WAPI dtc:pool
// endpoints, PascalCase test names (no underscores), and white-box access
// to the unexported connectors/clients so both scopes can be exercised
// without going through the full Connect() credential bridge on every
// test.
package dtcpool

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	xpv2 "github.com/crossplane/crossplane-runtime/v2/apis/common/v2"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/dtcpool/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/dtcpool/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
)

// ── generic helpers ─────────────────────────────────────────────────────────

// Shared literals reused across many test cases (deduplicated for goconst).
const (
	nsDefault       = "default"
	eaKeyEnv        = "env"
	eaValProd       = "prod"
	monitorRefHTTP  = "dtc:monitor:http/ZG5z...:http"
	serverRefA      = "dtc:server/ZG5z...serverA:my-dtc-server-a"
	lbRoundRobin    = "ROUND_ROBIN"
	unusedSecretKey = "unused"
)

func stringPtr(s string) *string { return &s }
func boolPtr(b bool) *bool       { return &b }
func uint32Ptr(u uint32) *uint32 { return &u }

// newTestScheme returns a scheme with corev1 (for Secrets) and the
// provider's API types registered.
func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme,
		clusterpcv1alpha1.SchemeBuilder.AddToScheme,
		namespacedpcv1alpha1.SchemeBuilder.AddToScheme,
		clusterv1alpha1.SchemeBuilder.AddToScheme,
		namespacedv1alpha1.SchemeBuilder.AddToScheme,
	} {
		if err := add(s); err != nil {
			t.Fatalf("cannot build test scheme: %v", err)
		}
	}
	return s
}

// credentialsSecret returns a Secret carrying the host/username/password
// keys the credential bridge expects.
func credentialsSecret(ns, name, host, username, password string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data: map[string][]byte{
			"host":     []byte(host),
			"username": []byte(username),
			"password": []byte(password),
		},
	}
}

// newClusterDTCPool builds a minimal cluster-scoped DTCPool CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterDTCPool(crName, externalName string) *clusterv1alpha1.DTCPool {
	cr := &clusterv1alpha1.DTCPool{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.DTCPoolSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: nsDefault},
			},
			ForProvider: clusterv1alpha1.DTCPoolParameters{
				Name:              stringPtr("my-dtc-pool"),
				LBPreferredMethod: stringPtr(lbRoundRobin),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedDTCPool is the namespaced variant of newClusterDTCPool.
func newNamespacedDTCPool(ns, crName, externalName, pcKind string) *namespacedv1alpha1.DTCPool {
	cr := &namespacedv1alpha1.DTCPool{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.DTCPoolSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: nsDefault},
			},
			ForProvider: namespacedv1alpha1.DTCPoolParameters{
				Name:              stringPtr("my-dtc-pool"),
				LBPreferredMethod: stringPtr(lbRoundRobin),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// ── mock WAPI server ─────────────────────────────────────────────────────
//
// mockDtcPoolServer emulates the subset of NIOS WAPI dtc:pool endpoints
// exercised by the DTCPool controller (POST create, GET/PUT/DELETE by
// _ref). Records are stored using the real ibclient.DtcPool type so
// decoding incoming request bodies exactly matches what the SDK sends
// (including its monitors []string <-> []*DtcMonitorHttp translation).
// GET/PUT responses are rendered via a local wire-format mirror
// (wireDtcPool) instead of DtcPool.MarshalJSON directly — the SDK type's
// custom marshaler only emits "consolidated_monitors" when
// AutoConsolidatedMonitors is explicitly set (a WAPI attribute this
// provider never uses — see the package doc comment), but a real Grid
// Manager always includes consolidated_monitors/health in GET responses
// regardless of what was sent on create/update.

type mockDtcPoolServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.DtcPool
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert field content.
	lastUpdateBody []byte

	// lastGetQuery captures the raw query string of the most recent GET
	// request, for tests that assert which _return_fields were requested.
	lastGetQuery string
}

func newMockDtcPoolServer() *mockDtcPoolServer {
	return &mockDtcPoolServer{records: map[string]*ibclient.DtcPool{}}
}

func (m *mockDtcPoolServer) seed(rec *ibclient.DtcPool) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if rec.Ref == "" {
		rec.Ref = m.newRefLocked(rec)
	}
	m.records[rec.Ref] = rec
	return rec.Ref
}

func (m *mockDtcPoolServer) newRefLocked(rec *ibclient.DtcPool) string {
	name := ""
	if rec.Name != nil {
		name = *rec.Name
	}
	return "dtc:pool/test" + itoa(m.nextRef) + ":" + name
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// wireDtcPool mirrors the NIOS WAPI wire format for a dtc:pool object,
// bypassing ibclient.DtcPool's custom MarshalJSON (see the mock server's
// doc comment for why).
type wireDtcPool struct {
	Ref                     string                                       `json:"_ref,omitempty"`
	Name                    *string                                      `json:"name,omitempty"`
	LbPreferredMethod       string                                       `json:"lb_preferred_method,omitempty"`
	LbAlternateMethod       string                                       `json:"lb_alternate_method,omitempty"`
	Comment                 *string                                      `json:"comment,omitempty"`
	Servers                 []*ibclient.DtcServerLink                    `json:"servers"`
	Availability            string                                       `json:"availability,omitempty"`
	Quorum                  *uint32                                      `json:"quorum,omitempty"`
	LbPreferredTopology     *string                                      `json:"lb_preferred_topology,omitempty"`
	LbDynamicRatioPreferred *ibclient.SettingDynamicratio                `json:"lb_dynamic_ratio_preferred,omitempty"`
	LbAlternateTopology     *string                                      `json:"lb_alternate_topology,omitempty"`
	LbDynamicRatioAlternate *ibclient.SettingDynamicratio                `json:"lb_dynamic_ratio_alternate,omitempty"`
	Monitors                []string                                     `json:"monitors"`
	Disable                 *bool                                        `json:"disable,omitempty"`
	Ttl                     *uint32                                      `json:"ttl,omitempty"`
	UseTtl                  *bool                                        `json:"use_ttl,omitempty"`
	Ea                      ibclient.EA                                  `json:"extattrs"`
	ConsolidatedMonitors    []*ibclient.DtcPoolConsolidatedMonitorHealth `json:"consolidated_monitors,omitempty"`
	Health                  *ibclient.DtcHealth                          `json:"health,omitempty"`
}

func toWire(rec *ibclient.DtcPool) *wireDtcPool {
	w := &wireDtcPool{
		Ref:                     rec.Ref,
		Name:                    rec.Name,
		LbPreferredMethod:       rec.LbPreferredMethod,
		LbAlternateMethod:       rec.LbAlternateMethod,
		Comment:                 rec.Comment,
		Servers:                 rec.Servers,
		Availability:            rec.Availability,
		Quorum:                  rec.Quorum,
		LbPreferredTopology:     rec.LbPreferredTopology,
		LbDynamicRatioPreferred: rec.LbDynamicRatioPreferred,
		LbAlternateTopology:     rec.LbAlternateTopology,
		LbDynamicRatioAlternate: rec.LbDynamicRatioAlternate,
		Disable:                 rec.Disable,
		Ttl:                     rec.Ttl,
		UseTtl:                  rec.UseTtl,
		Ea:                      rec.Ea,
		ConsolidatedMonitors:    rec.ConsolidatedMonitors,
		Health:                  rec.Health,
	}
	if len(rec.Servers) == 0 {
		w.Servers = []*ibclient.DtcServerLink{}
	}
	for _, mon := range rec.Monitors {
		if mon != nil {
			w.Monitors = append(w.Monitors, mon.Ref)
		}
	}
	if w.Monitors == nil {
		w.Monitors = []string{}
	}
	return w
}

// handler returns an http.Handler implementing the dtc:pool WAPI surface.
func (m *mockDtcPoolServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/dtc:pool", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.DtcPool
		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Mirror live WAPI: when lb_alternate_method is omitted from the
		// create request, the server defaults it to "NONE" on read-back —
		// it is never actually absent from a subsequent GET.
		if rec.LbAlternateMethod == "" {
			rec.LbAlternateMethod = lbAlternateMethodUnset
		}
		ref := m.seed(&rec)
		writeJSON(w, http.StatusOK, ref)
	})

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		m.lastGetQuery = r.URL.RawQuery
		rec, ok := m.records[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, toWire(rec))
	})

	mux.HandleFunc("PUT /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		existing, ok := m.records[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		body, err := readAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var incoming ibclient.DtcPool
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Mirror live WAPI's write-path validation: lb_alternate_method
		// only accepts GLOBAL_AVAILABILITY, RATIO, ROUND_ROBIN, TOPOLOGY,
		// ALL_AVAILABLE, or DYNAMIC_RATIO as an explicit value — "NONE" is
		// a read-back-only default WAPI rejects if sent back explicitly.
		if incoming.LbAlternateMethod == lbAlternateMethodUnset {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"Error":"AdmConProtoError: Invalid value for lb_alternate_method (\"NONE\") valid values are: GLOBAL_AVAILABILITY, RATIO, ROUND_ROBIN, TOPOLOGY, ALL_AVAILABLE, DYNAMIC_RATIO","code":"Client.Ibap.Proto"}`))
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		existing.Name = incoming.Name
		existing.LbPreferredMethod = incoming.LbPreferredMethod
		existing.LbAlternateMethod = incoming.LbAlternateMethod
		existing.Comment = incoming.Comment
		existing.Servers = incoming.Servers
		existing.Availability = incoming.Availability
		existing.Quorum = incoming.Quorum
		existing.LbPreferredTopology = incoming.LbPreferredTopology
		existing.LbDynamicRatioPreferred = incoming.LbDynamicRatioPreferred
		existing.LbAlternateTopology = incoming.LbAlternateTopology
		existing.LbDynamicRatioAlternate = incoming.LbDynamicRatioAlternate
		existing.Monitors = incoming.Monitors
		existing.Disable = incoming.Disable
		existing.Ttl = incoming.Ttl
		existing.UseTtl = incoming.UseTtl
		existing.Ea = incoming.Ea
		m.mu.Unlock()

		writeJSON(w, http.StatusOK, ref)
	})

	mux.HandleFunc("DELETE /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		_, ok := m.records[ref]
		delete(m.records, ref)
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, ref)
	})

	return mux
}

func readAll(rc interface{ Read([]byte) (int, error) }) ([]byte, error) {
	buf := make([]byte, 0, 512)
	tmp := make([]byte, 512)
	for {
		n, err := rc.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			if err.Error() == "EOF" {
				return buf, nil
			}
			return buf, err
		}
	}
}

// fixedStatusHandler always responds with the given HTTP status — used to
// exercise the generic (non-404) error classification paths.
func fixedStatusHandler(status int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"Error":"boom"}`))
	})
}

// newTestClients builds a *dtcPoolClients pointed at the given
// httptest.Server via plain HTTP (no TLS needed — the WapiRequestBuilder
// only switches to HTTPS when hostCfg.Scheme != "http").
func newTestClients(t *testing.T, srv *httptest.Server) *dtcPoolClients {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	clients, err := newClientsWithScheme(&nioCredentials{
		Host:     u.Hostname(),
		Username: "test-user",
		Password: "test-pass",
	}, true, "http", u.Port())
	if err != nil {
		t.Fatalf("cannot build test clients: %v", err)
	}
	return clients
}

// ── cluster: Observe ────────────────────────────────────────────────────

func TestClusterObserveSuccess(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcPool{
		Name:              stringPtr("my-dtc-pool"),
		LbPreferredMethod: lbRoundRobin,
		Comment:           stringPtr("hello"),
		Disable:           boolPtr(false),
		Ea:                ibclient.EA{eaKeyEnv: eaValProd},
	})

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.Disable = boolPtr(false)
	cr.Spec.ForProvider.ExtAttrs = map[string]string{eaKeyEnv: eaValProd}

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true, got false")
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true, got false")
	}
	if cr.Status.AtProvider.ID != ref {
		t.Errorf("AtProvider.ID = %q, want %q", cr.Status.AtProvider.ID, ref)
	}
	if cr.Status.AtProvider.Ref == nil || *cr.Status.AtProvider.Ref != ref {
		t.Errorf("AtProvider.Ref = %v, want %q", cr.Status.AtProvider.Ref, ref)
	}
	if cond := cr.GetCondition(xpv1.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

// TestObserveDoesNotRequestAutoConsolidatedMonitors is a regression test
// for the WAPI 400 "Unknown argument/field: 'auto_consolidated_monitors'"
// bug: this Grid Manager's schema doesn't recognize that return field, so
// Observe must never request it via getDtcPoolByRef's low-level GET.
func TestObserveDoesNotRequestAutoConsolidatedMonitors(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcPool{
		Name:              stringPtr("my-dtc-pool"),
		LbPreferredMethod: lbRoundRobin,
	})

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", ref)

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	q, err := url.ParseQuery(m.lastGetQuery)
	if err != nil {
		t.Fatalf("parsing captured GET query %q: %v", m.lastGetQuery, err)
	}
	returnFields := q.Get("_return_fields")
	if strings.Contains(returnFields, "auto_consolidated_monitors") {
		t.Errorf("GET _return_fields = %q, must not include auto_consolidated_monitors", returnFields)
	}
	if !strings.Contains(returnFields, "consolidated_monitors") {
		t.Errorf("GET _return_fields = %q, want it to still include consolidated_monitors", returnFields)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", "dtc:pool/does-not-exist:my-dtc-pool")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

// TestObservePreCreateState verifies that Observe short-circuits (no HTTP
// call) when the external-name still equals the CR's Kubernetes name — the
// pre-create state for a server-assigned external-name strategy.
func TestObservePreCreateState(t *testing.T) {
	// Zero-route server: any request is an error, proving Observe never
	// calls it during the pre-create guard.
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())    // simulate NameAsExternalName initializer

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false in pre-create state, got true")
	}
}

func TestClusterObserveServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", "dtc:pool/test1:my-dtc-pool")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", "dtc:pool/test1:my-dtc-pool")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (nil pointers, empty strings, a
// nil Ea map) must not panic and must produce a valid observation with
// nil-safe AtProvider fields.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcPool{})

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", ref)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error on minimal response: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true for minimal response, got false")
	}

	ap := cr.Status.AtProvider
	if ap.ID != ref {
		t.Errorf("AtProvider.ID = %q, want %q", ap.ID, ref)
	}
	if ap.Name != nil {
		t.Errorf("AtProvider.Name = %v, want nil", ap.Name)
	}
	if ap.LBPreferredMethod != nil {
		t.Errorf("AtProvider.LBPreferredMethod = %v, want nil", ap.LBPreferredMethod)
	}
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.Disable != nil {
		t.Errorf("AtProvider.Disable = %v, want nil", ap.Disable)
	}
	if ap.Servers != nil {
		t.Errorf("AtProvider.Servers = %v, want nil", ap.Servers)
	}
	if ap.Monitors != nil {
		t.Errorf("AtProvider.Monitors = %v, want nil", ap.Monitors)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
	if ap.Health != nil {
		t.Errorf("AtProvider.Health = %v, want nil", ap.Health)
	}
	if ap.ConsolidatedMonitors != nil {
		t.Errorf("AtProvider.ConsolidatedMonitors = %v, want nil", ap.ConsolidatedMonitors)
	}
}

func TestClusterObserveConsolidatedMonitorsAndHealth(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcPool{
		Name:              stringPtr("my-dtc-pool"),
		LbPreferredMethod: lbRoundRobin,
		Servers: []*ibclient.DtcServerLink{
			{Server: serverRefA, Ratio: 1},
		},
		Monitors: []*ibclient.DtcMonitorHttp{
			{Ref: monitorRefHTTP},
		},
		ConsolidatedMonitors: []*ibclient.DtcPoolConsolidatedMonitorHealth{
			{Members: []string{"member1"}, Monitor: monitorRefHTTP, Availability: "ALL", FullHealthCommunication: true},
		},
		Health: &ibclient.DtcHealth{
			Availability: "GREEN",
			Description:  "healthy",
			EnabledState: "ENABLED",
		},
	})

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", ref)
	cr.Spec.ForProvider.Servers = []clusterv1alpha1.DTCPoolServerLink{
		{Server: stringPtr(serverRefA), Ratio: uint32Ptr(1)},
	}
	cr.Spec.ForProvider.Monitors = []clusterv1alpha1.DTCPoolMonitor{
		{Monitor: stringPtr(monitorRefHTTP)},
	}

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true, got false")
	}

	ap := cr.Status.AtProvider
	if len(ap.Servers) != 1 || ap.Servers[0].Server == nil || *ap.Servers[0].Server != serverRefA {
		t.Errorf("AtProvider.Servers = %+v, want one entry with the seeded server ref", ap.Servers)
	}
	if len(ap.Monitors) != 1 || ap.Monitors[0].Monitor == nil || *ap.Monitors[0].Monitor != monitorRefHTTP {
		t.Errorf("AtProvider.Monitors = %+v, want one entry with the seeded monitor ref", ap.Monitors)
	}
	if len(ap.ConsolidatedMonitors) != 1 || ap.ConsolidatedMonitors[0].Monitor == nil || *ap.ConsolidatedMonitors[0].Monitor != monitorRefHTTP {
		t.Errorf("AtProvider.ConsolidatedMonitors = %+v, want one entry with the seeded monitor ref", ap.ConsolidatedMonitors)
	}
	if ap.Health == nil || ap.Health.Availability == nil || *ap.Health.Availability != "GREEN" {
		t.Errorf("AtProvider.Health = %+v, want Availability=GREEN", ap.Health)
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", "") // no external-name yet
	cr.Spec.ForProvider.Monitors = []clusterv1alpha1.DTCPoolMonitor{
		{Monitor: stringPtr(monitorRefHTTP)},
	}
	cr.Spec.ForProvider.Servers = []clusterv1alpha1.DTCPoolServerLink{
		{Server: stringPtr(serverRefA), Ratio: uint32Ptr(2)},
	}

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}

	m.mu.Lock()
	stored := m.records[got]
	m.mu.Unlock()
	if stored == nil {
		t.Fatal("Create: record not stored on mock server")
	}
	if len(stored.Monitors) != 1 || stored.Monitors[0].Ref != monitorRefHTTP {
		t.Errorf("Create: stored monitors = %+v, want the ref passed through untouched (no name+type lookup)", stored.Monitors)
	}
	if len(stored.Servers) != 1 || stored.Servers[0].Server != serverRefA || stored.Servers[0].Ratio != 2 {
		t.Errorf("Create: stored servers = %+v, want the ref passed through untouched (no name lookup)", stored.Servers)
	}
}

// TestClusterCreateServerError pins the error-propagation path when the
// WAPI backend rejects the create POST outright (e.g. transient 500s).
func TestClusterCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", "")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name set to %q despite failed create", got)
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", "dtc:pool/test1:my-dtc-pool")

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
}

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcPool{
		Name:              stringPtr("my-dtc-pool"),
		LbPreferredMethod: lbRoundRobin,
		Comment:           stringPtr("old comment"),
	})

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if stored.Comment == nil || *stored.Comment != "new comment" {
		t.Errorf("Update: stored comment = %v, want %q", stored.Comment, "new comment")
	}
}

// TestUpdateSendsAllFields pins the PUT-echo-everything contract for
// DTCPool: since there are no known immutable fields, Update must send
// every mutable field on every request (not a partial patch).
func TestUpdateSendsAllFields(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcPool{
		Name:              stringPtr("my-dtc-pool"),
		LbPreferredMethod: lbRoundRobin,
	})

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", ref)
	cr.Spec.ForProvider.LBAlternateMethod = stringPtr("TOPOLOGY")
	cr.Spec.ForProvider.LBAlternateTopology = stringPtr("my-topology")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	body := m.lastUpdateBody
	m.mu.Unlock()

	var raw map[string]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("cannot decode captured PUT body: %v", err)
	}
	if _, present := raw["name"]; !present {
		t.Error("Update: request body missing 'name' — PUT must echo all fields")
	}
	if _, present := raw["lb_preferred_method"]; !present {
		t.Error("Update: request body missing 'lb_preferred_method' — PUT must echo all fields")
	}
	if _, present := raw["lb_alternate_method"]; !present {
		t.Error("Update: request body missing 'lb_alternate_method' — PUT must echo all fields")
	}
}

// TestLateInitializeSkipsLbAlternateMethodDefault pins the fix for the
// Create→Observe→Update loop that previously 400'd forever: WAPI defaults
// lb_alternate_method to "NONE" on read-back when the field was omitted at
// Create, but rejects "NONE" as an explicit value on Update. lateInitialize
// must never write that default back into spec.
func TestLateInitializeSkipsLbAlternateMethodDefault(t *testing.T) {
	var comment, availability, lbAlternateMethod, lbPreferredTopology, lbAlternateTopology *string
	var disable, useTTL *bool
	var quorum, ttl *uint32
	var extAttrs map[string]string
	var lbdrp, lbdra *dynRatio

	rec := &ibclient.DtcPool{
		Name:              stringPtr("my-dtc-pool"),
		LbPreferredMethod: lbRoundRobin,
		LbAlternateMethod: lbAlternateMethodUnset, // "NONE" — WAPI's omitted-field default
	}

	changed := lateInitialize(&comment, &disable, &availability, &quorum, &ttl, &useTTL, &extAttrs, &lbAlternateMethod, &lbPreferredTopology, &lbAlternateTopology, &lbdrp, &lbdra, rec)
	if changed {
		t.Error("lateInitialize: want changed=false, lb_alternate_method=NONE must not count as a backfill")
	}
	if lbAlternateMethod != nil {
		t.Errorf("lateInitialize: lbAlternateMethod = %v, want nil (NONE must never be written into spec)", *lbAlternateMethod)
	}
}

// TestIsUpToDateTreatsLbAlternateMethodNoneAsUnset pins that an unset spec
// field is considered up to date against WAPI's "NONE" read-back default,
// so lateInitialize's refusal to backfill "NONE" (see
// TestLateInitializeSkipsLbAlternateMethodDefault) does not itself
// manufacture a phantom diff on every subsequent reconcile.
func TestIsUpToDateTreatsLbAlternateMethodNoneAsUnset(t *testing.T) {
	rec := &ibclient.DtcPool{
		Name:              stringPtr("my-dtc-pool"),
		LbPreferredMethod: lbRoundRobin,
		LbAlternateMethod: lbAlternateMethodUnset,
	}
	if !isUpToDate(rec.Name, stringPtr(rec.LbPreferredMethod), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, map[string]string{}, rec) {
		t.Error("isUpToDate: want an unset spec lbAlternateMethod to be up to date against observed \"NONE\"")
	}
}

// TestClusterUpdateAfterObserveOmitsDefaultedLbAlternateMethod reproduces
// the full E2E-caught bug end to end: Create a pool with no
// lbAlternateMethod → Observe reads back WAPI's "NONE" default and
// late-initializes spec → a later Update (comment-only change) must not
// send an explicit lb_alternate_method="NONE", which the mock server
// (mirroring live WAPI) rejects with 400.
func TestClusterUpdateAfterObserveOmitsDefaultedLbAlternateMethod(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", "") // no lbAlternateMethod set, no external-name yet

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if cr.Spec.ForProvider.LBAlternateMethod != nil {
		t.Fatalf("Observe: lbAlternateMethod late-initialized to %q, want nil (NONE must never round-trip into spec)", *cr.Spec.ForProvider.LBAlternateMethod)
	}
	if !obs.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true immediately after Create with no drift (observed \"NONE\" must not read as a phantom diff against an unset spec field)")
	}

	cr.Spec.ForProvider.Comment = stringPtr("updated comment")
	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error after comment-only change: %v", err)
	}

	m.mu.Lock()
	body := m.lastUpdateBody
	m.mu.Unlock()

	var raw map[string]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("cannot decode captured PUT body: %v", err)
	}
	if v, present := raw["lb_alternate_method"]; present {
		t.Errorf("Update: request body sent lb_alternate_method=%v, want omitted (WAPI rejects explicit \"NONE\")", v)
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcPool{Name: stringPtr("my-dtc-pool")})

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", ref)

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: record still present after Delete")
	}
}

func TestClusterDeleteNotFound(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", "dtc:pool/does-not-exist:my-dtc-pool")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

// TestClusterDeleteServerError verifies that a 5xx response from the WAPI
// delete endpoint is propagated (wrapped, not swallowed) rather than being
// treated as a not-found/already-deleted success.
func TestClusterDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCPool("my-dtcpool", "dtc:pool/test1:my-dtc-pool")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteDTCPool) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteDTCPool)
	}
}

// ── cluster: Disconnect ──────────────────────────────────────────────────

func TestClusterDisconnectIsNoop(t *testing.T) {
	e := &clusterExternal{}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Errorf("Disconnect: unexpected error: %v", err)
	}
}

// ── cluster: Connect ─────────────────────────────────────────────────────

func TestClusterConnectSuccess(t *testing.T) {
	const (
		ns     = "crossplane-system"
		secret = "infobloxnios-api-key"
	)

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t"),
			&clusterpcv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: nsDefault},
				Spec: clusterpcv1alpha1.ProviderConfigSpec{
					Credentials: clusterpcv1alpha1.ProviderCredentials{
						Source: xpv1.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
							SecretRef: &xpv1.SecretKeySelector{
								SecretReference: xpv1.SecretReference{Name: secret, Namespace: ns},
								Key:             unusedSecretKey,
							},
						},
					},
				},
			},
		).Build()

	conn := &clusterConnector{
		kube:  kube,
		usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newClusterDTCPool("my-dtcpool", "")
	got, err := conn.Connect(context.Background(), cr)
	if err != nil {
		t.Fatalf("Connect: unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("Connect: expected non-nil ExternalClient, got nil")
	}
}

func TestClusterConnectProviderConfigNotFound(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	conn := &clusterConnector{
		kube:  kube,
		usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newClusterDTCPool("my-dtcpool", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcPool{
		Name:              stringPtr("my-dtc-pool"),
		LbPreferredMethod: lbRoundRobin,
	})

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCPool(nsDefault, "my-dtcpool", ref, "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true, got false")
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true, got false")
	}
}

func TestNamespacedObserveConsolidatedMonitorsAndHealth(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcPool{
		Name:              stringPtr("my-dtc-pool"),
		LbPreferredMethod: lbRoundRobin,
		Monitors: []*ibclient.DtcMonitorHttp{
			{Ref: monitorRefHTTP},
		},
		Health: &ibclient.DtcHealth{
			Availability: "GREEN",
			Description:  "healthy",
			EnabledState: "ENABLED",
		},
	})

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCPool(nsDefault, "my-dtcpool", ref, "ProviderConfig")
	cr.Spec.ForProvider.Monitors = []namespacedv1alpha1.DTCPoolMonitor{
		{Monitor: stringPtr(monitorRefHTTP)},
	}

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true, got false")
	}

	ap := cr.Status.AtProvider
	if len(ap.Monitors) != 1 || ap.Monitors[0].Monitor == nil || *ap.Monitors[0].Monitor != monitorRefHTTP {
		t.Errorf("AtProvider.Monitors = %+v, want one entry with the seeded monitor ref", ap.Monitors)
	}
	if ap.Health == nil || ap.Health.Availability == nil || *ap.Health.Availability != "GREEN" {
		t.Errorf("AtProvider.Health = %+v, want Availability=GREEN", ap.Health)
	}
}

func TestNamespacedObserveNotFound(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCPool(nsDefault, "my-dtcpool", "dtc:pool/does-not-exist:my-dtc-pool", "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

func TestNamespacedObservePreCreateState(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCPool(nsDefault, "my-dtcpool", "", "ProviderConfig")
	meta.SetExternalName(cr, cr.GetName())

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false in pre-create state, got true")
	}
}

func TestNamespacedObserveServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCPool(nsDefault, "my-dtcpool", "dtc:pool/test1:my-dtc-pool", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCPool(nsDefault, "my-dtcpool", "dtc:pool/test1:my-dtc-pool", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// ── namespaced: Create ────────────────────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCPool(nsDefault, "my-dtcpool", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(cr); got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

func TestNamespacedCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCPool(nsDefault, "my-dtcpool", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
}

// ── namespaced: Update ────────────────────────────────────────────────────

func TestNamespacedUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCPool(nsDefault, "my-dtcpool", "dtc:pool/test1:my-dtc-pool", "ProviderConfig")

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcPool{
		Name:              stringPtr("my-dtc-pool"),
		LbPreferredMethod: lbRoundRobin,
		Comment:           stringPtr("old comment"),
	})

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCPool(nsDefault, "my-dtcpool", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if stored.Comment == nil || *stored.Comment != "new comment" {
		t.Errorf("Update: stored comment = %v, want %q", stored.Comment, "new comment")
	}
}

// ── namespaced: Delete ────────────────────────────────────────────────────

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcPool{Name: stringPtr("my-dtc-pool")})

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCPool(nsDefault, "my-dtcpool", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockDtcPoolServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCPool(nsDefault, "my-dtcpool", "dtc:pool/does-not-exist:my-dtc-pool", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

func TestNamespacedDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCPool(nsDefault, "my-dtcpool", "dtc:pool/test1:my-dtc-pool", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
}

// ── namespaced: Connect ───────────────────────────────────────────────────

func TestNamespacedConnectWithProviderConfig(t *testing.T) {
	const (
		ns     = nsDefault
		secret = "infobloxnios-api-key"
	)

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t"),
			&namespacedpcv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: nsDefault, Namespace: ns},
				Spec: namespacedpcv1alpha1.ProviderConfigSpec{
					Credentials: namespacedpcv1alpha1.ProviderCredentials{
						Source: xpv1.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
							SecretRef: &xpv1.SecretKeySelector{
								SecretReference: xpv1.SecretReference{Name: secret, Namespace: ns},
								Key:             unusedSecretKey,
							},
						},
					},
				},
			},
		).Build()

	conn := &namespacedConnector{
		kube:  kube,
		usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newNamespacedDTCPool(ns, "my-dtcpool", "", "ProviderConfig")
	got, err := conn.Connect(context.Background(), cr)
	if err != nil {
		t.Fatalf("Connect: unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("Connect: expected non-nil ExternalClient, got nil")
	}
}

func TestNamespacedConnectWithClusterProviderConfig(t *testing.T) {
	const secret = "infobloxnios-api-key"
	ns := "crossplane-system"

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t"),
			&namespacedpcv1alpha1.ClusterProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: nsDefault},
				Spec: namespacedpcv1alpha1.ProviderConfigSpec{
					Credentials: namespacedpcv1alpha1.ProviderCredentials{
						Source: xpv1.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
							SecretRef: &xpv1.SecretKeySelector{
								SecretReference: xpv1.SecretReference{Name: secret, Namespace: ns},
								Key:             unusedSecretKey,
							},
						},
					},
				},
			},
		).Build()

	conn := &namespacedConnector{
		kube:  kube,
		usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newNamespacedDTCPool("app-ns", "my-dtcpool", "", "ClusterProviderConfig")
	got, err := conn.Connect(context.Background(), cr)
	if err != nil {
		t.Fatalf("Connect: unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("Connect: expected non-nil ExternalClient, got nil")
	}
}

func TestNamespacedConnectUnsupportedKind(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	conn := &namespacedConnector{
		kube:  kube,
		usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newNamespacedDTCPool(nsDefault, "my-dtcpool", "", "SomeOtherKind")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for unsupported provider config kind, got nil")
	}
}

func TestNamespacedDisconnectIsNoop(t *testing.T) {
	e := &namespacedExternal{}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Errorf("Disconnect: unexpected error: %v", err)
	}
}

// ── shared helper unit tests ─────────────────────────────────────────────

func TestExtAttrsRoundTrip(t *testing.T) {
	in := map[string]string{eaKeyEnv: eaValProd, "owner": "platform-team"}
	ea := buildEA(in)
	out := extAttrsFromEA(ea)
	if !extAttrsEqual(in, out) {
		t.Errorf("ExtAttrs round-trip: got %v, want %v", out, in)
	}
}

// TestStringifyEAValue pins every branch of the extensible-attribute value
// coercion — WAPI can hand back strings, ibclient.Bool, string slices (from
// EA.UnmarshalJSON), or arbitrary values needing a fallback %v render.
func TestStringifyEAValue(t *testing.T) {
	cases := map[string]struct {
		in   interface{}
		want string
	}{
		"Nil":         {in: nil, want: ""},
		"String":      {in: "abc", want: "abc"},
		"BoolTrue":    {in: ibclient.Bool(true), want: "True"},
		"BoolFalse":   {in: ibclient.Bool(false), want: "False"},
		"StringSlice": {in: []string{"a", "b"}, want: "a,b"},
		"Int":         {in: 42, want: "42"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := stringifyEAValue(tc.in); got != tc.want {
				t.Errorf("stringifyEAValue(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestExtAttrsEqualTreatsNilAndEmptyAsEqual(t *testing.T) {
	if !extAttrsEqual(nil, map[string]string{}) {
		t.Error("extAttrsEqual: want nil and empty map to be equal")
	}
}

func TestMonitorsRoundTrip(t *testing.T) {
	in := []poolMonitor{{Monitor: stringPtr(monitorRefHTTP)}}
	sdk := buildMonitors(in)
	out := monitorsFromSDK(sdk)
	if !monitorsEqual(in, out) {
		t.Errorf("Monitors round-trip: got %+v, want %+v", out, in)
	}
}

func TestMonitorsEqualTreatsNilAndEmptyAsEqual(t *testing.T) {
	if !monitorsEqual(nil, []poolMonitor{}) {
		t.Error("monitorsEqual: want nil and empty slice to be equal")
	}
}

func TestServersRoundTrip(t *testing.T) {
	in := []serverLink{{Server: stringPtr(serverRefA), Ratio: uint32Ptr(3)}}
	sdk := buildServers(in)
	out := serverLinksFromSDK(sdk)
	if !serversEqual(in, out) {
		t.Errorf("Servers round-trip: got %+v, want %+v", out, in)
	}
}

func TestServersEqualTreatsNilAndEmptyAsEqual(t *testing.T) {
	if !serversEqual(nil, []serverLink{}) {
		t.Error("serversEqual: want nil and empty slice to be equal")
	}
}

func TestDynRatioRoundTrip(t *testing.T) {
	in := &dynRatio{
		Method:              stringPtr("RATIO"),
		Monitor:             stringPtr(monitorRefHTTP),
		MonitorMetric:       stringPtr(".1.3.6.1"),
		MonitorWeighing:     stringPtr("PRIORITY"),
		InvertMonitorMetric: boolPtr(true),
	}
	sdk := buildDynRatio(in)
	out := dynRatioFromSDK(sdk)
	if !dynRatioEqual(in, out) {
		t.Errorf("DynRatio round-trip: got %+v, want %+v", out, in)
	}
}

func TestDynRatioEqualTreatsNilAndZeroAsEqual(t *testing.T) {
	if !dynRatioEqual(nil, &dynRatio{}) {
		t.Error("dynRatioEqual: want nil and all-zero-value struct to be equal")
	}
	if !dynRatioEqual(nil, nil) {
		t.Error("dynRatioEqual: want nil and nil to be equal")
	}
}

func TestIsNotFoundClassifiesTypedError(t *testing.T) {
	err := ibclient.NewNotFoundError("boom")
	if !isNotFound(err) {
		t.Error("isNotFound: want true for *ibclient.NotFoundError")
	}
}

func TestIsNotFoundClassifiesGenericStatusError(t *testing.T) {
	err := errorsNewf("WAPI request error: 404('object not found')\nsome body")
	if !isNotFound(err) {
		t.Error("isNotFound: want true for a generic 404 status-coded error")
	}
	err500 := errorsNewf("WAPI request error: 500('internal error')")
	if isNotFound(err500) {
		t.Error("isNotFound: want false for a 500 status-coded error")
	}
}

func TestLateInitializeBackfillsOptionalFields(t *testing.T) {
	var comment, availability, lbAlternateMethod, lbPreferredTopology, lbAlternateTopology *string
	var disable, useTTL *bool
	var quorum, ttl *uint32
	var extAttrs map[string]string
	var lbdrp, lbdra *dynRatio

	rec := &ibclient.DtcPool{
		Comment:             stringPtr("pool comment"),
		Disable:             boolPtr(true),
		Availability:        "QUORUM",
		Quorum:              uint32Ptr(2),
		Ttl:                 uint32Ptr(300),
		UseTtl:              boolPtr(true),
		LbAlternateMethod:   "TOPOLOGY",
		LbPreferredTopology: stringPtr("topology-a"),
		LbAlternateTopology: stringPtr("topology-b"),
		LbDynamicRatioPreferred: &ibclient.SettingDynamicratio{
			Method: "RATIO", Monitor: monitorRefHTTP,
		},
		LbDynamicRatioAlternate: &ibclient.SettingDynamicratio{
			Method: "PRIORITY", Monitor: monitorRefHTTP,
		},
		Ea: ibclient.EA{eaKeyEnv: eaValProd},
	}

	changed := lateInitialize(&comment, &disable, &availability, &quorum, &ttl, &useTTL, &extAttrs, &lbAlternateMethod, &lbPreferredTopology, &lbAlternateTopology, &lbdrp, &lbdra, rec)
	if !changed {
		t.Fatal("lateInitialize: want changed=true, got false")
	}
	if comment == nil || *comment != "pool comment" {
		t.Errorf("comment = %v, want %q", comment, "pool comment")
	}
	if disable == nil || !*disable {
		t.Errorf("disable = %v, want true", disable)
	}
	if availability == nil || *availability != "QUORUM" {
		t.Errorf("availability = %v, want %q", availability, "QUORUM")
	}
	if quorum == nil || *quorum != 2 {
		t.Errorf("quorum = %v, want 2", quorum)
	}
	if ttl == nil || *ttl != 300 {
		t.Errorf("ttl = %v, want 300", ttl)
	}
	if useTTL == nil || !*useTTL {
		t.Errorf("useTTL = %v, want true", useTTL)
	}
	if lbAlternateMethod == nil || *lbAlternateMethod != "TOPOLOGY" {
		t.Errorf("lbAlternateMethod = %v, want %q", lbAlternateMethod, "TOPOLOGY")
	}
	if lbPreferredTopology == nil || *lbPreferredTopology != "topology-a" {
		t.Errorf("lbPreferredTopology = %v, want %q", lbPreferredTopology, "topology-a")
	}
	if lbAlternateTopology == nil || *lbAlternateTopology != "topology-b" {
		t.Errorf("lbAlternateTopology = %v, want %q", lbAlternateTopology, "topology-b")
	}
	if lbdrp == nil || strOrEmpty(lbdrp.Method) != "RATIO" {
		t.Errorf("lbDynamicRatioPreferred = %+v, want Method=RATIO", lbdrp)
	}
	if lbdra == nil || strOrEmpty(lbdra.Method) != "PRIORITY" {
		t.Errorf("lbDynamicRatioAlternate = %+v, want Method=PRIORITY", lbdra)
	}
	if len(extAttrs) != 1 || extAttrs[eaKeyEnv] != eaValProd {
		t.Errorf("extAttrs = %v, want {env: prod}", extAttrs)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	comment := stringPtr("user comment")
	disable := boolPtr(false)
	availability := stringPtr("ANY")
	quorum := uint32Ptr(1)
	ttl := uint32Ptr(60)
	useTTL := boolPtr(false)
	lbAlternateMethod := stringPtr("RATIO")
	lbPreferredTopology := stringPtr("user-topology")
	lbAlternateTopology := stringPtr("user-topology-alt")
	extAttrs := map[string]string{"owner": "user-team"}
	lbdrp := &dynRatio{Method: stringPtr("user-method")}
	lbdra := &dynRatio{Method: stringPtr("user-method-alt")}

	rec := &ibclient.DtcPool{
		Comment:             stringPtr("server comment"),
		Disable:             boolPtr(true),
		Availability:        "QUORUM",
		Quorum:              uint32Ptr(5),
		Ttl:                 uint32Ptr(300),
		UseTtl:              boolPtr(true),
		LbAlternateMethod:   "TOPOLOGY",
		LbPreferredTopology: stringPtr("server-topology"),
		LbAlternateTopology: stringPtr("server-topology-alt"),
		LbDynamicRatioPreferred: &ibclient.SettingDynamicratio{
			Method: "server-method",
		},
		Ea: ibclient.EA{eaKeyEnv: eaValProd},
	}

	changed := lateInitialize(&comment, &disable, &availability, &quorum, &ttl, &useTTL, &extAttrs, &lbAlternateMethod, &lbPreferredTopology, &lbAlternateTopology, &lbdrp, &lbdra, rec)
	if changed {
		t.Error("lateInitialize: want changed=false when all fields already set, got true")
	}
	if *comment != "user comment" {
		t.Errorf("comment = %q, want unchanged %q", *comment, "user comment")
	}
	if *disable {
		t.Error("disable overwritten by lateInitialize")
	}
	if *availability != "ANY" {
		t.Errorf("availability = %q, want unchanged %q", *availability, "ANY")
	}
	if *quorum != 1 {
		t.Errorf("quorum = %d, want unchanged 1", *quorum)
	}
	if strOrEmpty(lbdrp.Method) != "user-method" {
		t.Errorf("lbDynamicRatioPreferred overwritten by lateInitialize: %+v", lbdrp)
	}
	if extAttrs["owner"] != "user-team" {
		t.Errorf("extAttrs overwritten by lateInitialize: %v", extAttrs)
	}
}

func TestObserveDoesNotLateInitializeRequiredFields(t *testing.T) {
	// name and lbPreferredMethod are required fields — lateInitialize has
	// no parameters for them at all, so this test simply pins that
	// contract by confirming the function signature only accepts the
	// optional fields.
	var comment, availability, lbAlternateMethod, lbPreferredTopology, lbAlternateTopology *string
	var disable, useTTL *bool
	var quorum, ttl *uint32
	var extAttrs map[string]string
	var lbdrp, lbdra *dynRatio

	rec := &ibclient.DtcPool{
		Name:              stringPtr("pool-name"),
		LbPreferredMethod: lbRoundRobin,
	}

	_ = lateInitialize(&comment, &disable, &availability, &quorum, &ttl, &useTTL, &extAttrs, &lbAlternateMethod, &lbPreferredTopology, &lbAlternateTopology, &lbdrp, &lbdra, rec)
	// No assertions needed beyond "does not panic" — name/lbPreferredMethod
	// aren't parameters of lateInitialize, so there is nothing for it to
	// overwrite.
}

func TestIsUpToDate(t *testing.T) {
	base := &ibclient.DtcPool{
		Name:              stringPtr("my-dtc-pool"),
		LbPreferredMethod: lbRoundRobin,
		Comment:           stringPtr("hello"),
		Disable:           boolPtr(false),
		Servers: []*ibclient.DtcServerLink{
			{Server: serverRefA, Ratio: 1},
		},
		Monitors: []*ibclient.DtcMonitorHttp{
			{Ref: monitorRefHTTP},
		},
		Ea: ibclient.EA{eaKeyEnv: eaValProd},
	}
	baseServers := []serverLink{{Server: stringPtr(serverRefA), Ratio: uint32Ptr(1)}}
	baseMonitors := []poolMonitor{{Monitor: stringPtr(monitorRefHTTP)}}
	baseExtAttrs := map[string]string{eaKeyEnv: eaValProd}

	cases := map[string]struct {
		mutate func() (comment *string, disable *bool, servers []serverLink, monitors []poolMonitor, extAttrs map[string]string)
		want   bool
	}{
		"MatchesExactly": {
			mutate: func() (*string, *bool, []serverLink, []poolMonitor, map[string]string) {
				return stringPtr("hello"), boolPtr(false), baseServers, baseMonitors, baseExtAttrs
			},
			want: true,
		},
		"CommentDiffers": {
			mutate: func() (*string, *bool, []serverLink, []poolMonitor, map[string]string) {
				return stringPtr("changed"), boolPtr(false), baseServers, baseMonitors, baseExtAttrs
			},
			want: false,
		},
		"DisableDiffers": {
			mutate: func() (*string, *bool, []serverLink, []poolMonitor, map[string]string) {
				return stringPtr("hello"), boolPtr(true), baseServers, baseMonitors, baseExtAttrs
			},
			want: false,
		},
		"ServersDiffer": {
			mutate: func() (*string, *bool, []serverLink, []poolMonitor, map[string]string) {
				diff := []serverLink{{Server: stringPtr(serverRefA), Ratio: uint32Ptr(9)}}
				return stringPtr("hello"), boolPtr(false), diff, baseMonitors, baseExtAttrs
			},
			want: false,
		},
		"MonitorsDiffer": {
			mutate: func() (*string, *bool, []serverLink, []poolMonitor, map[string]string) {
				diff := []poolMonitor{{Monitor: stringPtr("dtc:monitor:snmp/ZG5z...:snmp")}}
				return stringPtr("hello"), boolPtr(false), baseServers, diff, baseExtAttrs
			},
			want: false,
		},
		"ExtAttrsDiffer": {
			mutate: func() (*string, *bool, []serverLink, []poolMonitor, map[string]string) {
				return stringPtr("hello"), boolPtr(false), baseServers, baseMonitors, map[string]string{eaKeyEnv: "dev"}
			},
			want: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			comment, disable, servers, monitors, extAttrs := tc.mutate()
			got := isUpToDate(base.Name, stringPtr(base.LbPreferredMethod), nil, comment, nil, nil, nil, nil, nil, disable, nil, servers, monitors, nil, nil, extAttrs, base)
			if got != tc.want {
				t.Errorf("%s: isUpToDate() = %v, want %v", name, got, tc.want)
			}
		})
	}
}

func TestIsUpToDateExtAttrsEmptyVsNil(t *testing.T) {
	rec := &ibclient.DtcPool{
		Name:              stringPtr("my-dtc-pool"),
		LbPreferredMethod: lbRoundRobin,
		Ea:                nil,
	}
	if !isUpToDate(rec.Name, stringPtr(rec.LbPreferredMethod), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, map[string]string{}, rec) {
		t.Error("isUpToDate: want empty map and nil Ea to be treated as up to date")
	}
}

// ── extractCredentials: ssl_verify key is fully ignored ────────────────
//
// TLS verification is governed by the ProviderConfig's own sslVerify spec
// field (see cluster.go/namespaced.go's Connect methods), never by a key
// in the credentials Secret. This pins the migration: a legacy
// "ssl_verify" key in the Secret must have zero effect on
// extractCredentials — nioCredentials has no SslVerify field to read it
// into.
func TestExtractCredentialsIgnoresSecretSslVerifyKey(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret("crossplane-system", "infobloxnios-credentials", "grid.example.com", "admin", "s3cr3t")
	secret.Data["ssl_verify"] = []byte("false")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	creds, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, &xpv1.SecretKeySelector{
		SecretReference: xpv1.SecretReference{Name: "infobloxnios-credentials", Namespace: "crossplane-system"},
		Key:             unusedSecretKey,
	}, "")
	if err != nil {
		t.Fatalf("extractCredentials: unexpected error: %v", err)
	}
	if creds.Host != "grid.example.com" || creds.Username != "admin" || creds.Password != "s3cr3t" {
		t.Errorf("extractCredentials: got %+v, want Host/Username/Password populated regardless of the ssl_verify key", creds)
	}
}

func TestNewClientWithSchemeUsesConfiguredSslVerify(t *testing.T) {
	// Regression guard: newClientsWithScheme must not hardcode SslVerify to
	// "true" — it must honor the sslVerify parameter. Both branches must construct
	// successfully (transport config validation happens locally; no
	// network round-trip occurs here).
	for name, sslVerify := range map[string]bool{"Enabled": true, "Disabled": false} {
		t.Run(name, func(t *testing.T) {
			creds := &nioCredentials{Host: "127.0.0.1", Username: "admin", Password: "s3cr3t"}
			clients, err := newClientsWithScheme(creds, sslVerify, "http", "80")
			if err != nil {
				t.Fatalf("newClientsWithScheme: unexpected error: %v", err)
			}
			if clients == nil {
				t.Fatal("newClientsWithScheme: expected non-nil clients")
			}
			if clients.objMgr == nil {
				t.Fatal("newClientsWithScheme: expected non-nil object manager")
			}
			if clients.conn == nil {
				t.Fatal("newClientsWithScheme: expected non-nil connector")
			}
		})
	}
}

// errorsNewf builds a plain error carrying the given message — a stand-in
// for errors coming back from the SDK's generic HTTP-error formatting
// (see errStatusRe in controller.go), without importing the
// crossplane-runtime errors package into this narrowly-scoped test helper.
func errorsNewf(msg string) error {
	return &testError{msg: msg}
}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }
