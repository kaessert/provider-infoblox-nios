// Package ipv4sharednetwork unit tests for the IPv4SharedNetwork MR
// controllers. Tests use inline httptest.NewServer mocks that emulate the
// WAPI sharednetwork endpoints, PascalCase test names (no underscores),
// and white-box access to the unexported connectors/clients so both
// scopes can be exercised without going through the full Connect()
// credential bridge on every test.
package ipv4sharednetwork

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/ipv4sharednetwork/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/ipv4sharednetwork/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
)

const (
	testUIDCluster    = "test-uid-cluster"
	testUIDNamespaced = "test-uid-namespaced"
)

func stringPtr(s string) *string { return &s }

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

func newClusterSharedNetwork(crName, externalName string) *clusterv1alpha1.IPv4SharedNetwork {
	cr := &clusterv1alpha1.IPv4SharedNetwork{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: testUIDCluster},
		Spec: clusterv1alpha1.IPv4SharedNetworkSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.IPv4SharedNetworkParameters{
				Name:     stringPtr("test-shared-network"),
				Networks: []string{"10.0.0.0/24"},
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

func newNamespacedSharedNetwork(ns, crName, externalName, pcKind string) *namespacedv1alpha1.IPv4SharedNetwork {
	cr := &namespacedv1alpha1.IPv4SharedNetwork{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: testUIDNamespaced},
		Spec: namespacedv1alpha1.IPv4SharedNetworkSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.IPv4SharedNetworkParameters{
				Name:     stringPtr("test-shared-network"),
				Networks: []string{"10.0.0.0/24"},
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
// storedSharedNetwork is the mock server's internal representation of a
// shared network. It deliberately does NOT reuse ibclient.SharedNetwork's
// Networks field type ([]*ibclient.Ipv4Network): the real SDK's
// MarshalJSON detects CIDR-formatted Ref values and emits a compound
// {"_ref": {"network":..., "network_view":...}} wire shape for them, but
// its own UnmarshalJSON can only decode the bare-string "_ref" shape a
// real GET response returns. Round-tripping CIDR-looking values through
// the SDK's own type would trip that asymmetry — so the mock stores/wires
// networks as bare strings itself (see wireSharedNetwork below) and never
// invokes ibclient.SharedNetwork's (de)serialization for the "networks"
// field.
//
// Networks holds plain CIDRs — what the client SDK actually sends on
// Create/Update — but toWire renders each one as a realistic WAPI object
// _ref ("network/<opaque id>:<cidr>/<view>") for GET responses, matching
// what a real Grid returns. A mock that echoed the plain CIDR straight
// back as the wire "_ref" would let the controller's CIDR-vs-ref
// comparison bug pass unnoticed (see cidrFromNetworkRef).
type storedSharedNetwork struct {
	Ref         string
	Name        *string
	NetworkView string
	Networks    []string
	Comment     *string
	Ea          ibclient.EA
	Disable     *bool
	UseOptions  *bool
	Options     []*ibclient.Dhcpoption
}

type mockWapiServer struct {
	mu          sync.Mutex
	nets        map[string]*storedSharedNetwork
	nextRef     int
	searchCalls int

	eaDefExists       bool
	eaDefCreateStatus int
	eaDefCreateBody   string
	// eaDefSearchCalls counts requests to the extensibleattributedef
	// existence-check endpoint — used to prove the identity-prerequisite
	// probe fires only reactively.
	eaDefSearchCalls int
	// undefinedEASearch simulates a Grid where the identity extensible
	// attribute definition does not exist: any object search carrying an
	// EA filter fails with WAPI's real "Unknown extensible attribute"
	// 400, instead of an ordinary filtered result.
	undefinedEASearch bool
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{
		nets:        map[string]*storedSharedNetwork{},
		eaDefExists: true,
	}
}

func (m *mockWapiServer) seed(sn *storedSharedNetwork) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if sn.Ref == "" {
		name := ""
		if sn.Name != nil {
			name = *sn.Name
		}
		sn.Ref = "sharednetwork/test" + itoa(m.nextRef) + ":" + name
	}
	m.nets[sn.Ref] = sn
	return sn.Ref
}

// wireNetworkRef is the bare-string "_ref" wire shape a real WAPI GET
// response uses for each member-network entry.
type wireNetworkRef struct {
	Ref string `json:"_ref"`
}

// wireSharedNetwork is the hand-rolled wire representation the mock uses
// for GET responses, bypassing ibclient.SharedNetwork's own MarshalJSON.
type wireSharedNetwork struct {
	Ref         string                 `json:"_ref,omitempty"`
	Name        *string                `json:"name,omitempty"`
	NetworkView string                 `json:"network_view,omitempty"`
	Networks    []wireNetworkRef       `json:"networks,omitempty"`
	Comment     *string                `json:"comment,omitempty"`
	Ea          ibclient.EA            `json:"extattrs"`
	Disable     *bool                  `json:"disable,omitempty"`
	UseOptions  *bool                  `json:"use_options,omitempty"`
	Options     []*ibclient.Dhcpoption `json:"options,omitempty"`
}

// networkMemberRef renders a plain CIDR as a realistic WAPI network
// object _ref — "network/<opaque id>:<cidr>/<view>" — the shape a real
// Grid GET response uses. idx only feeds the opaque id portion so
// distinct member networks get distinct (fake) identities; it carries no
// meaning beyond that.
func networkMemberRef(cidr, networkView string, idx int) string {
	view := networkView
	if view == "" {
		view = "default"
	}
	return "network/testnetref" + itoa(idx) + ":" + cidr + "/" + view
}

func toWire(sn *storedSharedNetwork) wireSharedNetwork {
	nets := make([]wireNetworkRef, 0, len(sn.Networks))
	for i, n := range sn.Networks {
		nets = append(nets, wireNetworkRef{Ref: networkMemberRef(n, sn.NetworkView, i)})
	}
	return wireSharedNetwork{
		Ref:         sn.Ref,
		Name:        sn.Name,
		NetworkView: sn.NetworkView,
		Networks:    nets,
		Comment:     sn.Comment,
		Ea:          sn.Ea,
		Disable:     sn.Disable,
		UseOptions:  sn.UseOptions,
		Options:     sn.Options,
	}
}

// wireIncoming is the generic shape used to decode incoming POST/PUT
// request bodies — the client SDK encodes each "networks" entry as
// either a bare "_ref" string or a compound {"_ref": {"network":...,
// "network_view":...}} object depending on whether the supplied value
// looks like a CIDR.
type wireIncoming struct {
	Name        *string                  `json:"name"`
	NetworkView string                   `json:"network_view"`
	Networks    []map[string]interface{} `json:"networks"`
	Comment     *string                  `json:"comment"`
	Ea          ibclient.EA              `json:"extattrs"`
	Disable     *bool                    `json:"disable"`
	UseOptions  *bool                    `json:"use_options"`
	Options     []*ibclient.Dhcpoption   `json:"options"`
}

func decodeIncoming(body []byte) (*wireIncoming, error) {
	var raw wireIncoming
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	return &raw, nil
}

func extractNetworkRefs(entries []map[string]interface{}) []string {
	out := make([]string, 0, len(entries))
	for _, item := range entries {
		switch v := item["_ref"].(type) {
		case string:
			out = append(out, v)
		case map[string]interface{}:
			if nw, ok := v["network"].(string); ok {
				out = append(out, nw)
			}
		}
	}
	return out
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

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
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

func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/sharednetwork", func(w http.ResponseWriter, r *http.Request) {
		body, err := readAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		incoming, err := decodeIncoming(body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		sn := &storedSharedNetwork{
			Name:        incoming.Name,
			NetworkView: incoming.NetworkView,
			Networks:    extractNetworkRefs(incoming.Networks),
			Comment:     incoming.Comment,
			Ea:          incoming.Ea,
			Disable:     incoming.Disable,
			UseOptions:  incoming.UseOptions,
			Options:     incoming.Options,
		}
		ref := m.seed(sn)
		writeJSON(w, http.StatusOK, ref)
	})

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.eaDefSearchCalls++
		exists := m.eaDefExists
		m.mu.Unlock()
		if !exists {
			writeJSON(w, http.StatusOK, []ibclient.EADefinition{})
			return
		}
		name := identity.EAKey
		writeJSON(w, http.StatusOK, []ibclient.EADefinition{{Name: &name}})
	})

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		status := m.eaDefCreateStatus
		body := m.eaDefCreateBody
		m.mu.Unlock()
		if status != 0 {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
			return
		}
		m.mu.Lock()
		m.eaDefExists = true
		m.mu.Unlock()
		writeJSON(w, http.StatusOK, "extensibleattributedef/test:"+url.QueryEscape(identity.EAKey))
	})

	// Search endpoint: GET with no ref path segment, filtered by name
	// and/or a "*<EA name>" identity filter.
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/sharednetwork", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.searchCalls++
		m.mu.Unlock()

		q := r.URL.Query()
		name := q.Get("name")
		eaFilters := map[string]string{}
		for k, vals := range q {
			if strings.HasPrefix(k, "*") && len(vals) > 0 {
				eaFilters[strings.TrimPrefix(k, "*")] = vals[0]
			}
		}

		m.mu.Lock()
		undefinedEA := m.undefinedEASearch
		m.mu.Unlock()
		if len(eaFilters) > 0 && undefinedEA {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"Error":"AdmConProtoError: Unknown extensible attribute: ` + identity.EAKey + `","code":"Client.Ibap.Proto","text":"Unknown extensible attribute: ` + identity.EAKey + `"}`))
			return
		}

		m.mu.Lock()
		var matches []wireSharedNetwork
		for _, sn := range m.nets {
			if name != "" && (sn.Name == nil || *sn.Name != name) {
				continue
			}
			mismatch := false
			for k, v := range eaFilters {
				got, ok := sn.Ea[k]
				if !ok {
					mismatch = true
					break
				}
				if s, ok := got.(string); !ok || s != v {
					mismatch = true
					break
				}
			}
			if mismatch {
				continue
			}
			matches = append(matches, toWire(sn))
		}
		m.mu.Unlock()
		writeJSON(w, http.StatusOK, matches)
	})

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		sn, ok := m.nets[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, toWire(sn))
	})

	mux.HandleFunc("PUT /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		existing, ok := m.nets[ref]
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
		incoming, err := decodeIncoming(body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		m.mu.Lock()
		// UNSTABLE _ref: renaming a shared network mints a new _ref —
		// mirrors live NIOS Grid Manager behavior (the _ref encodes name).
		renamed := strOrEmpty(existing.Name) != strOrEmpty(incoming.Name)
		existing.Name = incoming.Name
		existing.Comment = incoming.Comment
		existing.Ea = incoming.Ea
		existing.Networks = extractNetworkRefs(incoming.Networks)
		existing.Disable = incoming.Disable
		existing.UseOptions = incoming.UseOptions
		existing.Options = incoming.Options
		respRef := ref
		if renamed {
			delete(m.nets, ref)
			m.nextRef++
			respRef = "sharednetwork/test" + itoa(m.nextRef) + ":" + strOrEmpty(existing.Name)
			existing.Ref = respRef
			m.nets[respRef] = existing
		}
		m.mu.Unlock()
		writeJSON(w, http.StatusOK, respRef)
	})

	mux.HandleFunc("DELETE /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		_, ok := m.nets[ref]
		delete(m.nets, ref)
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, ref)
	})

	return mux
}

func newTestClient(t *testing.T, srv *httptest.Server) identity.ManagerAndConnector {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	mc, err := newObjectManagerWithScheme(&nioCredentials{
		Host:     u.Hostname(),
		Username: "test-user",
		Password: "test-pass",
	}, true, "http", u.Port())
	if err != nil {
		t.Fatalf("cannot build test client: %v", err)
	}
	return mc
}

// ── Observe ──────────────────────────────────────────────────────────────

func TestClusterObserveResolvedUpToDate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	sn := &storedSharedNetwork{Name: stringPtr("test-shared-network"), Networks: []string{"10.0.0.0/24"}}
	sn.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(sn)

	cr := newClusterSharedNetwork("my-net", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !obs.ResourceExists || !obs.ResourceUpToDate {
		t.Fatalf("expected exists+up-to-date, got %+v", obs)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterSharedNetwork("my-net", "sharednetwork/doesnotexist:my-net")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.ResourceExists {
		t.Fatalf("expected ResourceExists=false, got %+v", obs)
	}
}

func TestObservePreCreateState(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterSharedNetwork("my-net", "my-net")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.ResourceExists {
		t.Fatalf("expected ResourceExists=false, got %+v", obs)
	}

	m.mu.Lock()
	searchCalls := m.searchCalls
	m.mu.Unlock()
	if searchCalls == 0 {
		t.Fatal("expected the identity ladder to search by uid even in the pre-create state, got zero search calls")
	}
}

func TestClusterObserveAdoptsUnstampedObjectAndForcesUpdate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	sn := &storedSharedNetwork{Name: stringPtr("test-shared-network"), Networks: []string{"10.0.0.0/24"}}
	ref := m.seed(sn)

	cr := newClusterSharedNetwork("my-net", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !obs.ResourceExists {
		t.Fatalf("expected exists, got %+v", obs)
	}
	if obs.ResourceUpToDate {
		t.Fatal("adopted object must never report up to date")
	}
}

func TestClusterObserveRecoversRotatedRefAndPersistsAnnotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	sn := &storedSharedNetwork{Name: stringPtr("test-shared-network")}
	sn.Ea = identity.Stamp(nil, testUIDCluster)
	realRef := m.seed(sn)

	cr := newClusterSharedNetwork("my-net", "sharednetwork/stale:my-net")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !obs.ResourceExists || !obs.ResourceLateInitialized {
		t.Fatalf("expected exists+late-init, got %+v", obs)
	}
	if meta.GetExternalName(cr) != realRef {
		t.Fatalf("expected external-name refreshed to %q, got %q", realRef, meta.GetExternalName(cr))
	}
}

func TestClusterObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	sn := &storedSharedNetwork{Name: stringPtr("test-shared-network")}
	sn.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(sn)

	cr := newClusterSharedNetwork("my-net", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("expected an error for foreign identity")
	}
}

// ── Create ───────────────────────────────────────────────────────────────

func TestClusterCreateStampsIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterSharedNetwork("my-net", "my-net")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta.GetExternalName(cr) == "my-net" {
		t.Fatal("expected external-name to be set to the server _ref")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, sn := range m.nets {
		got, ok := sn.Ea[identity.EAKey]
		if !ok || got != testUIDCluster {
			t.Fatalf("expected identity stamp %q, got %v", testUIDCluster, sn.Ea)
		}
	}
}

func TestCreateIPv4SharedNetworkRefusesEmptyUID(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	if _, err := createIPv4SharedNetwork(mc.Manager, stringPtr("x"), []string{"10.0.0.0/24"}, nil, nil, nil, nil, nil, nil, ""); err == nil {
		t.Fatal("expected an error for empty uid")
	}
}

// ── Update ───────────────────────────────────────────────────────────────

func TestClusterUpdateReassertsIdentityStamp(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	sn := &storedSharedNetwork{Name: stringPtr("test-shared-network"), Networks: []string{"10.0.0.0/24"}}
	sn.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(sn)

	cr := newClusterSharedNetwork("my-net", ref)
	cr.Spec.ForProvider.Comment = stringPtr("updated")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	updated := m.nets[ref]
	if updated.Ea[identity.EAKey] != testUIDCluster {
		t.Fatalf("expected identity stamp to survive update, got %v", updated.Ea)
	}
}

// ── Delete ───────────────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	sn := &storedSharedNetwork{Name: stringPtr("test-shared-network")}
	sn.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(sn)

	cr := newClusterSharedNetwork("my-net", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.nets[ref]; ok {
		t.Fatal("expected the object to be deleted")
	}
}

func TestClusterDeleteNotFoundIsSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterSharedNetwork("my-net", "sharednetwork/gone:my-net")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("expected nil error for already-gone object, got %v", err)
	}
}

func TestClusterDeleteRefusesUnverifiedOwnership(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	sn := &storedSharedNetwork{Name: stringPtr("test-shared-network")}
	ref := m.seed(sn)

	cr := newClusterSharedNetwork("my-net", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Delete(context.Background(), cr); err == nil {
		t.Fatal("expected delete to be refused for an unstamped object")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.nets[ref]; !ok {
		t.Fatal("object must not be deleted when ownership cannot be verified")
	}
}

// ── Connect ──────────────────────────────────────────────────────────────

func TestClusterConnectProviderConfigNotFound(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	c := &clusterConnector{kube: kube, usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{})}
	cr := newClusterSharedNetwork("my-net", "")

	if _, err := c.Connect(context.Background(), cr); err == nil {
		t.Fatal("expected an error when ProviderConfig is missing")
	}
}

func TestClusterConnectSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	u, _ := url.Parse(srv.URL)

	scheme := newTestScheme(t)
	pc := &clusterpcv1alpha1.ProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
		Spec: clusterpcv1alpha1.ProviderConfigSpec{
			Credentials: clusterpcv1alpha1.ProviderCredentials{
				Source: xpv1.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
					SecretRef: &xpv1.SecretKeySelector{Key: "creds", SecretReference: xpv1.SecretReference{Name: "creds", Namespace: "ns"}},
				},
			},
		},
	}
	secret := credentialsSecret("ns", "creds", u.Hostname(), "user", "pass")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pc, secret).Build()
	c := &clusterConnector{kube: kube, usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{})}
	cr := newClusterSharedNetwork("my-net", "")

	if _, err := c.Connect(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNamespacedConnectUnsupportedKind(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	c := &namespacedConnector{kube: kube, usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{})}
	cr := newNamespacedSharedNetwork("ns", "my-net", "", "SomethingElse")

	if _, err := c.Connect(context.Background(), cr); err == nil {
		t.Fatal("expected an error for an unsupported providerConfigRef Kind")
	}
}

func TestNamespacedConnectWithClusterProviderConfig(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	u, _ := url.Parse(srv.URL)

	scheme := newTestScheme(t)
	cpc := &namespacedpcv1alpha1.ClusterProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
		Spec: namespacedpcv1alpha1.ProviderConfigSpec{
			Credentials: namespacedpcv1alpha1.ProviderCredentials{
				Source: xpv1.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
					SecretRef: &xpv1.SecretKeySelector{Key: "creds", SecretReference: xpv1.SecretReference{Name: "creds", Namespace: "ns"}},
				},
			},
		},
	}
	secret := credentialsSecret("ns", "creds", u.Hostname(), "user", "pass")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cpc, secret).Build()
	c := &namespacedConnector{kube: kube, usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{})}
	cr := newNamespacedSharedNetwork("ns", "my-net", "", "ClusterProviderConfig")

	if _, err := c.Connect(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ── newEmpty correctness (dual-object-type gate — single-type resource) ──

func TestNewEmptyIpv4SharedNetworkCorrectness(t *testing.T) {
	sn := ibclient.NewEmptyIpv4SharedNetwork()
	if sn.ObjectType() != "sharednetwork" {
		t.Fatalf("expected ObjectType sharednetwork, got %q", sn.ObjectType())
	}
	found := false
	for _, f := range sn.ReturnFields() {
		if f == "extattrs" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected ReturnFields to include extattrs, got %v", sn.ReturnFields())
	}
}

// ── Identity EA must never late-init into spec.forProvider ───────────────

func TestLateInitializeDoesNotLeakIdentityKeyIntoExtAttrs(t *testing.T) {
	sn := &ibclient.SharedNetwork{Name: stringPtr("test-shared-network")}
	sn.Ea = identity.Stamp(ibclient.EA{"Site": "dc1"}, "some-uid")

	var networkView, comment *string
	var disable, useOptions *bool
	var extAttrs map[string]string
	lateInitialize(&networkView, &comment, &disable, &useOptions, &extAttrs, sn)

	if _, ok := extAttrs[identity.EAKey]; ok {
		t.Fatalf("identity key must never late-init into spec.forProvider.extAttrs, got %v", extAttrs)
	}
	if extAttrs["Site"] != "dc1" {
		t.Fatalf("expected non-reserved EA to still be back-filled, got %v", extAttrs)
	}
}

func TestIsUpToDateIgnoresIdentityEA(t *testing.T) {
	sn := &ibclient.SharedNetwork{Name: stringPtr("test-shared-network"), Networks: []*ibclient.Ipv4Network{{Ref: "10.0.0.0/24"}}}
	sn.Ea = identity.Stamp(nil, "some-uid")

	if !isUpToDate(stringPtr("test-shared-network"), []string{"10.0.0.0/24"}, nil, nil, nil, nil, nil, sn) {
		t.Fatal("expected isUpToDate to ignore the identity EA when spec.extAttrs is empty")
	}
}

// ── networks CIDR-vs-ref comparison (reconciliation loop regression) ─────
//
// Live evidence: a WAPI GET response's networks[].Ref is a full object
// reference, e.g.
// "network/ZG5zLm5ldHdvcmskMTAwLjY0LjExNi42NC8yOC8w:100.64.116.64/28/default",
// never the plain CIDR spec.forProvider.networks holds
// ("100.64.116.64/28"). Comparing them directly made isUpToDate return
// false on every reconcile regardless of actual drift.

func TestCidrFromNetworkRefParsesWapiRef(t *testing.T) {
	cases := map[string]string{
		"network/ZG5zLm5ldHdvcmskMTAwLjY0LjExNi42NC8yOC8w:100.64.116.64/28/default":                  "100.64.116.64/28",
		"network/ZG5zLm5ldHdvcmskMTkyLjE2OC4wLjAvMjQvbm9uZGVmYXVsdA:192.168.0.0/24/non-default-view": "192.168.0.0/24",
		// Already a plain CIDR (e.g. the SDK's own request-wrapper value
		// before a server response overwrites it) — returned unchanged.
		"10.0.0.0/24": "10.0.0.0/24",
		// Malformed/legacy shapes fall back to the raw value rather than
		// silently collapsing to an empty string.
		"network/onlyid": "network/onlyid",
		"":               "",
	}
	for ref, want := range cases {
		if got := cidrFromNetworkRef(ref); got != want {
			t.Errorf("cidrFromNetworkRef(%q) = %q, want %q", ref, got, want)
		}
	}
}

func TestIsUpToDateConvergesOnRefVsCidrMismatch(t *testing.T) {
	sn := &ibclient.SharedNetwork{
		Name: stringPtr("test-shared-network"),
		Networks: []*ibclient.Ipv4Network{
			{Ref: "network/ZG5zLm5ldHdvcmskMTAwLjY0LjExNi42NC8yOC8w:100.64.116.64/28/default"},
		},
	}

	if !isUpToDate(stringPtr("test-shared-network"), []string{"100.64.116.64/28"}, nil, nil, nil, nil, nil, sn) {
		t.Fatal("expected isUpToDate to converge when the desired CIDR matches the ref-embedded CIDR")
	}

	// Regression coverage: repeated reconciles against the same
	// unchanged observed state must stay stable (no flapping), proving
	// there is no permanent reconciliation loop.
	for i := 0; i < 3; i++ {
		if !isUpToDate(stringPtr("test-shared-network"), []string{"100.64.116.64/28"}, nil, nil, nil, nil, nil, sn) {
			t.Fatalf("isUpToDate flapped to false on repeated reconcile %d", i)
		}
	}
}

func TestIsUpToDateDetectsRealNetworksDrift(t *testing.T) {
	sn := &ibclient.SharedNetwork{
		Name: stringPtr("test-shared-network"),
		Networks: []*ibclient.Ipv4Network{
			{Ref: "network/ZG5zLm5ldHdvcmskMTAwLjY0LjExNi42NC8yOC8w:100.64.116.64/28/default"},
		},
	}

	if isUpToDate(stringPtr("test-shared-network"), []string{"10.0.0.0/24"}, nil, nil, nil, nil, nil, sn) {
		t.Fatal("expected isUpToDate to detect a genuine networks mismatch, not just tolerate the ref format")
	}
}

func TestNetworksFromSDKNormalizesRefsToCidrs(t *testing.T) {
	got := networksFromSDK([]*ibclient.Ipv4Network{
		{Ref: "network/ZG5zLm5ldHdvcmskMTAwLjY0LjExNi42NC8yOC8w:100.64.116.64/28/default"},
		{Ref: "network/ZG5zLm5ldHdvcmskMTAuMC4wLjAvMjQvZGVmYXVsdA:10.0.0.0/24/default"},
	})
	want := []string{"100.64.116.64/28", "10.0.0.0/24"}
	if !stringSliceEqualUnordered(got, want) {
		t.Fatalf("networksFromSDK() = %v, want %v", got, want)
	}
}

func TestClusterObserveConvergesRealWapiNetworkRefs(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	sn := &storedSharedNetwork{Name: stringPtr("test-shared-network"), Networks: []string{"100.64.116.64/28"}}
	sn.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(sn)

	cr := newClusterSharedNetwork("my-net", ref)
	cr.Spec.ForProvider.Networks = []string{"100.64.116.64/28"}
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	// Two consecutive reconciles against unchanged desired/observed state
	// must both report up to date — proving there is no
	// observe-forever-drifts reconciliation loop.
	for i := 0; i < 2; i++ {
		obs, err := e.Observe(context.Background(), cr)
		if err != nil {
			t.Fatalf("reconcile %d: unexpected error: %v", i, err)
		}
		if !obs.ResourceExists || !obs.ResourceUpToDate {
			t.Fatalf("reconcile %d: expected exists+up-to-date, got %+v", i, obs)
		}
	}
}

func TestClusterDisconnectIsNoop(t *testing.T) {
	e := &clusterExternal{}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNamespacedDisconnectIsNoop(t *testing.T) {
	e := &namespacedExternal{}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
