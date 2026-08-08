// Package zoneforward read/write endpoint split (read-routing) tests.
//
// ZoneForward is not an authoritative zone — it produces no zone_auth
// entry (ADR-IN-0005 §7), so it is wired PRIMARY-ONLY: hasZoneSerial is
// unconditionally false, which forces the router's decision to
// PrimaryOnly and BeginObserve never attempts a candidate call, even when
// a candidate/gate is configured on the ProviderConfig.
package zoneforward

import (
	"context"
	"net/http/httptest"
	"testing"

	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"

	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/convergence"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/readrouting"
)

// poisonIBConnector fails the test if any of its methods is ever called —
// used to prove Observe never issues a candidate call when the router's
// decision is PrimaryOnly.
type poisonIBConnector struct{ t *testing.T }

func (p poisonIBConnector) CreateObject(ibclient.IBObject) (string, error) {
	p.t.Helper()
	p.t.Fatal("unexpected poisoned-connector CreateObject call")
	return "", nil
}

func (p poisonIBConnector) GetObject(ibclient.IBObject, string, *ibclient.QueryParams, interface{}) error {
	p.t.Helper()
	p.t.Fatal("unexpected poisoned-connector GetObject call")
	return nil
}

func (p poisonIBConnector) DeleteObject(string) (string, error) {
	p.t.Helper()
	p.t.Fatal("unexpected poisoned-connector DeleteObject call")
	return "", nil
}

func (p poisonIBConnector) UpdateObject(ibclient.IBObject, string) (string, error) {
	p.t.Helper()
	p.t.Fatal("unexpected poisoned-connector UpdateObject call")
	return "", nil
}

// TestClusterObserveReadEndpointAbsentNoCandidateCall proves that with no
// readEndpoint configured (router.Gate == nil), Observe never touches a
// candidate connector — readFrom is always the primary, and no
// ReadRouting condition is set.
func TestClusterObserveReadEndpointAbsentNoCandidateCall(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:      "forward.example.com",
		View:      stringPtr("default"),
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Ea:        ibclient.EA{identity.EAKey: "test-uid-cluster"},
	})

	mc := newTestClients(t, srv)
	e := &clusterExternal{
		kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector,
		router: readrouting.Router{Candidate: poisonIBConnector{t: t}}, // gate is nil, so this must never be touched
		prober: identity.NewProber(), endpoint: t.Name(),
	}
	cr := newClusterZoneForward("my-zone", ref)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true, got false")
	}
	if cond := cr.GetCondition(convergence.ReadRoutingConditionType); cond.Status != corev1.ConditionUnknown {
		t.Fatalf("Observe: expected no ReadRouting condition when no readEndpoint is configured, got %+v", cond)
	}
}

// TestNamespacedObserveReadEndpointAbsentNoCandidateCall is the
// namespaced-scope variant.
func TestNamespacedObserveReadEndpointAbsentNoCandidateCall(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:      "forward.example.com",
		View:      stringPtr("default"),
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Ea:        ibclient.EA{identity.EAKey: "test-uid-namespaced"},
	})

	mc := newTestClients(t, srv)
	e := &namespacedExternal{
		kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector,
		router: readrouting.Router{Candidate: poisonIBConnector{t: t}},
		prober: identity.NewProber(), endpoint: t.Name(),
	}
	cr := newNamespacedZoneForward("team-a", "my-zone", ref, "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true, got false")
	}
	if cond := cr.GetCondition(convergence.ReadRoutingConditionType); cond.Status != corev1.ConditionUnknown {
		t.Fatalf("Observe: expected no ReadRouting condition when no readEndpoint is configured, got %+v", cond)
	}
}

// TestClusterObservePrimaryOnlyNeverAttemptsCandidateCall proves ADR §7's
// classification: even with a fully configured candidate/gate, ZoneForward
// has no SOA serial to gate on, so hasZoneSerial=false forces the router's
// decision to PrimaryOnly. The candidate connector is poisoned — a
// candidate call here would be a REJECT-worthy defect (a zone the Grid
// does not serve authoritatively has no zone_auth match, so no candidate
// read may be attempted).
func TestClusterObservePrimaryOnlyNeverAttemptsCandidateCall(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:      "forward.example.com",
		View:      stringPtr("default"),
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Ea:        ibclient.EA{identity.EAKey: "test-uid-cluster"},
	})

	mc := newTestClients(t, srv)
	gate := convergence.NewGate(nil, convergence.NewClientWithScheme("http", "unused", "0", wapiVersion, "u", "p", true), nil, "candidate-host")

	e := &clusterExternal{
		kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector,
		router: readrouting.Router{
			Candidate: poisonIBConnector{t: t},
			Gate:      gate,
			Mode:      convergence.ModeSOASerial,
			Timeout:   convergence.DefaultTimeout,
		},
		prober: identity.NewProber(), endpoint: t.Name(),
	}
	cr := newClusterZoneForward("my-zone", ref)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true, got false")
	}
	cond := cr.GetCondition(convergence.ReadRoutingConditionType)
	if cond.Status != corev1.ConditionFalse || string(cond.Reason) != convergence.ReasonPrimaryOnly {
		t.Fatalf("Observe: unexpected ReadRouting condition: %+v", cond)
	}
}

// TestNamespacedObservePrimaryOnlyNeverAttemptsCandidateCall is the
// namespaced-scope variant.
func TestNamespacedObservePrimaryOnlyNeverAttemptsCandidateCall(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:      "forward.example.com",
		View:      stringPtr("default"),
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Ea:        ibclient.EA{identity.EAKey: "test-uid-namespaced"},
	})

	mc := newTestClients(t, srv)
	gate := convergence.NewGate(nil, convergence.NewClientWithScheme("http", "unused", "0", wapiVersion, "u", "p", true), nil, "candidate-host")

	e := &namespacedExternal{
		kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector,
		router: readrouting.Router{
			Candidate: poisonIBConnector{t: t},
			Gate:      gate,
			Mode:      convergence.ModeSOASerial,
			Timeout:   convergence.DefaultTimeout,
		},
		prober: identity.NewProber(), endpoint: t.Name(),
	}
	cr := newNamespacedZoneForward("team-a", "my-zone", ref, "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true, got false")
	}
	cond := cr.GetCondition(convergence.ReadRoutingConditionType)
	if cond.Status != corev1.ConditionFalse || string(cond.Reason) != convergence.ReasonPrimaryOnly {
		t.Fatalf("Observe: unexpected ReadRouting condition: %+v", cond)
	}
}
