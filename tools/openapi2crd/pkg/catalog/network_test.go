package catalog

import "testing"

// TestFindResourceNetwork verifies FindResource returns the Network
// descriptor for its slug, with the expected dual-scope API groups.
func TestFindResourceNetwork(t *testing.T) {
	rd, ok := FindResource("network")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "network")
	}
	if rd.Kind != "Network" {
		t.Errorf("Kind = %q, want Network", rd.Kind)
	}
	if rd.ClusterGroup != "network.infobloxnios.crossplane.io" {
		t.Errorf("ClusterGroup = %q, want network.infobloxnios.crossplane.io", rd.ClusterGroup)
	}
	if rd.NamespacedGroup != "network.infobloxnios.m.crossplane.io" {
		t.Errorf("NamespacedGroup = %q, want network.infobloxnios.m.crossplane.io", rd.NamespacedGroup)
	}
}

// TestNetworkFieldCounts pins the request/response/both field counts
// documented in tools/openapi/inventory.md's "### Network" section
// (request=0, response=2, both=4) — a regression guard: uniform or
// drifted counts would indicate a catalog authoring bug.
func TestNetworkFieldCounts(t *testing.T) {
	rd, ok := FindResource("network")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "network")
	}

	var req, resp, both int
	for _, f := range rd.Fields {
		switch f.Scope {
		case FieldScopeRequest:
			req++
		case FieldScopeResponse:
			resp++
		case FieldScopeBoth:
			both++
		}
	}

	if req != 0 {
		t.Errorf("request-scope field count = %d, want 0", req)
	}
	if resp != 2 {
		t.Errorf("response-scope field count = %d, want 2", resp)
	}
	if both != 4 {
		t.Errorf("both-scope field count = %d, want 4", both)
	}
}

// TestNetworkImmutableFields verifies network_view and network are marked
// Immutable — the CEL self == oldSelf XValidation rule depends on this flag.
func TestNetworkImmutableFields(t *testing.T) {
	rd, ok := FindResource("network")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "network")
	}

	wantImmutable := map[string]bool{
		"networkView": true,
		"network":     true,
		"comment":     false,
		"extAttrs":    false,
		"ref":         false,
	}

	seen := map[string]bool{}
	for _, f := range rd.Fields {
		want, ok := wantImmutable[f.JSONName]
		if !ok {
			continue
		}
		seen[f.JSONName] = true
		if f.Immutable != want {
			t.Errorf("field %q Immutable = %v, want %v", f.JSONName, f.Immutable, want)
		}
	}
	for name := range wantImmutable {
		if !seen[name] {
			t.Errorf("expected field %q not found in Network descriptor", name)
		}
	}
}

// TestNetworkNestedTypes verifies the Network descriptor carries the
// NetworkMember nested type used by the response-only members field.
func TestNetworkNestedTypes(t *testing.T) {
	rd, ok := FindResource("network")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "network")
	}
	found := false
	for _, nt := range rd.NestedTypes {
		if nt.TypeName == "NetworkMember" {
			found = true
			if len(nt.Fields) == 0 {
				t.Errorf("NetworkMember nested type has zero fields")
			}
		}
	}
	if !found {
		t.Errorf("Network descriptor missing NetworkMember nested type")
	}
}
