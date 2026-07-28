package catalog

import "testing"

// TestFindResourceNetworkView verifies FindResource returns the NetworkView
// descriptor for its slug, with the expected cluster/namespaced API groups.
func TestFindResourceNetworkView(t *testing.T) {
	rd, ok := FindResource("networkview")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "networkview")
	}
	if rd.Kind != "NetworkView" {
		t.Errorf("Kind = %q, want NetworkView", rd.Kind)
	}
	if rd.ClusterGroup != "networkview.infobloxnios.crossplane.io" {
		t.Errorf("ClusterGroup = %q, want networkview.infobloxnios.crossplane.io", rd.ClusterGroup)
	}
	if rd.NamespacedGroup != "networkview.infobloxnios.m.crossplane.io" {
		t.Errorf("NamespacedGroup = %q, want networkview.infobloxnios.m.crossplane.io", rd.NamespacedGroup)
	}
}

// TestNetworkViewFieldCounts pins the request/response/both field counts for
// the cataloged NetworkView fields (name, comment, extattrs -> both; ref,
// isDefault -> response) — a regression guard against accidental catalog
// drift.
func TestNetworkViewFieldCounts(t *testing.T) {
	rd, ok := FindResource("networkview")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "networkview")
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
		t.Errorf("response-scope field count = %d, want 2 (ref, isDefault)", resp)
	}
	if both != 3 {
		t.Errorf("both-scope field count = %d, want 3 (name, comment, extattrs)", both)
	}
}

// TestNetworkViewIsDefaultImmutable verifies is_default is cataloged as
// Immutable and FieldScopeResponse (no ForProvider representation — WAPI
// assigns it, it is never a create/update parameter).
func TestNetworkViewIsDefaultImmutable(t *testing.T) {
	rd, ok := FindResource("networkview")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "networkview")
	}

	found := false
	for _, f := range rd.Fields {
		if f.Name != "IsDefault" {
			continue
		}
		found = true
		if !f.Immutable {
			t.Errorf("IsDefault: expected Immutable=true")
		}
		if f.Scope != FieldScopeResponse {
			t.Errorf("IsDefault: expected Scope=FieldScopeResponse, got %v", f.Scope)
		}
	}
	if !found {
		t.Errorf("NetworkView descriptor has no IsDefault field")
	}
}
