package catalog

import "testing"

// TestFindResourceDTCServer verifies FindResource returns the DTCServer
// descriptor for its slug, with the expected cluster/namespaced API groups.
func TestFindResourceDTCServer(t *testing.T) {
	rd, ok := FindResource("dtcserver")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "dtcserver")
	}
	if rd.Kind != "DTCServer" {
		t.Errorf("Kind = %q, want DTCServer", rd.Kind)
	}
	if rd.ClusterGroup != "dtcserver.infobloxnios.crossplane.io" {
		t.Errorf("ClusterGroup = %q, want dtcserver.infobloxnios.crossplane.io", rd.ClusterGroup)
	}
	if rd.NamespacedGroup != "dtcserver.infobloxnios.m.crossplane.io" {
		t.Errorf("NamespacedGroup = %q, want dtcserver.infobloxnios.m.crossplane.io", rd.NamespacedGroup)
	}
}

// TestDTCServerFieldCounts pins the request/response/both field counts
// documented in tools/openapi/inventory.md's "### DTCServer" section
// (request=0, response=2, both=9) — a regression guard against drifted
// catalog authoring.
func TestDTCServerFieldCounts(t *testing.T) {
	rd, ok := FindResource("dtcserver")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "dtcserver")
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
	if both != 9 {
		t.Errorf("both-scope field count = %d, want 9", both)
	}
}

// TestDTCServerRequiredFields verifies name and host are the only required
// ForProvider fields (per tools/openapi/inventory.md and the ph6 ticket
// acceptance criteria).
func TestDTCServerRequiredFields(t *testing.T) {
	rd, ok := FindResource("dtcserver")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "dtcserver")
	}

	required := map[string]bool{}
	for _, f := range rd.Fields {
		if f.Required {
			required[f.Name] = true
		}
	}

	if !required["Name"] {
		t.Errorf("expected Name to be required")
	}
	if !required["Host"] {
		t.Errorf("expected Host to be required")
	}
	if len(required) != 2 {
		t.Errorf("expected exactly 2 required fields, got %d: %v", len(required), required)
	}
}

// TestDTCServerNoCrossResourceReferences verifies no field carries a
// Reference — DTCServer is a reference TARGET (for DTCPool.servers) but
// does not itself reference any other cataloged resource.
func TestDTCServerNoCrossResourceReferences(t *testing.T) {
	rd, ok := FindResource("dtcserver")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "dtcserver")
	}

	for _, f := range rd.Fields {
		if f.Reference != nil {
			t.Errorf("field %q carries a Reference, but DTCServer has no cross-resource references", f.Name)
		}
	}
}
