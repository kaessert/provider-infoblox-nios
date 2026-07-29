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

// TestFindResourceDTCPool verifies FindResource returns the DTCPool
// descriptor for its slug, with the expected cluster/namespaced API groups.
func TestFindResourceDTCPool(t *testing.T) {
	rd, ok := FindResource("dtcpool")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "dtcpool")
	}
	if rd.Kind != "DTCPool" {
		t.Errorf("Kind = %q, want DTCPool", rd.Kind)
	}
	if rd.ClusterGroup != "dtcpool.infobloxnios.crossplane.io" {
		t.Errorf("ClusterGroup = %q, want dtcpool.infobloxnios.crossplane.io", rd.ClusterGroup)
	}
	if rd.NamespacedGroup != "dtcpool.infobloxnios.m.crossplane.io" {
		t.Errorf("NamespacedGroup = %q, want dtcpool.infobloxnios.m.crossplane.io", rd.NamespacedGroup)
	}
}

// TestDTCPoolFieldCounts pins the request/response/both field counts for
// the DTCPool catalog entry — request=0, response=3 (ref, consolidatedMonitors,
// health), both=16 (17 SDK "both" fields per inventory.md minus the excluded
// auto_consolidated_monitors, per ADR-IN-0004's live-verification finding
// that the field does not exist in WAPI).
func TestDTCPoolFieldCounts(t *testing.T) {
	rd, ok := FindResource("dtcpool")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "dtcpool")
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
	if resp != 3 {
		t.Errorf("response-scope field count = %d, want 3", resp)
	}
	if both != 16 {
		t.Errorf("both-scope field count = %d, want 16", both)
	}
}

// TestDTCPoolNoAutoConsolidatedMonitors verifies the SDK-only
// auto_consolidated_monitors field (no corresponding WAPI _schema entry,
// per ADR-IN-0004) is never cataloged for DTCPool.
func TestDTCPoolNoAutoConsolidatedMonitors(t *testing.T) {
	rd, ok := FindResource("dtcpool")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "dtcpool")
	}

	for _, f := range rd.Fields {
		if f.Name == "AutoConsolidatedMonitors" || f.JSONName == "autoConsolidatedMonitors" || f.JSONName == "auto_consolidated_monitors" {
			t.Errorf("field %q (json %q) must not be cataloged — SDK-only artifact absent from WAPI", f.Name, f.JSONName)
		}
	}
}

// TestDTCPoolRequiredFields verifies name and lbPreferredMethod are the
// only required ForProvider fields.
func TestDTCPoolRequiredFields(t *testing.T) {
	rd, ok := FindResource("dtcpool")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "dtcpool")
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
	if !required["LBPreferredMethod"] {
		t.Errorf("expected LBPreferredMethod to be required")
	}
	if len(required) != 2 {
		t.Errorf("expected exactly 2 required fields, got %d: %v", len(required), required)
	}
}

// TestDTCPoolConsolidatedAndHealthAreResponseOnly verifies
// consolidatedMonitors and health are response-scope (AtProvider-only) per
// the ph6 ticket acceptance criteria.
func TestDTCPoolConsolidatedAndHealthAreResponseOnly(t *testing.T) {
	rd, ok := FindResource("dtcpool")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "dtcpool")
	}

	wantResponseOnly := map[string]bool{"ConsolidatedMonitors": false, "Health": false}
	for _, f := range rd.Fields {
		if _, ok := wantResponseOnly[f.Name]; ok {
			wantResponseOnly[f.Name] = true
			if f.Scope != FieldScopeResponse {
				t.Errorf("field %q scope = %v, want FieldScopeResponse", f.Name, f.Scope)
			}
		}
	}
	for name, found := range wantResponseOnly {
		if !found {
			t.Errorf("expected field %q not found on DTCPool", name)
		}
	}
}

// TestDTCPoolServersCrossResourceReference verifies the servers field's
// nested DTCPoolServerLink.Server carries a Reference to DTCServer
// (cluster-scoped), per this provider's cross-resource reference
// convention.
func TestDTCPoolServersCrossResourceReference(t *testing.T) {
	rd, ok := FindResource("dtcpool")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "dtcpool")
	}

	var nt *NestedTypeDef
	for i := range rd.NestedTypes {
		if rd.NestedTypes[i].TypeName == "DTCPoolServerLink" {
			nt = &rd.NestedTypes[i]
			break
		}
	}
	if nt == nil {
		t.Fatalf("expected a DTCPoolServerLink nested type")
	}

	var serverField *FieldDef
	for i := range nt.Fields {
		if nt.Fields[i].Name == "Server" {
			serverField = &nt.Fields[i]
			break
		}
	}
	if serverField == nil {
		t.Fatalf("expected a Server field on DTCPoolServerLink")
	}
	if serverField.Reference == nil {
		t.Fatalf("expected Server field to carry a Reference")
	}
	if serverField.Reference.TargetKind != "DTCServer" {
		t.Errorf("Reference.TargetKind = %q, want DTCServer", serverField.Reference.TargetKind)
	}
	if serverField.Reference.TargetSlug != "dtcserver" {
		t.Errorf("Reference.TargetSlug = %q, want dtcserver", serverField.Reference.TargetSlug)
	}
	if serverField.Reference.TargetScope != "cluster" {
		t.Errorf("Reference.TargetScope = %q, want cluster", serverField.Reference.TargetScope)
	}
}
