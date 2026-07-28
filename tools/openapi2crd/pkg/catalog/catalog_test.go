package catalog

import "testing"

// TestFindResource verifies FindResource returns the ARecord descriptor for
// its slug and reports false for an unknown slug.
func TestFindResource(t *testing.T) {
	rd, ok := FindResource("recorda")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "recorda")
	}
	if rd.Kind != "ARecord" {
		t.Errorf("Kind = %q, want ARecord", rd.Kind)
	}
	if rd.ClusterGroup != "recorda.infobloxnios.crossplane.io" {
		t.Errorf("ClusterGroup = %q, want recorda.infobloxnios.crossplane.io", rd.ClusterGroup)
	}
	if rd.NamespacedGroup != "recorda.infobloxnios.m.crossplane.io" {
		t.Errorf("NamespacedGroup = %q, want recorda.infobloxnios.m.crossplane.io", rd.NamespacedGroup)
	}

	if _, ok := FindResource("does-not-exist"); ok {
		t.Errorf("FindResource(%q): expected not found", "does-not-exist")
	}
}

// TestAllContainsARecord verifies the catalog's All() includes the ARecord
// pilot resource with a non-empty field list (a zero-field ARecord would
// indicate a wiring bug — every real resource in this catalog has at least
// one field).
func TestAllContainsARecord(t *testing.T) {
	found := false
	for _, rd := range All() {
		if rd.Slug != "recorda" {
			continue
		}
		found = true
		if len(rd.Fields) == 0 {
			t.Errorf("ARecord descriptor has zero fields")
		}
		if len(rd.NestedTypes) == 0 {
			t.Errorf("ARecord descriptor has zero nested types (expected discoveredData, cloudInfo, etc.)")
		}
	}
	if !found {
		t.Errorf("All() does not contain the recorda resource")
	}
}

// TestARecordFieldCounts pins the request/response/both field counts
// documented in tools/openapi/inventory.md's "### ARecord" section
// (request=1, response=15, both=7) — a regression guard: uniform or
// drifted counts would indicate a catalog authoring bug.
func TestARecordFieldCounts(t *testing.T) {
	rd, ok := FindResource("recorda")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "recorda")
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

	if req != 1 {
		t.Errorf("request-scope field count = %d, want 1", req)
	}
	if resp != 15 {
		t.Errorf("response-scope field count = %d, want 15", resp)
	}
	if both != 7 {
		t.Errorf("both-scope field count = %d, want 7", both)
	}
}
