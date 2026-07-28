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

// TestAllContainsTXTRecord verifies the catalog's All() includes TXTRecord
// with a non-empty field list and nested types (AwsRte53RecordInfo,
// CloudInfo, MsAdUserData — a zero-field or zero-nested-type descriptor
// would indicate a wiring bug).
func TestAllContainsTXTRecord(t *testing.T) {
	rd, ok := FindResource("recordtxt")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "recordtxt")
	}
	if rd.Kind != "TXTRecord" {
		t.Errorf("Kind = %q, want TXTRecord", rd.Kind)
	}
	if rd.ClusterGroup != "recordtxt.infobloxnios.crossplane.io" {
		t.Errorf("ClusterGroup = %q, want recordtxt.infobloxnios.crossplane.io", rd.ClusterGroup)
	}
	if rd.NamespacedGroup != "recordtxt.infobloxnios.m.crossplane.io" {
		t.Errorf("NamespacedGroup = %q, want recordtxt.infobloxnios.m.crossplane.io", rd.NamespacedGroup)
	}
	if len(rd.Fields) == 0 {
		t.Errorf("TXTRecord descriptor has zero fields")
	}
	if len(rd.NestedTypes) == 0 {
		t.Errorf("TXTRecord descriptor has zero nested types (expected awsRte53RecordInfo, cloudInfo, msAdUserData)")
	}
}

// TestTXTRecordFieldCounts pins the request/response/both field counts
// documented in tools/openapi/inventory.md's "### TXTRecord" section
// (request=0, response=14, both=7) — a regression guard: uniform or
// drifted counts would indicate a catalog authoring bug.
func TestTXTRecordFieldCounts(t *testing.T) {
	rd, ok := FindResource("recordtxt")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "recordtxt")
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
	if resp != 14 {
		t.Errorf("response-scope field count = %d, want 14", resp)
	}
	if both != 7 {
		t.Errorf("both-scope field count = %d, want 7", both)
	}
}

// TestFindResourceZoneDelegated verifies FindResource returns the
// ZoneDelegated descriptor for its slug.
func TestFindResourceZoneDelegated(t *testing.T) {
	rd, ok := FindResource("zonedelegated")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "zonedelegated")
	}
	if rd.Kind != "ZoneDelegated" {
		t.Errorf("Kind = %q, want ZoneDelegated", rd.Kind)
	}
	if rd.ClusterGroup != "zonedelegated.infobloxnios.crossplane.io" {
		t.Errorf("ClusterGroup = %q, want zonedelegated.infobloxnios.crossplane.io", rd.ClusterGroup)
	}
	if rd.NamespacedGroup != "zonedelegated.infobloxnios.m.crossplane.io" {
		t.Errorf("NamespacedGroup = %q, want zonedelegated.infobloxnios.m.crossplane.io", rd.NamespacedGroup)
	}
}

// TestZoneDelegatedFieldCounts pins the field counts documented in
// tools/openapi/inventory.md's "### ZoneDelegated" section (request=0,
// response=1, both=11) — a regression guard against catalog authoring
// drift.
func TestZoneDelegatedFieldCounts(t *testing.T) {
	rd, ok := FindResource("zonedelegated")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "zonedelegated")
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
	if resp != 1 {
		t.Errorf("response-scope field count = %d, want 1", resp)
	}
	if both != 11 {
		t.Errorf("both-scope field count = %d, want 11", both)
	}
}

// TestZoneDelegatedImmutableFields verifies fqdn, view, and zoneFormat are
// marked Immutable — the fields absent from UpdateZoneDelegated's SDK
// signature.
func TestZoneDelegatedImmutableFields(t *testing.T) {
	rd, ok := FindResource("zonedelegated")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "zonedelegated")
	}

	wantImmutable := map[string]bool{
		"fqdn":       true,
		"view":       true,
		"zoneFormat": true,
	}
	for _, f := range rd.Fields {
		if want, ok := wantImmutable[f.JSONName]; ok {
			if f.Immutable != want {
				t.Errorf("field %q Immutable = %v, want %v", f.JSONName, f.Immutable, want)
			}
			delete(wantImmutable, f.JSONName)
		} else if f.Immutable {
			t.Errorf("field %q unexpectedly marked Immutable", f.JSONName)
		}
	}
	for name := range wantImmutable {
		t.Errorf("expected field %q not found in ZoneDelegated descriptor", name)
	}
}

// TestZoneDelegatedNestedTypes verifies ZoneDelegated has the expected
// NameServer nested type (used by delegateTo) with name/address sub-fields.
func TestZoneDelegatedNestedTypes(t *testing.T) {
	rd, ok := FindResource("zonedelegated")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "zonedelegated")
	}

	if len(rd.NestedTypes) != 1 {
		t.Fatalf("ZoneDelegated nested type count = %d, want 1", len(rd.NestedTypes))
	}

	ns := rd.NestedTypes[0]
	if ns.TypeName != "ZoneDelegatedNameServer" {
		t.Errorf("NestedTypes[0].TypeName = %q, want ZoneDelegatedNameServer", ns.TypeName)
	}
	if len(ns.Fields) != 2 {
		t.Fatalf("ZoneDelegatedNameServer field count = %d, want 2", len(ns.Fields))
	}

	var haveName, haveAddress bool
	for _, f := range ns.Fields {
		switch f.JSONName {
		case "name":
			haveName = true
		case "address":
			haveAddress = true
		}
	}
	if !haveName {
		t.Errorf("ZoneDelegatedNameServer missing name field")
	}
	if !haveAddress {
		t.Errorf("ZoneDelegatedNameServer missing address field")
	}
}

// TestZoneDelegatedDelegateToField verifies delegateTo is a required,
// mutable slice of the ZoneDelegatedNameServer nested type.
func TestZoneDelegatedDelegateToField(t *testing.T) {
	rd, ok := FindResource("zonedelegated")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "zonedelegated")
	}

	var found bool
	for _, f := range rd.Fields {
		if f.JSONName != "delegateTo" {
			continue
		}
		found = true
		if f.GoType != "[]ZoneDelegatedNameServer" {
			t.Errorf("delegateTo GoType = %q, want []ZoneDelegatedNameServer", f.GoType)
		}
		if !f.Required {
			t.Errorf("delegateTo should be Required")
		}
		if f.Immutable {
			t.Errorf("delegateTo should NOT be Immutable (mutable per ticket)")
		}
	}
	if !found {
		t.Errorf("no field with JSONName=delegateTo found in ZoneDelegated")
	}
}

// TestAllContainsMXRecord verifies the catalog's All() includes MXRecord
// with a non-empty field list and nested types (mirrors
// TestAllContainsARecord).
func TestAllContainsMXRecord(t *testing.T) {
	found := false
	for _, rd := range All() {
		if rd.Slug != "recordmx" {
			continue
		}
		found = true
		if len(rd.Fields) == 0 {
			t.Errorf("MXRecord descriptor has zero fields")
		}
		if len(rd.NestedTypes) == 0 {
			t.Errorf("MXRecord descriptor has zero nested types (expected cloudInfo, msAdUserData, etc.)")
		}
	}
	if !found {
		t.Errorf("All() does not contain the recordmx resource")
	}
}

// TestMXRecordFieldCounts pins the request/response/both field counts
// documented in tools/openapi/inventory.md's "### MXRecord" section
// (request=0, response=15, both=8) — a regression guard: uniform or
// drifted counts would indicate a catalog authoring bug.
func TestMXRecordFieldCounts(t *testing.T) {
	rd, ok := FindResource("recordmx")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "recordmx")
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
	if resp != 15 {
		t.Errorf("response-scope field count = %d, want 15", resp)
	}
	if both != 8 {
		t.Errorf("both-scope field count = %d, want 8", both)
	}
}
