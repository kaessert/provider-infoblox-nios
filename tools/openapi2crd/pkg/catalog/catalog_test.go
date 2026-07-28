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

// TestAllContainsNSRecord verifies the catalog's All() includes the
// NSRecord resource with a non-empty field list and its supporting nested
// types.
func TestAllContainsNSRecord(t *testing.T) {
	found := false
	for _, rd := range All() {
		if rd.Slug != "recordns" {
			continue
		}
		found = true
		if len(rd.Fields) == 0 {
			t.Errorf("NSRecord descriptor has zero fields")
		}
		if len(rd.NestedTypes) == 0 {
			t.Errorf("NSRecord descriptor has zero nested types (expected NSRecordAddress, NSRecordCloudInfo, etc.)")
		}
	}
	if !found {
		t.Errorf("All() does not contain the recordns resource")
	}
}

// TestNSRecordFieldCounts pins the request/response/both field counts for
// the ground-truth RecordNS SDK struct (request=0, response=7, both=5) — a
// regression guard against catalog authoring drift. This intentionally
// differs from inventory.md's stale request=0/response=8/both=5 count: the
// inventory table lists creation_time/ms_ad_user_data (not real RecordNS
// fields) and omits creator (a real field); see the nsRecord() doc comment
// and ADR-IN-0004 for the correction.
func TestNSRecordFieldCounts(t *testing.T) {
	rd, ok := FindResource("recordns")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "recordns")
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
	if resp != 7 {
		t.Errorf("response-scope field count = %d, want 7", resp)
	}
	if both != 5 {
		t.Errorf("both-scope field count = %d, want 5", both)
	}
}

// TestNSRecordImmutableFields verifies the live-verified immutable fields
// (ADR-IN-0004) carry Immutable=true: name and view. addresses must be
// Required but NOT Immutable.
func TestNSRecordImmutableFields(t *testing.T) {
	rd, ok := FindResource("recordns")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "recordns")
	}

	wantImmutable := map[string]bool{
		jsonNameName: true,
		"view":       true,
	}
	wantRequired := map[string]bool{
		jsonNameName: true,
		"nameserver": true,
		"view":       true,
		"addresses":  true,
	}

	for _, f := range rd.Fields {
		if wantImmutable[f.JSONName] && !f.Immutable {
			t.Errorf("field %q: Immutable = false, want true", f.JSONName)
		}
		if wantRequired[f.JSONName] && !f.Required {
			t.Errorf("field %q: Required = false, want true", f.JSONName)
		}
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

// TestFindResourcePTRRecord verifies FindResource returns the PTRRecord
// descriptor for its slug, with correctly formed cluster/namespaced groups.
func TestFindResourcePTRRecord(t *testing.T) {
	rd, ok := FindResource("recordptr")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "recordptr")
	}
	if rd.Kind != "PTRRecord" {
		t.Errorf("Kind = %q, want PTRRecord", rd.Kind)
	}
	if rd.ClusterGroup != "recordptr.infobloxnios.crossplane.io" {
		t.Errorf("ClusterGroup = %q, want recordptr.infobloxnios.crossplane.io", rd.ClusterGroup)
	}
	if rd.NamespacedGroup != "recordptr.infobloxnios.m.crossplane.io" {
		t.Errorf("NamespacedGroup = %q, want recordptr.infobloxnios.m.crossplane.io", rd.NamespacedGroup)
	}
}

// TestAllContainsPTRRecord verifies the catalog's All() includes PTRRecord
// with a non-empty field list and nested types (Discoverydata, CloudInfo,
// etc. mirrored from the shared DNS record SDK structs).
func TestAllContainsPTRRecord(t *testing.T) {
	found := false
	for _, rd := range All() {
		if rd.Slug != "recordptr" {
			continue
		}
		found = true
		if len(rd.Fields) == 0 {
			t.Errorf("PTRRecord descriptor has zero fields")
		}
		if len(rd.NestedTypes) == 0 {
			t.Errorf("PTRRecord descriptor has zero nested types (expected discoveredData, cloudInfo, etc.)")
		}
	}
	if !found {
		t.Errorf("All() does not contain the recordptr resource")
	}
}

// TestPTRRecordFieldCounts pins the request/response/both field counts
// documented in tools/openapi/inventory.md's "### PTRRecord" section
// (request=0, response=16, both=9) — a regression guard: uniform or
// drifted counts would indicate a catalog authoring bug.
func TestPTRRecordFieldCounts(t *testing.T) {
	rd, ok := FindResource("recordptr")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "recordptr")
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
	if resp != 16 {
		t.Errorf("response-scope field count = %d, want 16", resp)
	}
	if both != 9 {
		t.Errorf("both-scope field count = %d, want 9", both)
	}
}

// TestPTRRecordPtrdnameHasReference verifies ptrdname is cataloged with a
// Reference descriptor targeting the namespaced ARecord — PTRRecord's
// ptrdname commonly names an ARecord's FQDN, so the generated type carries
// the standard three-field reference pattern (value + Ref + Selector) even
// though WAPI itself does not enforce that the target exists.
func TestPTRRecordPtrdnameHasReference(t *testing.T) {
	rd, ok := FindResource("recordptr")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "recordptr")
	}

	for _, f := range rd.Fields {
		if f.Name != "Ptrdname" {
			continue
		}
		if f.Reference == nil {
			t.Fatalf("Ptrdname.Reference = nil, want a ReferenceDescriptor targeting ARecord")
		}
		if f.Reference.TargetKind != "ARecord" {
			t.Errorf("Ptrdname.Reference.TargetKind = %q, want ARecord", f.Reference.TargetKind)
		}
		if f.Reference.TargetSlug != "recorda" {
			t.Errorf("Ptrdname.Reference.TargetSlug = %q, want recorda", f.Reference.TargetSlug)
		}
		if f.Reference.TargetScope != "namespaced" {
			t.Errorf("Ptrdname.Reference.TargetScope = %q, want namespaced", f.Reference.TargetScope)
		}
		if f.Reference.Extractor != "" {
			t.Errorf("Ptrdname.Reference.Extractor = %q, want empty (default reference.ExternalName())", f.Reference.Extractor)
		}
		return
	}
	t.Errorf("PTRRecord descriptor has no Ptrdname field")
}

// TestFindResourceSRVRecord verifies FindResource returns the SRVRecord
// descriptor for its slug with the expected API groups.
func TestFindResourceSRVRecord(t *testing.T) {
	rd, ok := FindResource("recordsrv")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "recordsrv")
	}
	if rd.Kind != "SRVRecord" {
		t.Errorf("Kind = %q, want SRVRecord", rd.Kind)
	}
	if rd.ClusterGroup != "recordsrv.infobloxnios.crossplane.io" {
		t.Errorf("ClusterGroup = %q, want recordsrv.infobloxnios.crossplane.io", rd.ClusterGroup)
	}
	if rd.NamespacedGroup != "recordsrv.infobloxnios.m.crossplane.io" {
		t.Errorf("NamespacedGroup = %q, want recordsrv.infobloxnios.m.crossplane.io", rd.NamespacedGroup)
	}
}

// TestAllContainsSRVRecord verifies the catalog's All() includes SRVRecord
// with a non-empty field list and its response-only nested types.
func TestAllContainsSRVRecord(t *testing.T) {
	found := false
	for _, rd := range All() {
		if rd.Slug != "recordsrv" {
			continue
		}
		found = true
		if len(rd.Fields) == 0 {
			t.Errorf("SRVRecord descriptor has zero fields")
		}
		if len(rd.NestedTypes) == 0 {
			t.Errorf("SRVRecord descriptor has zero nested types (expected awsRte53RecordInfo, cloudInfo, msAdUserData)")
		}
	}
	if !found {
		t.Errorf("All() does not contain the recordsrv resource")
	}
}

// TestSRVRecordFieldCounts pins the request/response/both field counts
// documented in tools/openapi/inventory.md's "### SRVRecord" section
// (request=0, response=15, both=10) — a regression guard: drifted counts
// would indicate a catalog authoring bug.
func TestSRVRecordFieldCounts(t *testing.T) {
	rd, ok := FindResource("recordsrv")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "recordsrv")
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
	if both != 10 {
		t.Errorf("both-scope field count = %d, want 10", both)
	}
}

// TestSRVRecordImmutableFields verifies only `view` and `zone` are marked
// Immutable, matching the blueprint's per-resource immutable field table
// (view is the only ForProvider field carrying a CEL rule; zone is
// AtProvider-only and derived, per FieldDef.Immutable doc).
func TestSRVRecordImmutableFields(t *testing.T) {
	rd, ok := FindResource("recordsrv")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "recordsrv")
	}

	var immutable []string
	for _, f := range rd.Fields {
		if f.Immutable {
			immutable = append(immutable, f.JSONName)
		}
	}

	want := map[string]bool{"view": true, "zone": true}
	if len(immutable) != len(want) {
		t.Fatalf("immutable fields = %v, want exactly %v", immutable, want)
	}
	for _, name := range immutable {
		if !want[name] {
			t.Errorf("unexpected immutable field %q", name)
		}
	}
}
