package catalog

import "testing"

// testKindNetworkView is the shared literal for the NetworkView resource's
// Kind, reused across several catalog test files (catalog_test.go,
// network_test.go, network_view_test.go) that assert on cross-resource
// Reference descriptors and Kind values pointing at NetworkView.
const testKindNetworkView = "NetworkView"

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

// TestFindResourceRangeTemplate verifies FindResource returns the
// RangeTemplate descriptor with the expected API groups.
func TestFindResourceRangeTemplate(t *testing.T) {
	rd, ok := FindResource("rangetemplate")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "rangetemplate")
	}
	if rd.Kind != "RangeTemplate" {
		t.Errorf("Kind = %q, want RangeTemplate", rd.Kind)
	}
	if rd.ClusterGroup != "rangetemplate.infobloxnios.crossplane.io" {
		t.Errorf("ClusterGroup = %q, want rangetemplate.infobloxnios.crossplane.io", rd.ClusterGroup)
	}
	if rd.NamespacedGroup != "rangetemplate.infobloxnios.m.crossplane.io" {
		t.Errorf("NamespacedGroup = %q, want rangetemplate.infobloxnios.m.crossplane.io", rd.NamespacedGroup)
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

// TestAllContainsRangeTemplate verifies the catalog's All() includes the
// RangeTemplate resource with a non-empty field list and its two DHCP
// nested types (RangeTemplateOption, RangeTemplateMember).
func TestAllContainsRangeTemplate(t *testing.T) {
	found := false
	for _, rd := range All() {
		if rd.Slug != "rangetemplate" {
			continue
		}
		found = true
		if len(rd.Fields) == 0 {
			t.Errorf("RangeTemplate descriptor has zero fields")
		}
		if len(rd.NestedTypes) != 2 {
			t.Errorf("RangeTemplate descriptor has %d nested types, want 2 (RangeTemplateOption, RangeTemplateMember)", len(rd.NestedTypes))
		}
	}
	if !found {
		t.Errorf("All() does not contain the rangetemplate resource")
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

// TestFindResourceHostRecord verifies FindResource returns the
// HostRecord descriptor for its slug.
func TestFindResourceHostRecord(t *testing.T) {
	rd, ok := FindResource("hostrecord")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "hostrecord")
	}
	if rd.Kind != "HostRecord" {
		t.Errorf("Kind = %q, want HostRecord", rd.Kind)
	}
	if rd.ClusterGroup != "hostrecord.infobloxnios.crossplane.io" {
		t.Errorf("ClusterGroup = %q, want hostrecord.infobloxnios.crossplane.io", rd.ClusterGroup)
	}
	if rd.NamespacedGroup != "hostrecord.infobloxnios.m.crossplane.io" {
		t.Errorf("NamespacedGroup = %q, want hostrecord.infobloxnios.m.crossplane.io", rd.NamespacedGroup)
	}
}

// TestHostRecordFieldCounts pins the field counts for HostRecord — the
// ticket specifies ForProvider fields (settable) and AtProvider fields
// (response-only). Field scopes: request=0 (mac_address/duid omitted —
// see catalog doc), response=4, both=12.
func TestHostRecordFieldCounts(t *testing.T) {
	rd, ok := FindResource("hostrecord")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "hostrecord")
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
	if resp != 4 {
		t.Errorf("response-scope field count = %d, want 4", resp)
	}
	if both != 12 {
		t.Errorf("both-scope field count = %d, want 12", both)
	}
}

// TestHostRecordNestedTypes verifies HostRecord has the expected nested
// types (HostIpv4Addr and HostIpv6Addr) with correct sub-field counts.
func TestHostRecordNestedTypes(t *testing.T) {
	rd, ok := FindResource("hostrecord")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "hostrecord")
	}

	if len(rd.NestedTypes) != 2 {
		t.Fatalf("HostRecord nested type count = %d, want 2", len(rd.NestedTypes))
	}

	ipv4 := rd.NestedTypes[0]
	if ipv4.TypeName != "HostRecordIpv4Addr" {
		t.Errorf("NestedTypes[0].TypeName = %q, want HostRecordIpv4Addr", ipv4.TypeName)
	}
	if len(ipv4.Fields) != 4 {
		t.Errorf("HostRecordIpv4Addr field count = %d, want 4", len(ipv4.Fields))
	}

	ipv6 := rd.NestedTypes[1]
	if ipv6.TypeName != "HostRecordIpv6Addr" {
		t.Errorf("NestedTypes[1].TypeName = %q, want HostRecordIpv6Addr", ipv6.TypeName)
	}
	if len(ipv6.Fields) != 3 {
		t.Errorf("HostRecordIpv6Addr field count = %d, want 3", len(ipv6.Fields))
	}
}

// TestHostRecordNetworkViewReference verifies the networkView field carries
// a cross-resource reference descriptor targeting NetworkView.
func TestHostRecordNetworkViewReference(t *testing.T) {
	rd, ok := FindResource("hostrecord")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "hostrecord")
	}

	var found bool
	for _, f := range rd.Fields {
		if f.JSONName != "networkView" {
			continue
		}
		found = true
		if f.Reference == nil {
			t.Fatalf("networkView field has nil Reference")
		}
		if f.Reference.TargetKind != testKindNetworkView {
			t.Errorf("Reference.TargetKind = %q, want NetworkView", f.Reference.TargetKind)
		}
		if f.Reference.TargetSlug != "networkview" {
			t.Errorf("Reference.TargetSlug = %q, want networkview", f.Reference.TargetSlug)
		}
		if f.Reference.TargetScope != "cluster" {
			t.Errorf("Reference.TargetScope = %q, want cluster", f.Reference.TargetScope)
		}
		if !f.Immutable {
			t.Errorf("networkView field should be Immutable")
		}
	}
	if !found {
		t.Errorf("no field with JSONName=networkView found in HostRecord")
	}
}

// TestRangeTemplateFieldCounts pins the request/response/both field counts
// documented in tools/openapi/inventory.md's "### RangeTemplate" section
// (request=1, response=1, both=11) — a regression guard: uniform or
// drifted counts would indicate a catalog authoring bug.
func TestRangeTemplateFieldCounts(t *testing.T) {
	rd, ok := FindResource("rangetemplate")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "rangetemplate")
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
	if resp != 1 {
		t.Errorf("response-scope field count = %d, want 1", resp)
	}
	if both != 11 {
		t.Errorf("both-scope field count = %d, want 11", both)
	}
}

// TestRangeTemplateMsServerOmittedFromObservation verifies the write-only
// msServer field is excluded from the AtProvider full mirror (the SDK
// persists it as a nested Msdhcpserver struct on the response, a different
// shape than this flat field, so it is never echoed back).
func TestRangeTemplateMsServerOmittedFromObservation(t *testing.T) {
	rd, ok := FindResource("rangetemplate")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "rangetemplate")
	}

	for _, f := range rd.Fields {
		if f.Name != "MsServer" {
			continue
		}
		if !f.OmitFromObservation {
			t.Errorf("MsServer.OmitFromObservation = false, want true")
		}
		if f.Scope != FieldScopeRequest {
			t.Errorf("MsServer.Scope = %v, want FieldScopeRequest", f.Scope)
		}
		return
	}
	t.Errorf("rangetemplate descriptor has no MsServer field")
}

// TestAllContainsZoneAuth verifies the catalog's All() includes ZoneAuth
// with a non-empty field list and its two supporting nested types
// (MemberServer, ExternalServer).
func TestAllContainsZoneAuth(t *testing.T) {
	found := false
	for _, rd := range All() {
		if rd.Slug != "zoneauth" {
			continue
		}
		found = true
		if len(rd.Fields) == 0 {
			t.Errorf("ZoneAuth descriptor has zero fields")
		}
		if len(rd.NestedTypes) != 2 {
			t.Errorf("ZoneAuth descriptor has %d nested types, want 2 (MemberServer, ExternalServer)", len(rd.NestedTypes))
		}
	}
	if !found {
		t.Errorf("All() does not contain the zoneauth resource")
	}
}

// TestZoneAuthFieldCounts pins the request/response/both field counts for
// ZoneAuth (request=0, response=1, both=16) — a regression guard against a
// catalog authoring bug. Every ForProvider field is Scope=Both except the
// server-assigned `ref`, which is response-only (never settable by the
// controller).
func TestZoneAuthFieldCounts(t *testing.T) {
	rd, ok := FindResource("zoneauth")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "zoneauth")
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
	if both != 16 {
		t.Errorf("both-scope field count = %d, want 16", both)
	}
}

// TestZoneAuthImmutableFields verifies the three live-verified immutable
// fields (fqdn, view, zoneFormat) carry Immutable=true, and that every
// other field does not.
func TestZoneAuthImmutableFields(t *testing.T) {
	rd, ok := FindResource("zoneauth")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "zoneauth")
	}

	wantImmutable := map[string]bool{
		"fqdn":       true,
		"view":       true,
		"zoneFormat": true,
	}

	for _, f := range rd.Fields {
		want := wantImmutable[f.JSONName]
		if f.Immutable != want {
			t.Errorf("field %q Immutable = %v, want %v", f.JSONName, f.Immutable, want)
		}
	}
}

// TestFindResourceNetworkContainer verifies NetworkContainer's descriptor is
// registered in the catalog with the expected slug and API groups.
func TestFindResourceNetworkContainer(t *testing.T) {
	rd, ok := FindResource("networkcontainer")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "networkcontainer")
	}
	if rd.Kind != "NetworkContainer" {
		t.Errorf("Kind = %q, want NetworkContainer", rd.Kind)
	}
	if rd.ClusterGroup != "networkcontainer.infobloxnios.crossplane.io" {
		t.Errorf("ClusterGroup = %q, want networkcontainer.infobloxnios.crossplane.io", rd.ClusterGroup)
	}
	if rd.NamespacedGroup != "networkcontainer.infobloxnios.m.crossplane.io" {
		t.Errorf("NamespacedGroup = %q, want networkcontainer.infobloxnios.m.crossplane.io", rd.NamespacedGroup)
	}
}

// TestNetworkContainerFieldCounts pins the request/response/both field
// counts documented in tools/openapi/inventory.md's "### NetworkContainer"
// section (request=0, response=1, both=4).
func TestNetworkContainerFieldCounts(t *testing.T) {
	rd, ok := FindResource("networkcontainer")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "networkcontainer")
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
	if both != 4 {
		t.Errorf("both-scope field count = %d, want 4", both)
	}
}

// TestNetworkContainerNetworkViewReference verifies the networkView field
// carries a cross-resource reference targeting NetworkView (cluster-scoped),
// per the blueprint's cross-resource reference table.
func TestNetworkContainerNetworkViewReference(t *testing.T) {
	rd, ok := FindResource("networkcontainer")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "networkcontainer")
	}

	var found bool
	for _, f := range rd.Fields {
		if f.Name != testKindNetworkView {
			continue
		}
		found = true
		if f.Reference == nil {
			t.Fatalf("NetworkView field has no Reference descriptor")
		}
		if f.Reference.TargetKind != testKindNetworkView {
			t.Errorf("Reference.TargetKind = %q, want NetworkView", f.Reference.TargetKind)
		}
		if f.Reference.TargetSlug != "networkview" {
			t.Errorf("Reference.TargetSlug = %q, want networkview", f.Reference.TargetSlug)
		}
		if f.Reference.TargetScope != "cluster" {
			t.Errorf("Reference.TargetScope = %q, want cluster", f.Reference.TargetScope)
		}
		if !f.Immutable {
			t.Errorf("NetworkView field must be Immutable (network_view is absent from UpdateNetworkContainer)")
		}
	}
	if !found {
		t.Fatalf("NetworkContainer descriptor has no NetworkView field")
	}
}
