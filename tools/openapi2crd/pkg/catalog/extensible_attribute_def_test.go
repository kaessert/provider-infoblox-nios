package catalog

import "testing"

// TestFindResourceExtensibleAttributeDef verifies FindResource returns the
// ExtensibleAttributeDef descriptor for its slug, with the expected
// cluster/namespaced API groups.
func TestFindResourceExtensibleAttributeDef(t *testing.T) {
	rd, ok := FindResource("extensibleattributedef")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "extensibleattributedef")
	}
	if rd.Kind != "ExtensibleAttributeDef" {
		t.Errorf("Kind = %q, want ExtensibleAttributeDef", rd.Kind)
	}
	if rd.ClusterGroup != "extensibleattributedef.infobloxnios.crossplane.io" {
		t.Errorf("ClusterGroup = %q, want extensibleattributedef.infobloxnios.crossplane.io", rd.ClusterGroup)
	}
	if rd.NamespacedGroup != "extensibleattributedef.infobloxnios.m.crossplane.io" {
		t.Errorf("NamespacedGroup = %q, want extensibleattributedef.infobloxnios.m.crossplane.io", rd.NamespacedGroup)
	}
}

// TestExtensibleAttributeDefFieldCounts pins the request/response/both field
// counts for the cataloged ExtensibleAttributeDef fields — a regression
// guard against accidental catalog drift. descendantsAction is
// request-only (write-only field); ref and namespace are response-only;
// the remaining eight fields (name, type, comment, defaultValue, min, max,
// flags, listValues, allowedObjectTypes) are both.
func TestExtensibleAttributeDefFieldCounts(t *testing.T) {
	rd, ok := FindResource("extensibleattributedef")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "extensibleattributedef")
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
		t.Errorf("request-scope field count = %d, want 1 (descendantsAction)", req)
	}
	if resp != 2 {
		t.Errorf("response-scope field count = %d, want 2 (ref, namespace)", resp)
	}
	if both != 9 {
		t.Errorf("both-scope field count = %d, want 9", both)
	}
}

// TestExtensibleAttributeDefImmutableFields verifies type, min, and max are
// cataloged as Immutable, and that name/comment/defaultValue are not (the
// live-verified correction over the Phase 1 inventory, which listed every
// field as immutable due to the SDK wrapper's missing Update method).
func TestExtensibleAttributeDefImmutableFields(t *testing.T) {
	rd, ok := FindResource("extensibleattributedef")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "extensibleattributedef")
	}

	wantImmutable := map[string]bool{
		"Type":         true,
		"Min":          true,
		"Max":          true,
		"Name":         false,
		"Comment":      false,
		"DefaultValue": false,
		"Flags":        false,
	}

	seen := map[string]bool{}
	for _, f := range rd.Fields {
		want, ok := wantImmutable[f.Name]
		if !ok {
			continue
		}
		seen[f.Name] = true
		if f.Immutable != want {
			t.Errorf("%s: Immutable = %v, want %v", f.Name, f.Immutable, want)
		}
	}
	for name := range wantImmutable {
		if !seen[name] {
			t.Errorf("ExtensibleAttributeDef descriptor has no %s field", name)
		}
	}
}

// TestExtensibleAttributeDefDescendantsActionWriteOnly verifies
// descendantsAction is cataloged as request-only and excluded from the
// AtProvider full mirror (the WAPI field schema marks it not readable).
func TestExtensibleAttributeDefDescendantsActionWriteOnly(t *testing.T) {
	rd, ok := FindResource("extensibleattributedef")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "extensibleattributedef")
	}

	found := false
	for _, f := range rd.Fields {
		if f.Name != "DescendantsAction" {
			continue
		}
		found = true
		if f.Scope != FieldScopeRequest {
			t.Errorf("DescendantsAction: Scope = %v, want FieldScopeRequest", f.Scope)
		}
		if !f.OmitFromObservation {
			t.Errorf("DescendantsAction: expected OmitFromObservation=true")
		}
	}
	if !found {
		t.Errorf("ExtensibleAttributeDef descriptor has no DescendantsAction field")
	}
}

// TestExtensibleAttributeDefTypeEnum verifies the type field carries the
// six documented WAPI extensible-attribute data types.
func TestExtensibleAttributeDefTypeEnum(t *testing.T) {
	rd, ok := FindResource("extensibleattributedef")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "extensibleattributedef")
	}

	wantEnum := []string{"STRING", "INTEGER", "ENUM", "DATE", "EMAIL", "URL"}

	found := false
	for _, f := range rd.Fields {
		if f.Name != "Type" {
			continue
		}
		found = true
		if len(f.Enum) != len(wantEnum) {
			t.Fatalf("Type: Enum = %v, want %v", f.Enum, wantEnum)
		}
		for i, v := range wantEnum {
			if f.Enum[i] != v {
				t.Errorf("Type: Enum[%d] = %q, want %q", i, f.Enum[i], v)
			}
		}
	}
	if !found {
		t.Errorf("ExtensibleAttributeDef descriptor has no Type field")
	}
}

// TestExtensibleAttributeDefListValuesNestedType verifies the ListValues
// field references the EADefListValue nested type, and that the nested
// type is cataloged with a single required Value field.
func TestExtensibleAttributeDefListValuesNestedType(t *testing.T) {
	rd, ok := FindResource("extensibleattributedef")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "extensibleattributedef")
	}

	foundField := false
	for _, f := range rd.Fields {
		if f.Name != "ListValues" {
			continue
		}
		foundField = true
		if f.GoType != "[]EADefListValue" {
			t.Errorf("ListValues: GoType = %q, want []EADefListValue", f.GoType)
		}
		if f.Scope != FieldScopeBoth {
			t.Errorf("ListValues: Scope = %v, want FieldScopeBoth", f.Scope)
		}
	}
	if !foundField {
		t.Errorf("ExtensibleAttributeDef descriptor has no ListValues field")
	}

	foundType := false
	for _, nt := range rd.NestedTypes {
		if nt.TypeName != "EADefListValue" {
			continue
		}
		foundType = true
		if len(nt.Fields) != 1 || nt.Fields[0].Name != "Value" {
			t.Errorf("EADefListValue fields = %v, want a single Value field", nt.Fields)
		}
	}
	if !foundType {
		t.Errorf("ExtensibleAttributeDef descriptor has no EADefListValue nested type")
	}
}
