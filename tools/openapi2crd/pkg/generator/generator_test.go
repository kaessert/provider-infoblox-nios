package generator

import (
	"strings"
	"testing"

	"github.com/crossplane-contrib/provider-infoblox-nios/tools/openapi2crd/pkg/catalog"
)

// aRecordDescriptor returns the ARecord catalog entry for use in tests.
func aRecordDescriptor(t *testing.T) catalog.ResourceDescriptor {
	t.Helper()
	rd, ok := catalog.FindResource("recorda")
	if !ok {
		t.Fatalf("catalog.FindResource(%q): not found", "recorda")
	}
	return *rd
}

// TestBuildFieldSetDataFieldScopeMapping verifies the field-scope mapping
// contract: FieldScopeRequest -> ForProvider only, FieldScopeResponse ->
// AtProvider only, FieldScopeBoth -> both.
func TestBuildFieldSetDataFieldScopeMapping(t *testing.T) {
	rd := aRecordDescriptor(t)
	data := BuildFieldSetData(rd, true)

	forProviderNames := fieldNames(data.ForProvider)
	atProviderNames := fieldNames(data.AtProvider)

	// "View" is FieldScopeBoth: must be in both.
	if !forProviderNames["View"] {
		t.Errorf("expected ForProvider to contain View")
	}
	if !atProviderNames["View"] {
		t.Errorf("expected AtProvider to contain View (full-mirror)")
	}

	// "Zone" is FieldScopeResponse: AtProvider only, never ForProvider.
	if forProviderNames["Zone"] {
		t.Errorf("Zone is FieldScopeResponse and must NOT appear in ForProvider")
	}
	if !atProviderNames["Zone"] {
		t.Errorf("expected AtProvider to contain Zone")
	}

	// "RemoveAssociatedPtr" is FieldScopeRequest with OmitFromObservation:
	// ForProvider only, excluded from AtProvider by design.
	if !forProviderNames["RemoveAssociatedPtr"] {
		t.Errorf("expected ForProvider to contain RemoveAssociatedPtr")
	}
	if atProviderNames["RemoveAssociatedPtr"] {
		t.Errorf("RemoveAssociatedPtr has OmitFromObservation=true and must NOT appear in AtProvider")
	}

	// Every ForProvider field except OmitFromObservation ones must also be
	// in AtProvider (full-mirror invariant, convention governing Observe-
	// mode import).
	for _, f := range rd.Fields {
		if f.Scope == catalog.FieldScopeResponse {
			continue
		}
		if f.OmitFromObservation {
			continue
		}
		if !atProviderNames[f.Name] {
			t.Errorf("ForProvider field %q is missing from AtProvider (full-mirror violation)", f.Name)
		}
	}
}

// TestNoOmitEmptyOnSliceOrMapFields verifies that map (and slice) fields
// never carry omitempty, in ForProvider, AtProvider, or nested types — the
// convention that keeps CEL `self == oldSelf` immutability rules from
// seeing a spurious null diff on round-trip.
func TestNoOmitEmptyOnSliceOrMapFields(t *testing.T) {
	rd := aRecordDescriptor(t)
	for _, isCluster := range []bool{true, false} {
		data := BuildScopeData(rd, isCluster)
		checkNoOmitEmpty(t, "ForProvider", data.ForProvider)
		checkNoOmitEmpty(t, "AtProvider", data.AtProvider)
		for _, nt := range data.NestedTypes {
			checkNoOmitEmpty(t, "nested type "+nt.TypeName, nt.Fields)
		}
	}
}

func checkNoOmitEmpty(t *testing.T, label string, fields []FieldData) {
	t.Helper()
	for _, f := range fields {
		if (strings.HasPrefix(f.GoType, "[]") || strings.HasPrefix(f.GoType, "map[")) && f.OmitEmpty {
			t.Errorf("%s field %q (%s): slice/map fields must never have OmitEmpty=true", label, f.Name, f.GoType)
		}
	}
}

// TestImmutableFieldCarriesCELRule verifies that ARecord's "view" field
// (a ForProvider field marked Immutable in the catalog) renders the CEL
// self==oldSelf XValidation rule, that mutable fields (e.g. "comment") do
// not, and that a response-only Immutable field with no ForProvider
// representation ("zone") still carries the rule on its AtProvider
// (status) mirror.
func TestImmutableFieldCarriesCELRule(t *testing.T) {
	rd := aRecordDescriptor(t)
	src, err := RenderScopeTypes(BuildScopeData(rd, true))
	if err != nil {
		t.Fatalf("RenderScopeTypes: %v", err)
	}
	s := string(src)

	if !strings.Contains(s, `message="view is immutable after creation"`) {
		t.Errorf("expected CEL immutability rule for view field, got:\n%s", s)
	}
	if strings.Contains(s, `message="comment is immutable after creation"`) {
		t.Errorf("comment is mutable and must not carry a CEL immutability rule")
	}
	// zone is Immutable=true in the catalog but FieldScopeResponse (no
	// ForProvider representation — confirmed absent from the
	// CreateARecord SDK signature) — the CEL rule is still expected, just
	// on the AtProvider mirror field instead of a ForProvider field.
	if !strings.Contains(s, `message="zone is immutable after creation"`) {
		t.Errorf("zone has no ForProvider field but must still carry a CEL immutability rule on its AtProvider mirror, got:\n%s", s)
	}
}

// TestCRDCategories verifies the rendered scope types carry the
// {crossplane,managed,infobloxnios} categories marker (not
// {crossplane,provider,...}, which is reserved for ProviderConfig types).
func TestCRDCategories(t *testing.T) {
	rd := aRecordDescriptor(t)
	for _, isCluster := range []bool{true, false} {
		src, err := RenderScopeTypes(BuildScopeData(rd, isCluster))
		if err != nil {
			t.Fatalf("RenderScopeTypes(isCluster=%v): %v", isCluster, err)
		}
		if !strings.Contains(string(src), "categories={crossplane,managed,infobloxnios}") {
			t.Errorf("isCluster=%v: expected categories={crossplane,managed,infobloxnios}, got:\n%s", isCluster, src)
		}
	}
}

// TestDualScopeSpecEmbedding verifies the cluster variant embeds
// xpv1.ResourceSpec and the namespaced variant embeds
// xpv2.ManagedResourceSpec (convention governing dual-scope type
// embedding).
func TestDualScopeSpecEmbedding(t *testing.T) {
	rd := aRecordDescriptor(t)

	clusterSrc, err := RenderScopeTypes(BuildScopeData(rd, true))
	if err != nil {
		t.Fatalf("RenderScopeTypes(cluster): %v", err)
	}
	if !strings.Contains(string(clusterSrc), "xpv1.ResourceSpec") {
		t.Errorf("cluster-scoped ARecordSpec must embed xpv1.ResourceSpec")
	}
	if strings.Contains(string(clusterSrc), "xpv2.ManagedResourceSpec") {
		t.Errorf("cluster-scoped ARecordSpec must NOT embed xpv2.ManagedResourceSpec")
	}

	namespacedSrc, err := RenderScopeTypes(BuildScopeData(rd, false))
	if err != nil {
		t.Fatalf("RenderScopeTypes(namespaced): %v", err)
	}
	if !strings.Contains(string(namespacedSrc), "xpv2.ManagedResourceSpec") {
		t.Errorf("namespaced ARecordSpec must embed xpv2.ManagedResourceSpec")
	}
}

// TestGeneratedSourceIsValidGo is a structural safety net: every template
// output must already be gofmt-formatted valid Go (RenderScopeTypes/
// RenderCommonReference both run the result through go/format.Source, so a
// failure here indicates a template bug, not just a style nit).
func TestGeneratedSourceIsValidGo(t *testing.T) {
	rd := aRecordDescriptor(t)

	if _, err := RenderCommonReference(BuildFieldSetData(rd, true)); err != nil {
		t.Errorf("RenderCommonReference: %v", err)
	}
	for _, isCluster := range []bool{true, false} {
		if _, err := RenderScopeTypes(BuildScopeData(rd, isCluster)); err != nil {
			t.Errorf("RenderScopeTypes(isCluster=%v): %v", isCluster, err)
		}
	}
}

// TestRenderReferenceHelpers verifies the generic cross-resource reference
// extractor renders as valid, gofmt-formatted Go source in its own leaf
// package (referencehelpers, NOT the root apis package — see
// referenceHelpersTemplate's doc comment for why importing the root apis
// package back from a scope package's zz_generated.resolvers.go would be an
// import cycle).
func TestRenderReferenceHelpers(t *testing.T) {
	src, err := RenderReferenceHelpers()
	if err != nil {
		t.Fatalf("RenderReferenceHelpers: %v", err)
	}
	got := string(src)
	if !strings.Contains(got, "package referencehelpers") {
		t.Errorf("reference helpers source missing %q package declaration:\n%s", "referencehelpers", got)
	}
	if !strings.Contains(got, "func ExtractField(path string) reference.ExtractValueFn") {
		t.Errorf("reference helpers source missing ExtractField signature:\n%s", got)
	}
	if strings.Contains(got, "package apis\n") {
		t.Errorf("reference helpers source must NOT live in the root apis package (import cycle risk):\n%s", got)
	}
}

func fieldNames(fields []FieldData) map[string]bool {
	m := make(map[string]bool, len(fields))
	for _, f := range fields {
		m[f.Name] = true
	}
	return m
}
