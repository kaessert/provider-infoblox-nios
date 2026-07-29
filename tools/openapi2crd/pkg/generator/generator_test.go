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

// networkContainerDescriptor returns the NetworkContainer catalog entry for
// use in tests. NetworkContainer is the first cataloged resource with a
// cross-resource reference field (networkView -> NetworkView), so it
// exercises the reference three-field pattern end-to-end.
func networkContainerDescriptor(t *testing.T) catalog.ResourceDescriptor {
	t.Helper()
	rd, ok := catalog.FindResource("networkcontainer")
	if !ok {
		t.Fatalf("catalog.FindResource(%q): not found", "networkcontainer")
	}
	return *rd
}

// TestReferenceFieldRendersThreeFieldPattern verifies that a catalog field
// with a Reference descriptor (NetworkContainer's networkView) renders the
// full three-field cross-resource reference pattern: the value field with
// its +crossplane:generate:reference:type marker, plus companion Ref and
// Selector fields, in both cluster and namespaced scopes.
func TestReferenceFieldRendersThreeFieldPattern(t *testing.T) {
	rd := networkContainerDescriptor(t)

	clusterSrc, err := RenderScopeTypes(BuildScopeData(rd, true))
	if err != nil {
		t.Fatalf("RenderScopeTypes(cluster): %v", err)
	}
	cs := string(clusterSrc)

	if !strings.Contains(cs, "// +crossplane:generate:reference:type=github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/networkview/v1alpha1.NetworkView") {
		t.Errorf("expected cluster reference type marker targeting NetworkView, got:\n%s", cs)
	}
	if !strings.Contains(cs, "NetworkViewRef *xpv1.Reference") {
		t.Errorf("expected cluster NetworkViewRef *xpv1.Reference field, got:\n%s", cs)
	}
	if !strings.Contains(cs, "NetworkViewSelector *xpv1.Selector") {
		t.Errorf("expected cluster NetworkViewSelector *xpv1.Selector field, got:\n%s", cs)
	}

	namespacedSrc, err := RenderScopeTypes(BuildScopeData(rd, false))
	if err != nil {
		t.Fatalf("RenderScopeTypes(namespaced): %v", err)
	}
	ns := string(namespacedSrc)

	if !strings.Contains(ns, "NetworkViewRef *xpv1.NamespacedReference") {
		t.Errorf("expected namespaced NetworkViewRef *xpv1.NamespacedReference field, got:\n%s", ns)
	}
	if !strings.Contains(ns, "NetworkViewSelector *xpv1.NamespacedSelector") {
		t.Errorf("expected namespaced NetworkViewSelector *xpv1.NamespacedSelector field, got:\n%s", ns)
	}

	// The value field itself must also carry the immutability CEL rule
	// (network_view is immutable — absent from UpdateNetworkContainer).
	if !strings.Contains(cs, `message="networkView is immutable after creation"`) {
		t.Errorf("expected CEL immutability rule for networkView field, got:\n%s", cs)
	}
}

// TestSafeGoPackageName verifies keyword collisions get a "pkg" suffix and
// ordinary slugs pass through unchanged.
func TestSafeGoPackageName(t *testing.T) {
	cases := []struct {
		slug string
		want string
	}{
		{slug: "range", want: "rangepkg"}, // Go reserved keyword
		{slug: "recorda", want: "recorda"},
		{slug: "networkview", want: "networkview"},
		{slug: "type", want: "typepkg"}, // Go reserved keyword
	}
	for _, tc := range cases {
		if got := safeGoPackageName(tc.slug); got != tc.want {
			t.Errorf("safeGoPackageName(%q) = %q, want %q", tc.slug, got, tc.want)
		}
	}
}

// TestRangeCommonReferencePackageNameIsValidGo is a regression guard for the
// "range" slug collision with the Go reserved keyword: the standalone
// apis/common/<package-name> reference copy must declare a valid Go package
// identifier ("rangepkg", not "range") so it — and, critically, angryjet's
// jennifer-based code generator, which derives the `package` clause it
// writes from the LAST PATH SEGMENT of the import path rather than the
// file's actual `package` declaration — never emits an uncompilable
// `package range`.
func TestRangeCommonReferencePackageNameIsValidGo(t *testing.T) {
	rd, ok := catalog.FindResource("range")
	if !ok {
		t.Fatalf("catalog.FindResource(%q): not found", "range")
	}

	data := BuildFieldSetData(*rd, true)
	if data.PackageName != "rangepkg" {
		t.Fatalf("BuildFieldSetData(Range).PackageName = %q, want %q", data.PackageName, "rangepkg")
	}

	src, err := RenderCommonReference(data)
	if err != nil {
		t.Fatalf("RenderCommonReference(Range): %v", err)
	}
	if !strings.Contains(string(src), "package rangepkg") {
		t.Errorf("expected rendered source to declare `package rangepkg`, got:\n%s", src)
	}
}
