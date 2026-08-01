package main

import (
	"os"
	"path/filepath"
	"testing"
)

// Field JSON names and validation statuses reused across the table-driven
// cases below. Declaring them once keeps the package's total literal
// occurrence count under the repo's goconst threshold (min-occurrences: 5)
// instead of re-typing the same status/field-name strings validator.go
// already emits.
const (
	fieldName              = "name"
	fieldCanonical         = "canonical"
	fieldCanonicalRef      = "canonicalRef"
	fieldCanonicalSelector = "canonicalSelector"
	fieldComment           = "comment"
	fieldView              = "view"

	statusTested      = "tested"
	statusSkipped     = "skipped"
	statusImmutable   = "immutable"
	statusRefPlumbing = "reference-plumbing"
	statusMissing     = "MISSING"
)

// TestIsReferencePlumbingField covers the *Ref/*Selector exclusion logic:
// a field is only treated as generated reference-plumbing when its
// same-prefixed base value field is present in the struct's field set.
func TestIsReferencePlumbingField(t *testing.T) {
	cases := map[string]struct {
		reason   string
		jsonName string
		fieldSet map[string]bool
		want     bool
	}{
		"RefWithBaseField": {
			reason:   "canonicalRef is reference plumbing when canonical exists",
			jsonName: fieldCanonicalRef,
			fieldSet: map[string]bool{fieldCanonical: true, fieldCanonicalRef: true},
			want:     true,
		},
		"SelectorWithBaseField": {
			reason:   "canonicalSelector is reference plumbing when canonical exists",
			jsonName: fieldCanonicalSelector,
			fieldSet: map[string]bool{fieldCanonical: true, fieldCanonicalSelector: true},
			want:     true,
		},
		"RefWithoutBaseField": {
			reason:   "a *Ref field with no matching base value field is not excused — it must be reported MISSING",
			jsonName: "danglingRef",
			fieldSet: map[string]bool{"danglingRef": true},
			want:     false,
		},
		"SelectorWithoutBaseField": {
			reason:   "a *Selector field with no matching base value field is not excused",
			jsonName: "danglingSelector",
			fieldSet: map[string]bool{"danglingSelector": true},
			want:     false,
		},
		"PlainFieldNoSuffix": {
			reason:   "a field with no Ref/Selector suffix is never reference plumbing",
			jsonName: fieldComment,
			fieldSet: map[string]bool{fieldComment: true},
			want:     false,
		},
		"BareRefSuffixNoBase": {
			reason:   "a field literally named 'Ref' has an empty base and must not match",
			jsonName: "ref",
			fieldSet: map[string]bool{"ref": true, "": true},
			want:     false,
		},
		"BareSelectorSuffixNoBase": {
			reason:   "a field literally named 'Selector' has an empty base and must not match",
			jsonName: "selector",
			fieldSet: map[string]bool{"selector": true, "": true},
			want:     false,
		},
		"CaseSensitiveSuffix": {
			reason:   "suffix matching is case-sensitive — 'ref' (lowercase) is not the 'Ref' suffix",
			jsonName: "canonicalref",
			fieldSet: map[string]bool{fieldCanonical: true, "canonicalref": true},
			want:     false,
		},
		"RefSuffixBaseMissingFromSet": {
			reason:   "the base field name is derived correctly but must actually be present in fieldSet",
			jsonName: "ownerRef",
			fieldSet: map[string]bool{"ownerRef": true, "somethingElse": true},
			want:     false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := isReferencePlumbingField(tc.jsonName, tc.fieldSet)
			if got != tc.want {
				t.Errorf("%s: isReferencePlumbingField(%q, %v) = %v, want %v",
					tc.reason, tc.jsonName, tc.fieldSet, got, tc.want)
			}
		})
	}
}

// TestValidateManifestReferencePlumbingExclusion is the regression test for
// the false-positive this fix closes: canonicalRef/canonicalSelector must no
// longer be reported MISSING when only the base canonical field is covered
// by the update-test annotation.
func TestValidateManifestReferencePlumbingExclusion(t *testing.T) {
	fields := []FieldInfo{
		{GoName: "Name", JSONName: fieldName},
		{GoName: "Canonical", JSONName: fieldCanonical},
		{GoName: "CanonicalRef", JSONName: fieldCanonicalRef},
		{GoName: "CanonicalSelector", JSONName: fieldCanonicalSelector},
		{GoName: "Comment", JSONName: fieldComment},
		{GoName: "View", JSONName: fieldView, Immutable: true},
	}

	m := &Manifest{
		Kind: "CNAMERecord",
		Tests: []UpdateTest{
			{Field: fieldName, Skip: "renaming changes external identity"},
			{Field: fieldCanonical, Value: "updated-target.example.com"},
			{Field: fieldComment, Value: "Updated by uptest"},
		},
	}

	result := ValidateManifest(m, fields)

	if !result.AllGood {
		t.Fatalf("expected AllGood=true (canonicalRef/canonicalSelector should be excluded, not MISSING); got fields: %+v", result.Fields)
	}

	statusByName := statusMap(result)

	want := map[string]string{
		fieldName:              statusSkipped,
		fieldCanonical:         statusTested,
		fieldCanonicalRef:      statusRefPlumbing,
		fieldCanonicalSelector: statusRefPlumbing,
		fieldComment:           statusTested,
		fieldView:              statusImmutable,
	}
	for field, wantStatus := range want {
		if got := statusByName[field]; got != wantStatus {
			t.Errorf("field %q: status = %q, want %q", field, got, wantStatus)
		}
	}
}

// TestValidateManifestGenuinelyMissingBaseField ensures the exclusion is
// conditional: if the base value field itself is dropped from the struct
// (renamed, removed) — not merely uncovered by the annotation — the Ref/
// Selector pair has no base field in fieldSet and must be scoped normally,
// i.e. reported MISSING like any other uncovered field rather than silently
// excused.
func TestValidateManifestGenuinelyMissingBaseField(t *testing.T) {
	fields := []FieldInfo{
		{GoName: "CanonicalRef", JSONName: fieldCanonicalRef},
		{GoName: "CanonicalSelector", JSONName: fieldCanonicalSelector},
	}

	m := &Manifest{Kind: "CNAMERecord"}

	result := ValidateManifest(m, fields)

	if result.AllGood {
		t.Fatalf("expected AllGood=false — canonicalRef/canonicalSelector have no matching base field (canonical) in the struct, so they must not be excused as reference plumbing")
	}

	for _, f := range result.Fields {
		if f.Status != statusMissing {
			t.Errorf("field %q: status = %q, want %s (no base value field present)", f.JSONName, f.Status, statusMissing)
		}
	}
}

// TestValidateManifestMissingField confirms a genuinely uncovered mutable
// field (no reference-suffix, no annotation entry) still fails validation.
func TestValidateManifestMissingField(t *testing.T) {
	fields := []FieldInfo{
		{GoName: "Comment", JSONName: fieldComment},
	}
	m := &Manifest{Kind: "Widget"}

	result := ValidateManifest(m, fields)

	if result.AllGood {
		t.Fatal("expected AllGood=false for an uncovered mutable field")
	}
	if len(result.Fields) != 1 || result.Fields[0].Status != statusMissing {
		t.Errorf("got fields %+v, want a single %s comment field", result.Fields, statusMissing)
	}
}

// TestParseGoTypes verifies the Go-source struct scanner against a small
// synthetic types file containing the three-field reference pattern, an
// immutable field, and a sibling *Parameters struct that must be skipped.
func TestParseGoTypes(t *testing.T) {
	src := `// Code generated by openapi2crd. DO NOT EDIT.
package v1alpha1

// OtherParameters is a decoy struct that must be skipped while scanning
// for WidgetParameters.
type OtherParameters struct {
	Unrelated *string ` + "`json:\"unrelated\"`" + `
}

// WidgetParameters are the configurable fields of a Widget.
type WidgetParameters struct {
	Name *string ` + "`json:\"name\"`" + `
	// +crossplane:generate:reference:type=github.com/example/apis/v1alpha1.Owner
	Owner *string ` + "`json:\"owner\"`" + `
	// +optional
	OwnerRef *xpv1.Reference ` + "`json:\"ownerRef,omitempty\"`" + `
	// +optional
	OwnerSelector *xpv1.Selector ` + "`json:\"ownerSelector,omitempty\"`" + `
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="region is immutable after creation"
	Region *string ` + "`json:\"region\"`" + `
}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "widget_types.go")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatalf("writing fixture: %v", err)
	}

	fields, err := ParseGoTypes(path, "Widget")
	if err != nil {
		t.Fatalf("ParseGoTypes: %v", err)
	}

	want := []FieldInfo{
		{GoName: "Name", JSONName: fieldName},
		{GoName: "Owner", JSONName: "owner"},
		{GoName: "OwnerRef", JSONName: "ownerRef"},
		{GoName: "OwnerSelector", JSONName: "ownerSelector"},
		{GoName: "Region", JSONName: "region", Immutable: true},
	}
	if len(fields) != len(want) {
		t.Fatalf("got %d fields, want %d: %+v", len(fields), len(want), fields)
	}
	for i, w := range want {
		if fields[i] != w {
			t.Errorf("field %d: got %+v, want %+v", i, fields[i], w)
		}
	}
}

// TestParseGoTypesStructNotFound confirms a clear error when the target
// Parameters struct doesn't exist in the file.
func TestParseGoTypesStructNotFound(t *testing.T) {
	src := `package v1alpha1

type OtherParameters struct {
	Name *string ` + "`json:\"name\"`" + `
}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "types.go")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatalf("writing fixture: %v", err)
	}

	if _, err := ParseGoTypes(path, "Widget"); err == nil {
		t.Fatal("expected an error when WidgetParameters is not present in the file")
	}
}

// TestParseGoTypesMissingFile confirms a clear error for a nonexistent path.
func TestParseGoTypesMissingFile(t *testing.T) {
	if _, err := ParseGoTypes(filepath.Join(t.TempDir(), "does-not-exist.go"), "Widget"); err == nil {
		t.Fatal("expected an error for a nonexistent types file")
	}
}

// TestParseGoTypesRealFixture exercises the parser + validator + exclusion
// logic end to end against the real generated CNAMERecord types file and
// example manifest, matching the exact reproduction case from the defect
// report this fix closes.
func TestParseGoTypesRealFixture(t *testing.T) {
	typesPath := filepath.Join("..", "..", "apis", "cluster", "recordcname", "v1alpha1", "recordcname_types.go")
	if _, err := os.Stat(typesPath); err != nil {
		t.Skipf("real fixture not available in this checkout: %v", err)
	}
	manifestPath := filepath.Join("..", "..", "examples", "record-cname", "record-cname.yaml")
	if _, err := os.Stat(manifestPath); err != nil {
		t.Skipf("real fixture not available in this checkout: %v", err)
	}

	m, err := ParseManifest(manifestPath)
	if err != nil {
		t.Fatalf("ParseManifest: %v", err)
	}

	fields, err := ParseGoTypes(typesPath, m.Kind)
	if err != nil {
		t.Fatalf("ParseGoTypes: %v", err)
	}

	result := ValidateManifest(m, fields)
	if !result.AllGood {
		t.Fatalf("expected the record-cname manifest to fully cover CNAMERecordParameters (canonicalRef/canonicalSelector excluded as reference plumbing); got: %+v", result.Fields)
	}

	statusByName := statusMap(result)
	for _, refField := range []string{fieldCanonicalRef, fieldCanonicalSelector} {
		if got := statusByName[refField]; got != statusRefPlumbing {
			t.Errorf("field %q: status = %q, want %s", refField, got, statusRefPlumbing)
		}
	}
}

// TestPrintValidationDoesNotPanic is a smoke test for the output formatter
// across every known status value, including the reference-plumbing status
// this fix introduces.
func TestPrintValidationDoesNotPanic(t *testing.T) {
	r := &ValidationResult{
		Kind: "Widget",
		Fields: []FieldValidation{
			{JSONName: statusTested, Status: statusTested},
			{JSONName: statusSkipped, Status: statusSkipped},
			{JSONName: statusImmutable, Status: statusImmutable},
			{JSONName: "refField", Status: statusRefPlumbing},
			{JSONName: "missing", Status: statusMissing},
		},
		AllGood: false,
	}
	PrintValidation(r)
}

// statusMap builds a JSONName → Status lookup from a ValidationResult, used
// by several tests above to assert per-field status without depending on
// slice ordering.
func statusMap(r *ValidationResult) map[string]string {
	m := make(map[string]string, len(r.Fields))
	for _, f := range r.Fields {
		m[f.JSONName] = f.Status
	}
	return m
}
