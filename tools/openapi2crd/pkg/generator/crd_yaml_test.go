package generator

import (
	"os"
	"path/filepath"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

// crdYAMLPath resolves a package/crds/<file>.yaml path relative to this
// package. The generator itself only renders Go types (apis/common,
// apis/cluster, apis/namespaced); the checked-in CRD YAML under
// package/crds/ is produced downstream by `make generate` (controller-gen)
// from those types. These tests pin that final, committed artifact so a
// template change that silently breaks CRD YAML generation (invalid
// markers, wrong group, dropped CEL rule) is caught here rather than only
// discovered at `make reviewable` or apply time.
func crdYAMLPath(t *testing.T, file string) string {
	t.Helper()
	// tools/openapi2crd/pkg/generator -> provider root is four levels up.
	path := filepath.Join("..", "..", "..", "..", "package", "crds", file)
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("resolving %s: %v", path, err)
	}
	return abs
}

// loadCRD reads and parses a CustomResourceDefinition manifest from
// package/crds/.
func loadCRD(t *testing.T, file string) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	path := crdYAMLPath(t, file)
	data, err := os.ReadFile(path) //nolint:gosec // test-only, path built from a fixed testdata literal
	if err != nil {
		t.Fatalf("reading CRD yaml %s (run `make generate` if this file is missing): %v", path, err)
	}

	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.UnmarshalStrict(data, &crd); err != nil {
		t.Fatalf("parsing CRD yaml %s: %v", path, err)
	}
	return &crd
}

// TestCRDYAMLClusterARecordIsValid verifies the checked-in cluster-scoped
// ARecord CRD YAML parses as a valid CustomResourceDefinition and carries
// the expected group, scope, categories, and immutability CEL rule.
func TestCRDYAMLClusterARecordIsValid(t *testing.T) {
	crd := loadCRD(t, "recorda.infobloxnios.crossplane.io_arecords.yaml")

	if crd.APIVersion != "apiextensions.k8s.io/v1" {
		t.Errorf("apiVersion = %q, want apiextensions.k8s.io/v1", crd.APIVersion)
	}
	if crd.Kind != "CustomResourceDefinition" {
		t.Errorf("kind = %q, want CustomResourceDefinition", crd.Kind)
	}
	if crd.Spec.Group != "recorda.infobloxnios.crossplane.io" {
		t.Errorf("spec.group = %q, want recorda.infobloxnios.crossplane.io", crd.Spec.Group)
	}
	if crd.Spec.Scope != apiextensionsv1.ClusterScoped {
		t.Errorf("spec.scope = %q, want Cluster", crd.Spec.Scope)
	}
	if crd.Spec.Names.Kind != "ARecord" {
		t.Errorf("spec.names.kind = %q, want ARecord", crd.Spec.Names.Kind)
	}

	assertCategories(t, crd.Spec.Names.Categories)
	assertImmutableViewCELRule(t, crd)
}

// TestCRDYAMLNamespacedARecordIsValid verifies the checked-in namespaced
// ARecord CRD YAML parses as a valid CustomResourceDefinition and carries
// the ".m." infixed group, Namespaced scope, categories, and immutability
// CEL rule.
func TestCRDYAMLNamespacedARecordIsValid(t *testing.T) {
	crd := loadCRD(t, "recorda.infobloxnios.m.crossplane.io_arecords.yaml")

	if crd.APIVersion != "apiextensions.k8s.io/v1" {
		t.Errorf("apiVersion = %q, want apiextensions.k8s.io/v1", crd.APIVersion)
	}
	if crd.Spec.Group != "recorda.infobloxnios.m.crossplane.io" {
		t.Errorf("spec.group = %q, want recorda.infobloxnios.m.crossplane.io", crd.Spec.Group)
	}
	if crd.Spec.Scope != apiextensionsv1.NamespaceScoped {
		t.Errorf("spec.scope = %q, want Namespaced", crd.Spec.Scope)
	}
	if crd.Spec.Names.Kind != "ARecord" {
		t.Errorf("spec.names.kind = %q, want ARecord", crd.Spec.Names.Kind)
	}

	assertCategories(t, crd.Spec.Names.Categories)
	assertImmutableViewCELRule(t, crd)
}

// assertCategories verifies the CRD carries exactly the
// {crossplane,managed,infobloxnios} category set (NOT the ProviderConfig
// {crossplane,provider,...} set).
func assertCategories(t *testing.T, categories []string) {
	t.Helper()
	want := map[string]bool{"crossplane": true, "managed": true, "infobloxnios": true}
	if len(categories) != len(want) {
		t.Fatalf("categories = %v, want exactly %v", categories, want)
	}
	for _, c := range categories {
		if !want[c] {
			t.Errorf("unexpected category %q in %v", c, categories)
		}
	}
}

// assertImmutableViewCELRule walks every served version's schema for the
// spec.forProvider.view property and asserts it carries the
// self==oldSelf CEL immutability rule with the expected message.
func assertImmutableViewCELRule(t *testing.T, crd *apiextensionsv1.CustomResourceDefinition) {
	t.Helper()
	for _, v := range crd.Spec.Versions {
		if v.Schema == nil || v.Schema.OpenAPIV3Schema == nil {
			t.Fatalf("version %s: missing openAPIV3Schema", v.Name)
		}
		specProps := v.Schema.OpenAPIV3Schema.Properties["spec"].Properties
		forProvider, ok := specProps["forProvider"]
		if !ok {
			t.Fatalf("version %s: spec.forProvider not found in schema", v.Name)
		}
		view, ok := forProvider.Properties["view"]
		if !ok {
			t.Fatalf("version %s: spec.forProvider.view not found in schema", v.Name)
		}
		if len(view.XValidations) == 0 {
			t.Fatalf("version %s: spec.forProvider.view has no x-kubernetes-validations rules", v.Name)
		}
		found := false
		for _, rule := range view.XValidations {
			if rule.Rule == "self == oldSelf" && rule.Message == "view is immutable after creation" {
				found = true
			}
		}
		if !found {
			t.Errorf("version %s: spec.forProvider.view is missing the self==oldSelf immutability rule, got %+v", v.Name, view.XValidations)
		}
	}
}
