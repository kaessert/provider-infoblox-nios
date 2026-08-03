package generator

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

// updateGolden regenerates the golden files in testdata/golden/ from the
// current generator output. Run:
//
//	go test ./tools/openapi2crd/... -run TestGolden -update-golden
var updateGolden = flag.Bool("update-golden", false, "update golden files for generator output")

// goldenPath resolves a golden file name relative to testdata/golden/.
func goldenPath(name string) string {
	return filepath.Join("testdata", "golden", name)
}

// compareGolden either (re)writes the golden file at testdata/golden/<name>
// (when -update-golden is passed) or asserts got is byte-identical to the
// committed golden file.
func compareGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := goldenPath(name)

	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatalf("creating golden dir for %s: %v", path, err)
		}
		if err := os.WriteFile(path, got, 0o600); err != nil {
			t.Fatalf("writing golden file %s: %v", path, err)
		}
		return
	}

	want, err := os.ReadFile(path) //nolint:gosec // test-only, path is a fixed testdata literal
	if err != nil {
		t.Fatalf("reading golden file %s (run with -update-golden to create it): %v", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("generated output does not match golden file %s (run `go test ./tools/openapi2crd/... -update-golden` to regenerate if this change is intentional)\n--- got ---\n%s\n--- want ---\n%s", path, got, want)
	}
}

// TestGoldenCommonReferenceARecord pins the exact rendered output of
// apis/common/recorda/recorda_types.go — the standalone reference copy of
// the ARecord Parameters/Observation/nested types (struct tags, CEL
// immutability annotations, field types, doc comments).
func TestGoldenCommonReferenceARecord(t *testing.T) {
	rd := aRecordDescriptor(t)
	src, err := RenderCommonReference(BuildFieldSetData(rd, true))
	if err != nil {
		t.Fatalf("RenderCommonReference: %v", err)
	}
	compareGolden(t, "recorda_common_types.go.golden", src)
}

// TestGoldenScopeTypesARecordCluster pins the exact rendered output of the
// cluster-scoped recorda_types.go (embeds xpv2.ClusterManagedResourceSpec,
// categories {crossplane,managed,infobloxnios}).
func TestGoldenScopeTypesARecordCluster(t *testing.T) {
	rd := aRecordDescriptor(t)
	src, err := RenderScopeTypes(BuildScopeData(rd, true))
	if err != nil {
		t.Fatalf("RenderScopeTypes(cluster): %v", err)
	}
	compareGolden(t, "recorda_cluster_types.go.golden", src)
}

// TestGoldenScopeTypesARecordNamespaced pins the exact rendered output of
// the namespaced recorda_types.go (embeds xpv2.ManagedResourceSpec).
func TestGoldenScopeTypesARecordNamespaced(t *testing.T) {
	rd := aRecordDescriptor(t)
	src, err := RenderScopeTypes(BuildScopeData(rd, false))
	if err != nil {
		t.Fatalf("RenderScopeTypes(namespaced): %v", err)
	}
	compareGolden(t, "recorda_namespaced_types.go.golden", src)
}

// TestGoldenRegisterARecordCluster pins register.go for the cluster scope.
func TestGoldenRegisterARecordCluster(t *testing.T) {
	rd := aRecordDescriptor(t)
	src, err := renderRegister(BuildScopeData(rd, true))
	if err != nil {
		t.Fatalf("renderRegister(cluster): %v", err)
	}
	compareGolden(t, "recorda_cluster_register.go.golden", src)
}

// TestGoldenRegisterARecordNamespaced pins register.go for the namespaced
// scope (different Group — the ".m." infix).
func TestGoldenRegisterARecordNamespaced(t *testing.T) {
	rd := aRecordDescriptor(t)
	src, err := renderRegister(BuildScopeData(rd, false))
	if err != nil {
		t.Fatalf("renderRegister(namespaced): %v", err)
	}
	compareGolden(t, "recorda_namespaced_register.go.golden", src)
}
