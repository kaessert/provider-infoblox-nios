package generator

import (
	"strings"
	"testing"

	"github.com/crossplane-contrib/provider-infoblox-nios/tools/openapi2crd/pkg/catalog"
)

// TestZeroFieldResource verifies the generator does not panic or produce
// malformed Go source for a resource descriptor with no fields at all (a
// degenerate but structurally valid catalog entry — e.g. a placeholder
// resource added before its field list has been populated).
func TestZeroFieldResource(t *testing.T) {
	rd := catalog.ResourceDescriptor{
		Kind:                 "EmptyThing",
		Slug:                 "emptything",
		ClusterGroup:         "emptything.infobloxnios.crossplane.io",
		NamespacedGroup:      "emptything.infobloxnios.m.crossplane.io",
		ExternalNameStrategy: catalog.StrategyServerAssigned,
		// Fields and NestedTypes both nil.
	}

	for _, isCluster := range []bool{true, false} {
		src, err := RenderScopeTypes(BuildScopeData(rd, isCluster))
		if err != nil {
			t.Fatalf("RenderScopeTypes(isCluster=%v) for zero-field resource: %v", isCluster, err)
		}
		s := string(src)
		if !strings.Contains(s, "type EmptyThingParameters struct {") {
			t.Errorf("expected EmptyThingParameters struct in output, got:\n%s", s)
		}
		if !strings.Contains(s, "type EmptyThingObservation struct {") {
			t.Errorf("expected EmptyThingObservation struct in output, got:\n%s", s)
		}
	}

	if _, err := RenderCommonReference(BuildFieldSetData(rd, true)); err != nil {
		t.Fatalf("RenderCommonReference for zero-field resource: %v", err)
	}
}

// TestNestedTypeWithZeroFields verifies the generator handles a
// NestedTypeDef with an empty Fields slice (a degenerate catalog entry)
// without panicking or producing invalid Go.
func TestNestedTypeWithZeroFields(t *testing.T) {
	rd := catalog.ResourceDescriptor{
		Kind:                 "Thing",
		Slug:                 "thing",
		ClusterGroup:         "thing.infobloxnios.crossplane.io",
		NamespacedGroup:      "thing.infobloxnios.m.crossplane.io",
		ExternalNameStrategy: catalog.StrategyServerAssigned,
		Fields: []catalog.FieldDef{
			{
				Name:        "Detail",
				JSONName:    "detail",
				GoType:      "*ThingDetail",
				Scope:       catalog.FieldScopeResponse,
				Description: "opaque detail blob.",
			},
		},
		NestedTypes: []catalog.NestedTypeDef{
			{
				TypeName:    "ThingDetail",
				Description: "is currently empty.",
				Fields:      nil,
			},
		},
	}

	src, err := RenderScopeTypes(BuildScopeData(rd, true))
	if err != nil {
		t.Fatalf("RenderScopeTypes with zero-field nested type: %v", err)
	}
	if !strings.Contains(string(src), "type ThingDetail struct {") {
		t.Errorf("expected ThingDetail struct in output, got:\n%s", src)
	}
}

// TestResourceWithNoImmutableFields verifies a resource with no Immutable
// fields at all renders with no CEL self==oldSelf rule anywhere in its
// output — the absence case for the immutability marker.
func TestResourceWithNoImmutableFields(t *testing.T) {
	rd := catalog.ResourceDescriptor{
		Kind:                 "Mutable",
		Slug:                 "mutable",
		ClusterGroup:         "mutable.infobloxnios.crossplane.io",
		NamespacedGroup:      "mutable.infobloxnios.m.crossplane.io",
		ExternalNameStrategy: catalog.StrategyServerAssigned,
		Fields: []catalog.FieldDef{
			{Name: "Comment", JSONName: "comment", GoType: "*string", Scope: catalog.FieldScopeBoth, Description: "a comment."},
		},
	}

	src, err := RenderScopeTypes(BuildScopeData(rd, true))
	if err != nil {
		t.Fatalf("RenderScopeTypes: %v", err)
	}
	if strings.Contains(string(src), "self == oldSelf") {
		t.Errorf("resource with no Immutable fields must not contain a CEL self==oldSelf rule, got:\n%s", src)
	}
}

// TestFindResourceUnknownSlug verifies FindResource reports (zero,false)
// for a slug not present in the catalog rather than panicking.
func TestFindResourceUnknownSlug(t *testing.T) {
	if _, ok := catalog.FindResource("does-not-exist"); ok {
		t.Errorf("expected FindResource to report false for an unknown slug")
	}
}
