package catalog

import (
	"testing"

	"github.com/crossplane-contrib/provider-infoblox-nios/tools/openapi/pkg/model"
)

func TestResourcesNoDuplicateSlugsOrKinds(t *testing.T) {
	slugs := map[string]bool{}
	kinds := map[string]bool{}
	for _, r := range Resources() {
		if slugs[r.Slug] {
			t.Errorf("duplicate slug %q", r.Slug)
		}
		slugs[r.Slug] = true
		if kinds[r.Kind] {
			t.Errorf("duplicate kind %q", r.Kind)
		}
		kinds[r.Kind] = true
	}
}

func TestResourcesHaveRequiredMetadata(t *testing.T) {
	for _, r := range Resources() {
		if r.Slug == "" {
			t.Errorf("resource with GoStructName %q has empty Slug", r.GoStructName)
		}
		if r.Kind == "" {
			t.Errorf("resource %q has empty Kind", r.Slug)
		}
		if r.GoStructName == "" {
			t.Errorf("resource %q has empty GoStructName", r.Slug)
		}
		if r.ExternalNameStrategy == "" {
			t.Errorf("resource %q has no ExternalNameStrategy", r.Slug)
		}
		if r.ExternalNameStrategy != model.StrategyServerAssigned {
			t.Errorf("resource %q has ExternalNameStrategy %q, want %q (every NIOS object is server-assigned)", r.Slug, r.ExternalNameStrategy, model.StrategyServerAssigned)
		}
		if r.ExternalNameRationale == "" {
			t.Errorf("resource %q has no ExternalNameRationale", r.Slug)
		}
		if r.DeleteBehavior == "" {
			t.Errorf("resource %q has no DeleteBehavior documented", r.Slug)
		}
		if len(r.CRUD) == 0 {
			t.Errorf("resource %q has no CRUD methods recorded", r.Slug)
		}
		if len(r.Fields) == 0 {
			t.Errorf("resource %q has no Fields recorded", r.Slug)
		}
		// ImmutableFields/MutableFields must be non-nil (explicitly evaluated),
		// even if empty, per the "none known" convention.
		if r.ImmutableFields == nil {
			t.Errorf("resource %q has nil ImmutableFields (must be an explicit, possibly empty, slice)", r.Slug)
		}
		if r.MutableFields == nil {
			t.Errorf("resource %q has nil MutableFields (must be an explicit, possibly empty, slice)", r.Slug)
		}
	}
}

func TestResourcesHaveCreateOrReadMethod(t *testing.T) {
	// Every resource must be observable/creatable via at least one CRUD
	// method — a resource with zero CRUD entries would not be a resource at
	// all (this is a stricter subset check than TestResourcesHaveRequiredMetadata's
	// len(r.CRUD)==0 check, verifying specifically that Create and Read/ReadByRef
	// operations are represented).
	for _, r := range Resources() {
		hasCreate, hasRead := false, false
		for _, m := range r.CRUD {
			switch m.Operation {
			case "Create":
				hasCreate = true
			case "Read", "ReadByRef":
				hasRead = true
			}
		}
		if !hasCreate {
			t.Errorf("resource %q has no Create CRUD method", r.Slug)
		}
		if !hasRead {
			t.Errorf("resource %q has no Read/ReadByRef CRUD method", r.Slug)
		}
	}
}

func TestFieldsHaveJSONNameAndScope(t *testing.T) {
	for _, r := range Resources() {
		seen := map[string]bool{}
		for _, field := range r.Fields {
			if field.JSONName == "" {
				t.Errorf("resource %q has a field with empty JSONName", r.Slug)
			}
			if seen[field.JSONName] {
				t.Errorf("resource %q has duplicate field %q", r.Slug, field.JSONName)
			}
			seen[field.JSONName] = true
			switch field.Scope {
			case model.FieldScopeRequest, model.FieldScopeResponse, model.FieldScopeBoth:
				// ok
			default:
				t.Errorf("resource %q field %q has invalid Scope %q", r.Slug, field.JSONName, field.Scope)
			}
			if field.Description == "" {
				t.Errorf("resource %q field %q has no Description", r.Slug, field.JSONName)
			}
			if len(field.Enum) > 0 && field.EnumSource == "" {
				t.Errorf("resource %q field %q declares Enum values but no EnumSource", r.Slug, field.JSONName)
			}
		}
	}
}

func TestImmutableFieldsAreMarkedOnTheirField(t *testing.T) {
	// Every JSONName listed in ImmutableFields must correspond to a field
	// with Immutable=true (and vice versa is not required, since a field can
	// be Immutable=true without being surfaced in the summary list — but the
	// summary list must not reference a non-existent or non-immutable field).
	for _, r := range Resources() {
		byName := map[string]model.Field{}
		for _, field := range r.Fields {
			byName[field.JSONName] = field
		}
		for _, name := range r.ImmutableFields {
			field, ok := byName[name]
			if !ok {
				t.Errorf("resource %q lists %q in ImmutableFields but has no such field", r.Slug, name)
				continue
			}
			if !field.Immutable {
				t.Errorf("resource %q lists %q in ImmutableFields but the field's Immutable flag is false", r.Slug, name)
			}
		}
	}
}

func TestFindResource(t *testing.T) {
	r, ok := FindResource("record_a")
	if !ok {
		t.Fatal("FindResource(\"record_a\") not found")
	}
	if r.Kind != "ARecord" {
		t.Errorf("Kind = %q, want %q", r.Kind, "ARecord")
	}

	_, ok = FindResource("does_not_exist") //nolint:staticcheck // intentional negative test
	if ok {
		t.Error("FindResource(\"does_not_exist\") unexpectedly found")
	}
}

func TestResourcesCount(t *testing.T) {
	// Guards against accidental duplication/removal during future edits —
	// bump this alongside adding/removing a resource function in Resources().
	const want = 26
	if got := len(Resources()); got != want {
		t.Errorf("len(Resources()) = %d, want %d", got, want)
	}
}

func TestActionEndpointsNonEmpty(t *testing.T) {
	if len(ActionEndpoints()) == 0 {
		t.Error("ActionEndpoints() returned no entries — expected documentation of non-CRUD SDK methods")
	}
	for _, a := range ActionEndpoints() {
		if a.Method == "" {
			t.Error("ActionEndpoint with empty Method")
		}
		if a.Notes == "" {
			t.Errorf("ActionEndpoint %q has no Notes", a.Method)
		}
	}
}

func TestPilotResourceIsARecord(t *testing.T) {
	r, ok := FindResource("record_a")
	if !ok {
		t.Fatal("record_a not found")
	}
	if !r.HasFullCRUD() {
		t.Error("record_a (the recommended pilot) must have full CRUD support")
	}
	if !r.LiveVerified {
		t.Error("record_a (the recommended pilot) must be live-verified")
	}
}
