package report

import (
	"strings"
	"testing"
	"time"

	"github.com/crossplane-contrib/provider-infoblox-nios/tools/openapi/pkg/model"
)

func sampleResources() []model.Resource {
	return []model.Resource{
		{
			Slug:                   "record_a",
			Kind:                   "ARecord",
			WAPIObjectType:         "record:a",
			Pattern:                model.PatternCRUD,
			GoStructName:           "RecordA",
			ExternalNameStrategy:   model.StrategyServerAssigned,
			ExternalNameRationale:  "server-assigned _ref",
			ExternalNameSourcePath: "_ref",
			CRUD: []model.CRUDMethod{
				{Operation: "Create", Method: "CreateARecord", Receiver: "ObjectManager"},
				{Operation: "Read", Method: "GetARecord", Receiver: "ObjectManager"},
				{Operation: "Update", Method: "UpdateARecord", Receiver: "ObjectManager"},
				{Operation: "Delete", Method: "DeleteARecord", Receiver: "ObjectManager"},
			},
			Fields: []model.Field{
				{JSONName: "name", GoType: "*string", Scope: model.FieldScopeBoth, Required: true, Description: "Owner name."},
				{JSONName: "_ref", GoType: "string", Scope: model.FieldScopeResponse, Description: "Server ref."},
			},
			ImmutableFields: []string{"view"},
			MutableFields:   []string{"name"},
			DeleteBehavior:  "hard-delete (404 on subsequent GET)",
			LiveVerified:    true,
			CrossReferences: []model.CrossReference{
				{FieldPath: "networkView", TargetKind: "NetworkView", TargetScope: "cluster", Extractor: "external-name"},
			},
		},
		{
			Slug:                  "zone_auth",
			Kind:                  "ZoneAuth",
			WAPIObjectType:        "zone_auth",
			Pattern:               model.PatternCreateReadDelete,
			GoStructName:          "ZoneAuth",
			ExternalNameStrategy:  model.StrategyServerAssigned,
			ExternalNameRationale: "server-assigned _ref",
			CRUD: []model.CRUDMethod{
				{Operation: "Create", Method: "CreateZoneAuth", Receiver: "ObjectManager"},
				{Operation: "ReadByRef", Method: "GetZoneAuthByRef", Receiver: "ObjectManager"},
				{Operation: "Delete", Method: "DeleteZoneAuth", Receiver: "ObjectManager"},
			},
			Fields: []model.Field{
				{JSONName: "fqdn", GoType: "string", Scope: model.FieldScopeBoth, Required: true, Immutable: true, Description: "FQDN."},
			},
			ImmutableFields: []string{"fqdn"},
			MutableFields:   []string{},
			DeleteBehavior:  "hard-delete (inferred)",
			Notes:           "No Update method exists.",
		},
	}
}

func TestWriteProducesExpectedSections(t *testing.T) {
	var buf strings.Builder
	opts := Options{
		GeneratedAt:     time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC),
		SDKCommit:       "abc123",
		PilotSlug:       "record_a",
		ActionEndpoints: []model.ActionEndpoint{{Method: "AllocateNextAvailableIp", Notes: "composite"}},
	}
	if err := Write(&buf, sampleResources(), opts); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	out := buf.String()

	wantSections := []string{
		"# Infoblox NIOS Provider — Resource Inventory",
		"## Methodology",
		"## Pilot Resource",
		"**Confirmed pilot: ARecord**",
		"## Resource Summary Table",
		"## External-Name Strategy Table (§7.13)",
		"## Immutable Fields Table (§7.14)",
		"## Connection Details (§7.16)",
		"## Cross-Resource References (§7.17)",
		"## Non-Standard CRUD Patterns",
		"- **ZoneAuth**: No Update method exists.",
		"## Sub-Resource / Action Endpoints",
		"`AllocateNextAvailableIp`",
		"## Detailed Resource Catalog",
		"### ARecord",
		"### ZoneAuth",
	}
	for _, want := range wantSections {
		if !strings.Contains(out, want) {
			t.Errorf("output missing expected section/content: %q", want)
		}
	}

	// ARecord has full CRUD with conventional naming, so it must NOT appear
	// in the non-standard CRUD list.
	nonStdIdx := strings.Index(out, "## Non-Standard CRUD Patterns")
	subResIdx := strings.Index(out, "## Sub-Resource / Action Endpoints")
	if nonStdIdx < 0 || subResIdx < 0 || subResIdx < nonStdIdx {
		t.Fatalf("could not locate Non-Standard CRUD Patterns section bounds")
	}
	nonStdSection := out[nonStdIdx:subResIdx]
	if strings.Contains(nonStdSection, "**ARecord**") {
		t.Error("ARecord (full conventional CRUD) should not appear in Non-Standard CRUD Patterns")
	}
}

func TestWritePilotNotFoundWarns(t *testing.T) {
	var buf strings.Builder
	opts := Options{PilotSlug: "does_not_exist"}
	if err := Write(&buf, sampleResources(), opts); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if !strings.Contains(buf.String(), "WARNING") {
		t.Error("expected WARNING when pilot slug is not found")
	}
}

func TestWriteNoConnectionDetails(t *testing.T) {
	var buf strings.Builder
	if err := Write(&buf, sampleResources(), Options{}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if !strings.Contains(buf.String(), "_(none identified)_") {
		t.Error("expected placeholder for empty Connection Details table")
	}
}

func TestCrudNamesStandard(t *testing.T) {
	standard := model.Resource{CRUD: []model.CRUDMethod{
		{Operation: "Create", Method: "CreateARecord"},
		{Operation: "Update", Method: "UpdateARecord"},
		{Operation: "Delete", Method: "DeleteARecord"},
	}}
	if !crudNamesStandard(standard) {
		t.Error("expected standard = true")
	}

	nonStandard := model.Resource{CRUD: []model.CRUDMethod{
		{Operation: "Create", Method: "AllocateIP"},
		{Operation: "Update", Method: "UpdateFixedAddress"},
		{Operation: "Delete", Method: "DeleteFixedAddress"},
	}}
	if crudNamesStandard(nonStandard) {
		t.Error("expected standard = false for AllocateIP")
	}
}

func TestEscapePipes(t *testing.T) {
	if got := escapePipes(""); got != emDash {
		t.Errorf("escapePipes(\"\") = %q, want %q", got, emDash)
	}
	if got := escapePipes("a | b\nc"); got != "a \\| b c" {
		t.Errorf("escapePipes() = %q", got)
	}
}
