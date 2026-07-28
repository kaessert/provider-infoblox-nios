// Package model defines the shared data structures produced by the
// investigation tool: per-resource field descriptors and the resource-level
// metadata (external-name strategy, immutability, CRUD surface, connection
// details, cross-resource references) recorded in inventory.md.
//
// Infoblox NIOS does not publish an OpenAPI/Swagger specification. The
// catalog populated in pkg/catalog is derived from the official Go SDK
// (github.com/infobloxopen/infoblox-go-client/v2, pinned under
// tools/openapi/specs/) plus live WAPI probing. See pkg/catalog for the
// source-of-truth resource list.
package model

// FieldScope classifies where a field appears in the wire protocol.
type FieldScope string

const (
	// FieldScopeRequest means the field is only accepted in create/update
	// calls and is not part of the response object the SDK returns.
	FieldScopeRequest FieldScope = "request"
	// FieldScopeResponse means the field is only present in API responses
	// (e.g. server-assigned identifiers, computed/read-only attributes) and
	// is not accepted as a create/update parameter.
	FieldScopeResponse FieldScope = "response"
	// FieldScopeBoth means the field appears in both requests and responses
	// (e.g. a user-supplied value that is also echoed back on GET).
	FieldScopeBoth FieldScope = "both"
)

// Pattern classifies the CRUD shape of a resource.
type Pattern string

const (
	// PatternCRUD is a standard resource with create, read, update, and
	// delete operations addressed by a server-assigned `_ref`.
	PatternCRUD Pattern = "CRUD"
	// PatternCreateReadDelete is a resource with create/read/delete but no
	// update operation exposed by the SDK's ObjectManager wrapper (updates,
	// if needed, require the generic Connector.UpdateObject call).
	PatternCreateReadDelete Pattern = "create-read-delete"
	// PatternWellKnownDefault is a resource type where at least one instance
	// ("default") always exists and cannot be deleted, but additional
	// instances can be created/deleted normally.
	PatternWellKnownDefault Pattern = "well-known-default"
)

// ExternalNameStrategy classifies how the Crossplane external-name maps to
// the API's resource identifier.
type ExternalNameStrategy string

const (
	// StrategyServerAssigned means the WAPI assigns an opaque `_ref` string
	// in the create response; the external name is set from that value.
	// This is the only strategy used by NIOS objects — every resource has a
	// server-assigned `_ref`, never a user-supplied identifier.
	StrategyServerAssigned ExternalNameStrategy = "server-assigned"
)

// Field describes a single request/response field on a resource.
type Field struct {
	// JSONName is the field name as it appears on the wire (WAPI field
	// name).
	JSONName string
	// GoType is the Go type as declared in the SDK struct (informational —
	// the Phase 3 generator produces the authoritative CRD types). Nested
	// struct types are referenced by their SDK type name (e.g.
	// "*Discoverydata", "[]*Dhcpoption").
	GoType string
	// Scope classifies where the field appears (request/response/both).
	Scope FieldScope
	// Required is true if the field is a required (non-optional) parameter
	// of the SDK's Create<Resource> method.
	Required bool
	// Immutable is true if the field is accepted by Create<Resource> but is
	// absent from the corresponding Update<Resource> method's parameter
	// list — the SDK provides no way to change it after creation.
	Immutable bool
	// Enum lists valid values, if the SDK's doc comment declares a
	// "Valid values are ..." constraint.
	Enum []string
	// EnumSource documents where the enum values came from (file:line or
	// field doc reference) for traceability.
	EnumSource string
	// Description is the SDK struct field's doc comment.
	Description string
}

// CRUDMethod records one ObjectManager (or, if none exists, the generic
// Connector) Go method backing a CRUD operation.
type CRUDMethod struct {
	Operation string // Create | Read | ReadByRef | Update | Delete
	Method    string // Go method name, e.g. "CreateARecord"
	Receiver  string // "ObjectManager" or "Connector" (generic fallback)
	Notes     string
}

// ConnectionDetail records a sensitive response field candidate (convention
// on publishing sensitive fields to a Crossplane connection secret).
type ConnectionDetail struct {
	Key          string
	SourceField  string
	Availability string // "create-only" | "always"
	Notes        string
}

// CrossReference records a field on this resource that identifies another
// managed resource by name/external-name (convention governing
// crossplane-runtime reference resolution).
type CrossReference struct {
	FieldPath   string // e.g. "networkView"
	TargetKind  string // e.g. "NetworkView"
	TargetScope string // "cluster" | "namespaced"
	Extractor   string // "external-name" | "<jsonpath>"
	Compound    bool
	Notes       string
}

// Resource is the fully analyzed metadata for one NIOS object type.
type Resource struct {
	// Slug is the lower_snake_case resource identifier (matches the WAPI
	// object type where practical, e.g. "record_a").
	Slug string
	// Kind is the PascalCase Crossplane Kind name (e.g. "ARecord").
	Kind string
	// WAPIObjectType is the WAPI object type string used in URL paths (e.g.
	// "record:a"). Empty if the type is runtime-selected (e.g. Network is
	// "network" or "ipv6network" depending on an isIPv6 flag).
	WAPIObjectType string
	// WAPIObjectTypeNotes documents runtime-selected object types.
	WAPIObjectTypeNotes string
	// Pattern classifies the CRUD shape.
	Pattern Pattern

	// GoStructName is the SDK struct type name backing this resource (e.g.
	// "RecordA").
	GoStructName string

	CRUD []CRUDMethod

	// ExternalName strategy classification — always explicit.
	ExternalNameStrategy  ExternalNameStrategy
	ExternalNameRationale string
	// ExternalNameSourcePath is the JSON path in the create response
	// holding the server-assigned identifier (always "_ref" for NIOS).
	ExternalNameSourcePath string

	// Fields is the union of request+response fields exposed through the
	// SDK's ObjectManager wrapper methods, deduplicated by JSONName, with
	// Scope reflecting where each was observed. Fields present in the full
	// WAPI object model but not surfaced by any ObjectManager method are
	// NOT included here — see FullSchemaNotes.
	Fields []Field

	// FullSchemaNotes documents when the full WAPI struct (objects_generated.go)
	// exposes materially more fields than the ObjectManager convenience
	// methods accept — a future Phase 3 decision point (whether to widen
	// the controller to use the generic Connector for full-field control).
	FullSchemaNotes string

	// ImmutableFields lists JSONNames of fields accepted on create but not
	// on update. Empty slice (not nil) means "none known" was explicitly
	// evaluated.
	ImmutableFields []string
	// MutableFields lists JSONNames of fields that can be changed via the
	// update method.
	MutableFields []string

	// DeleteBehavior documents observed/expected delete semantics.
	DeleteBehavior string

	// ConnectionDetails lists sensitive response fields.
	ConnectionDetails []ConnectionDetail

	// CrossReferences lists fields on this resource that reference another
	// managed resource.
	CrossReferences []CrossReference

	// Live-probe enrichment (optional — populated only when credentials are
	// available).
	LiveVerified       bool
	UndocumentedFields []string
	LiveNotes          string

	Notes string
}

// ActionEndpoint records an SDK method that performs an imperative operation
// rather than CRUD on a single object type (e.g. AllocateNextAvailableIp,
// ReleaseIP). These are NOT modeled as managed resources — recorded here for
// completeness and later controller-design reference.
type ActionEndpoint struct {
	Method string
	Notes  string
}

// HasFullCRUD reports whether the resource supports create, read, update,
// and delete via the ObjectManager wrapper.
func (r Resource) HasFullCRUD() bool {
	has := map[string]bool{}
	for _, m := range r.CRUD {
		has[m.Operation] = true
	}
	return has["Create"] && has["Read"] && has["Update"] && has["Delete"]
}

// FieldCounts returns the number of request-only, response-only, and
// both-scope fields, in that order.
func (r Resource) FieldCounts() (req, resp, both int) {
	for _, f := range r.Fields {
		switch f.Scope {
		case FieldScopeRequest:
			req++
		case FieldScopeResponse:
			resp++
		case FieldScopeBoth:
			both++
		}
	}
	return req, resp, both
}

// UpdateMethod returns the Go method name backing the Update operation, or
// "" if none exists.
func (r Resource) UpdateMethod() string {
	for _, m := range r.CRUD {
		if m.Operation == "Update" {
			return m.Method
		}
	}
	return ""
}

// DeleteMethod returns the Go method name backing the Delete operation, or
// "" if none exists.
func (r Resource) DeleteMethod() string {
	for _, m := range r.CRUD {
		if m.Operation == "Delete" {
			return m.Method
		}
	}
	return ""
}
