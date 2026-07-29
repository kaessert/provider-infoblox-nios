// Package catalog defines the resource catalog consumed by the openapi2crd
// generate subcommand.
//
// Infoblox NIOS does not publish an OpenAPI/Swagger specification (input
// format rest-nospec-sdk). The field-level data encoded here as Go literals
// is re-expressed from the already-verified resource investigation recorded
// in tools/openapi/inventory.md (itself derived from the pinned
// infoblox-go-client/v2 SDK source under tools/openapi/specs/, plus live
// WAPI probing — see tools/openapi/pkg/catalog for the original
// investigation catalog). This package has no runtime SDK/spec-parsing
// dependency of its own — all spec processing happened once, in Go, ahead
// of time, in the Phase 1 investigation tool.
//
// New ResourceDescriptor entries are added here manually as additional
// resources are promoted from the inventory (see the /add-resource skill).
package catalog

// FieldScope classifies where a field appears in the wire protocol.
type FieldScope int

const (
	// FieldScopeRequest means the field is accepted in request bodies
	// only (create/update SDK method parameters). Maps to ForProvider
	// only.
	FieldScopeRequest FieldScope = iota
	// FieldScopeResponse means the field is present in API responses
	// only (the SDK's Get<Resource> return struct). Maps to AtProvider
	// only.
	FieldScopeResponse
	// FieldScopeBoth means the field appears in both requests and
	// responses. Maps to BOTH ForProvider and AtProvider.
	FieldScopeBoth
)

// ExternalNameStrategy classifies how the external-name annotation is
// populated.
type ExternalNameStrategy string

const (
	// StrategyServerAssigned means the WAPI assigns an opaque `_ref` on
	// POST; the external name is set from that create-response value.
	// Every resource in the NIOS catalog uses this strategy — WAPI never
	// exposes a stable user-supplied identifier that survives a rename.
	StrategyServerAssigned ExternalNameStrategy = "server-assigned"
)

// Common GoType literals, factored out to satisfy the goconst linter (they
// recur across many FieldDef entries, especially inside the large
// Discoverydata nested type).
const (
	goTypeString     = "*string"
	goTypeBool       = "*bool"
	goTypeInt64      = "*int64"
	goTypeUint32     = "*uint32"
	goTypeUint       = "*uint"
	goTypeStringMap  = "map[string]string"
	groupSuffixCrd   = "infobloxnios.crossplane.io"
	groupSuffixCrdNS = "infobloxnios.m.crossplane.io"
	// fieldNameGo/fieldNameJSON are the recurring Go/JSON names for a
	// resource or nested type's "name" field (ARecord's Dhcpmember-derived
	// nested types, RangeTemplate's top-level Name field and its
	// Dhcpoption/Dhcpmember-derived nested types all carry one), factored
	// out to satisfy the goconst linter.
	fieldNameGo   = "Name"
	fieldNameJSON = "name"
	// extractFieldFuncPath is the fully-qualified reference to the generic
	// field-path cross-resource reference extractor generated at
	// apis/common/referencehelpers/zz_referencehelpers.go
	// (referencehelpers.ExtractField). Used as the prefix for a
	// ReferenceDescriptor.Extractor value whenever a field must resolve to
	// something other than the target's external name — e.g.
	// IPv4SharedNetwork.networks resolves to the referenced Network's
	// spec.forProvider.network CIDR, not its crossplane.io/external-name.
	extractFieldFuncPath = "github.com/crossplane-contrib/provider-infoblox-nios/apis/common/referencehelpers.ExtractField"
	// fieldNameName/jsonNameName are the same recurring "Name"/"name"
	// FieldDef.Name/JSONName pair, used both by top-level resource fields
	// (e.g. ARecord.Name, NetworkView.Name) and by nested Grid-member-style
	// types (e.g. ARecordCloudInfoDelegatedMember.Name,
	// FixedAddressCloudInfoDelegatedMember.Name).
	fieldNameName = "Name"
	jsonNameName  = "name"
	// kindNetworkView recurs across the HostRecord, Network, and Range
	// networkView cross-resource reference fields (Name, TargetKind) plus
	// the NetworkView resource's own Kind, and their table-driven tests.
	kindNetworkView = "NetworkView"
	// slugNetworkView is the recurring slug literal for the NetworkView
	// resource, used both by production catalog entries (TargetSlug) and
	// by table-driven tests across the catalog package.
	slugNetworkView = "networkview"
)

// FieldDef describes one field of a resource or nested type.
type FieldDef struct {
	// Name is the Go field name (PascalCase).
	Name string
	// JSONName is the camelCase JSON/CRD field name.
	JSONName string
	// GoType is the full Go type as it should appear in the ForProvider
	// struct (e.g. "string", "*string", "*int64", "map[string]string").
	// AtProvider mirrors use a pointer form regardless for top-level
	// resource fields (see generator.atProviderGoType); nested-type
	// fields are always AtProvider-only and use their final Go type
	// directly (see NestedTypeDef doc).
	GoType string
	// Scope classifies where the field appears on the wire. Ignored for
	// nested-type fields (always AtProvider-only).
	Scope FieldScope
	// Required is true if the field is required on create.
	Required bool
	// Immutable is true if the field cannot change after creation via
	// the SDK's ObjectManager wrapper; the generator emits a CEL
	// `self == oldSelf` XValidation rule for it. For fields with a
	// ForProvider representation (Scope Request or Both), the rule is
	// emitted on the ForProvider (spec) field. For a field that is
	// Immutable but Scope=Response — derived server-side, never
	// user-settable, e.g. ARecord's `zone` (derived from name+view) or
	// NetworkView's or DNSView's `is_default` (Grid-assigned) — there is
	// no ForProvider field to attach the rule to, so the generator
	// instead emits it on the AtProvider (status) mirror field, guarding
	// against the observed value ever appearing to change.
	Immutable bool
	// Enum lists the valid string values for this field, taken from the
	// SDK struct field's doc comment ("Valid values are ..."). When
	// non-empty, the generator emits a +kubebuilder:validation:Enum
	// marker on the ForProvider field.
	Enum []string
	// Description is the field's doc comment, taken from the SDK
	// struct field comment (see inventory.md).
	Description string
	// Reference describes a cross-resource reference for this field.
	// Nil for fields that are not references. No ARecord field carries
	// one — ARecord is referenced BY other DNS record resources (e.g. a
	// CNAMERecord's canonical field or a PTRRecord's ptrdname field) but
	// does not itself reference anything.
	Reference *ReferenceDescriptor
	// OmitFromObservation excludes this ForProvider (Scope Request or
	// Both) field from the AtProvider full mirror. Use only for fields
	// the API genuinely never echoes back; each omission must be
	// documented in the field's Description. ARecord's
	// remove_associated_ptr is the only such field — it is a
	// delete-time option flag, not part of the RecordA response
	// struct returned by GetARecord.
	OmitFromObservation bool
}

// ReferenceDescriptor describes a cross-resource reference for a FieldDef.
// Not used by any ARecord field (see FieldDef.Reference doc) but retained
// on the shared ResourceDescriptor/generator so later resources (e.g.
// CNAMERecord.canonical -> ARecord) can populate it without a generator
// rework.
type ReferenceDescriptor struct {
	// TargetKind is the referenced resource's Kind (e.g. "ARecord").
	TargetKind string
	// TargetSlug is the referenced resource's package slug, used to
	// build the generated Go import path (e.g. "recorda").
	TargetSlug string
	// TargetScope is "cluster" or "namespaced".
	TargetScope string
	// Extractor overrides the default reference.ExternalName() extractor
	// with a fully-qualified function reference. Empty means the
	// generator default.
	Extractor string
	// ImmutableOnceSet opts a field with Immutable=true into a two-tier
	// immutability pattern (see the vultr/tailscale generator precedent
	// this catalog followed) — not exercised by any current NIOS
	// resource.
	ImmutableOnceSet bool
}

// NestedTypeDef describes a supporting nested struct type referenced from a
// resource's fields (e.g. ARecord's `discoveredData` field).
type NestedTypeDef struct {
	// TypeName is the Go type name (e.g. "ARecordDiscoveredData").
	TypeName string
	// Description is the nested type's doc comment.
	Description string
	// Fields are the nested type's fields. Nested type fields are
	// always treated as AtProvider-only (every nested SDK struct
	// cataloged so far is a response-only object — Discoverydata,
	// GridCloudapiInfo, MsserverAduserData, Awsrte53recordinfo — none is
	// accepted as a create/update parameter), so the generator applies
	// AtProvider optionality rules to them (every field gets a
	// `// +optional` marker; no field is ever Required).
	Fields []FieldDef
}

// ValidationRule is a cross-field kubebuilder XValidation rule rendered on
// the {{.Kind}}Parameters struct. Not used by ARecord (no cross-field
// constraint identified) but retained on ResourceDescriptor for later
// resources.
type ValidationRule struct {
	// Rule is the CEL expression (kubebuilder XValidation "rule" value).
	Rule string
	// Message is the human-readable validation failure message
	// (kubebuilder XValidation "message" value).
	Message string
}

// ResourceDescriptor holds everything the generator needs to emit Go types
// for one resource, in both cluster and namespaced scopes.
type ResourceDescriptor struct {
	// Kind is the Kubernetes Kind (e.g. "ARecord").
	Kind string
	// Slug is the lowercase, filename-safe resource slug used both for
	// the types filename and the per-resource API group directory
	// (apis/{cluster,namespaced}/<slug>/). This is deliberately the
	// no-underscore CRD-facing slug ("recorda"), distinct from the
	// WAPI-derived inventory slug ("record_a") used in
	// tools/openapi/inventory.md and tools/openapi/pkg/catalog.
	Slug string
	// ClusterGroup is the API group for the cluster-scoped variant
	// (e.g. "recorda.infobloxnios.crossplane.io").
	ClusterGroup string
	// NamespacedGroup is the API group for the namespaced variant
	// (e.g. "recorda.infobloxnios.m.crossplane.io").
	NamespacedGroup string
	// ExternalNameStrategy records the strategy (informational —
	// consumed by the controller, not by the generator).
	ExternalNameStrategy ExternalNameStrategy
	// Fields are the resource's top-level fields.
	Fields []FieldDef
	// NestedTypes are supporting nested struct types.
	NestedTypes []NestedTypeDef
	// ParameterValidations are resource-level (cross-field) CEL rules
	// rendered on the {{.Kind}}Parameters struct.
	ParameterValidations []ValidationRule
}

// FindResource returns the ResourceDescriptor for the given slug.
func FindResource(slug string) (*ResourceDescriptor, bool) {
	for _, rd := range All() {
		if rd.Slug == slug {
			return &rd, true
		}
	}
	return nil, false
}

// All returns every resource descriptor currently registered in the
// catalog. Entries are added manually as additional resources are
// onboarded (see the /add-resource skill).
func All() []ResourceDescriptor {
	return []ResourceDescriptor{
		aRecord(),
		aaaaRecord(),
		aliasRecord(),
		cnameRecord(),
		hostRecord(),
		mxRecord(),
		nsRecord(),
		ptrRecord(),
		srvRecord(),
		txtRecord(),
		zoneAuth(),
		zoneDelegated(),
		network(),
		extensibleAttributeDef(),
		networkView(),
		fixedAddress(),
		rangeResource(),
		rangeTemplate(),
		ipv4SharedNetwork(),
		networkContainer(),
		zoneForward(),
		dtcServer(),
		dnsView(),
		dtcPool(),
		dtcLBDN(),
	}
}

// clusterGroup builds the cluster-scoped API group for a resource slug.
func clusterGroup(slug string) string {
	return slug + "." + groupSuffixCrd
}

// namespacedGroup builds the namespaced API group for a resource slug.
func namespacedGroup(slug string) string {
	return slug + "." + groupSuffixCrdNS
}
