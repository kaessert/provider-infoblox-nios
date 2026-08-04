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
	// ttlMinimumSeconds/ttlMaximumSeconds bound every TTL-like field in
	// the catalog (DNS/DHCP cache and delegation TTLs, all expressed in
	// seconds). WAPI stores TTLs as a signed 32-bit integer internally,
	// so 2147483647 (max int32) is the practical upper bound. Negative
	// values have no DNS/DHCP meaning — some Terraform-style tooling
	// uses a negative sentinel (e.g. -2147483648) to mean "inherit the
	// zone/grid default", but WAPI has no such convention; the correct
	// way to inherit the default is to set the field's paired use-flag
	// (e.g. useTtl, useDelegatedTtl) to false instead of passing a
	// sentinel TTL value.
	ttlMinimumSeconds = 0
	ttlMaximumSeconds = 2147483647
	// reservedEAKey is the extensible attribute key the provider stamps
	// into every managed object's extattrs to carry its identity (the
	// managed resource's metadata.uid). It is reserved in every
	// ForProvider extensible-attribute map field — see
	// reservedEAKeyValidations — because a user-supplied value would
	// race the provider for its own identity key and make the object
	// unrecoverable.
	reservedEAKey = "Crossplane Internal ID"
)

// int64Ptr returns a pointer to v. Used to populate FieldDef.Minimum and
// FieldDef.Maximum from an untyped constant, since a constant's address
// cannot be taken directly in a Go literal.
func int64Ptr(v int64) *int64 {
	return &v
}

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
	// Minimum sets the lower bound for a numeric field. When non-nil,
	// the generator emits a +kubebuilder:validation:Minimum marker on
	// the ForProvider field, rejecting out-of-range values at admission
	// time instead of letting them reach the controller (e.g. a
	// negative TTL, which some Terraform-style tooling treats as an
	// "inherit default" sentinel but which crashes NIOS's unsigned wire
	// types).
	Minimum *int64
	// Maximum sets the upper bound for a numeric field. When non-nil,
	// the generator emits a +kubebuilder:validation:Maximum marker on
	// the ForProvider field.
	Maximum *int64
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
	// ForProviderValidations are field-level kubebuilder XValidation
	// rules rendered ONLY on the ForProvider (spec) copy of this field —
	// never on the AtProvider (status) mirror, even for a Scope=Both
	// field. Used for constraints that apply to user input only, e.g.
	// every extensible-attribute map field rejects the reserved
	// "Crossplane Internal ID" key in ForProvider (see
	// reservedEAKeyValidations) while AtProvider keeps mirroring the
	// Grid's real extattrs map, identity stamp included, for the
	// full-mirror observation invariant.
	ForProviderValidations []ValidationRule
}

// reservedEAKeyValidations returns the field-level CEL validation that
// rejects the reserved reservedEAKey extensible attribute key in a
// ForProvider extAttrs map. Shared by every resource whose catalog entry
// models the extAttrs field, so the rule text and message stay identical
// catalog-wide. Tolerates an absent or empty map: the "all" CEL macro is
// vacuously true over zero keys, and per convention 0030 this map field
// carries no omitempty, so an empty map serialises as {} rather than null.
func reservedEAKeyValidations() []ValidationRule {
	return []ValidationRule{
		{
			Rule: "self.all(k, k != '" + reservedEAKey + "')",
			// The message is embedded in a kubebuilder marker as
			// message="...", so it deliberately uses single quotes
			// around the key name rather than double quotes, which
			// would prematurely terminate the marker's quoted value.
			Message: "the '" + reservedEAKey + "' extensible attribute is reserved for the provider's identity stamp and cannot be set in spec.forProvider.extAttrs",
		},
	}
}

// ReferenceDescriptor describes a cross-resource reference for a FieldDef.
// Used by any field whose value is resolved from another managed resource
// (e.g. NetworkContainer.networkView -> NetworkView, CNAMERecord.canonical
// -> ARecord).
//
// A field that is BOTH Immutable AND carries a Reference gets special CEL
// treatment from the generator: the resolver populates the value field
// AFTER the CR is admitted (Resolve runs post-admission, as part of
// reconciliation), so the field's first write is an empty-to-populated
// transition. A bare `self == oldSelf` rule rejects that transition,
// which admits the CR successfully and then makes it fail to reconcile
// forever with a CEL error that does not obviously point at the
// reference. The generator therefore renders the empty-tolerant form
// (`self == oldSelf || oldSelf == ”`, or the slice equivalent) for every
// Immutable+Reference field automatically — this is intentionally NOT an
// opt-in flag, because an opt-in that nobody remembers to set is exactly
// how the bare rule reached six sites across three resources on this
// provider.
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
