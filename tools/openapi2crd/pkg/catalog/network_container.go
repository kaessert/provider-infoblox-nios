package catalog

// networkContainer returns the NetworkContainer resource descriptor.
//
// Source: tools/openapi/inventory.md, "### NetworkContainer" section
// (fields request=0, response=1, both=4) and corrections found by live
// CRUD verification against a NIOS Grid Manager appliance — derived
// from the pinned infoblox-go-client/v2 SDK
// (tools/openapi/specs/infobloxopen/).
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateNetworkContainer).
//
// Immutable fields: `network_view` and `network` (CIDR) are both
// CreateNetworkContainer parameters absent from UpdateNetworkContainer's
// parameter list — same identity model as Network, which shares this
// immutability pattern.
//
// Cross-resource reference: `networkView` identifies a NetworkView by
// name (the target's Name field is stable across most operations, unlike
// its `_ref` which changes on rename). The default reference extractor
// (reference.ExternalName(), reading the target's crossplane.io/external-
// name annotation) is used per this provider's cross-resource reference
// convention.
//
// `parentCidr`, `allocatePrefixLen`, and `filterParams` are create-time-only
// dynamic CIDR allocation parameters — they drive the
// AllocateNetworkContainer/AllocateNetworkContainerByEA SDK calls (an
// alternate creation path to the static CreateNetworkContainer call driven
// by `network`) and are never echoed back by the WAPI, so they are
// modeled as request-only fields. Unlike Network's AllocateNetworkByEA
// call, AllocateNetworkContainerByEA takes no `object` parameter, so no
// corresponding field is cataloged here. `network` itself is now optional
// at the type level (one of `network`, `parentCidr`, or `filterParams` is
// required — enforced by a struct-level CEL rule) since dynamic allocation
// supplies the CIDR only after creation, at which point the controller
// late-initializes `network` from the allocated value so the resource has
// a stable identity going forward.
func networkContainer() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "NetworkContainer",
		Slug:                 "networkcontainer",
		ClusterGroup:         clusterGroup("networkcontainer"),
		NamespacedGroup:      namespacedGroup("networkcontainer"),
		ExternalNameStrategy: StrategyServerAssigned,
		ParameterValidations: []ValidationRule{
			{
				Rule:    "has(self.network) || has(self.parentCidr) || has(self.filterParams)",
				Message: "one of network, parentCidr, or filterParams is required",
			},
		},
		Fields: []FieldDef{
			{
				Name:        "NetworkView",
				JSONName:    "networkView",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Immutable:   true,
				Description: "Network view the container belongs to. Immutable — absent from UpdateNetworkContainer. Identifies a NetworkView by name.",
				Reference: &ReferenceDescriptor{
					TargetKind:  "NetworkView",
					TargetSlug:  slugNetworkView,
					TargetScope: "cluster",
				},
			},
			{
				Name:        "Network",
				JSONName:    "network",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Immutable:   true,
				Description: "CIDR of the container network, e.g. \"10.0.0.0/16\". Immutable — absent from UpdateNetworkContainer. Optional at the type level: one of network, parentCidr, or filterParams is required (enforced by a struct-level validation rule); when parentCidr or filterParams is used instead, the controller late-initializes this field from the allocated CIDR once creation succeeds.",
			},
			{
				Name:        "ParentCidr",
				JSONName:    "parentCidr",
				GoType:      goTypeString,
				Scope:       FieldScopeRequest,
				Description: "Parent CIDR from which to allocate a subnet container, e.g. \"10.0.0.0/8\". Create-time-only — drives the AllocateNetworkContainer SDK call. Mutually exclusive with network. When set, allocatePrefixLen is required.",
			},
			{
				Name:        "AllocatePrefixLen",
				JSONName:    "allocatePrefixLen",
				GoType:      goTypeUint,
				Scope:       FieldScopeRequest,
				Description: "Prefix length of the subnet container to allocate from parentCidr or filterParams, e.g. 16 for a /16. Required when parentCidr or filterParams is set.",
			},
			{
				Name:        "FilterParams",
				JSONName:    "filterParams",
				GoType:      goTypeStringMap,
				Scope:       FieldScopeRequest,
				Description: "Extensible attribute key/value filter for EA-based allocation, driving the AllocateNetworkContainerByEA SDK call. Keys must carry the WAPI extensible-attribute search prefix \"*\" (e.g. \"*Site\": \"nyc\" to filter on the Site EA) — a bare key such as \"Site\" is rejected by WAPI with \"Unknown argument/field\". Mutually exclusive with parentCidr. When set, allocatePrefixLen is required.",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the network container.",
			},
			{
				Name:                   "ExtAttrs",
				JSONName:               "extAttrs",
				GoType:                 goTypeStringMap,
				Scope:                  FieldScopeBoth,
				Description:            "Extensible attributes (arbitrary key/value metadata defined in Grid Manager). The WAPI wire format wraps each value as {\"value\": ...}; this map is the simplified string-valued CRD representation (the controller translates to/from the SDK's EA map[string]interface{} type).",
				ForProviderValidations: reservedEAKeyValidations(),
			},
			{
				Name:        "Ref",
				JSONName:    "ref",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Server-assigned opaque object reference (WAPI `_ref`). Mirrors the crossplane.io/external-name annotation for observability and uptest import verification.",
			},
		},
	}
}
