package catalog

// networkContainer returns the NetworkContainer resource descriptor.
//
// Source: tools/openapi/inventory.md, "### NetworkContainer" section
// (fields request=0, response=1, both=4) and manifests blueprint decision
// ADR-IN-0004 (Phase 6 live API verification) — derived from the pinned
// infoblox-go-client/v2 SDK (tools/openapi/specs/infobloxopen/).
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
func networkContainer() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "NetworkContainer",
		Slug:                 "networkcontainer",
		ClusterGroup:         clusterGroup("networkcontainer"),
		NamespacedGroup:      namespacedGroup("networkcontainer"),
		ExternalNameStrategy: StrategyServerAssigned,
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
				Required:    true,
				Immutable:   true,
				Description: "CIDR of the container network, e.g. \"10.0.0.0/16\". Immutable — absent from UpdateNetworkContainer.",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the network container.",
			},
			{
				Name:        "ExtAttrs",
				JSONName:    "extAttrs",
				GoType:      goTypeStringMap,
				Scope:       FieldScopeBoth,
				Description: "Extensible attributes (arbitrary key/value metadata defined in Grid Manager). The WAPI wire format wraps each value as {\"value\": ...}; this map is the simplified string-valued CRD representation (the controller translates to/from the SDK's EA map[string]interface{} type).",
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
