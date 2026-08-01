package catalog

// rangeResource returns the Range resource descriptor. (Named rangeResource,
// not range, because range is a Go keyword.)
//
// Source: tools/openapi/inventory.md, "### Range" section (fields
// request=1, response=1, both=14), narrowed by live WAPI verification: the
// `range` object only exposes seven fields as meaningful create/update
// parameters through the ObjectManager wrapper — start_addr, end_addr,
// network_view, network, template, comment, and extattrs. The remaining
// SDK-visible fields (name, disable, member, failover_association,
// server_association_type, options, use_options, ms_server) are
// DHCP-member/failover administrative detail outside this resource's
// live-verified scope and are omitted here rather than cataloged as fields
// with no confirmed working setter — same precedent as NetworkView's
// Grid-administrative field omissions (see network_view.go).
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateNetworkRange).
//
// Immutable fields: `template` is a CreateNetworkRange-only convenience
// parameter — it copies a Range Template's settings once at creation time
// rather than maintaining a live link, so UpdateNetworkRange does not
// accept it. It is also absent from the GetNetworkRange response
// (inventory.md records response=1, i.e. only `_ref`), so it has no
// AtProvider mirror (OmitFromObservation) — same pattern as ARecord's
// removeAssociatedPtr (see dns_records.go).
//
// Cross-resource references: `networkView` identifies the owning
// NetworkView by name (NetworkView's Name field is stable across most
// operations, unlike its `_ref` which changes on rename), resolved via the
// default reference.ExternalName() extractor against the cluster-scoped
// NetworkView (or namespaced NetworkView for the namespaced Range variant)
// — see blueprint.md §7.17.
func rangeResource() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "Range",
		Slug:                 "range",
		ClusterGroup:         clusterGroup("range"),
		NamespacedGroup:      namespacedGroup("range"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "StartAddr",
				JSONName:    "startAddr",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "First address in the DHCP range.",
			},
			{
				Name:        "EndAddr",
				JSONName:    "endAddr",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Last address in the DHCP range.",
			},
			{
				Name:        kindNetworkView,
				JSONName:    "networkView",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Name of the network view the range belongs to. Referenced by name (not _ref), since NetworkView's name is stable across most operations while its _ref changes on rename.",
				Reference: &ReferenceDescriptor{
					TargetKind: kindNetworkView,
					TargetSlug: slugNetworkView,
				},
			},
			{
				Name:        "Network",
				JSONName:    "network",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "CIDR of the parent network the range belongs to.",
			},
			{
				Name:                "Template",
				JSONName:            "template",
				GoType:              goTypeString,
				Scope:               FieldScopeRequest,
				Immutable:           true,
				OmitFromObservation: true,
				Description:         "Name of the Range Template used to pre-populate this range's settings at creation. CreateNetworkRange accepts it; UpdateNetworkRange does not — the template link is create-only (applies its settings once, then the range is independent). Not part of the GetNetworkRange response, so it has no AtProvider mirror.",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the range; maximum 256 characters.",
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
