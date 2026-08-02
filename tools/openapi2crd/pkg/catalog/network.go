package catalog

// network returns the Network resource descriptor.
//
// Source: tools/openapi/inventory.md, "### Network" section (fields
// request=0, response=2, both=4) — derived from the pinned
// infoblox-go-client/v2 SDK (tools/openapi/specs/infobloxopen/). Not
// independently live-verified against a real NIOS Grid Manager appliance
// (see inventory.md's "Live-verified: no" note for this resource); the
// immutability of network_view/network is confirmed by comparing the
// CreateNetwork and UpdateNetwork SDK method signatures directly.
//
// WAPI object type: runtime-selected. The SDK constructs the underlying
// object as "network" (IPv4) or "ipv6network" (IPv6) depending on the CIDR
// format of the `network` field — the Go struct and ObjectManager methods
// are shared between both address families, so a single Kind models both.
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateNetwork).
//
// Immutable fields: both `network_view` and `network` (CIDR) are CreateNetwork
// parameters absent from UpdateNetwork's parameter list:
//
//	CreateNetwork(netview, cidr, isIPv6, comment, ea)
//	UpdateNetwork(ref, netview, cidr, isIPv6, comment, eas)
//
// `network_view` identifies a NetworkView by name — a cross-resource
// reference (tools/openapi/inventory.md's "Cross-Resource References"
// subsection for this resource lists NetworkView as the target, resolved
// by name rather than by the target's `_ref`, since NetworkView's name is
// stable across most operations). The NetworkView managed resource is now
// generated (cluster-scoped: apis/cluster/networkview/v1alpha1), so the
// field carries the standard three-field reference pattern (value + Ref +
// Selector).
//
// `members` is populated on read but is not a CreateNetwork/UpdateNetwork
// parameter via the ObjectManager wrapper (the generic WAPI Connector is
// required to set Grid DHCP member assignments) — it is modeled as a
// response-only nested type. The underlying SDK NetworkMember type is a
// hand-marshaled union of a `dhcpmember` or `msdhcpserver` entry; this
// catalog flattens both branches into named optional fields since the
// generator emits plain structs (no custom JSON union marshaling).
//
// `parentCidr`, `allocatePrefixLen`, `filterParams`, and `object` are
// create-time-only dynamic CIDR allocation parameters — they drive the
// AllocateNetwork/AllocateNetworkByEA SDK calls (an alternate creation path
// to the static CreateNetwork call driven by `network`) and are never
// echoed back by the WAPI, so they are modeled as request-only fields.
// `network` itself is now optional at the type level (one of `network`,
// `parentCidr`, or `filterParams` is required — enforced by a struct-level
// CEL rule) since dynamic allocation supplies the CIDR only after creation,
// at which point the controller late-initializes `network` from the
// allocated value so the resource has a stable identity going forward.
func network() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "Network",
		Slug:                 "network",
		ClusterGroup:         clusterGroup("network"),
		NamespacedGroup:      namespacedGroup("network"),
		ExternalNameStrategy: StrategyServerAssigned,
		ParameterValidations: []ValidationRule{
			{
				Rule:    "has(self.network) || has(self.parentCidr) || has(self.filterParams)",
				Message: "one of network, parentCidr, or filterParams is required",
			},
		},
		Fields: []FieldDef{
			{
				Name:        kindNetworkView,
				JSONName:    "networkView",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Immutable:   true,
				Description: "Network view the network belongs to, identified by NetworkView name. Fixed at creation — confirmed absent from the UpdateNetwork SDK method signature.",
				Reference: &ReferenceDescriptor{
					TargetKind:  kindNetworkView,
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
				Description: "CIDR of the network, e.g. \"10.0.0.0/24\" (IPv4) or an IPv6 CIDR. Fixed at creation — confirmed absent from the UpdateNetwork SDK method signature. The WAPI object type (network vs ipv6network) is selected at runtime from this value's format. Optional at the type level: one of network, parentCidr, or filterParams is required (enforced by a struct-level validation rule); when parentCidr or filterParams is used instead, the controller late-initializes this field from the allocated CIDR once creation succeeds.",
			},
			{
				Name:        "ParentCidr",
				JSONName:    "parentCidr",
				GoType:      goTypeString,
				Scope:       FieldScopeRequest,
				Description: "Parent CIDR from which to allocate a subnet, e.g. \"10.0.0.0/8\". Create-time-only — drives the AllocateNetwork SDK call. Mutually exclusive with network. When set, allocatePrefixLen is required.",
			},
			{
				Name:        "AllocatePrefixLen",
				JSONName:    "allocatePrefixLen",
				GoType:      goTypeUint,
				Scope:       FieldScopeRequest,
				Description: "Prefix length of the subnet to allocate from parentCidr or filterParams, e.g. 24 for a /24. Required when parentCidr or filterParams is set.",
			},
			{
				Name:        "FilterParams",
				JSONName:    "filterParams",
				GoType:      goTypeStringMap,
				Scope:       FieldScopeRequest,
				Description: "Extensible attribute key/value filter for EA-based allocation, driving the AllocateNetworkByEA SDK call. Keys must carry the WAPI extensible-attribute search prefix \"*\" (e.g. \"*Site\": \"nyc\" to filter on the Site EA) — a bare key such as \"Site\" is rejected by WAPI with \"Unknown argument/field\". Mutually exclusive with parentCidr. When set, allocatePrefixLen is required.",
			},
			{
				Name:        "Object",
				JSONName:    "object",
				GoType:      goTypeString,
				Scope:       FieldScopeRequest,
				Description: "WAPI object type filter for AllocateNetworkByEA, e.g. \"networkcontainer\". Only valid when filterParams is set; ignored otherwise.",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the network; maximum 256 characters.",
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
			{
				Name:        "Members",
				JSONName:    "members",
				GoType:      "[]NetworkMember",
				Scope:       FieldScopeResponse,
				Description: "Grid members serving DHCP for this network. Populated on read; not settable via CreateNetwork/UpdateNetwork through the ObjectManager wrapper.",
			},
		},
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "NetworkMember",
				Description: "is a Grid member DHCP-service assignment for a network. The underlying WAPI object is a union of a dhcpmember or msdhcpserver entry; at most one field group below is populated for a given entry.",
				Fields: []FieldDef{
					{Name: "DhcpMemberName", JSONName: "dhcpMemberName", GoType: goTypeString, Description: "Grid member name serving DHCP for this network (dhcpmember entry)."},
					{Name: "DhcpMemberIPv4Addr", JSONName: "dhcpMemberIpv4Addr", GoType: goTypeString, Description: "IPv4 address of the Grid member serving DHCP (dhcpmember entry)."},
					{Name: "DhcpMemberIPv6Addr", JSONName: "dhcpMemberIpv6Addr", GoType: goTypeString, Description: "IPv6 address of the Grid member serving DHCP (dhcpmember entry)."},
					{Name: "MsDhcpServerIPv4Addr", JSONName: "msDhcpServerIpv4Addr", GoType: goTypeString, Description: "IPv4 address or FQDN of the Microsoft DHCP server (msdhcpserver entry)."},
				},
			},
		},
	}
}
