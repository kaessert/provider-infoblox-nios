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
// reference candidate (tools/openapi/inventory.md's "Cross-Resource
// References" subsection for this resource lists NetworkView as the
// target, resolved by name rather than by the target's `_ref`, since
// NetworkView's name is stable across most operations). The NetworkView
// managed resource is not yet generated in this provider, so the field is
// modeled as a plain value for now — wiring the Ref/Selector companion
// fields is deferred until the NetworkView types exist, to avoid emitting
// a reference resolver that imports a package that does not exist yet.
//
// `members` is populated on read but is not a CreateNetwork/UpdateNetwork
// parameter via the ObjectManager wrapper (the generic WAPI Connector is
// required to set Grid DHCP member assignments) — it is modeled as a
// response-only nested type. The underlying SDK NetworkMember type is a
// hand-marshaled union of a `dhcpmember` or `msdhcpserver` entry; this
// catalog flattens both branches into named optional fields since the
// generator emits plain structs (no custom JSON union marshaling).
func network() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "Network",
		Slug:                 "network",
		ClusterGroup:         clusterGroup("network"),
		NamespacedGroup:      namespacedGroup("network"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "NetworkView",
				JSONName:    "networkView",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Immutable:   true,
				Description: "Network view the network belongs to, identified by NetworkView name. Fixed at creation — confirmed absent from the UpdateNetwork SDK method signature.",
			},
			{
				Name:        "Network",
				JSONName:    "network",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Immutable:   true,
				Description: "CIDR of the network, e.g. \"10.0.0.0/24\" (IPv4) or an IPv6 CIDR. Fixed at creation — confirmed absent from the UpdateNetwork SDK method signature. The WAPI object type (network vs ipv6network) is selected at runtime from this value's format.",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the network; maximum 256 characters.",
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
