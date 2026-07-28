package catalog

// ipv4SharedNetwork returns the IPv4SharedNetwork resource descriptor.
//
// Source: tools/openapi/inventory.md, "### IPv4SharedNetwork" section
// (fields request=0, response=7, both=8) — derived from the pinned
// infoblox-go-client/v2 SDK (tools/openapi/specs/infobloxopen/). Not
// independently live-verified against a real NIOS Grid Manager appliance
// for most fields (inventory.md's "Live-verified: no" note for this
// resource), EXCEPT `network_view`'s immutability, which a live probe
// against a real appliance corrected from the inventory's original
// "tentatively mutable" note (see the immutable-fields doc below).
//
// WAPI object type: sharednetwork. SDK struct: SharedNetwork.
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateIpv4SharedNetwork).
//
// ObjectManager wrapper signatures (tools/openapi/specs/infobloxopen,
// object_manager.go.txt):
//
//	CreateIpv4SharedNetwork(name string, networks []string, networkView string, eas EA, comment string, disable bool, useOptions bool, options []*Dhcpoption) (*SharedNetwork, error)
//	UpdateIpv4SharedNetwork(ref string, name string, networks []string, networkView string, comment string, eas EA, disable bool, useOptions bool, options []*Dhcpoption) (*SharedNetwork, error)
//
// Immutable fields: `network_view` is present in both the Create and
// Update wrapper signatures above (so a naive SDK-signature diff would
// call it mutable), but live WAPI `_schema` probing found `supports=rws`
// (no `u`) for this field on the underlying sharednetwork object — the
// wrapper accepts the parameter on update calls but the WAPI schema itself
// rejects changing it. The live result wins over the SDK-signature
// heuristic per this provider's immutable-fields methodology, so this
// catalog marks `network_view` Immutable and the generator emits a CEL
// `self == oldSelf` XValidation rule for it.
//
// `networks` is a cross-resource reference candidate — each entry is a
// CIDR that must match an existing Network resource's `network` (CIDR)
// field, not a `_ref` or name (see tools/openapi/inventory.md's
// "Cross-Resource References" subsection: cidr-match extractor, novel
// pattern). The Network managed resource is not yet merged to main in
// this provider (its wave review ticket is still open), so wiring the
// Ref/Selector companion fields now would make angryjet emit a reference
// resolver that imports a package that does not exist yet on this branch,
// breaking the build. Per this provider's cross-resource-reference
// ordering rule, this field is modeled as a plain `[]string` value for
// now; wiring the three-field reference pattern (plus the cidr-match
// extractor function, which does not yet exist in any cataloged resource)
// is deferred to a follow-up stage:apigen ticket once Network merges.
func ipv4SharedNetwork() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "IPv4SharedNetwork",
		Slug:                 "ipv4sharednetwork",
		ClusterGroup:         clusterGroup("ipv4sharednetwork"),
		NamespacedGroup:      namespacedGroup("ipv4sharednetwork"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "Name",
				JSONName:    fieldNameJSON,
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Display name of the shared network.",
			},
			{
				Name:     "Networks",
				JSONName: "networks",
				GoType:   "[]string",
				Scope:    FieldScopeBoth,
				Required: true,
				Description: "CIDRs of the member networks combined into this shared network. Each entry " +
					"must match an existing Network resource's network (CIDR) field — a cidr-match " +
					"cross-resource reference, not a _ref or name lookup. Not yet wired with the " +
					"Ref/Selector reference pattern: the Network managed resource is not merged to main " +
					"in this provider yet; wiring is deferred to a follow-up ticket to avoid an angryjet " +
					"resolver import of a nonexistent package.",
			},
			{
				Name:      "NetworkView",
				JSONName:  "networkView",
				GoType:    goTypeString,
				Scope:     FieldScopeBoth,
				Immutable: true,
				Description: "Network view the shared network belongs to. Present in both the Create and " +
					"Update ObjectManager wrapper signatures, but live WAPI _schema probing found " +
					"supports=rws (no u) for this field — the WAPI schema rejects changing it after " +
					"creation even though the wrapper accepts the parameter on update calls.",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the shared network.",
			},
			{
				Name:        "ExtAttrs",
				JSONName:    "extAttrs",
				GoType:      goTypeStringMap,
				Scope:       FieldScopeBoth,
				Description: "Extensible attributes (arbitrary key/value metadata defined in Grid Manager). The WAPI wire format wraps each value as {\"value\": ...}; this map is the simplified string-valued CRD representation (the controller translates to/from the SDK's EA map[string]interface{} type).",
			},
			{
				Name:        "Disable",
				JSONName:    "disable",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Whether the shared network is disabled. When false, the shared network is enabled.",
			},
			{
				Name:        "UseOptions",
				JSONName:    "useOptions",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for the options field.",
			},
			{
				Name:        "Options",
				JSONName:    "options",
				GoType:      "[]*IPv4SharedNetworkDhcpOption",
				Scope:       FieldScopeBoth,
				Description: "DHCP options associated with the shared network.",
			},
			{
				Name:        "Ref",
				JSONName:    "ref",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Server-assigned opaque object reference (WAPI `_ref`). Mirrors the crossplane.io/external-name annotation for observability and uptest import verification.",
			},
			{
				Name:        "Authority",
				JSONName:    "authority",
				GoType:      goTypeBool,
				Scope:       FieldScopeResponse,
				Description: "Authority for the shared network.",
			},
			{
				Name:        "DdnsTTL",
				JSONName:    "ddnsTtl",
				GoType:      goTypeUint32,
				Scope:       FieldScopeResponse,
				Description: "DNS update Time-to-live (TTL) value, in seconds. Zero means the update is not cached.",
			},
			{
				Name:        "EnableDdns",
				JSONName:    "enableDdns",
				GoType:      goTypeBool,
				Scope:       FieldScopeResponse,
				Description: "Whether the DHCP server sends DDNS updates for this shared network.",
			},
			{
				Name:        "DhcpUtilization",
				JSONName:    "dhcpUtilization",
				GoType:      goTypeUint32,
				Scope:       FieldScopeResponse,
				Description: "DHCP utilization percentage across member networks, multiplied by 1000.",
			},
			{
				Name:        "DhcpUtilizationStatus",
				JSONName:    "dhcpUtilizationStatus",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Utilization level descriptor (e.g. \"NORMAL\", \"WARNING\").",
			},
			{
				Name:        "DynamicHosts",
				JSONName:    "dynamicHosts",
				GoType:      goTypeUint32,
				Scope:       FieldScopeResponse,
				Description: "Total number of DHCP leases issued for the shared network.",
			},
		},
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "IPv4SharedNetworkDhcpOption",
				Description: "is a DHCP option associated with an IPv4SharedNetwork, mirroring the SDK's Dhcpoption struct.",
				Fields: []FieldDef{
					{Name: "Name", JSONName: fieldNameJSON, GoType: goTypeString, Description: "Name of the DHCP option."},
					{Name: "Num", JSONName: "num", GoType: goTypeUint32, Description: "Code of the DHCP option."},
					{Name: "VendorClass", JSONName: "vendorClass", GoType: goTypeString, Description: "Name of the space this DHCP option is associated with."},
					{Name: "Value", JSONName: "value", GoType: goTypeString, Description: "Value of the DHCP option."},
					{Name: "UseOption", JSONName: "useOption", GoType: goTypeBool, Description: "Only applies to special options displayed separately from other options with a use flag (e.g. routers, domain-name-servers, dhcp-lease-time)."},
				},
			},
		},
	}
}
