package catalog

// hostRecord returns the HostRecord resource descriptor.
//
// Source: tools/openapi/inventory.md, "### HostRecord" section (fields
// request=2, response=14, both=12) — itself derived from the pinned
// infoblox-go-client/v2 SDK (tools/openapi/specs/infobloxopen/) and
// corrected by live probing against a real NIOS Grid Manager appliance.
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateHostRecord).
//
// Immutable fields: `networkView` has `supports=rws` (no `u`): "Field is
// not allowed for update" (live-verified, correcting the Phase 1 inventory
// which listed it as mutable). `zone` is response-only (derived from
// name/view) and never settable.
//
// Cross-resource references: `networkView` → NetworkView (cluster-scoped,
// field-path extractor resolving against the target's spec.forProvider.name
// — WAPI's network_view field requires the view NAME, not the target's
// server-assigned external-name). This is an immutable create-time reference.
//
// ForProvider nested types:
//   - HostIpv4Addr: IPv4 address entry with optional MAC/DHCP config
//   - HostIpv6Addr: IPv6 address entry with optional DUID/DHCP config
//
// These are user-settable nested types (unlike ARecord's response-only
// DiscoveredData) — they appear in ForProvider and are mirrored in
// AtProvider via the parent field's FieldScopeBoth scope.
//
// Write-only SDK parameters `mac_address` and `duid` are top-level
// request-only fields in the SDK but they are persisted on the nested
// ipv4addrs/ipv6addrs entries rather than as top-level response fields.
// They are omitted from this catalog (not ForProvider-visible) — the
// nested HostIpv4Addr.mac and HostIpv6Addr.duid fields cover the same
// use case at the correct structural level.
//
// Response-only fields omitted from this catalog (same precedent as
// ARecord's discoveredData/cloudInfo — complex nested SDK objects the
// ObjectManager wrapper never accepts as create/update parameters):
// cloud_info, ms_ad_user_data, creation_time (does not exist on this WAPI
// version's HostRecord object), ddns_protected, last_queried,
// device_description, device_location, device_type, device_vendor,
// rrset_order.
func hostRecord() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "HostRecord",
		Slug:                 "hostrecord",
		ClusterGroup:         clusterGroup("hostrecord"),
		NamespacedGroup:      namespacedGroup("hostrecord"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "Name",
				JSONName:    "name",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Host name in FQDN format. Renaming changes the record's _ref.",
			},
			{
				Name:        "Ipv4Addrs",
				JSONName:    "ipv4Addrs",
				GoType:      "[]HostRecordIpv4Addr",
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "List of IPv4 addresses for the host. At least one entry is required on create.",
			},
			{
				Name:        "Ipv6Addrs",
				JSONName:    "ipv6Addrs",
				GoType:      "[]HostRecordIpv6Addr",
				Scope:       FieldScopeBoth,
				Description: "List of IPv6 addresses for the host.",
			},
			{
				Name:      kindNetworkView,
				JSONName:  "networkView",
				GoType:    goTypeString,
				Scope:     FieldScopeBoth,
				Immutable: true,
				Description: "Network view the host record resides in. Immutable after creation " +
					"(live-verified: supports=rws, no u — corrects Phase 1 inventory).",
				Reference: &ReferenceDescriptor{
					TargetKind:  kindNetworkView,
					TargetSlug:  slugNetworkView,
					TargetScope: "cluster",
					Extractor:   extractFieldFuncPath + `("spec.forProvider.name")`,
				},
			},
			{
				Name:        "View",
				JSONName:    "view",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "DNS view in which the record resides. Mutable — changing it moves the record between DNS views and changes the _ref.",
			},
			{
				Name:        "Aliases",
				JSONName:    "aliases",
				GoType:      "[]string",
				Scope:       FieldScopeBoth,
				Description: "List of alias FQDNs for the host.",
			},
			{
				Name:        "ConfigureForDNS",
				JSONName:    "configureForDns",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Whether the host has DNS (parent zone) information. When false, no DNS records are created.",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the record; maximum 256 characters.",
			},
			{
				Name:        "Disable",
				JSONName:    "disable",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Whether the record is disabled.",
			},
			{
				Name:        "TTL",
				JSONName:    "ttl",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Minimum:     int64Ptr(ttlMinimumSeconds),
				Maximum:     int64Ptr(ttlMaximumSeconds),
				Description: "Time-to-live in seconds. Zero means the record is not cached. Must be non-negative (0-2147483647); to inherit the zone/grid default, set useTtl to false rather than passing a negative sentinel value.",
			},
			{
				Name:        "UseTTL",
				JSONName:    "useTtl",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for ttl — when false the zone/grid default TTL applies.",
			},
			{
				Name:                   "ExtAttrs",
				JSONName:               "extAttrs",
				GoType:                 goTypeStringMap,
				Scope:                  FieldScopeBoth,
				Description:            "Extensible attributes (arbitrary key/value metadata defined in Grid Manager). The WAPI wire format wraps each value as {\"value\": ...}; this map is the simplified string-valued CRD representation (the controller translates to/from the SDK's EA map[string]interface{} type).",
				ForProviderValidations: reservedEAKeyValidations(),
			},
			// Create-time-only dynamic IP allocation hints (WAPI
			// func:nextavailableip). These are accepted by
			// CreateHostRecord/AllocateNextAvailableIp but are not part of
			// the GetHostRecord response, so they are FieldScopeRequest —
			// still mirrored to Observation via the full-mirror rule so an
			// Observe-mode import can compare desired to applied allocation
			// hints. The SDK's UpdateHostRecord ignores these parameters,
			// so they have no effect after creation.
			{
				Name:        "Ipv4Cidr",
				JSONName:    "ipv4Cidr",
				GoType:      goTypeString,
				Scope:       FieldScopeRequest,
				Description: "CIDR of the IPv4 network from which to allocate the next available IP address (WAPI func:nextavailableip). Create-time-only — ignored on Update. Mutually exclusive with static ipv4Addrs entries that specify an address.",
			},
			{
				Name:        "Ipv6Cidr",
				JSONName:    "ipv6Cidr",
				GoType:      goTypeString,
				Scope:       FieldScopeRequest,
				Description: "CIDR of the IPv6 network from which to allocate the next available IP address. Create-time-only — ignored on Update. Mutually exclusive with static ipv6Addrs entries that specify an address.",
			},
			{
				Name:        "FilterParams",
				JSONName:    "filterParams",
				GoType:      goTypeStringMap,
				Scope:       FieldScopeRequest,
				Description: "Extensible attribute key/value filter for EA-based IP allocation via AllocateNextAvailableIp. Mutually exclusive with ipv4Cidr/ipv6Cidr. When set, ipAddressType is required.",
			},
			{
				Name:        "IpAddressType",
				JSONName:    "ipAddressType",
				GoType:      goTypeString,
				Scope:       FieldScopeRequest,
				Enum:        []string{"IPV4", "IPV6", "Both"},
				Description: "IP address type to allocate when using filterParams: \"IPV4\", \"IPV6\", or \"Both\". Required when filterParams is set; ignored otherwise.",
			},
			// Response-only fields
			{
				Name:        "Ref",
				JSONName:    "ref",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Server-assigned opaque object reference (WAPI `_ref`). Mirrors the crossplane.io/external-name annotation for observability and uptest import verification.",
			},
			{
				Name:        "Zone",
				JSONName:    "zone",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Immutable:   true,
				Description: "Zone in which the record resides. Derived from name/view by WAPI — not a CreateHostRecord parameter.",
			},
			{
				Name:        "DNSName",
				JSONName:    "dnsName",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Host name in punycode format (derived from name).",
			},
			{
				Name:        "DNSAliases",
				JSONName:    "dnsAliases",
				GoType:      "[]string",
				Scope:       FieldScopeResponse,
				Description: "Alias FQDNs in punycode format.",
			},
		},
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "HostRecordIpv4Addr",
				Description: "holds one IPv4 address entry for a HostRecord.",
				Fields: []FieldDef{
					{Name: "Ipv4Addr", JSONName: "ipv4Addr", GoType: "string", Required: true, Description: "IPv4 address of the host (static or CIDR-allocated)."},
					{Name: "MAC", JSONName: "mac", GoType: goTypeString, Description: "MAC address for DHCP association."},
					{Name: "ConfigureForDHCP", JSONName: "configureForDhcp", GoType: goTypeBool, Description: "Whether to configure the address for DHCP."},
					{Name: "NextServer", JSONName: "nextServer", GoType: goTypeString, Description: "Next server address for PXE boot."},
				},
			},
			{
				TypeName:    "HostRecordIpv6Addr",
				Description: "holds one IPv6 address entry for a HostRecord.",
				Fields: []FieldDef{
					{Name: "Ipv6Addr", JSONName: "ipv6Addr", GoType: "string", Required: true, Description: "IPv6 address of the host."},
					{Name: "Duid", JSONName: "duid", GoType: goTypeString, Description: "DHCP unique identifier for IPv6 DHCP association."},
					{Name: "ConfigureForDHCP", JSONName: "configureForDhcp", GoType: goTypeBool, Description: "Whether to configure the address for DHCP."},
				},
			},
		},
	}
}
