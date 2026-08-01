package catalog

// zoneForward returns the ZoneForward resource descriptor.
//
// Source: tools/openapi/inventory.md, "### ZoneForward" section (fields
// request=0, response=1, both=11) — itself derived from the pinned
// infoblox-go-client/v2 SDK (tools/openapi/specs/infobloxopen/) and
// re-verified against a live NIOS Grid Manager appliance on 2026-07-28,
// which takes precedence over the static inventory scan for immutability.
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateZoneForward).
//
// Immutable fields: `fqdn`, `view`, `zone_format` are all absent from the
// UpdateZoneForward SDK method signature (confirmed against the actual SDK
// interface):
//
//	CreateZoneForward(comment, disable, eas, forwardTo, forwardersOnly, forwardingServers, fqdn, nsGroup, view, zoneFormat, externalNsGroup)
//	UpdateZoneForward(ref, comment, disable, eas, forwardTo, forwardersOnly, forwardingServers, nsGroup, externalNsGroup)
//
// `view` additionally rejects changes at the data level even where a schema
// might otherwise appear to permit it — WAPI returns "Cannot move zones
// between views".
//
// No cross-resource references: ZoneForward is not a reference source in
// the cross-resource reference map (it does not point at any other
// cataloged resource by name/ref/CIDR).
func zoneForward() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "ZoneForward",
		Slug:                 "zoneforward",
		ClusterGroup:         clusterGroup("zoneforward"),
		NamespacedGroup:      namespacedGroup("zoneforward"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "Fqdn",
				JSONName:    "fqdn",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Immutable:   true,
				Description: "The name of this DNS zone in FQDN format. Fixed at creation — confirmed absent from the UpdateZoneForward SDK method signature.",
			},
			{
				Name:        "ForwardTo",
				JSONName:    "forwardTo",
				GoType:      "[]NameServer",
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "The remote name servers to which the Infoblox appliance forwards queries for this zone.",
			},
			{
				Name:        "View",
				JSONName:    "view",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Immutable:   true,
				Description: "The name of the DNS view in which the zone resides, e.g. \"external\". Fixed at creation — confirmed absent from the UpdateZoneForward SDK method signature; the WAPI additionally rejects a view change at the data level with \"Cannot move zones between views\".",
			},
			{
				Name:        "ZoneFormat",
				JSONName:    "zoneFormat",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Immutable:   true,
				Description: "Determines the format of this zone (e.g. FORWARD, IPV4, IPV6). Fixed at creation — confirmed absent from the UpdateZoneForward SDK method signature.",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the zone; maximum 256 characters.",
			},
			{
				Name:        "Disable",
				JSONName:    "disable",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Determines whether the zone is disabled. When false, the zone is enabled.",
			},
			{
				Name:        "ForwardersOnly",
				JSONName:    "forwardersOnly",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Determines if the appliance sends queries to the servers in forwardTo only, and not to other internal or Internet root servers.",
			},
			{
				Name:        "NsGroup",
				JSONName:    "nsGroup",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "A forwarding stub server name server group name.",
			},
			{
				Name:        "ExternalNsGroup",
				JSONName:    "externalNsGroup",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "A forward stub server name server group name.",
			},
			{
				Name:        "ForwardingServers",
				JSONName:    "forwardingServers",
				GoType:      "[]ForwardingServer",
				Scope:       FieldScopeBoth,
				Description: "Per-Grid-member forwarding server overrides for this zone.",
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
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "NameServer",
				Description: "identifies an external DNS server used as a forwarding target.",
				Fields: []FieldDef{
					{Name: "Name", JSONName: "name", GoType: goTypeString, Description: "A resolvable domain name for the external DNS server."},
					{Name: "Address", JSONName: "address", GoType: goTypeString, Description: "The IPv4 or IPv6 address of the server."},
				},
			},
			{
				TypeName:    "ForwardingServer",
				Description: "overrides the forwarders used by a specific Grid member for this zone.",
				Fields: []FieldDef{
					{Name: "Name", JSONName: "name", GoType: goTypeString, Description: "The name of this Grid member in FQDN format."},
					{Name: "ForwardersOnly", JSONName: "forwardersOnly", GoType: goTypeBool, Description: "Determines if this Grid member sends queries to forwardTo only, and not to other internal or Internet root servers."},
					{Name: "ForwardTo", JSONName: "forwardTo", GoType: "[]NameServer", Description: "The remote name servers to which this Grid member forwards queries for this zone, overriding the zone-level forwardTo list."},
					{Name: "UseOverrideForwarders", JSONName: "useOverrideForwarders", GoType: goTypeBool, Description: "Use flag for forwardTo — when false this Grid member uses the zone-level forwarders instead of the override."},
				},
			},
		},
	}
}
