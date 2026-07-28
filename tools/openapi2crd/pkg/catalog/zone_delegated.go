package catalog

// zoneDelegated returns the ZoneDelegated resource descriptor.
//
// Source: tools/openapi/inventory.md, "### ZoneDelegated" section (fields
// request=0, response=1, both=11) — derived from the pinned
// infoblox-go-client/v2 SDK (tools/openapi/specs/infobloxopen/). Not
// independently live-verified against a real NIOS Grid Manager appliance
// (see inventory.md's "Live-verified: no" note for this resource); the
// immutability of fqdn/view/zone_format is confirmed by comparing the
// CreateZoneDelegated and UpdateZoneDelegated SDK method signatures
// directly:
//
//	CreateZoneDelegated(fqdn, delegateTo, comment, disable, locked, nsGroup, delegatedTtl, useDelegatedTtl, ea, view, zoneFormat)
//	UpdateZoneDelegated(ref, delegateTo, comment, disable, locked, nsGroup, delegatedTtl, useDelegatedTtl, ea)
//
// `view` is additionally rejected at the data level — attempting to move an
// existing delegated zone between views returns "Cannot move zones between
// views" from the WAPI.
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateZoneDelegated).
//
// `delegateTo` (WAPI `delegate_to`) is the list of remote name servers the
// zone is delegated to. The SDK represents it as NullableNameServers, a
// hand-marshaled wrapper around []NameServer used to distinguish an absent
// value from an explicit empty list; this catalog models the CRD-facing
// field as a plain slice of the simplified NameServer shape (name,
// address), the same simplification ARecord/Network apply to the EA map.
//
// No cross-resource references: ZoneDelegated does not reference any other
// cataloged resource — `nsGroup` names a Grid nameserver group, which is
// Grid configuration rather than a resource type in this catalog.
func zoneDelegated() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "ZoneDelegated",
		Slug:                 "zonedelegated",
		ClusterGroup:         clusterGroup("zonedelegated"),
		NamespacedGroup:      namespacedGroup("zonedelegated"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "Fqdn",
				JSONName:    "fqdn",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Immutable:   true,
				Description: "FQDN of the delegated zone. Fixed at creation — confirmed absent from the UpdateZoneDelegated SDK method signature.",
			},
			{
				Name:        "DelegateTo",
				JSONName:    "delegateTo",
				GoType:      "[]ZoneDelegatedNameServer",
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "List of remote name servers the zone is delegated to. The Infoblox appliance redirects queries for the delegated zone to these servers.",
			},
			{
				Name:        "View",
				JSONName:    "view",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Immutable:   true,
				Description: "DNS view in which the zone resides, e.g. \"external\". Fixed at creation — confirmed absent from the UpdateZoneDelegated SDK method signature. The WAPI additionally rejects moving an existing zone between views at the data level.",
			},
			{
				Name:        "ZoneFormat",
				JSONName:    "zoneFormat",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Immutable:   true,
				Description: "Format of the zone (e.g. FORWARD, IPV4, IPV6). Fixed at creation — confirmed absent from the UpdateZoneDelegated SDK method signature.",
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
				Name:        "Locked",
				JSONName:    "locked",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "If set, other administrators cannot make conflicting changes to this zone. The zone continues to serve DNS data while locked.",
			},
			{
				Name:        "NsGroup",
				JSONName:    "nsGroup",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Delegation name server group bound to this delegated zone.",
			},
			{
				Name:        "DelegatedTTL",
				JSONName:    "delegatedTtl",
				GoType:      "*uint32",
				Scope:       FieldScopeBoth,
				Description: "Time-to-live, in seconds, of the auto-generated NS and glue records for this delegation.",
			},
			{
				Name:        "UseDelegatedTTL",
				JSONName:    "useDelegatedTtl",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for delegatedTtl — when false the zone/grid default TTL applies.",
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
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "ZoneDelegatedNameServer",
				Description: "identifies one remote name server a delegated zone forwards queries to (mirrors the SDK's NameServer struct).",
				Fields: []FieldDef{
					{Name: "Name", JSONName: "name", GoType: goTypeString, Description: "A resolvable domain name for the external DNS server."},
					{Name: "Address", JSONName: "address", GoType: goTypeString, Description: "The IPv4 Address or IPv6 Address of the server."},
				},
			},
		},
	}
}
