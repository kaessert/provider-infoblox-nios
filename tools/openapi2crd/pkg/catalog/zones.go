package catalog

// zoneAuth returns the ZoneAuth resource descriptor.
//
// Source: the Phase 6 live-verification convention recording corrections to
// tools/openapi/inventory.md's "### ZoneAuth" section — the SDK's
// ObjectManager wrapper exposes Create+Read+Delete only (no UpdateZoneAuth
// method), but a full CRUD lifecycle probe against a live NIOS Grid Manager
// appliance (WAPI v2.9.7) confirmed a direct WAPI PUT on the zone's `_ref`
// accepts comment/disable/soa_*/ns_group/extattrs/grid_primary/
// grid_secondaries/external_primaries/external_secondaries — the resource
// is CRUD, not create-read-delete, once the controller bypasses the SDK
// wrapper for updates.
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateZoneAuth, e.g. "zone_auth/ZG5z...:example.com/Internal"). `_ref` is
// STABLE — every field baked into it (fqdn, view, zone_format) is
// immutable, so the external-name annotation never needs to be updated
// after create.
//
// Immutable fields: `fqdn`/`zone_format` return WAPI's hard-immutable
// `supports=rws` (no `u`) signature — a PUT touching them fails with
// "Field is not allowed for update". `view` is soft-immutable
// (`supports=rwus`) but WAPI rejects any attempted change at the data
// level with "Cannot move zones between views" — so all three are treated
// as immutable (CEL `self == oldSelf`) regardless of the WAPI schema
// `supports` flag distinction.
//
// No cross-resource references: ZoneAuth is a reference TARGET (e.g.
// DTCLBDN's authZones field names a ZoneAuth by its FQDN) but does not
// itself reference any other cataloged resource.
func zoneAuth() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "ZoneAuth",
		Slug:                 "zoneauth",
		ClusterGroup:         clusterGroup("zoneauth"),
		NamespacedGroup:      namespacedGroup("zoneauth"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "FQDN",
				JSONName:    "fqdn",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Immutable:   true,
				Description: "Fully-qualified domain name of the zone, e.g. \"example.com\". Immutable — WAPI rejects a PUT that changes it with \"Field is not allowed for update\".",
			},
			{
				Name:        "View",
				JSONName:    "view",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Immutable:   true,
				Description: "DNS view in which the zone resides, e.g. \"external\". Defaults to the Grid's default view if unset at creation. Immutable — WAPI rejects a PUT that changes it with \"Cannot move zones between views\".",
			},
			{
				Name:        "ZoneFormat",
				JSONName:    "zoneFormat",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Immutable:   true,
				Enum:        []string{"FORWARD", "IPV4", "IPV6"},
				Description: "Format of the zone. Immutable — WAPI rejects a PUT that changes it with \"Field is not allowed for update\".",
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
				Name:        "SoaDefaultTTL",
				JSONName:    "soaDefaultTtl",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Minimum:     int64Ptr(ttlMinimumSeconds),
				Maximum:     int64Ptr(ttlMaximumSeconds),
				Description: "TTL (in seconds) of the SOA record of this zone — the length of time data from the zone is cached. Must be non-negative (0-2147483647); NIOS has no zone/grid-default sentinel for this field, so out-of-range values are rejected outright.",
			},
			{
				Name:        "SoaExpire",
				JSONName:    "soaExpire",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Description: "Number of seconds after which a secondary server stops answering for the zone because its data is considered too old to be useful.",
			},
			{
				Name:        "SoaNegativeTTL",
				JSONName:    "soaNegativeTtl",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Minimum:     int64Ptr(ttlMinimumSeconds),
				Maximum:     int64Ptr(ttlMaximumSeconds),
				Description: "Negative-caching TTL (in seconds) of the zone's SOA record — how long a secondary server caches \"Does Not Respond\" answers. Must be non-negative (0-2147483647); NIOS has no zone/grid-default sentinel for this field, so out-of-range values are rejected outright.",
			},
			{
				Name:        "SoaRefresh",
				JSONName:    "soaRefresh",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Description: "Interval (in seconds) at which a secondary server checks the primary server for fresh zone data.",
			},
			{
				Name:        "SoaRetry",
				JSONName:    "soaRetry",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Description: "Number of seconds a secondary server waits before retrying the primary server after a failed connection.",
			},
			{
				Name:        "NsGroup",
				JSONName:    "nsGroup",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Name server group that serves DNS for this zone.",
			},
			{
				Name:        "ExtAttrs",
				JSONName:    "extAttrs",
				GoType:      goTypeStringMap,
				Scope:       FieldScopeBoth,
				Description: "Extensible attributes (arbitrary key/value metadata defined in Grid Manager). The WAPI wire format wraps each value as {\"value\": ...}; this map is the simplified string-valued CRD representation (the controller translates to/from the SDK's EA map[string]interface{} type).",
			},
			{
				Name:        "GridPrimary",
				JSONName:    "gridPrimary",
				GoType:      "[]MemberServer",
				Scope:       FieldScopeBoth,
				Description: "Grid members that act as primary servers for this zone.",
			},
			{
				Name:        "GridSecondaries",
				JSONName:    "gridSecondaries",
				GoType:      "[]MemberServer",
				Scope:       FieldScopeBoth,
				Description: "Grid members that act as secondary servers for this zone.",
			},
			{
				Name:        "ExternalPrimaries",
				JSONName:    "externalPrimaries",
				GoType:      "[]ExternalServer",
				Scope:       FieldScopeBoth,
				Description: "External (non-Grid) primary servers for this zone.",
			},
			{
				Name:        "ExternalSecondaries",
				JSONName:    "externalSecondaries",
				GoType:      "[]ExternalServer",
				Scope:       FieldScopeBoth,
				Description: "External (non-Grid) secondary servers for this zone.",
			},
			{
				Name:        "Ref",
				JSONName:    "ref",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Server-assigned opaque object reference (WAPI `_ref`). Mirrors the crossplane.io/external-name annotation for observability and uptest import verification. Stable across the resource's lifetime — every field it embeds (fqdn, view, zoneFormat) is immutable.",
			},
		},
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "MemberServer",
				Description: "identifies a Grid member acting as a primary or secondary server for a zone (mirrors the SDK's Memberserver struct).",
				Fields: []FieldDef{
					{Name: "Name", JSONName: "name", GoType: goTypeString, Description: "Grid member name."},
					{Name: "Stealth", JSONName: "stealth", GoType: goTypeBool, Description: "Whether this Grid member is in stealth mode (its NS record is hidden from DNS queries)."},
					{Name: "GridReplicate", JSONName: "gridReplicate", GoType: goTypeBool, Description: "Whether zone data reaches this member via DNS zone transfer (true) or Grid replication (false)."},
					{Name: "Lead", JSONName: "lead", GoType: goTypeBool, Description: "Whether the Grid lead secondary performs zone transfers to non-lead secondaries."},
					{Name: "PreferredPrimaries", JSONName: "preferredPrimaries", GoType: "[]ExternalServer", Description: "Primary preference list of Grid members and/or external servers for this member."},
					{Name: "EnablePreferredPrimaries", JSONName: "enablePreferredPrimaries", GoType: goTypeBool, Description: "Whether preferredPrimaries is used for this member."},
				},
			},
			{
				TypeName:    "ExternalServer",
				Description: "identifies an external (non-Grid) DNS server acting as a primary or secondary for a zone, or a preferred-primaries entry (mirrors the SDK's NameServer struct).",
				Fields: []FieldDef{
					{Name: "Address", JSONName: "address", GoType: goTypeString, Description: "IPv4 or IPv6 address of the server."},
					{Name: "Name", JSONName: "name", GoType: goTypeString, Description: "Resolvable domain name of the external DNS server."},
					{Name: "Stealth", JSONName: "stealth", GoType: goTypeBool, Description: "Whether the NS record for this server is hidden from DNS queries."},
					{Name: "SharedWithMsParentDelegation", JSONName: "sharedWithMsParentDelegation", GoType: goTypeBool, Description: "Whether this name server is shared with the parent Microsoft primary zone's delegation server."},
					{Name: "TsigKey", JSONName: "tsigKey", GoType: goTypeString, Description: "Generated TSIG key."},
					{Name: "TsigKeyAlg", JSONName: "tsigKeyAlg", GoType: goTypeString, Description: "TSIG key algorithm."},
					{Name: "TsigKeyName", JSONName: "tsigKeyName", GoType: goTypeString, Description: "TSIG key name."},
					{Name: "UseTsigKeyName", JSONName: "useTsigKeyName", GoType: goTypeBool, Description: "Use flag for tsigKeyName."},
				},
			},
		},
	}
}
