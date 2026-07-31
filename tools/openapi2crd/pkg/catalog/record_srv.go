package catalog

// srvRecord returns the SRVRecord resource descriptor.
//
// Source: tools/openapi/inventory.md, "### SRVRecord" section (fields
// request=0, response=15, both=10) — itself derived from the pinned
// infoblox-go-client/v2 SDK (tools/openapi/specs/infobloxopen/). Not yet
// live-verified against a real NIOS Grid Manager appliance (inventory.md
// records "Live-verified: no" for this resource); delete behavior is
// inferred from the live-verified ARecord pattern (hard-delete, 404 on
// subsequent GET).
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateSRVRecord).
//
// Immutable fields: `view` is a CreateSRVRecord parameter absent from
// UpdateSRVRecord's parameter list, matching the pattern confirmed live for
// ARecord/AAAARecord/PTRRecord — every DNS record's _ref is tied to
// (view, zone, name) and WAPI does not support moving a record between
// views via update. `zone` is derived from name+view by WAPI, not a
// CreateSRVRecord parameter at all, so it has no ForProvider representation
// and no CEL rule is emitted for it (see FieldDef.Immutable doc).
//
// `name`, `target`, `priority`, `weight`, and `port` are all accepted by
// both CreateSRVRecord and UpdateSRVRecord — they are mutable via the SDK
// wrapper even though changing any of them changes the record's identity
// (and therefore its `_ref`) at the WAPI level. That _ref churn is an
// operational characteristic (the controller must re-observe with the new
// _ref after such an update), not a CEL-enforced immutability constraint.
//
// `disable` appears on the underlying WAPI object but is NOT a parameter of
// CreateSRVRecord/UpdateSRVRecord — the SDK's ObjectManager wrapper cannot
// set it, so it is omitted from this catalog entirely rather than cataloged
// as a field with no working setter.
//
// No cross-resource references: SRVRecord is not listed as a reference
// source in the blueprint's cross-resource reference map, and no other
// cataloged resource references it either.
func srvRecord() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "SRVRecord",
		Slug:                 "recordsrv",
		ClusterGroup:         clusterGroup("recordsrv"),
		NamespacedGroup:      namespacedGroup("recordsrv"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "Name",
				JSONName:    "name",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Owner name in FQDN format the SRV record applies to. Changing this changes the record's _ref.",
			},
			{
				Name:        "Target",
				JSONName:    "target",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Target host in FQDN format. Changing this changes the record's _ref.",
			},
			{
				Name:        "Priority",
				JSONName:    "priority",
				GoType:      goTypeInt64,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Priority of the record (0-65535) — lower values are preferred. Changing this changes the record's _ref.",
			},
			{
				Name:        "Weight",
				JSONName:    "weight",
				GoType:      goTypeInt64,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Relative weight (0-65535) for records with the same priority. Changing this changes the record's _ref.",
			},
			{
				Name:        "Port",
				JSONName:    "port",
				GoType:      goTypeInt64,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "TCP/UDP port (0-65535) on the target host. Changing this changes the record's _ref.",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the record; maximum 256 characters.",
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
				Name:        "ExtAttrs",
				JSONName:    "extAttrs",
				GoType:      goTypeStringMap,
				Scope:       FieldScopeBoth,
				Description: "Extensible attributes (arbitrary key/value metadata defined in Grid Manager). The WAPI wire format wraps each value as {\"value\": ...}; this map is the simplified string-valued CRD representation (the controller translates to/from the SDK's EA map[string]interface{} type).",
			},
			{
				Name:        "View",
				JSONName:    "view",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Immutable:   true,
				Description: "DNS view in which the record resides, e.g. \"external\". Fixed at creation — WAPI ties the record's _ref to (view, zone, name). Confirmed absent from the UpdateSRVRecord SDK method signature.",
			},
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
				Description: "Zone in which the record resides, e.g. \"zone.com\". Derived from name/view by WAPI — not a CreateSRVRecord parameter, so it has no ForProvider counterpart.",
			},
			{
				Name:        "DNSName",
				JSONName:    "dnsName",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Record name in punycode format (derived from name).",
			},
			{
				Name:        "DNSTarget",
				JSONName:    "dnsTarget",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Target host in punycode format (derived from target).",
			},
			{
				Name:        "Creator",
				JSONName:    "creator",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Record creator. Changing to/from 'SYSTEM' is not allowed.",
			},
			{
				Name:        "CreationTime",
				JSONName:    "creationTime",
				GoType:      goTypeInt64,
				Scope:       FieldScopeResponse,
				Description: "Record creation time (Unix epoch seconds).",
			},
			{
				Name:        "LastQueried",
				JSONName:    "lastQueried",
				GoType:      goTypeInt64,
				Scope:       FieldScopeResponse,
				Description: "Time of the last DNS query for this record (Unix epoch seconds).",
			},
			{
				Name:        "DdnsPrincipal",
				JSONName:    "ddnsPrincipal",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "GSS-TSIG principal that owns this record (DDNS-created records only).",
			},
			{
				Name:        "DdnsProtected",
				JSONName:    "ddnsProtected",
				GoType:      goTypeBool,
				Scope:       FieldScopeResponse,
				Description: "Whether DDNS updates for this record are protected.",
			},
			{
				Name:        "ForbidReclamation",
				JSONName:    "forbidReclamation",
				GoType:      goTypeBool,
				Scope:       FieldScopeResponse,
				Description: "Whether reclamation is forbidden for the record (DNS discovery feature).",
			},
			{
				Name:        "Reclaimable",
				JSONName:    "reclaimable",
				GoType:      goTypeBool,
				Scope:       FieldScopeResponse,
				Description: "Whether the record is reclaimable (DNS discovery feature).",
			},
			{
				Name:        "SharedRecordGroup",
				JSONName:    "sharedRecordGroup",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Name of the shared record group, if this is a shared record.",
			},
			{
				Name:        "AwsRte53RecordInfo",
				JSONName:    "awsRte53RecordInfo",
				GoType:      "*SRVRecordAwsRte53RecordInfo",
				Scope:       FieldScopeResponse,
				Description: "AWS Route 53 record information (cloud-managed records only).",
			},
			{
				Name:        "CloudInfo",
				JSONName:    "cloudInfo",
				GoType:      "*SRVRecordCloudInfo",
				Scope:       FieldScopeResponse,
				Description: "Cloud API related information for this object (cloud-managed records only).",
			},
			{
				Name:        "MsAdUserData",
				JSONName:    "msAdUserData",
				GoType:      "*SRVRecordMsAdUserData",
				Scope:       FieldScopeResponse,
				Description: "Microsoft Active Directory user information (MS-managed records only).",
			},
		},
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "SRVRecordAwsRte53RecordInfo",
				Description: "carries AWS Route 53 record information for a cloud-managed SRVRecord (mirrors the SDK's Awsrte53recordinfo struct).",
				Fields: []FieldDef{
					{Name: "AliasTargetDnsName", JSONName: "aliasTargetDnsName", GoType: goTypeString, Description: "DNS name of the alias target."},
					{Name: "AliasTargetHostedZoneId", JSONName: "aliasTargetHostedZoneId", GoType: goTypeString, Description: "Hosted zone ID of the alias target."},
					{Name: "AliasTargetEvaluateTargetHealth", JSONName: "aliasTargetEvaluateTargetHealth", GoType: goTypeBool, Description: "Indicates if Amazon Route 53 evaluates the health of the alias target."},
					{Name: "Failover", JSONName: "failover", GoType: goTypeString, Description: "Indicates whether this is the primary or secondary resource record for Amazon Route 53 failover routing."},
					{Name: "GeolocationContinentCode", JSONName: "geolocationContinentCode", GoType: goTypeString, Description: "Continent code for Amazon Route 53 geolocation routing."},
					{Name: "GeolocationCountryCode", JSONName: "geolocationCountryCode", GoType: goTypeString, Description: "Country code for Amazon Route 53 geolocation routing."},
					{Name: "GeolocationSubdivisionCode", JSONName: "geolocationSubdivisionCode", GoType: goTypeString, Description: "Subdivision code for Amazon Route 53 geolocation routing."},
					{Name: "HealthCheckId", JSONName: "healthCheckId", GoType: goTypeString, Description: "ID of the health check that Amazon Route 53 performs for this resource record."},
					{Name: "Region", JSONName: "region", GoType: goTypeString, Description: "Amazon EC2 region where this resource record resides for latency routing."},
					{Name: "SetIdentifier", JSONName: "setIdentifier", GoType: goTypeString, Description: "An identifier that differentiates records with the same DNS name and type for weighted, latency, geolocation, and failover routing."},
					{Name: "Type", JSONName: "type", GoType: goTypeString, Description: "Type of Amazon Route 53 resource record."},
					{Name: "Weight", JSONName: "weight", GoType: goTypeInt64, Description: "Value that determines the portion of traffic for this record in weighted routing. The range is from 0 to 255."},
				},
			},
			{
				TypeName:    "SRVRecordCloudInfo",
				Description: "carries Cloud API delegation/ownership information for a cloud-managed SRVRecord (mirrors the SDK's GridCloudapiInfo struct).",
				Fields: []FieldDef{
					{Name: "DelegatedMember", JSONName: "delegatedMember", GoType: "*SRVRecordCloudInfoDelegatedMember", Description: "The Cloud Platform Appliance to which authority of the object is delegated."},
					{Name: "DelegatedScope", JSONName: "delegatedScope", GoType: goTypeString, Description: "Indicates the scope of delegation for the object. This can be one of the following: NONE (outside any delegation), ROOT (the delegation point), SUBTREE (within the scope of a delegation), RECLAIMING (within the scope of a delegation being reclaimed, either as the delegation point or in the subtree)."},
					{Name: "DelegatedRoot", JSONName: "delegatedRoot", GoType: goTypeString, Description: "Indicates the root of the delegation if delegated_scope is SUBTREE or RECLAIMING. This is not set otherwise."},
					{Name: "OwnedByAdaptor", JSONName: "ownedByAdaptor", GoType: goTypeBool, Description: "Determines whether the object was created by the cloud adapter or not."},
					{Name: "Usage", JSONName: "usage", GoType: goTypeString, Description: "Indicates the cloud origin of the object."},
					{Name: "Tenant", JSONName: "tenant", GoType: goTypeString, Description: "Reference to the tenant object associated with the object, if any."},
					{Name: "MgmtPlatform", JSONName: "mgmtPlatform", GoType: goTypeString, Description: "Indicates the specified cloud management platform."},
					{Name: "AuthorityType", JSONName: "authorityType", GoType: goTypeString, Description: "Type of authority over the object."},
				},
			},
			{
				TypeName:    "SRVRecordCloudInfoDelegatedMember",
				Description: "identifies the Grid member a cloud-managed SRVRecord's authority is delegated to (mirrors the SDK's Dhcpmember struct).",
				Fields: []FieldDef{
					{Name: "Ipv4Addr", JSONName: "ipv4addr", GoType: goTypeString, Description: "The IPv4 Address of the Grid Member."},
					{Name: "Ipv6Addr", JSONName: "ipv6addr", GoType: goTypeString, Description: "The IPv6 Address of the Grid Member."},
					{Name: "Name", JSONName: "name", GoType: goTypeString, Description: "The Grid member name"},
				},
			},
			{
				TypeName:    "SRVRecordMsAdUserData",
				Description: "carries Microsoft Active Directory user information for an MS-managed SRVRecord (mirrors the SDK's MsserverAduserData struct).",
				Fields: []FieldDef{
					{Name: "ActiveUsersCount", JSONName: "activeUsersCount", GoType: goTypeInt64, Description: "The number of active users."},
				},
			},
		},
	}
}
