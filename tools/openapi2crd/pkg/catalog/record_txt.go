package catalog

// txtRecord returns the TXTRecord resource descriptor.
//
// Source: tools/openapi/inventory.md, "### TXTRecord" section (fields
// request=0, response=14, both=7) — itself derived from the pinned
// infoblox-go-client/v2 SDK (tools/openapi/specs/infobloxopen/). The
// `view` field's immutability classification is corrected by the Phase 6
// live-verification ADR: TXTRecord is in the "soft immutable" group
// (WAPI schema reports `supports=rwus`, but a PUT that changes view is
// rejected at runtime with "The action is not allowed. A parent was not
// found."). The Go SDK's UpdateTXTRecord method omits the view parameter
// entirely, so this catalog treats it the same as ARecord's hard-immutable
// view: a CEL `self == oldSelf` rule on the ForProvider field.
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateTXTRecord). The _ref is UNSTABLE — it changes whenever `name` or
// `text` is updated (Phase 6 live-verification ADR, "_ref Stability"
// table).
//
// Immutable fields: `view` (see above). `zone` is response-only and
// derived from name+view by WAPI — it is not a CreateTXTRecord/
// UpdateTXTRecord parameter at all, so it has no ForProvider
// representation and no CEL rule is emitted for it (same treatment as
// ARecord's `zone`; see FieldDef.Immutable doc).
//
// `disable` appears on the underlying WAPI object but is NOT a parameter
// of CreateTXTRecord/UpdateTXTRecord (see inventory.md's Full Schema Notes
// for TXTRecord) — the SDK's ObjectManager wrapper cannot set it, so it is
// omitted from this catalog entirely rather than cataloged as a field with
// no working setter.
//
// No cross-resource references: TXTRecord is not listed as a source
// resource in the blueprint's Cross-Resource References table.
func txtRecord() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "TXTRecord",
		Slug:                 "recordtxt",
		ClusterGroup:         clusterGroup("recordtxt"),
		NamespacedGroup:      namespacedGroup("recordtxt"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "Name",
				JSONName:    "name",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Owner name (FQDN) the TXT record applies to. Renaming changes the record's _ref.",
			},
			{
				Name:        "Text",
				JSONName:    "text",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Text content, up to 255 bytes per substring and 512 bytes total. Quote to preserve leading/trailing/embedded spaces. Changing this value changes the record's _ref.",
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
				Description: "Time-to-live in seconds. Zero means the record is not cached.",
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
				Description: "DNS view in which the record resides, e.g. \"external\". Soft-immutable: the WAPI schema reports it as updatable, but a PUT that changes view is rejected at runtime (\"The action is not allowed. A parent was not found.\"), and the SDK's UpdateTXTRecord method omits the parameter entirely.",
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
				Description: "Zone in which the record resides, e.g. \"zone.com\". Derived from name/view by WAPI — not a CreateTXTRecord parameter, so it has no ForProvider counterpart.",
			},
			{
				Name:        "DNSName",
				JSONName:    "dnsName",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Record name in punycode format (derived from name).",
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
				GoType:      "*TXTRecordAwsRte53RecordInfo",
				Scope:       FieldScopeResponse,
				Description: "AWS Route 53 record information (cloud-managed records only).",
			},
			{
				Name:        "CloudInfo",
				JSONName:    "cloudInfo",
				GoType:      "*TXTRecordCloudInfo",
				Scope:       FieldScopeResponse,
				Description: "Cloud API related information for this object (cloud-managed records only).",
			},
			{
				Name:        "MsAdUserData",
				JSONName:    "msAdUserData",
				GoType:      "*TXTRecordMsAdUserData",
				Scope:       FieldScopeResponse,
				Description: "Microsoft Active Directory user information (MS-managed records only).",
			},
		},
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "TXTRecordAwsRte53RecordInfo",
				Description: "carries AWS Route 53 record information for a cloud-managed TXTRecord (mirrors the SDK's Awsrte53recordinfo struct).",
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
				TypeName:    "TXTRecordCloudInfo",
				Description: "carries Cloud API delegation/ownership information for a cloud-managed TXTRecord (mirrors the SDK's GridCloudapiInfo struct).",
				Fields: []FieldDef{
					{Name: "DelegatedMember", JSONName: "delegatedMember", GoType: "*TXTRecordCloudInfoDelegatedMember", Description: "The Cloud Platform Appliance to which authority of the object is delegated."},
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
				TypeName:    "TXTRecordCloudInfoDelegatedMember",
				Description: "identifies the Grid member a cloud-managed TXTRecord's authority is delegated to (mirrors the SDK's Dhcpmember struct).",
				Fields: []FieldDef{
					{Name: "Ipv4Addr", JSONName: "ipv4addr", GoType: goTypeString, Description: "The IPv4 Address of the Grid Member."},
					{Name: "Ipv6Addr", JSONName: "ipv6addr", GoType: goTypeString, Description: "The IPv6 Address of the Grid Member."},
					{Name: "Name", JSONName: "name", GoType: goTypeString, Description: "The Grid member name"},
				},
			},
			{
				TypeName:    "TXTRecordMsAdUserData",
				Description: "carries Microsoft Active Directory user information for an MS-managed TXTRecord (mirrors the SDK's MsserverAduserData struct).",
				Fields: []FieldDef{
					{Name: "ActiveUsersCount", JSONName: "activeUsersCount", GoType: goTypeInt64, Description: "The number of active users."},
				},
			},
		},
	}
}
