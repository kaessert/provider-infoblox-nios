package catalog

// dtcPool returns the DTCPool resource descriptor.
//
// Source: tools/openapi/inventory.md, "### DTCPool" section (fields
// request=0, response=3, both=17) — derived from the pinned
// infoblox-go-client/v2 SDK's DtcPool struct (WAPI object type dtc:pool)
// under tools/openapi/specs/infobloxopen/, corrected by ADR-IN-0004's
// live-verification pass (2026-07-28): `auto_consolidated_monitors` is an
// SDK-only field with no corresponding WAPI `_schema` entry and is
// deliberately NOT cataloged here.
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateDtcPool) — name is mutable, so it cannot serve as the
// external-name.
//
// Immutable fields: none known. Every CreateDtcPool parameter is also
// accepted by UpdateDtcPool.
//
// `monitors`: CreateDtcPool/UpdateDtcPool accept a polymorphic `[]Monitor`
// SDK interface (concrete types include DtcMonitorHttp, DtcMonitorIcmp,
// DtcMonitorTcp, DtcMonitorSip, DtcMonitorSnmp, DtcMonitorPdp — one WAPI
// monitor sub-type per protocol), while GetDtcPool echoes back the fully
// expanded monitor objects. None of those monitor types is itself
// cataloged as a managed resource yet, so — mirroring the same
// reference-by-string simplification already used for DTCServer.Monitors
// — this field is modeled as a slice of DTCPoolMonitor, each holding only
// the associated monitor's WAPI `_ref` string (e.g.
// "dtc:monitor:http/ZG5z..."), not the full monitor payload.
//
// `lbDynamicRatioPreferred`/`lbDynamicRatioAlternate`: both share the same
// underlying SDK type (SettingDynamicratio), modeled as a single
// DTCPoolDynamicRatio nested type reused for both fields.
//
// `consolidatedMonitors`/`health`: response-only (GetDtcPool only) —
// DTCPoolConsolidatedMonitorHealth mirrors the SDK's
// DtcPoolConsolidatedMonitorHealth struct; DTCPoolHealth mirrors the SDK's
// DtcHealth struct (same shape as DTCServerHealth, duplicated here because
// every scope package in this generator is self-contained).
//
// Cross-resource reference: `servers` is a list of DtcServerLink entries
// ({server _ref, ratio}); the `server` field names a DTCServer by
// external-name (tools/openapi/inventory.md's "Cross-Resource References"
// subsection for this resource lists DTCServer, cluster-scoped, as the
// target) — the standard three-field pattern (value + Ref + Selector) is
// carried on the nested DTCPoolServerLink.Server field.
func dtcPool() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "DTCPool",
		Slug:                 "dtcpool",
		ClusterGroup:         clusterGroup("dtcpool"),
		NamespacedGroup:      namespacedGroup("dtcpool"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "Name",
				JSONName:    "name",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "DTC Pool display name. Mutable, but changes are not reference-resolved on the fly by referencing DTCLBDN resources — see the DTCLBDN resource note when it is onboarded.",
			},
			{
				Name:        "LBPreferredMethod",
				JSONName:    "lbPreferredMethod",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Enum:        []string{"ROUND_ROBIN", "RATIO", "TOPOLOGY", "DYNAMIC_RATIO", "GLOBAL_AVAILABILITY", "SOURCE_IP_HASH"},
				Description: "Preferred load balancing method used to select a server from the pool.",
			},
			{
				Name:        "LBAlternateMethod",
				JSONName:    "lbAlternateMethod",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Enum:        []string{"NONE", "ROUND_ROBIN", "RATIO", "TOPOLOGY", "DYNAMIC_RATIO"},
				Description: "Fallback load balancing method used if lbPreferredMethod returns no results.",
			},
			{
				Name:        "Servers",
				JSONName:    "servers",
				GoType:      "[]DTCPoolServerLink",
				Scope:       FieldScopeBoth,
				Description: "DTC Servers that are members of this pool, each with a per-server ratio/weight.",
			},
			{
				Name:        "Availability",
				JSONName:    "availability",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Enum:        []string{"ANY", "QUORUM", "ALL"},
				Description: "Determines when a pool member is considered available: ANY, at least QUORUM, or ALL of its monitors report it up.",
			},
			{
				Name:        "Quorum",
				JSONName:    "quorum",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Description: "Minimum number of monitors that must report a resource as up for it to be available. Only meaningful when availability=QUORUM.",
			},
			{
				Name:        "LBPreferredTopology",
				JSONName:    "lbPreferredTopology",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Topology ruleset name used when lbPreferredMethod=TOPOLOGY.",
			},
			{
				Name:        "LBDynamicRatioPreferred",
				JSONName:    "lbDynamicRatioPreferred",
				GoType:      "*DTCPoolDynamicRatio",
				Scope:       FieldScopeBoth,
				Description: "Dynamic-ratio settings used when lbPreferredMethod=DYNAMIC_RATIO.",
			},
			{
				Name:        "LBAlternateTopology",
				JSONName:    "lbAlternateTopology",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Topology ruleset name used when lbAlternateMethod=TOPOLOGY.",
			},
			{
				Name:        "LBDynamicRatioAlternate",
				JSONName:    "lbDynamicRatioAlternate",
				GoType:      "*DTCPoolDynamicRatio",
				Scope:       FieldScopeBoth,
				Description: "Dynamic-ratio settings used when lbAlternateMethod=DYNAMIC_RATIO.",
			},
			{
				Name:        "Monitors",
				JSONName:    "monitors",
				GoType:      "[]DTCPoolMonitor",
				Scope:       FieldScopeBoth,
				Description: "Health monitors associated with the pool.",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the DTC Pool; maximum 256 characters.",
			},
			{
				Name:        "Disable",
				JSONName:    "disable",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Whether the DTC Pool is disabled.",
			},
			{
				Name:        "TTL",
				JSONName:    "ttl",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Description: "Time-To-Live for the DTC Pool response, in seconds. Zero indicates the record should not be cached.",
			},
			{
				Name:        "UseTTL",
				JSONName:    "useTtl",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for ttl.",
			},
			{
				Name:        "ExtAttrs",
				JSONName:    "extattrs",
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
				Name:        "ConsolidatedMonitors",
				JSONName:    "consolidatedMonitors",
				GoType:      "[]DTCPoolConsolidatedMonitorHealth",
				Scope:       FieldScopeResponse,
				Description: "Consolidated monitor health status shared across pool members. Read-only — Update accepts a differently-shaped payload for this field via the SDK's generic consolidatedMonitors parameter, not this typed representation.",
			},
			{
				Name:        "Health",
				JSONName:    "health",
				GoType:      "*DTCPoolHealth",
				Scope:       FieldScopeResponse,
				Description: "Aggregate health status of the pool.",
			},
		},
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "DTCPoolServerLink",
				Description: "identifies one DTCServer member of a DTCPool along with its load-balancing weight (mirrors the SDK's DtcServerLink struct).",
				Fields: []FieldDef{
					{
						Name:        "Server",
						JSONName:    "server",
						GoType:      goTypeString,
						Description: "DTCServer member of the pool, identified by its Crossplane external-name (the DTCServer's WAPI `_ref`).",
						Reference: &ReferenceDescriptor{
							TargetKind:  "DTCServer",
							TargetSlug:  "dtcserver",
							TargetScope: "cluster",
						},
					},
					{Name: "Ratio", JSONName: "ratio", GoType: goTypeUint32, Description: "The load-balancing weight of the server within the pool."},
				},
			},
			{
				TypeName:    "DTCPoolMonitor",
				Description: "identifies a health monitor associated with a DTCPool by its WAPI `_ref` (mirrors the SDK's polymorphic Monitor interface, simplified to a reference — no monitor sub-type is independently cataloged today).",
				Fields: []FieldDef{
					{Name: "Monitor", JSONName: "monitor", GoType: goTypeString, Description: "Reference (`_ref`) of the monitor associated with the pool, e.g. \"dtc:monitor:http/ZG5z...\"."},
				},
			},
			{
				TypeName:    "DTCPoolDynamicRatio",
				Description: "carries dynamic-ratio load balancing settings for a DTCPool (mirrors the SDK's SettingDynamicratio struct); reused for both lbDynamicRatioPreferred and lbDynamicRatioAlternate.",
				Fields: []FieldDef{
					{Name: "Method", JSONName: "method", GoType: goTypeString, Description: "The method of the DTC dynamic ratio load balancing."},
					{Name: "Monitor", JSONName: "monitor", GoType: goTypeString, Description: "The DTC monitor whose output is used for dynamic ratio load balancing."},
					{Name: "MonitorMetric", JSONName: "monitorMetric", GoType: goTypeString, Description: "The metric of the DTC SNMP monitor used for dynamic weighing."},
					{Name: "MonitorWeighing", JSONName: "monitorWeighing", GoType: goTypeString, Description: "How the DTC monitor weight is applied: PRIORITY forwards clients to the least loaded server; RATIO calculates distribution from dynamic weights."},
					{Name: "InvertMonitorMetric", JSONName: "invertMonitorMetric", GoType: goTypeBool, Description: "Whether the inverted values of the DTC SNMP monitor metric are used."},
				},
			},
			{
				TypeName:    "DTCPoolConsolidatedMonitorHealth",
				Description: "carries consolidated monitor health status shared across pool members (mirrors the SDK's DtcPoolConsolidatedMonitorHealth struct).",
				Fields: []FieldDef{
					{Name: "Members", JSONName: "members", GoType: "[]string", Description: "Members whose monitor statuses are shared across other members in the pool."},
					{Name: "Monitor", JSONName: "monitor", GoType: goTypeString, Description: "Monitor whose statuses are shared across other members in the pool."},
					{Name: "Availability", JSONName: "availability", GoType: goTypeString, Description: "Servers assigned to the pool with this monitor are healthy if ANY or ALL members report a healthy status."},
					{Name: "FullHealthCommunication", JSONName: "fullHealthCommunication", GoType: goTypeBool, Description: "Whether health checks are performed on each DTC Grid member serving related LBDN(s) and shared across all DTC Grid members, not just the selected list."},
				},
			},
			{
				TypeName:    "DTCPoolHealth",
				Description: "carries the aggregate health status of a DTCPool (mirrors the SDK's DtcHealth struct).",
				Fields: []FieldDef{
					{Name: "Availability", JSONName: "availability", GoType: goTypeString, Description: "The availability color status."},
					{Name: "Description", JSONName: "description", GoType: goTypeString, Description: "The textual description of the object's status."},
					{Name: "EnabledState", JSONName: "enabledState", GoType: goTypeString, Description: "The enabled state of the object."},
				},
			},
		},
	}
}

// dtcServer returns the DTCServer resource descriptor.
//
// Source: tools/openapi/inventory.md, "### DTCServer" section (fields
// request=0, response=2, both=9) — derived from the pinned
// infoblox-go-client/v2 SDK's DtcServer struct (WAPI object type
// dtc:server) under tools/openapi/specs/infobloxopen/.
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateDtcServer) — name is mutable, so it cannot serve as the
// external-name.
//
// Immutable fields: none known. Every CreateDtcServer parameter is also
// accepted by UpdateDtcServer.
//
// No cross-resource references: DTCServer is a reference TARGET for
// DTCPool (a DTCPool's servers field names DTCServer instances by
// external-name) but does not itself reference any other cataloged
// resource.
func dtcServer() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "DTCServer",
		Slug:                 "dtcserver",
		ClusterGroup:         clusterGroup("dtcserver"),
		NamespacedGroup:      namespacedGroup("dtcserver"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "Name",
				JSONName:    "name",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "DTC Server display name.",
			},
			{
				Name:        "Host",
				JSONName:    "host",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Address or FQDN of the backend server.",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the DTC Server; maximum 256 characters.",
			},
			{
				Name:        "Disable",
				JSONName:    "disable",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Whether the DTC Server is disabled.",
			},
			{
				Name:        "AutoCreateHostRecord",
				JSONName:    "autoCreateHostRecord",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Whether a read-only A/AAAA/CNAME host record is auto-created for host and kept in sync with it.",
			},
			{
				Name:        "SniHostname",
				JSONName:    "sniHostname",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Hostname for Server Name Indication (SNI), in FQDN format.",
			},
			{
				Name:        "UseSniHostname",
				JSONName:    "useSniHostname",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for sniHostname.",
			},
			{
				Name:        "Monitors",
				JSONName:    "monitors",
				GoType:      "[]DTCServerMonitor",
				Scope:       FieldScopeBoth,
				Description: "IP/FQDN and monitor pairs used for additional health monitoring.",
			},
			{
				Name:        "ExtAttrs",
				JSONName:    "extattrs",
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
				Name:        "Health",
				JSONName:    "health",
				GoType:      "*DTCServerHealth",
				Scope:       FieldScopeResponse,
				Description: "Health status of the server.",
			},
		},
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "DTCServerMonitor",
				Description: "identifies one IP/FQDN and monitor pair used for additional health monitoring of a DTCServer (mirrors the SDK's DtcServerMonitor struct).",
				Fields: []FieldDef{
					{Name: "Monitor", JSONName: "monitor", GoType: goTypeString, Description: "Reference of the monitor associated with the server."},
					{Name: "Host", JSONName: "host", GoType: goTypeString, Description: "IP address or FQDN of the server used for monitoring."},
				},
			},
			{
				TypeName:    "DTCServerHealth",
				Description: "carries the aggregate health status of a DTCServer (mirrors the SDK's DtcHealth struct).",
				Fields: []FieldDef{
					{Name: "Availability", JSONName: "availability", GoType: goTypeString, Description: "The availability color status."},
					{Name: "Description", JSONName: "description", GoType: goTypeString, Description: "The textual description of the object's status."},
					{Name: "EnabledState", JSONName: "enabledState", GoType: goTypeString, Description: "The enabled state of the object."},
				},
			},
		},
	}
}
