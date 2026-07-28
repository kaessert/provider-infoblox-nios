package catalog

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
