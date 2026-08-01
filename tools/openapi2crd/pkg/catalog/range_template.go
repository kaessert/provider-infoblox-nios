package catalog

// rangeTemplate returns the RangeTemplate resource descriptor.
//
// Source: tools/openapi/inventory.md, "### RangeTemplate" section (fields
// request=1, response=1, both=11) — itself derived from the pinned
// infoblox-go-client/v2 SDK (tools/openapi/specs/infobloxopen/), confirmed
// against the CreateRangeTemplate/UpdateRangeTemplate ObjectManager wrapper
// signatures (object_manager_range_template.go) and the Rangetemplate
// struct (objects_generated.go). Not independently live-verified — inferred
// from static SDK analysis only (see inventory.md's "API Behavior
// (live-verified)" subsection: "no").
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateRangeTemplate). Name is mutable, so it cannot serve as the
// external-name.
//
// Immutable fields: none known — every CreateRangeTemplate parameter is
// also accepted by UpdateRangeTemplate:
//
//	CreateRangeTemplate(name, numberOfAdresses, offset, comment, ea, options, useOption, serverAssociationType, failOverAssociation, member, cloudApiCompatible, msServer)
//	UpdateRangeTemplate(ref, name, numberOfAddresses, offset, comment, ea, options, useOption, serverAssociationType, failOverAssociation, member, cloudApiCompatible, msServer)
//
// `msServer` is a write-only convenience parameter: the SDK wraps it into a
// nested Msdhcpserver struct (`{"ipv4addr": msServer}`) that is persisted on
// the response as the `ms_server` field — a different shape than the flat
// string this catalog accepts, so it is excluded from the AtProvider mirror
// (OmitFromObservation) rather than cataloged with a mismatched response
// type.
//
// No cross-resource references: RangeTemplate is not listed as a Source
// Resource in the blueprint's Cross-Resource References table — it is a
// config-only preset object referenced BY Range's `template` field
// (create-only convenience, not a persisted link), not a field on
// RangeTemplate itself.
func rangeTemplate() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "RangeTemplate",
		Slug:                 "rangetemplate",
		ClusterGroup:         clusterGroup("rangetemplate"),
		NamespacedGroup:      namespacedGroup("rangetemplate"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        fieldNameGo,
				JSONName:    fieldNameJSON,
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Name of the range template object.",
			},
			{
				Name:        "NumberOfAddresses",
				JSONName:    "numberOfAddresses",
				GoType:      "*uint32",
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Number of addresses the range should contain when instantiated.",
			},
			{
				Name:        "Offset",
				JSONName:    "offset",
				GoType:      "*uint32",
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Start address offset within the parent network when instantiated.",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the range template.",
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
				Name:        "Options",
				JSONName:    "options",
				GoType:      "[]*RangeTemplateOption",
				Scope:       FieldScopeBoth,
				Description: "DHCP options to apply to ranges created from this template.",
			},
			{
				Name:        "UseOptions",
				JSONName:    "useOptions",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for options.",
			},
			{
				Name:        "ServerAssociationType",
				JSONName:    "serverAssociationType",
				GoType:      "string",
				Scope:       FieldScopeBoth,
				Description: "Type of server that will serve ranges created from this template.",
			},
			{
				Name:        "FailoverAssociation",
				JSONName:    "failoverAssociation",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Failover association name.",
			},
			{
				Name:        "Member",
				JSONName:    "member",
				GoType:      "*RangeTemplateMember",
				Scope:       FieldScopeBoth,
				Description: "Grid member that will serve ranges created from this template.",
			},
			{
				Name:        "CloudApiCompatible",
				JSONName:    "cloudApiCompatible",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Whether this template can be used in cloud-computing deployments.",
			},
			{
				Name:                "MsServer",
				JSONName:            "msServer",
				GoType:              goTypeString,
				Scope:               FieldScopeRequest,
				OmitFromObservation: true,
				Description:         "Microsoft DHCP server name (write-only convenience parameter). The SDK wraps this into a nested Msdhcpserver struct persisted on the response as ms_server — a different shape than this flat field, so it is never echoed back and excluded from the AtProvider mirror.",
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
				TypeName:    "RangeTemplateOption",
				Description: "carries one DHCP option to apply to ranges created from a RangeTemplate (mirrors the SDK's Dhcpoption struct).",
				Fields: []FieldDef{
					{Name: fieldNameGo, JSONName: fieldNameJSON, GoType: goTypeString, Description: "Name of the DHCP option."},
					{Name: "Num", JSONName: "num", GoType: "*uint32", Description: "The code of the DHCP option."},
					{Name: "VendorClass", JSONName: "vendorClass", GoType: goTypeString, Description: "The name of the space this DHCP option is associated to."},
					{Name: "Value", JSONName: "value", GoType: goTypeString, Description: "Value of the DHCP option."},
					{Name: "UseOption", JSONName: "useOption", GoType: goTypeBool, Description: "Only applies to special options that are displayed separately from other options and have a use flag (routers, router-templates, domain-name-servers, domain-name, broadcast-address, broadcast-address-offset, dhcp-lease-time, dhcp6.name-servers)."},
				},
			},
			{
				TypeName:    "RangeTemplateMember",
				Description: "identifies the Grid member that will serve ranges created from a RangeTemplate (mirrors the SDK's Dhcpmember struct).",
				Fields: []FieldDef{
					{Name: "Ipv4Addr", JSONName: "ipv4addr", GoType: goTypeString, Description: "The IPv4 Address of the Grid Member."},
					{Name: "Ipv6Addr", JSONName: "ipv6addr", GoType: goTypeString, Description: "The IPv6 Address of the Grid Member."},
					{Name: fieldNameGo, JSONName: fieldNameJSON, GoType: goTypeString, Description: "The Grid member name."},
				},
			},
		},
	}
}
