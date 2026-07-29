package catalog

// fixedAddress returns the FixedAddress resource descriptor.
//
// Source: tools/openapi/inventory.md, "### FixedAddress" section (fields
// request=0, response=3, both=16) and manifests blueprint decision
// ADR-IN-0004 (Phase 6 live API verification), both derived from the
// pinned infoblox-go-client/v2 SDK (tools/openapi/specs/infobloxopen/,
// object_manager_fixed_address.go / objects.go's FixedAddress struct).
//
// WAPI object type: runtime-selected — "fixedaddress" for IPv4,
// "ipv6fixedaddress" for IPv6 (NewEmptyFixedAddress(isIPv6 bool)). The Go
// SDK struct is shared by both; which fields are meaningful depends on
// the family.
//
// Pattern: CRUD, but NON-STANDARD — creation goes through AllocateIP, not
// a CreateFixedAddress-named method (see object_manager_fixed_address.go).
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// AllocateIP). UNSTABLE — changes when `ipv4addr` changes (ADR-IN-0004).
//
// Immutable fields: none known. The IPv4/IPv6 address family is fixed at
// creation (an object cannot switch families — WAPI would need a delete +
// recreate against the other endpoint) but this is enforced by which
// endpoint the controller calls, not by an SDK-immutable field, so no CEL
// self==oldSelf rule is emitted for ipv4addr/ipv6addr.
//
// Address family: ipv4addr and ipv6addr are mutually exclusive — AllocateIP
// takes a single isIPv6 flag that selects which one applies. Modeled as a
// resource-level (cross-field) CEL rule (exactly one must be set) rather
// than a per-field Required marker, since marking both fields
// unconditionally Required would force every FixedAddress to set both
// addresses simultaneously, contradicting the mutual exclusivity.
//
// `mac`: AllocateIP's signature reveals this is conditionally required, not
// universally required as a flat reading of the field table might suggest.
// For IPv6 (where this field carries the DUID), AllocateIP returns an error
// if it is empty. For IPv4, AllocateIP defaults it to the zero MAC address
// (00:00:00:00:00:00) when omitted and no match_client is set. Modeled as
// optional (not catalog Required) to avoid forcing a value that the SDK is
// willing to default — this corrects a stricter reading in the wave's
// orchestration ticket, verified directly against
// object_manager_fixed_address.go's AllocateIP implementation.
//
// `duid`: exists on the Go SDK struct (shared by both fixedaddress and
// ipv6fixedaddress object types) but is only ever populated by the WAPI for
// ipv6fixedaddress — it does not exist on the plain fixedaddress (IPv4)
// object per ADR-IN-0004's live verification. Response-only (AtProvider),
// documented accordingly.
//
// Cross-resource reference: `networkView` identifies a NetworkView by name
// (the target's Name field is stable, unlike its `_ref` which changes on
// rename). The default reference extractor (reference.ExternalName(),
// reading the target's crossplane.io/external-name annotation) is used per
// this provider's cross-resource reference convention — same choice made
// for NetworkContainer's networkView field.
func fixedAddress() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "FixedAddress",
		Slug:                 "fixedaddress",
		ClusterGroup:         clusterGroup("fixedaddress"),
		NamespacedGroup:      namespacedGroup("fixedaddress"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "IPv4Addr",
				JSONName:    "ipv4addr",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "IPv4 address of the fixed address. Mutually exclusive with ipv6addr — exactly one of the two must be set. May be set statically or allocated dynamically from a CIDR (network) at create time. Changing this value changes the record's _ref (UNSTABLE external name, ADR-IN-0004).",
			},
			{
				Name:        "IPv6Addr",
				JSONName:    "ipv6addr",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "IPv6 address of the fixed address. Mutually exclusive with ipv4addr — exactly one of the two must be set. Selects the ipv6fixedaddress WAPI object type.",
			},
			{
				Name:        "MAC",
				JSONName:    "mac",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "MAC address for MAC_ADDRESS/CIRCUIT_ID/REMOTE_ID match_client modes (IPv4 only — for IPv6 this field carries the DHCP unique identifier and AllocateIP rejects an empty value). For IPv4, the SDK defaults this to the zero MAC address (00:00:00:00:00:00) when omitted and no match_client is set, so it is not catalog-Required.",
			},
			{
				Name:        "NetworkView",
				JSONName:    "networkView",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Network view the fixed address belongs to. Defaults to \"default\" when unset. Identifies a NetworkView by name.",
				Reference: &ReferenceDescriptor{
					TargetKind:  "NetworkView",
					TargetSlug:  slugNetworkView,
					TargetScope: "cluster",
				},
			},
			{
				Name:        "Network",
				JSONName:    "network",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "CIDR used to resolve a dynamically-allocated address at create time, e.g. \"10.0.0.0/24\". Only meaningful when ipv4addr/ipv6addr is not a literal address.",
			},
			{
				Name:        fieldNameName,
				JSONName:    jsonNameName,
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Display name of the fixed address.",
			},
			{
				Name:     "MatchClient",
				JSONName: "matchClient",
				GoType:   goTypeString,
				Scope:    FieldScopeBoth,
				Enum:     []string{"MAC_ADDRESS", "CLIENT_ID", "RESERVED", "CIRCUIT_ID", "REMOTE_ID"},
				Description: "How the fixed IP is matched to a requesting client. CIRCUIT_ID requires agentCircuitId; REMOTE_ID requires agentRemoteId; " +
					"CLIENT_ID requires dhcpClientIdentifier (business-rule constraints enforced by WAPI at apply time, not by CRD schema validation).",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the fixed address.",
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
				Description: "Whether the fixed address is disabled.",
			},
			{
				Name:        "AgentCircuitID",
				JSONName:    "agentCircuitId",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Agent circuit ID. Required by WAPI when matchClient is CIRCUIT_ID (business rule, not CRD-enforced).",
			},
			{
				Name:        "AgentRemoteID",
				JSONName:    "agentRemoteId",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Agent remote ID. Required by WAPI when matchClient is REMOTE_ID (business rule, not CRD-enforced).",
			},
			{
				Name:        "ClientIdentifierPrependZero",
				JSONName:    "clientIdentifierPrependZero",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Whether a leading zero is prepended to the client identifier.",
			},
			{
				Name:        "DHCPClientIdentifier",
				JSONName:    "dhcpClientIdentifier",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "DHCP client identifier. Required by WAPI when matchClient is CLIENT_ID (business rule, not CRD-enforced).",
			},
			{
				Name:        "Options",
				JSONName:    "options",
				GoType:      "[]*FixedAddressDhcpOption",
				Scope:       FieldScopeBoth,
				Description: "DHCP options for the fixed address.",
			},
			{
				Name:        "UseOptions",
				JSONName:    "useOptions",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for options.",
			},
			{
				Name:        "Ref",
				JSONName:    "ref",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Server-assigned opaque object reference (WAPI `_ref`). Mirrors the crossplane.io/external-name annotation for observability and uptest import verification. UNSTABLE — changes when ipv4addr changes (ADR-IN-0004).",
			},
			{
				Name:        "DUID",
				JSONName:    "duid",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "DHCP unique identifier. IPv6 only — this field exists on the shared Go SDK struct but is only ever populated by WAPI for the ipv6fixedaddress object type, not the IPv4 fixedaddress object type (ADR-IN-0004, live-verified).",
			},
			{
				Name:        "CloudInfo",
				JSONName:    "cloudInfo",
				GoType:      "*FixedAddressCloudInfo",
				Scope:       FieldScopeResponse,
				Description: "Cloud API related information for this object (cloud-managed fixed addresses only).",
			},
		},
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "FixedAddressDhcpOption",
				Description: "is one DHCP option entry for a FixedAddress (mirrors the SDK's Dhcpoption struct).",
				Fields: []FieldDef{
					{Name: fieldNameName, JSONName: jsonNameName, GoType: goTypeString, Description: "Name of the DHCP option."},
					{Name: "Num", JSONName: "num", GoType: goTypeInt64, Description: "The code of the DHCP option."},
					{Name: "VendorClass", JSONName: "vendorClass", GoType: goTypeString, Description: "The name of the space this DHCP option is associated to."},
					{Name: "Value", JSONName: "value", GoType: goTypeString, Description: "Value of the DHCP option."},
					{Name: "UseOption", JSONName: "useOption", GoType: goTypeBool, Description: "Only applies to special options that are displayed separately from other options and have a use flag."},
				},
			},
			{
				TypeName:    "FixedAddressCloudInfo",
				Description: "carries Cloud API delegation/ownership information for a cloud-managed FixedAddress (mirrors the SDK's GridCloudapiInfo struct).",
				Fields: []FieldDef{
					{Name: "DelegatedMember", JSONName: "delegatedMember", GoType: "*FixedAddressCloudInfoDelegatedMember", Description: "The Cloud Platform Appliance to which authority of the object is delegated."},
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
				TypeName:    "FixedAddressCloudInfoDelegatedMember",
				Description: "identifies the Grid member a cloud-managed FixedAddress's authority is delegated to (mirrors the SDK's Dhcpmember struct).",
				Fields: []FieldDef{
					{Name: "Ipv4Addr", JSONName: "ipv4addr", GoType: goTypeString, Description: "The IPv4 Address of the Grid Member."},
					{Name: "Ipv6Addr", JSONName: "ipv6addr", GoType: goTypeString, Description: "The IPv6 Address of the Grid Member."},
					{Name: fieldNameName, JSONName: jsonNameName, GoType: goTypeString, Description: "The Grid member name"},
				},
			},
		},
		ParameterValidations: []ValidationRule{
			{
				Rule:    "(has(self.ipv4addr) && !has(self.ipv6addr)) || (!has(self.ipv4addr) && has(self.ipv6addr))",
				Message: "exactly one of ipv4addr or ipv6addr must be set (mutually exclusive address families)",
			},
		},
	}
}
