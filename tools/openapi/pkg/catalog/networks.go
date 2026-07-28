package catalog

import "github.com/crossplane-contrib/provider-infoblox-nios/tools/openapi/pkg/model"

func networkView() model.Resource {
	return model.Resource{
		Slug:           "network_view",
		Kind:           "NetworkView",
		WAPIObjectType: "networkview",
		Pattern:        model.PatternWellKnownDefault,
		GoStructName:   "NetworkView",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateNetworkView", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetNetworkView", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetNetworkViewByRef", Receiver: "ObjectManager"},
			{Operation: "Update", Method: "UpdateNetworkView", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteNetworkView", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST (e.g. `networkview/ZG5z...:default/true`); the name is user-supplied but the `_ref` is still the canonical identifier used for read/update/delete.",
		ExternalNameSourcePath: "_ref",
		Fields: []model.Field{
			f("_ref", "string", model.FieldScopeResponse, false, false, "Server-assigned opaque object reference. This is the Crossplane external-name."),
			f("name", "*string", model.FieldScopeBoth, true, false, "Name of the network view. UpdateNetworkView accepts a new name — renaming is supported via the wrapper (verify the `_ref` updates accordingly, matching DNS record rename behavior)."),
			f("comment", "*string", model.FieldScopeBoth, false, false, "Comment for the network view; maximum 256 characters."),
			f("extattrs", "EA", model.FieldScopeBoth, false, false, "Extensible attributes."),
			f("is_default", "bool", model.FieldScopeResponse, false, true, "Whether this is the NIOS appliance's default network view. Exactly one exists per Grid and it cannot be deleted (live-verified: GET networkview always returns a `default` entry with is_default=true)."),
			f("associated_dns_views", "[]string", model.FieldScopeResponse, false, false, "DNS views associated with this network view."),
			f("associated_members", "[]*NetworkviewAssocmember", model.FieldScopeResponse, false, false, "Grid members associated with this network view."),
			f("cloud_info", "*GridCloudapiInfo", model.FieldScopeResponse, false, false, "Cloud API related information."),
			f("ddns_dns_view", "*string", model.FieldScopeResponse, false, false, "DNS view that receives DDNS updates."),
			f("ddns_zone_primaries", "[]*Dhcpddns", model.FieldScopeResponse, false, false, "Primary zones to which DDNS updates are sent."),
			f("internal_forward_zones", "[]*ZoneAuth", model.FieldScopeResponse, false, false, "Linked authoritative DNS zones."),
			f("mgm_private", "*bool", model.FieldScopeResponse, false, false, "Whether this object is excluded from Multi-Grid Master synchronization."),
			f("ms_ad_user_data", "*MsserverAduserData", model.FieldScopeResponse, false, false, "Microsoft Active Directory user information."),
			f("remote_forward_zones", "[]*Remoteddnszone", model.FieldScopeResponse, false, false, "Forward-mapping zones receiving DHCP-server DDNS updates."),
			f("remote_reverse_zones", "[]*Remoteddnszone", model.FieldScopeResponse, false, false, "Reverse-mapping zones receiving DHCP-server DDNS updates."),
		},
		ImmutableFields: []string{"is_default"},
		MutableFields:   []string{"name", "comment", "extattrs"},
		DeleteBehavior:  "hard-delete (404 on subsequent GET); DeleteNetworkView on the well-known \"default\" view is expected to be rejected by WAPI (a Grid always requires at least one network view) — not independently live-verified against the default view (would risk breaking Grid networking).",
		Notes: "Cross-reference target for ARecord/HostRecord/FixedAddress/Network/etc. `network_view` fields. Simplest cross-reference target: fully mutable via wrapper (name/comment/extattrs), server-assigned `_ref`, and a well-known `default` instance always exists. " +
			"Live-verified 2026-07-28: GET https://<host>/wapi/v2.9.7/networkview returned exactly one entry, `default`, with is_default=true.",
		LiveVerified: true,
	}
}

func network() model.Resource {
	return model.Resource{
		Slug:                "network",
		Kind:                "Network",
		WAPIObjectType:      "", // runtime-selected — see WAPIObjectTypeNotes
		WAPIObjectTypeNotes: "objectType is set at construction time via NewEmptyNetwork(isIPv6 bool): \"network\" for IPv4, \"ipv6network\" for IPv6. The Go struct and ObjectManager methods are shared; the WAPI object type differs. Modeling decision (Phase 3): either two Kinds (NetworkV4/NetworkV6) or one Kind with an ipVersion/isIPv6 discriminator field — Phase 3 must pick one.",
		Pattern:             model.PatternCRUD,
		GoStructName:        "Network",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateNetwork", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetNetwork", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetNetworkByRef", Receiver: "ObjectManager"},
			{Operation: "Update", Method: "UpdateNetwork", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteNetwork", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST (e.g. `network/ZG5z...:10.0.0.0/24/default`); no other stable identifier exists (the CIDR itself is immutable, but is not exposed as a separate lookup key by the wrapper beyond the _ref).",
		ExternalNameSourcePath: "_ref",
		Fields: []model.Field{
			f("_ref", "string", model.FieldScopeResponse, false, false, "Server-assigned opaque object reference. This is the Crossplane external-name."),
			f("network_view", "string", model.FieldScopeBoth, true, true, "Network view the network belongs to. CreateNetwork accepts it; UpdateNetwork does not \u2014 immutable after creation."),
			f("network", "string", model.FieldScopeBoth, true, true, "CIDR of the network, e.g. \"10.0.0.0/24\". CreateNetwork accepts it; UpdateNetwork does not \u2014 immutable after creation (matches blueprint's preliminary assessment)."),
			f("comment", "string", model.FieldScopeBoth, false, false, "Comment for the network."),
			f("extattrs", "EA", model.FieldScopeBoth, false, false, "Extensible attributes."),
			f("members", "[]NetworkMember", model.FieldScopeResponse, false, false, "Grid members serving DHCP for this network. Populated on GET but not a CreateNetwork/UpdateNetwork parameter via the wrapper (requires the generic Connector to set)."),
		},
		FullSchemaNotes: "The full WAPI network object (objects_generated.go type 'Network' is shadowed by the hand-written wrapper struct in objects.go used here) exposes dozens of additional DHCP options (lease times, DDNS settings, discovery settings, etc.) not reachable through CreateNetwork/UpdateNetwork — only the generic Connector can set them.",
		ImmutableFields: []string{"network_view", "network"},
		MutableFields:   []string{"comment", "extattrs"},
		DeleteBehavior:  "hard-delete (404 on subsequent GET) — inferred; not independently live-verified (would require allocating and deleting a real network, riskier than the A-record probe).",
		CrossReferences: []model.CrossReference{
			{FieldPath: "networkView", TargetKind: "NetworkView", TargetScope: "cluster", Extractor: "external-name", Compound: false, Notes: "network_view identifies a NetworkView by name, not by _ref \u2014 resolves via the NetworkView's Name field, which is stable (unlike _ref) since NetworkView.Name changes require an explicit rename."},
		},
		Notes: "IPv4/IPv6 dual-purpose type. Very thin field surface via the wrapper (network_view, network, comment, extattrs) \u2014 most DHCP configuration requires the generic Connector.",
	}
}

func networkContainer() model.Resource {
	return model.Resource{
		Slug:                "network_container",
		Kind:                "NetworkContainer",
		WAPIObjectType:      "",
		WAPIObjectTypeNotes: "Same runtime-selected pattern as Network: \"networkcontainer\" for IPv4, \"ipv6networkcontainer\" for IPv6.",
		Pattern:             model.PatternCRUD,
		GoStructName:        "NetworkContainer",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateNetworkContainer", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetNetworkContainer", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetNetworkContainerByRef", Receiver: "ObjectManager"},
			{Operation: "Update", Method: "UpdateNetworkContainer", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteNetworkContainer", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST; same identity model as Network.",
		ExternalNameSourcePath: "_ref",
		Fields: []model.Field{
			f("_ref", "string", model.FieldScopeResponse, false, false, "Server-assigned opaque object reference. This is the Crossplane external-name."),
			f("network_view", "string", model.FieldScopeBoth, true, true, "Network view the container belongs to. Immutable \u2014 absent from UpdateNetworkContainer."),
			f("network", "string", model.FieldScopeBoth, true, true, "CIDR of the container network, e.g. \"10.0.0.0/16\". Immutable \u2014 absent from UpdateNetworkContainer."),
			f("comment", "string", model.FieldScopeBoth, false, false, "Comment for the network container."),
			f("extattrs", "EA", model.FieldScopeBoth, false, false, "Extensible attributes."),
		},
		ImmutableFields: []string{"network_view", "network"},
		MutableFields:   []string{"comment", "extattrs"},
		DeleteBehavior:  "hard-delete (404 on subsequent GET) — inferred; not independently live-verified. WAPI is expected to reject deleting a container that still holds child networks/containers.",
		CrossReferences: []model.CrossReference{
			{FieldPath: "networkView", TargetKind: "NetworkView", TargetScope: "cluster", Extractor: "external-name", Compound: false, Notes: "Same pattern as Network.networkView."},
		},
		Notes: "Parent container for a hierarchy of Network objects (a network container can hold child networks and child containers by nested CIDR). No parent/child linkage field is exposed by the wrapper \u2014 containment is purely CIDR-range based on the WAPI server side.",
	}
}

func sharedNetwork() model.Resource {
	return model.Resource{
		Slug:           "ipv4_shared_network",
		Kind:           "IPv4SharedNetwork",
		WAPIObjectType: "sharednetwork",
		Pattern:        model.PatternCRUD,
		GoStructName:   "SharedNetwork",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateIpv4SharedNetwork", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetIpv4SharedNetworkByRef", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetAllIpv4SharedNetwork", Receiver: "ObjectManager", Notes: "List/query only \u2014 no single-object GetIpv4SharedNetwork(name) exists."},
			{Operation: "Update", Method: "UpdateIpv4SharedNetwork", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteIpv4SharedNetwork", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST; no other stable key exists (name is user-supplied but mutable).",
		ExternalNameSourcePath: "_ref",
		Fields: []model.Field{
			f("_ref", "string", model.FieldScopeResponse, false, false, "Server-assigned opaque object reference. This is the Crossplane external-name."),
			f("name", "*string", model.FieldScopeBoth, true, false, "Display name of the shared network."),
			f("networks", "[]string", model.FieldScopeBoth, true, false, "CIDRs of the member networks combined into this shared network."),
			f("network_view", "*string", model.FieldScopeBoth, false, false, "Network view the shared network belongs to. Present in both Create and Update signatures \u2014 treat as tentatively mutable-via-wrapper, but WAPI network-family objects are conventionally view-scoped at creation; verify live before relying on re-parenting."),
			f("comment", "*string", model.FieldScopeBoth, false, false, "Comment for the shared network."),
			f("extattrs", "EA", model.FieldScopeBoth, false, false, "Extensible attributes."),
			f("disable", "*bool", model.FieldScopeBoth, false, false, "Whether the shared network is disabled."),
			f("use_options", "*bool", model.FieldScopeBoth, false, false, "Use flag for options."),
			f("options", "[]*Dhcpoption", model.FieldScopeBoth, false, false, "DHCP options associated with the shared network."),
			f("authority", "*bool", model.FieldScopeResponse, false, false, "Authority for the shared network."),
			f("ddns_ttl", "*uint32", model.FieldScopeResponse, false, false, "DNS update TTL for the shared network."),
			f("enable_ddns", "*bool", model.FieldScopeResponse, false, false, "Whether the DHCP server sends DDNS updates for this shared network."),
			f("dhcp_utilization", "uint32", model.FieldScopeResponse, false, false, "DHCP utilization percentage x1000 across member networks."),
			f("dhcp_utilization_status", "string", model.FieldScopeResponse, false, false, "Utilization level descriptor (e.g. NORMAL, WARNING)."),
			f("dynamic_hosts", "uint32", model.FieldScopeResponse, false, false, "Total DHCP leases issued for the shared network."),
		},
		FullSchemaNotes: "SharedNetwork exposes ~20 additional DHCP tuning fields (bootfile, bootserver, ddns_*, deny_bootp, enable_pxe_lease_time, etc.) in the full struct not reachable through the two wrapper methods.",
		ImmutableFields: []string{},
		MutableFields:   []string{"name", "networks", "network_view", "comment", "extattrs", "disable", "use_options", "options"},
		DeleteBehavior:  "hard-delete (404 on subsequent GET) — inferred; not independently live-verified.",
		CrossReferences: []model.CrossReference{
			{FieldPath: "networks", TargetKind: "Network", TargetScope: "cluster", Extractor: "cidr-match", Compound: false, Notes: "networks lists CIDRs of member Network objects, not their _ref \u2014 reference resolution would need to match by CIDR, not by name, an unusual extractor shape worth flagging for the Phase 3 generator."},
		},
	}
}

func networkRange() model.Resource {
	return model.Resource{
		Slug:           "range",
		Kind:           "Range",
		WAPIObjectType: "range",
		Pattern:        model.PatternCRUD,
		GoStructName:   "Range",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateNetworkRange", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetNetworkRangeByRef", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetNetworkRange", Receiver: "ObjectManager", Notes: "Query-only signature: GetNetworkRange(queryParams) []Range \u2014 no single-object lookup by name/start/end exists; use GetNetworkRangeByRef after Create."},
			{Operation: "Update", Method: "UpdateNetworkRange", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteNetworkRange", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST.",
		ExternalNameSourcePath: "_ref",
		Fields: []model.Field{
			f("_ref", "string", model.FieldScopeResponse, false, false, "Server-assigned opaque object reference. This is the Crossplane external-name."),
			f("name", "*string", model.FieldScopeBoth, false, false, "Display name of the range."),
			f("network", "string", model.FieldScopeBoth, true, false, "CIDR of the parent network the range belongs to. Present in both Create and Update \u2014 tentatively mutable-via-wrapper (verify live before relying on resizing across parent networks)."),
			f("network_view", "string", model.FieldScopeBoth, true, false, "Network view the range belongs to. Present in both Create and Update signatures \u2014 same caution as `network`."),
			f("start_addr", "*string", model.FieldScopeBoth, true, false, "First address in the range."),
			f("end_addr", "*string", model.FieldScopeBoth, true, false, "Last address in the range."),
			f("comment", "*string", model.FieldScopeBoth, false, false, "Comment for the range."),
			f("extattrs", "EA", model.FieldScopeBoth, false, false, "Extensible attributes."),
			f("disable", "*bool", model.FieldScopeBoth, false, false, "Whether the range is disabled."),
			f("member", "*Dhcpmember", model.FieldScopeBoth, false, false, "Grid member serving DHCP for this range."),
			f("failover_association", "string", model.FieldScopeBoth, false, false, "Failover association name serving this range."),
			f("server_association_type", "string", model.FieldScopeBoth, false, false, "Type of server serving the range (MEMBER, FAILOVER, MS_SERVER, etc.)."),
			f("options", "[]*Dhcpoption", model.FieldScopeBoth, false, false, "DHCP options for the range."),
			f("use_options", "*bool", model.FieldScopeBoth, false, false, "Use flag for options."),
			f("ms_server", "*Msdhcpserver", model.FieldScopeBoth, false, false, "Microsoft DHCP server serving the range."),
			f("template", "string", model.FieldScopeRequest, false, true, "Name of the Range Template used to pre-populate this range's settings at creation. CreateNetworkRange accepts it; UpdateNetworkRange does not \u2014 the template link is create-only (applies its settings once, then the range is independent)."),
		},
		ImmutableFields: []string{"template"},
		MutableFields:   []string{"name", "network", "network_view", "start_addr", "end_addr", "comment", "extattrs", "disable", "member", "failover_association", "server_association_type", "options", "use_options", "ms_server"},
		DeleteBehavior:  "hard-delete (404 on subsequent GET) — inferred; not independently live-verified.",
		CrossReferences: []model.CrossReference{
			{FieldPath: "networkView", TargetKind: "NetworkView", TargetScope: "cluster", Extractor: "external-name", Compound: false},
		},
		Notes: "DHCP address range within a network. `template` (Range Template) is a create-only convenience \u2014 it copies settings once rather than maintaining a live link.",
	}
}

func rangeTemplate() model.Resource {
	return model.Resource{
		Slug:           "range_template",
		Kind:           "RangeTemplate",
		WAPIObjectType: "rangetemplate",
		Pattern:        model.PatternCRUD,
		GoStructName:   "Rangetemplate",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateRangeTemplate", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetRangeTemplateByRef", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetAllRangeTemplate", Receiver: "ObjectManager", Notes: "List/query only \u2014 no single-object GetRangeTemplate(name) exists."},
			{Operation: "Update", Method: "UpdateRangeTemplate", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteRangeTemplate", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST; name is mutable so it cannot serve as the external-name.",
		ExternalNameSourcePath: "_ref",
		Fields: []model.Field{
			f("_ref", "string", model.FieldScopeResponse, false, false, "Server-assigned opaque object reference. This is the Crossplane external-name."),
			f("name", "*string", model.FieldScopeBoth, true, false, "Name of the range template object."),
			f("number_of_addresses", "*uint32", model.FieldScopeBoth, true, false, "Number of addresses the range should contain when instantiated."),
			f("offset", "*uint32", model.FieldScopeBoth, true, false, "Start address offset within the parent network when instantiated."),
			f("comment", "*string", model.FieldScopeBoth, false, false, "Comment for the range template."),
			f("extattrs", "EA", model.FieldScopeBoth, false, false, "Extensible attributes."),
			f("options", "[]*Dhcpoption", model.FieldScopeBoth, false, false, "DHCP options to apply to ranges created from this template."),
			f("use_options", "*bool", model.FieldScopeBoth, false, false, "Use flag for options."),
			f("server_association_type", "string", model.FieldScopeBoth, false, false, "Type of server that will serve ranges created from this template."),
			f("failover_association", "*string", model.FieldScopeBoth, false, false, "Failover association name."),
			f("member", "*Dhcpmember", model.FieldScopeBoth, false, false, "Grid member that will serve ranges created from this template."),
			f("cloud_api_compatible", "*bool", model.FieldScopeBoth, false, false, "Whether this template can be used in cloud-computing deployments."),
			f("ms_server", "*string", model.FieldScopeRequest, false, false, "Microsoft DHCP server name (write-only convenience parameter; persisted as a nested Msdhcpserver struct, not a flat field, on the response)."),
		},
		FullSchemaNotes: "The full WAPI object exposes ~40 additional DHCP tuning fields (bootfile, watermarks, filter rules, DDNS settings, etc.) not reachable through the two wrapper methods.",
		ImmutableFields: []string{},
		MutableFields:   []string{"name", "number_of_addresses", "offset", "comment", "extattrs", "options", "use_options", "server_association_type", "failover_association", "member", "cloud_api_compatible"},
		DeleteBehavior:  "hard-delete (404 on subsequent GET) — inferred; not independently live-verified.",
		Notes:           "Config-only object (a stored preset), not itself provisioned network state \u2014 fully mutable via the wrapper, unlike Network/NetworkContainer.",
	}
}

func fixedAddress() model.Resource {
	return model.Resource{
		Slug:                "fixed_address",
		Kind:                "FixedAddress",
		WAPIObjectType:      "",
		WAPIObjectTypeNotes: "Runtime-selected via NewEmptyFixedAddress(isIPv6 bool): \"fixedaddress\" for IPv4, \"ipv6fixedaddress\" for IPv6.",
		Pattern:             model.PatternCRUD,
		GoStructName:        "FixedAddress",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "AllocateIP", Receiver: "ObjectManager", Notes: "NON-STANDARD: the create wrapper is named AllocateIP, not CreateFixedAddress \u2014 no method with the conventional Create<Resource> name exists for this type."},
			{Operation: "Read", Method: "GetFixedAddress", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetFixedAddressByRef", Receiver: "ObjectManager"},
			{Operation: "Update", Method: "UpdateFixedAddress", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteFixedAddress", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST.",
		ExternalNameSourcePath: "_ref",
		Fields: []model.Field{
			f("_ref", "string", model.FieldScopeResponse, false, false, "Server-assigned opaque object reference. This is the Crossplane external-name."),
			f("network_view", "string", model.FieldScopeBoth, true, false, "Network view. Present in both AllocateIP and UpdateFixedAddress signatures \u2014 tentatively mutable-via-wrapper (verify live)."),
			f("network", "string", model.FieldScopeBoth, false, false, "CIDR used to resolve a dynamically-allocated address at create time. Present in both signatures."),
			f("ipv4addr", "string", model.FieldScopeBoth, false, false, "IPv4 address (mutually exclusive with ipv6addr)."),
			f("ipv6addr", "string", model.FieldScopeBoth, false, false, "IPv6 address (mutually exclusive with ipv4addr). AllocateIP's isIPv6 flag selects which of ipv4addr/ipv6addr applies \u2014 fixed at creation (an object cannot switch address families)."),
			f("mac", "*string", model.FieldScopeBoth, false, false, "MAC address for MAC_ADDRESS/CIRCUIT_ID/REMOTE_ID match_client modes."),
			f("duid", "string", model.FieldScopeResponse, false, false, "DHCP unique identifier (IPv6)."),
			f("name", "*string", model.FieldScopeBoth, false, false, "Display name of the fixed address."),
			fEnum("match_client", "*string", model.FieldScopeBoth, false, false, "How the fixed IP is matched to a requesting client.", "MAC_ADDRESS", "CLIENT_ID", "RESERVED", "CIRCUIT_ID", "REMOTE_ID"),
			f("comment", "*string", model.FieldScopeBoth, false, false, "Comment for the fixed address."),
			f("extattrs", "EA", model.FieldScopeBoth, false, false, "Extensible attributes."),
			f("disable", "*bool", model.FieldScopeBoth, false, false, "Whether the fixed address is disabled."),
			f("agent_circuit_id", "*string", model.FieldScopeBoth, false, false, "Agent circuit ID, required when match_client is CIRCUIT_ID."),
			f("agent_remote_id", "*string", model.FieldScopeBoth, false, false, "Agent remote ID, required when match_client is REMOTE_ID."),
			f("client_identifier_prepend_zero", "*bool", model.FieldScopeBoth, false, false, "Whether a leading zero is prepended to the client identifier."),
			f("dhcp_client_identifier", "string", model.FieldScopeBoth, false, false, "DHCP client identifier, required when match_client is CLIENT_ID."),
			f("options", "[]*Dhcpoption", model.FieldScopeBoth, false, false, "DHCP options for the fixed address."),
			f("use_options", "*bool", model.FieldScopeBoth, false, false, "Use flag for options."),
			f("cloud_info", "*GridCloudapiInfo", model.FieldScopeResponse, false, false, "Cloud API related information."),
		},
		ImmutableFields: []string{},
		MutableFields:   []string{"network_view", "network", "ipv4addr", "ipv6addr", "mac", "name", "match_client", "comment", "extattrs", "disable", "agent_circuit_id", "agent_remote_id", "client_identifier_prepend_zero", "dhcp_client_identifier", "options", "use_options"},
		DeleteBehavior:  "hard-delete (404 on subsequent GET) — inferred; not independently live-verified.",
		CrossReferences: []model.CrossReference{
			{FieldPath: "networkView", TargetKind: "NetworkView", TargetScope: "cluster", Extractor: "external-name", Compound: false},
		},
		Notes: "NON-STANDARD CRUD PATTERN: creation goes through AllocateIP(...), not a Create<Resource>-named method. The controller's Create() implementation must call AllocateIP, not assume a uniform naming convention when generating controller code in Phase 4.",
	}
}
