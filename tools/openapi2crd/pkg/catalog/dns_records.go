package catalog

// aRecord returns the ARecord resource descriptor.
//
// Source: tools/openapi/inventory.md, "### ARecord" section (fields
// request=1, response=15, both=7) — itself derived from the pinned
// infoblox-go-client/v2 SDK (tools/openapi/specs/infobloxopen/) and
// live-verified 2026-07-28 against a real NIOS Grid Manager appliance (see
// inventory.md's "API Behavior (live-verified)" subsection).
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateARecord).
//
// Immutable fields: `view` is a CreateARecord parameter
// (netView/dnsView) absent from UpdateARecord's parameter list — confirmed
// against the actual SDK interface signatures:
//
//	CreateARecord(netView, dnsView, name, cidr, ipAddr, ttl, useTTL, comment, ea)
//	UpdateARecord(ref, name, ipAddr, cidr, netview, ttl, useTTL, comment, eas)
//
// `zone` is documented in inventory.md as immutable too, but it is NOT a
// CreateARecord parameter at all (confirmed against the signature above) —
// WAPI derives it from name+view. It therefore has no ForProvider
// representation; the CEL `self == oldSelf` rule is instead emitted on its
// AtProvider (status) mirror field (see FieldDef.Immutable doc).
//
// `disable` appears on the underlying WAPI object (objects_generated.go)
// but is NOT a parameter of CreateARecord/UpdateARecord (see
// FullSchemaNotes in tools/openapi/pkg/catalog/dns_records.go) — the SDK's
// ObjectManager wrapper cannot set it, so it is omitted from this catalog
// entirely rather than cataloged as a field with no working setter.
//
// No cross-resource references: ARecord is a reference TARGET for other
// DNS record types (e.g. a CNAMERecord's canonical field or a PTRRecord's
// ptrdname field commonly names an ARecord's FQDN) but does not itself
// reference any other cataloged resource.
func aRecord() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "ARecord",
		Slug:                 "recorda",
		ClusterGroup:         clusterGroup("recorda"),
		NamespacedGroup:      namespacedGroup("recorda"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        fieldNameGo,
				JSONName:    fieldNameJSON,
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Owner name in FQDN format for the A record. Renaming changes the record's _ref.",
			},
			{
				Name:        "IPv4Addr",
				JSONName:    "ipv4Addr",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "IPv4 address of the record. May be set statically or allocated dynamically from a CIDR at create time.",
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
				Name:                   "ExtAttrs",
				JSONName:               "extAttrs",
				GoType:                 goTypeStringMap,
				Scope:                  FieldScopeBoth,
				Description:            "Extensible attributes (arbitrary key/value metadata defined in Grid Manager). The WAPI wire format wraps each value as {\"value\": ...}; this map is the simplified string-valued CRD representation (the controller translates to/from the SDK's EA map[string]interface{} type).",
				ForProviderValidations: reservedEAKeyValidations(),
			},
			{
				Name:        "View",
				JSONName:    "view",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Immutable:   true,
				Description: "DNS view in which the record resides, e.g. \"external\". Fixed at creation — WAPI ties the record's _ref to (view, zone, name). Confirmed absent from the UpdateARecord SDK method signature.",
			},
			{
				Name:        "Cidr",
				JSONName:    "cidr",
				GoType:      goTypeString,
				Scope:       FieldScopeRequest,
				Description: "CIDR of the network from which to allocate the next available IP address (WAPI func:nextavailableip). Create-time-only — ignored on Update. Mutually exclusive with the static ipv4Addr field. When set, networkView is also required.",
			},
			{
				Name:        "NetworkView",
				JSONName:    "networkView",
				GoType:      goTypeString,
				Scope:       FieldScopeRequest,
				Description: "Network view to scope the CIDR for next-available-IP allocation. Create-time-only — ignored on Update. Required when cidr is set; ignored otherwise.",
			},
			{
				Name:                "RemoveAssociatedPtr",
				JSONName:            "removeAssociatedPtr",
				GoType:              goTypeBool,
				Scope:               FieldScopeRequest,
				OmitFromObservation: true,
				Description:         "Delete option: also remove the associated PTR record. Write-only — never present in the RecordA struct returned by GetARecord, so it is excluded from the AtProvider mirror.",
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
				Description: "Zone in which the record resides, e.g. \"zone.com\". Derived from name/view by WAPI — not a CreateARecord parameter, so it has no ForProvider counterpart.",
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
				GoType:      "*ARecordAwsRte53RecordInfo",
				Scope:       FieldScopeResponse,
				Description: "AWS Route 53 record information (cloud-managed records only).",
			},
			{
				Name:        "CloudInfo",
				JSONName:    "cloudInfo",
				GoType:      "*ARecordCloudInfo",
				Scope:       FieldScopeResponse,
				Description: "Cloud API related information for this object (cloud-managed records only).",
			},
			{
				Name:        "MsAdUserData",
				JSONName:    "msAdUserData",
				GoType:      "*ARecordMsAdUserData",
				Scope:       FieldScopeResponse,
				Description: "Microsoft Active Directory user information (MS-managed records only).",
			},
			{
				Name:        "DiscoveredData",
				JSONName:    "discoveredData",
				GoType:      "*ARecordDiscoveredData",
				Scope:       FieldScopeResponse,
				Description: "Discovered data for this A record (DNS discovery feature).",
			},
		},
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "ARecordAwsRte53RecordInfo",
				Description: "carries AWS Route 53 record information for a cloud-managed ARecord (mirrors the SDK's Awsrte53recordinfo struct).",
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
				TypeName:    "ARecordCloudInfo",
				Description: "carries Cloud API delegation/ownership information for a cloud-managed ARecord (mirrors the SDK's GridCloudapiInfo struct).",
				Fields: []FieldDef{
					{Name: "DelegatedMember", JSONName: "delegatedMember", GoType: "*ARecordCloudInfoDelegatedMember", Description: "The Cloud Platform Appliance to which authority of the object is delegated."},
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
				TypeName:    "ARecordCloudInfoDelegatedMember",
				Description: "identifies the Grid member a cloud-managed ARecord's authority is delegated to (mirrors the SDK's Dhcpmember struct).",
				Fields: []FieldDef{
					{Name: "Ipv4Addr", JSONName: "ipv4addr", GoType: goTypeString, Description: "The IPv4 Address of the Grid Member."},
					{Name: "Ipv6Addr", JSONName: "ipv6addr", GoType: goTypeString, Description: "The IPv6 Address of the Grid Member."},
					{Name: fieldNameGo, JSONName: fieldNameJSON, GoType: goTypeString, Description: "The Grid member name"},
				},
			},
			{
				TypeName:    "ARecordMsAdUserData",
				Description: "carries Microsoft Active Directory user information for an MS-managed ARecord (mirrors the SDK's MsserverAduserData struct).",
				Fields: []FieldDef{
					{Name: "ActiveUsersCount", JSONName: "activeUsersCount", GoType: goTypeInt64, Description: "The number of active users."},
				},
			},
			{
				TypeName:    "ARecordDiscoveredData",
				Description: "carries DNS-discovery-feature data for an ARecord (mirrors the SDK's Discoverydata struct — 96 fields, all response-only; populated only when the NIOS Discovery feature is enabled and has scanned the host).",
				Fields: []FieldDef{
					{Name: "DeviceModel", JSONName: "deviceModel", GoType: goTypeString, Description: "The model name of the end device in the vendor terminology."},
					{Name: "DevicePortName", JSONName: "devicePortName", GoType: goTypeString, Description: "The system name of the interface associated with the discovered IP address."},
					{Name: "DevicePortType", JSONName: "devicePortType", GoType: goTypeString, Description: "The hardware type of the interface associated with the discovered IP address."},
					{Name: "DeviceType", JSONName: "deviceType", GoType: goTypeString, Description: "The type of end host in vendor terminology."},
					{Name: "DeviceVendor", JSONName: "deviceVendor", GoType: goTypeString, Description: "The vendor name of the end host."},
					{Name: "DiscoveredName", JSONName: "discoveredName", GoType: goTypeString, Description: "The name of the network device associated with the discovered IP address."},
					{Name: "Discoverer", JSONName: "discoverer", GoType: goTypeString, Description: "Specifies whether the IP address was discovered by a NetMRI or NIOS discovery process."},
					{Name: "Duid", JSONName: "duid", GoType: goTypeString, Description: "For IPv6 address only. The DHCP unique identifier of the discovered host. This is an optional field, and data might not be included."},
					{Name: "FirstDiscovered", JSONName: "firstDiscovered", GoType: goTypeInt64, Description: "The date and time the IP address was first discovered in Epoch seconds format."},
					{Name: "IprgNo", JSONName: "iprgNo", GoType: goTypeInt64, Description: "The port redundant group number."},
					{Name: "IprgState", JSONName: "iprgState", GoType: goTypeString, Description: "The status for the IP address within port redundant group."},
					{Name: "IprgType", JSONName: "iprgType", GoType: goTypeString, Description: "The port redundant group type."},
					{Name: "LastDiscovered", JSONName: "lastDiscovered", GoType: goTypeInt64, Description: "The date and time the IP address was last discovered in Epoch seconds format."},
					{Name: "MacAddress", JSONName: "macAddress", GoType: goTypeString, Description: "The discovered MAC address for the host. This is the unique identifier of a network device. The discovery acquires the MAC address for hosts that are located on the same network as the Grid member that is running the discovery. This can also be the MAC address of a virtual entity on a specified vSphere server."},
					{Name: "MgmtIpAddress", JSONName: "mgmtIpAddress", GoType: goTypeString, Description: "The management IP address of the end host that has more than one IP."},
					{Name: "NetbiosName", JSONName: "netbiosName", GoType: goTypeString, Description: "The name returned in the NetBIOS reply or the name you manually register for the discovered host."},
					{Name: "NetworkComponentDescription", JSONName: "networkComponentDescription", GoType: goTypeString, Description: "A textual description of the switch that is connected to the end device."},
					{Name: "NetworkComponentIp", JSONName: "networkComponentIp", GoType: goTypeString, Description: "The IPv4 Address or IPv6 Address of the switch that is connected to the end device."},
					{Name: "NetworkComponentModel", JSONName: "networkComponentModel", GoType: goTypeString, Description: "Model name of the switch port connected to the end host in vendor terminology."},
					{Name: "NetworkComponentName", JSONName: "networkComponentName", GoType: goTypeString, Description: "If a reverse lookup was successful for the IP address associated with this switch, the host name is displayed in this field."},
					{Name: "NetworkComponentPortDescription", JSONName: "networkComponentPortDescription", GoType: goTypeString, Description: "A textual description of the switch port that is connected to the end device."},
					{Name: "NetworkComponentPortName", JSONName: "networkComponentPortName", GoType: goTypeString, Description: "The name of the switch port connected to the end device."},
					{Name: "NetworkComponentPortNumber", JSONName: "networkComponentPortNumber", GoType: goTypeString, Description: "The number of the switch port connected to the end device."},
					{Name: "NetworkComponentType", JSONName: "networkComponentType", GoType: goTypeString, Description: "Identifies the switch that is connected to the end device."},
					{Name: "NetworkComponentVendor", JSONName: "networkComponentVendor", GoType: goTypeString, Description: "The vendor name of the switch port connected to the end host."},
					{Name: "OpenPorts", JSONName: "openPorts", GoType: goTypeString, Description: "The list of opened ports on the IP address, represented as: \"TCP: 21,22,23 UDP: 137,139\". Limited to max total 1000 ports."},
					{Name: "Os", JSONName: "os", GoType: goTypeString, Description: "The operating system of the detected host or virtual entity. The OS can be one of the following: * Microsoft for all discovered hosts that have a non-null value in the MAC addresses using the NetBIOS discovery method. * A value that a TCP discovery returns. * The OS of a virtual entity on a vSphere server."},
					{Name: "PortDuplex", JSONName: "portDuplex", GoType: goTypeString, Description: "The negotiated or operational duplex setting of the switch port connected to the end device."},
					{Name: "PortLinkStatus", JSONName: "portLinkStatus", GoType: goTypeString, Description: "The link status of the switch port connected to the end device. Indicates whether it is connected."},
					{Name: "PortSpeed", JSONName: "portSpeed", GoType: goTypeString, Description: "The interface speed, in Mbps, of the switch port."},
					{Name: "PortStatus", JSONName: "portStatus", GoType: goTypeString, Description: "The operational status of the switch port. Indicates whether the port is up or down."},
					{Name: "PortType", JSONName: "portType", GoType: goTypeString, Description: "The type of switch port."},
					{Name: "PortVlanDescription", JSONName: "portVlanDescription", GoType: goTypeString, Description: "The description of the VLAN of the switch port that is connected to the end device."},
					{Name: "PortVlanName", JSONName: "portVlanName", GoType: goTypeString, Description: "The name of the VLAN of the switch port."},
					{Name: "PortVlanNumber", JSONName: "portVlanNumber", GoType: goTypeString, Description: "The ID of the VLAN of the switch port."},
					{Name: "VAdapter", JSONName: "vAdapter", GoType: goTypeString, Description: "The name of the physical network adapter through which the virtual entity is connected to the appliance."},
					{Name: "VCluster", JSONName: "vCluster", GoType: goTypeString, Description: "The name of the VMware cluster to which the virtual entity belongs."},
					{Name: "VDatacenter", JSONName: "vDatacenter", GoType: goTypeString, Description: "The name of the vSphere datacenter or container to which the virtual entity belongs."},
					{Name: "VEntityName", JSONName: "vEntityName", GoType: goTypeString, Description: "The name of the virtual entity."},
					{Name: "VEntityType", JSONName: "vEntityType", GoType: goTypeString, Description: "The virtual entity type. This can be blank or one of the following: Virtual Machine, Virtual Host, or Virtual Center. Virtual Center represents a VMware vCenter server."},
					{Name: "VHost", JSONName: "vHost", GoType: goTypeString, Description: "The name of the VMware server on which the virtual entity was discovered."},
					{Name: "VSwitch", JSONName: "vSwitch", GoType: goTypeString, Description: "The name of the switch to which the virtual entity is connected."},
					{Name: "VmiName", JSONName: "vmiName", GoType: goTypeString, Description: "Name of the virtual machine."},
					{Name: "VmiId", JSONName: "vmiId", GoType: goTypeString, Description: "ID of the virtual machine."},
					{Name: "VlanPortGroup", JSONName: "vlanPortGroup", GoType: goTypeString, Description: "Port group which the virtual machine belongs to."},
					{Name: "VswitchName", JSONName: "vswitchName", GoType: goTypeString, Description: "Name of the virtual switch."},
					{Name: "VswitchId", JSONName: "vswitchId", GoType: goTypeString, Description: "ID of the virtual switch."},
					{Name: "VswitchType", JSONName: "vswitchType", GoType: goTypeString, Description: "Type of the virtual switch: standard or distributed."},
					{Name: "VswitchIpv6Enabled", JSONName: "vswitchIpv6Enabled", GoType: goTypeBool, Description: "Indicates the virtual switch has IPV6 enabled."},
					{Name: "VportName", JSONName: "vportName", GoType: goTypeString, Description: "Name of the network adapter on the virtual switch connected with the virtual machine."},
					{Name: "VportMacAddress", JSONName: "vportMacAddress", GoType: goTypeString, Description: "MAC address of the network adapter on the virtual switch where the virtual machine connected to."},
					{Name: "VportLinkStatus", JSONName: "vportLinkStatus", GoType: goTypeString, Description: "Link status of the network adapter on the virtual switch where the virtual machine connected to."},
					{Name: "VportConfSpeed", JSONName: "vportConfSpeed", GoType: goTypeString, Description: "Configured speed of the network adapter on the virtual switch where the virtual machine connected to. Unit is kb."},
					{Name: "VportConfMode", JSONName: "vportConfMode", GoType: goTypeString, Description: "Configured mode of the network adapter on the virtual switch where the virtual machine connected to."},
					{Name: "VportSpeed", JSONName: "vportSpeed", GoType: goTypeString, Description: "Actual speed of the network adapter on the virtual switch where the virtual machine connected to. Unit is kb."},
					{Name: "VportMode", JSONName: "vportMode", GoType: goTypeString, Description: "Actual mode of the network adapter on the virtual switch where the virtual machine connected to."},
					{Name: "VswitchSegmentType", JSONName: "vswitchSegmentType", GoType: goTypeString, Description: "Type of the network segment on which the current virtual machine/vport connected to."},
					{Name: "VswitchSegmentName", JSONName: "vswitchSegmentName", GoType: goTypeString, Description: "Name of the network segment on which the current virtual machine/vport connected to."},
					{Name: "VswitchSegmentId", JSONName: "vswitchSegmentId", GoType: goTypeString, Description: "ID of the network segment on which the current virtual machine/vport connected to."},
					{Name: "VswitchSegmentPortGroup", JSONName: "vswitchSegmentPortGroup", GoType: goTypeString, Description: "Port group of the network segment on which the current virtual machine/vport connected to."},
					{Name: "VswitchAvailablePortsCount", JSONName: "vswitchAvailablePortsCount", GoType: goTypeInt64, Description: "Numer of available ports reported by the virtual switch on which the virtual machine/vport connected to."},
					{Name: "VswitchTepType", JSONName: "vswitchTepType", GoType: goTypeString, Description: "Type of virtual tunnel endpoint (VTEP) in the virtual switch."},
					{Name: "VswitchTepIp", JSONName: "vswitchTepIp", GoType: goTypeString, Description: "IP address of the virtual tunnel endpoint (VTEP) in the virtual switch."},
					{Name: "VswitchTepPortGroup", JSONName: "vswitchTepPortGroup", GoType: goTypeString, Description: "Port group of the virtual tunnel endpoint (VTEP) in the virtual switch."},
					{Name: "VswitchTepVlan", JSONName: "vswitchTepVlan", GoType: goTypeString, Description: "VLAN of the virtual tunnel endpoint (VTEP) in the virtual switch."},
					{Name: "VswitchTepDhcpServer", JSONName: "vswitchTepDhcpServer", GoType: goTypeString, Description: "DHCP server of the virtual tunnel endpoint (VTEP) in the virtual switch."},
					{Name: "VswitchTepMulticast", JSONName: "vswitchTepMulticast", GoType: goTypeString, Description: "Muticast address of the virtual tunnel endpoint (VTEP) in the virtual swtich."},
					{Name: "VmhostIpAddress", JSONName: "vmhostIpAddress", GoType: goTypeString, Description: "IP address of the physical node on which the virtual machine is hosted."},
					{Name: "VmhostName", JSONName: "vmhostName", GoType: goTypeString, Description: "Name of the physical node on which the virtual machine is hosted."},
					{Name: "VmhostMacAddress", JSONName: "vmhostMacAddress", GoType: goTypeString, Description: "MAC address of the physical node on which the virtual machine is hosted."},
					{Name: "VmhostSubnetCidr", JSONName: "vmhostSubnetCidr", GoType: goTypeInt64, Description: "CIDR subnet of the physical node on which the virtual machine is hosted."},
					{Name: "VmhostNicNames", JSONName: "vmhostNicNames", GoType: goTypeString, Description: "List of all physical port names used by the virtual switch on the physical node on which the virtual machine is hosted. Represented as: \"eth1,eth2,eth3\"."},
					{Name: "VmiTenantId", JSONName: "vmiTenantId", GoType: goTypeString, Description: "ID of the tenant which virtual machine belongs to."},
					{Name: "CmpType", JSONName: "cmpType", GoType: goTypeString, Description: "If the IP is coming from a Cloud environment, the Cloud Management Platform type."},
					{Name: "VmiIpType", JSONName: "vmiIpType", GoType: goTypeString, Description: "Discovered IP address type."},
					{Name: "VmiPrivateAddress", JSONName: "vmiPrivateAddress", GoType: goTypeString, Description: "Private IP address of the virtual machine."},
					{Name: "VmiIsPublicAddress", JSONName: "vmiIsPublicAddress", GoType: goTypeBool, Description: "Indicates whether the IP address is a public address."},
					{Name: "CiscoIseSsid", JSONName: "ciscoIseSsid", GoType: goTypeString, Description: "The Cisco ISE SSID."},
					{Name: "CiscoIseEndpointProfile", JSONName: "ciscoIseEndpointProfile", GoType: goTypeString, Description: "The Endpoint Profile created in Cisco ISE."},
					{Name: "CiscoIseSessionState", JSONName: "ciscoIseSessionState", GoType: goTypeString, Description: "The Cisco ISE connection session state."},
					{Name: "CiscoIseSecurityGroup", JSONName: "ciscoIseSecurityGroup", GoType: goTypeString, Description: "The Cisco ISE security group name."},
					{Name: "TaskName", JSONName: "taskName", GoType: goTypeString, Description: "The name of the discovery task."},
					{Name: "NetworkComponentLocation", JSONName: "networkComponentLocation", GoType: goTypeString, Description: "Location of the network component on which the IP address was discovered."},
					{Name: "NetworkComponentContact", JSONName: "networkComponentContact", GoType: goTypeString, Description: "Contact information from the network component on which the IP address was discovered."},
					{Name: "DeviceLocation", JSONName: "deviceLocation", GoType: goTypeString, Description: "Location of device on which the IP address was discovered."},
					{Name: "DeviceContact", JSONName: "deviceContact", GoType: goTypeString, Description: "Contact information from device on which the IP address was discovered."},
					{Name: "ApName", JSONName: "apName", GoType: goTypeString, Description: "Discovered name of Wireless Access Point."},
					{Name: "ApIpAddress", JSONName: "apIpAddress", GoType: goTypeString, Description: "Discovered IP address of Wireless Access Point."},
					{Name: "ApSsid", JSONName: "apSsid", GoType: goTypeString, Description: "Service set identifier (SSID) associated with Wireless Access Point."},
					{Name: "BridgeDomain", JSONName: "bridgeDomain", GoType: goTypeString, Description: "Discovered bridge domain."},
					{Name: "EndpointGroups", JSONName: "endpointGroups", GoType: goTypeString, Description: "A comma-separated list of the discovered endpoint groups."},
					{Name: "Tenant", JSONName: "tenant", GoType: goTypeString, Description: "Discovered tenant."},
					{Name: "VrfName", JSONName: "vrfName", GoType: goTypeString, Description: "The name of the VRF."},
					{Name: "VrfDescription", JSONName: "vrfDescription", GoType: goTypeString, Description: "Description of the VRF."},
					{Name: "VrfRd", JSONName: "vrfRd", GoType: goTypeString, Description: "Route distinguisher of the VRF."},
					{Name: "BgpAs", JSONName: "bgpAs", GoType: goTypeInt64, Description: "The BGP autonomous system number."},
				},
			},
		},
	}
}

// aliasRecord returns the AliasRecord resource descriptor.
//
// Source: tools/openapi/inventory.md, "### AliasRecord" section (fields
// request=0, response=13, both=9), corrected against direct live WAPI
// probing of a real NIOS Grid Manager appliance on 2026-07-28 (record:alias
// WAPI object type). The live probe is authoritative where it conflicts
// with the SDK-derived inventory notes.
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateAliasRecord). The `_ref` is UNSTABLE — it changes whenever `name`
// or another `_ref`-mutating field is updated.
//
// Immutable fields: `view` is soft-immutable — the WAPI schema's `supports`
// flags claim it is updatable, but a live PUT against a real record was
// rejected at the data level ("The action is not allowed. A parent was not
// found."), matching the same soft-immutable pattern observed on the
// CNAME/TXT/MX record types. `zone` is derived from name+view by WAPI, is
// not a CreateAliasRecord parameter at all, and therefore has no
// ForProvider representation — it is AtProvider-only, same as ARecord's
// `zone` field.
//
// No cross-resource references: AliasRecord's target_name field names
// another DNS record by FQDN, but WAPI does not require the target to
// exist, so it is not cataloged as a reference (best-effort resolution,
// not required for apply to succeed).
func aliasRecord() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "AliasRecord",
		Slug:                 "recordalias",
		ClusterGroup:         clusterGroup("recordalias"),
		NamespacedGroup:      namespacedGroup("recordalias"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "Name",
				JSONName:    "name",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Alias name in FQDN format. Renaming changes the record's _ref.",
			},
			{
				Name:        "TargetName",
				JSONName:    "targetName",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Target name in FQDN format that this alias resolves to. Live-verified: updating this field does not change the record's _ref.",
			},
			{
				Name:        "TargetType",
				JSONName:    "targetType",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Enum:        []string{"A", "AAAA", "MX", "NAPTR", "PTR", "SPF", "SRV", "TXT"},
				Description: "Record type the alias resolves to.",
			},
			{
				Name:        "View",
				JSONName:    "view",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Immutable:   true,
				Description: "DNS view in which the record resides, e.g. \"external\". Soft-immutable: the WAPI schema advertises this field as updatable, but a live update attempt is rejected at the data level, so it is treated as fixed at creation.",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the record; maximum 256 characters.",
			},
			{
				Name:        "Disable",
				JSONName:    "disable",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Whether the record is disabled. Unlike most other DNS record types in this catalog, Alias exposes this field via the SDK's ObjectManager wrapper.",
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
			{
				Name:        "Zone",
				JSONName:    "zone",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Immutable:   true,
				Description: "Zone in which the record resides, e.g. \"zone.com\". Derived from name/view by WAPI — not a CreateAliasRecord parameter, so it has no ForProvider counterpart.",
			},
		},
	}
}

// cnameRecord returns the CNAMERecord resource descriptor.
//
// Source: tools/openapi/inventory.md, "### CNAMERecord" section (request=0,
// response=15, both=7) — itself derived from the pinned
// infoblox-go-client/v2 SDK (tools/openapi/specs/infobloxopen/). Per
// inventory.md's notes, CNAMERecord is the "alternate pilot candidate —
// simple alias record, fewer fields than ARecord"; this catalog entry
// deliberately carries the curated field set that makes it useful as a
// standalone resource (identity + record-specific fields) rather than the
// full 22-field WAPI object mirror (cloud/AD/discovery integration fields
// carried by ARecord's catalog entry are not repeated here — CNAME records
// do not commonly participate in those integrations and no cataloged field
// in the CNAME family requires them).
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateCNAMERecord).
//
// Immutable fields: `view` is a CreateCNAMERecord parameter absent from
// UpdateCNAMERecord's parameter list. Live WAPI probing found CNAME's
// `view` is "soft-immutable" at the schema level (`supports=rwus`, i.e. the
// schema technically allows update) but WAPI rejects the actual PUT at the
// data level ("The action is not allowed. A parent was not found."). The
// practical result is identical to ARecord's hard-immutable `view`, so it
// carries the same CEL `self == oldSelf` treatment.
//
// `zone` is derived from name/view by WAPI (not a CreateCNAMERecord
// parameter), so — like ARecord's `zone` — it is AtProvider-only with no
// ForProvider counterpart and no CEL rule (see FieldDef.Immutable doc).
//
// `canonical` carries the standard three-field cross-resource reference
// pattern (value + Ref + Selector) targeting ARecord: WAPI accepts any FQDN
// value for the CNAME target and does not require a matching ARecord to
// exist in NIOS, so the Ref/Selector fields are optional convenience —
// users may still set `canonical` directly as a plain FQDN string without
// ever populating CanonicalRef/CanonicalSelector. TargetScope is left empty
// so the generator mirrors the CNAMERecord variant's own scope (cluster
// CNAMERecord references cluster ARecord; namespaced CNAMERecord references
// namespaced ARecord) rather than pinning to a single scope, since ARecord
// itself is dual-scope.
func cnameRecord() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "CNAMERecord",
		Slug:                 "recordcname",
		ClusterGroup:         clusterGroup("recordcname"),
		NamespacedGroup:      namespacedGroup("recordcname"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "Name",
				JSONName:    "name",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Alias name in FQDN format for the CNAME record. Renaming changes the record's _ref.",
			},
			{
				Name:        "Canonical",
				JSONName:    "canonical",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Canonical (target) name in FQDN format. WAPI does not require the target to exist, so canonicalRef/canonicalSelector are optional convenience for resolving an ARecord's FQDN — this field may also be set directly as a plain FQDN string.",
				Reference: &ReferenceDescriptor{
					TargetKind: "ARecord",
					TargetSlug: "recorda",
				},
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
				Name:                   "ExtAttrs",
				JSONName:               "extAttrs",
				GoType:                 goTypeStringMap,
				Scope:                  FieldScopeBoth,
				Description:            "Extensible attributes (arbitrary key/value metadata defined in Grid Manager). The WAPI wire format wraps each value as {\"value\": ...}; this map is the simplified string-valued CRD representation (the controller translates to/from the SDK's EA map[string]interface{} type).",
				ForProviderValidations: reservedEAKeyValidations(),
			},
			{
				Name:        "View",
				JSONName:    "view",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Immutable:   true,
				Description: "DNS view in which the record resides, e.g. \"external\". Fixed at creation — WAPI ties the record's _ref to (view, zone, name). Schema-level supports=rwus (soft-mutable) but WAPI rejects the update at the data level (\"A parent was not found\"), so it is treated as immutable.",
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
				Description: "Zone in which the record resides, e.g. \"zone.com\". Derived from name/view by WAPI — not a CreateCNAMERecord parameter, so it has no ForProvider counterpart.",
			},
		},
	}
}

// mxRecord returns the MXRecord resource descriptor.
//
// Source: tools/openapi/inventory.md, "### MXRecord" section (fields
// request=0, response=15, both=8) — itself derived from the pinned
// infoblox-go-client/v2 SDK (tools/openapi/specs/infobloxopen/).
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateMXRecord).
//
// Immutable fields: `view` is a CreateMXRecord parameter that WAPI rejects
// at the data level when changed, even though UpdateMXRecord's SDK
// signature retains a dnsView parameter (unlike Create/Update for
// A/AAAA/CNAME/PTR/SRV/TXT, where the view parameter is dropped entirely
// from Update) — soft-immutable: the WAPI schema `supports` flags allow the
// update call, but the server rejects the change at the data level, so it
// is cataloged as immutable. `zone` is documented as immutable too,
// but it is NOT a CreateMXRecord parameter at all — WAPI derives it from
// name+view. It therefore has no ForProvider representation and no CEL
// rule is emitted for it; it is AtProvider-only (see FieldDef.Immutable
// doc).
//
// No cross-resource references: no cataloged resource's fields point at an
// MXRecord.
func mxRecord() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "MXRecord",
		Slug:                 "recordmx",
		ClusterGroup:         clusterGroup("recordmx"),
		NamespacedGroup:      namespacedGroup("recordmx"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "Name",
				JSONName:    "name",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Owner name (FQDN) the MX record applies to.",
			},
			{
				Name:        "MailExchanger",
				JSONName:    "mailExchanger",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Mail exchanger hostname in FQDN format.",
			},
			{
				Name:        "Preference",
				JSONName:    "preference",
				GoType:      goTypeInt64,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Preference value, 0-65535 — lower values are preferred.",
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
				Name:                   "ExtAttrs",
				JSONName:               "extAttrs",
				GoType:                 goTypeStringMap,
				Scope:                  FieldScopeBoth,
				Description:            "Extensible attributes (arbitrary key/value metadata defined in Grid Manager). The WAPI wire format wraps each value as {\"value\": ...}; this map is the simplified string-valued CRD representation (the controller translates to/from the SDK's EA map[string]interface{} type).",
				ForProviderValidations: reservedEAKeyValidations(),
			},
			{
				Name:        "View",
				JSONName:    "view",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Immutable:   true,
				Description: "DNS view in which the record resides, e.g. \"external\". Fixed at creation — WAPI ties the record's _ref to (view, zone, name); rejected at the data level by WAPI even though UpdateMXRecord's SDK signature accepts a dnsView parameter.",
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
				Description: "Zone in which the record resides, e.g. \"zone.com\". Derived from name/view by WAPI — not a CreateMXRecord parameter, so it has no ForProvider counterpart.",
			},
			{
				Name:        "DNSName",
				JSONName:    "dnsName",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Record name in punycode format (derived from name).",
			},
			{
				Name:        "DNSMailExchanger",
				JSONName:    "dnsMailExchanger",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Mail exchanger name in punycode format (derived from mailExchanger).",
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
				GoType:      "*MXRecordAwsRte53RecordInfo",
				Scope:       FieldScopeResponse,
				Description: "AWS Route 53 record information (cloud-managed records only).",
			},
			{
				Name:        "CloudInfo",
				JSONName:    "cloudInfo",
				GoType:      "*MXRecordCloudInfo",
				Scope:       FieldScopeResponse,
				Description: "Cloud API related information for this object (cloud-managed records only).",
			},
			{
				Name:        "MsAdUserData",
				JSONName:    "msAdUserData",
				GoType:      "*MXRecordMsAdUserData",
				Scope:       FieldScopeResponse,
				Description: "Microsoft Active Directory user information (MS-managed records only).",
			},
		},
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "MXRecordAwsRte53RecordInfo",
				Description: "carries AWS Route 53 record information for a cloud-managed MXRecord (mirrors the SDK's Awsrte53recordinfo struct).",
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
				TypeName:    "MXRecordCloudInfo",
				Description: "carries Cloud API delegation/ownership information for a cloud-managed MXRecord (mirrors the SDK's GridCloudapiInfo struct).",
				Fields: []FieldDef{
					{Name: "DelegatedMember", JSONName: "delegatedMember", GoType: "*MXRecordCloudInfoDelegatedMember", Description: "The Cloud Platform Appliance to which authority of the object is delegated."},
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
				TypeName:    "MXRecordCloudInfoDelegatedMember",
				Description: "identifies the Grid member a cloud-managed MXRecord's authority is delegated to (mirrors the SDK's Dhcpmember struct).",
				Fields: []FieldDef{
					{Name: "Ipv4Addr", JSONName: "ipv4addr", GoType: goTypeString, Description: "The IPv4 Address of the Grid Member."},
					{Name: "Ipv6Addr", JSONName: "ipv6addr", GoType: goTypeString, Description: "The IPv6 Address of the Grid Member."},
					{Name: "Name", JSONName: "name", GoType: goTypeString, Description: "The Grid member name"},
				},
			},
			{
				TypeName:    "MXRecordMsAdUserData",
				Description: "carries Microsoft Active Directory user information for an MS-managed MXRecord (mirrors the SDK's MsserverAduserData struct).",
				Fields: []FieldDef{
					{Name: "ActiveUsersCount", JSONName: "activeUsersCount", GoType: goTypeInt64, Description: "The number of active users."},
				},
			},
		},
	}
}

// nsRecord returns the NSRecord resource descriptor.
//
// Source: tools/openapi/inventory.md, "### NSRecord" section, corrected
// against the pinned infoblox-go-client/v2 SDK's RecordNS struct
// (tools/openapi/specs/infobloxopen/) and live-verified 2026-07-28 against a
// real NIOS Grid Manager appliance (see
// manifests/infobloxnios/decisions/ADR-IN-0004, authoritative over
// inventory.md wherever they conflict).
//
// RecordNS's actual field set (read directly from objects_generated.go)
// does not include `creation_time` or `ms_ad_user_data` — inventory.md's
// static field table listed both in error (most likely carried over from
// the ARecord section); this catalog entry uses the real struct instead.
// `creator` IS a real RecordNS field that the static inventory table
// omitted entirely; ADR-IN-0004 confirms it live-verified read-only
// (`supports=rs`) rather than the scope=both the ADR's correction note
// implies the inventory would otherwise have guessed.
//
// Immutable fields (live-verified, ADR-IN-0004):
//   - `name`: `supports=rws` (no `u`) — absent from UpdateNSRecord's
//     mutable-field set at the data level even though the Go SDK method
//     signature still accepts it as a parameter (the SDK issues a PUT that
//     WAPI rejects for a changed name).
//   - `view`: `supports=rws` (no `u`) — same hard-immutable pattern as
//     AAAARecord/PTRRecord/SRVRecord.
//   - `zone`: derived from name+view, not a CreateNSRecord parameter, so it
//     has no ForProvider representation and no CEL rule is emitted (see
//     FieldDef.Immutable doc) — AtProvider-only.
//
// `addresses` is REQUIRED on create per live WAPI probing
// (`field for create missing: addresses`), correcting inventory.md's
// "optional" classification.
//
// No cross-resource references: NSRecord is not listed as a source
// resource in the blueprint's cross-resource reference map.
func nsRecord() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "NSRecord",
		Slug:                 "recordns",
		ClusterGroup:         clusterGroup("recordns"),
		NamespacedGroup:      namespacedGroup("recordns"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "Name",
				JSONName:    jsonNameName,
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Immutable:   true,
				Description: "Name of the NS record in FQDN format (the delegated zone/subdomain). Live-verified immutable (`supports=rws`, no `u`) — WAPI rejects a PUT that changes this field even though the Go SDK's UpdateNSRecord signature still accepts a name parameter.",
			},
			{
				Name:        "Nameserver",
				JSONName:    "nameserver",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "FQDN of the authoritative server for the redirected zone.",
			},
			{
				Name:        "View",
				JSONName:    "view",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Immutable:   true,
				Description: "DNS view in which the record resides, e.g. \"external\". Fixed at creation — WAPI ties the record's _ref to (view, zone, name). Live-verified hard immutable (`supports=rws`, no `u`); the Go SDK already drops view from UpdateNSRecord's request body.",
			},
			{
				Name:        "Addresses",
				JSONName:    "addresses",
				GoType:      "[]NSRecordAddress",
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Glue address records for the name server. Live-verified REQUIRED on create (`field for create missing: addresses`) — corrects inventory.md's \"optional\" classification.",
			},
			{
				Name:        "MsDelegationName",
				JSONName:    "msDelegationName",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "MS delegation point name.",
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
				Description: "Zone in which the record resides, e.g. \"zone.com\". Derived from name/view by WAPI — not a CreateNSRecord parameter, so it has no ForProvider counterpart.",
			},
			{
				Name:        "Creator",
				JSONName:    "creator",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Record creator. Live-verified read-only (`supports=rs`) — present on the RecordNS struct but omitted from inventory.md's static field table.",
			},
			{
				Name:        "DNSName",
				JSONName:    "dnsName",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Record name in punycode format (derived from name).",
			},
			{
				Name:        "LastQueried",
				JSONName:    "lastQueried",
				GoType:      goTypeInt64,
				Scope:       FieldScopeResponse,
				Description: "Time of the last DNS query for this record (Unix epoch seconds).",
			},
			{
				Name:        "CloudInfo",
				JSONName:    "cloudInfo",
				GoType:      "*NSRecordCloudInfo",
				Scope:       FieldScopeResponse,
				Description: "Cloud API related information for this object (cloud-managed records only).",
			},
			{
				Name:        "Policy",
				JSONName:    "policy",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Host name policy for the record.",
			},
		},
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "NSRecordAddress",
				Description: "identifies one glue name server address for an NSRecord (mirrors the SDK's ZoneNameServer struct).",
				Fields: []FieldDef{
					{Name: "Address", JSONName: "address", GoType: goTypeString, Description: "The address of the Zone Name Server."},
					{Name: "AutoCreatePtr", JSONName: "autoCreatePtr", GoType: goTypeBool, Description: "Flag to indicate if PTR records need to be auto created."},
				},
			},
			{
				TypeName:    "NSRecordCloudInfo",
				Description: "carries Cloud API delegation/ownership information for a cloud-managed NSRecord (mirrors the SDK's GridCloudapiInfo struct).",
				Fields: []FieldDef{
					{Name: "DelegatedMember", JSONName: "delegatedMember", GoType: "*NSRecordCloudInfoDelegatedMember", Description: "The Cloud Platform Appliance to which authority of the object is delegated."},
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
				TypeName:    "NSRecordCloudInfoDelegatedMember",
				Description: "identifies the Grid member an NSRecord's authority is delegated to (mirrors the SDK's Dhcpmember struct).",
				Fields: []FieldDef{
					{Name: "Ipv4Addr", JSONName: "ipv4addr", GoType: goTypeString, Description: "The IPv4 Address of the Grid Member."},
					{Name: "Ipv6Addr", JSONName: "ipv6addr", GoType: goTypeString, Description: "The IPv6 Address of the Grid Member."},
					{Name: "Name", JSONName: jsonNameName, GoType: goTypeString, Description: "The Grid member name"},
				},
			},
		},
	}
}

// ptrRecord describes PTRRecord.
//
// `ptrdname` carries the standard three-field cross-resource reference
// pattern (value + Ref + Selector) targeting ARecord: WAPI accepts any FQDN
// value for the PTR target and does not verify that a matching ARecord
// exists in NIOS, so the Ref/Selector fields are optional convenience —
// users may still set `ptrdname` directly as a plain FQDN string without
// ever populating PtrdnameRef/PtrdnameSelector. TargetScope is left empty
// so the generator mirrors the PTRRecord variant's own scope (cluster
// PTRRecord references cluster ARecord; namespaced PTRRecord references
// namespaced ARecord) rather than pinning to a single scope, since ARecord
// itself is dual-scope.
func ptrRecord() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "PTRRecord",
		Slug:                 "recordptr",
		ClusterGroup:         clusterGroup("recordptr"),
		NamespacedGroup:      namespacedGroup("recordptr"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "Ptrdname",
				JSONName:    "ptrdname",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Domain name this PTR record points to, in FQDN format. Changing it updates the record's _ref (best-effort target — WAPI does not verify that the referenced A/AAAA record exists).",
				Reference: &ReferenceDescriptor{
					TargetKind: "ARecord",
					TargetSlug: "recorda",
				},
			},
			{
				Name:        "Name",
				JSONName:    "name",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "PTR record name in FQDN (in-addr.arpa/ip6.arpa) format. Auto-derived from ipv4Addr/ipv6Addr when omitted; renaming changes the record's _ref.",
			},
			{
				Name:        "IPv4Addr",
				JSONName:    "ipv4Addr",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "IPv4 address the PTR record is keyed by (mutually exclusive with ipv6Addr).",
			},
			{
				Name:        "IPv6Addr",
				JSONName:    "ipv6Addr",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "IPv6 address the PTR record is keyed by (mutually exclusive with ipv4Addr).",
			},
			{
				Name:        "View",
				JSONName:    "view",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Immutable:   true,
				Description: "DNS view in which the record resides, e.g. \"external\". Hard immutable for PTRRecord — WAPI rejects updates with \"Field is not allowed for update: view\".",
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
				Name:                   "ExtAttrs",
				JSONName:               "extAttrs",
				GoType:                 goTypeStringMap,
				Scope:                  FieldScopeBoth,
				Description:            "Extensible attributes (arbitrary key/value metadata defined in Grid Manager). The WAPI wire format wraps each value as {\"value\": ...}; this map is the simplified string-valued CRD representation (the controller translates to/from the SDK's EA map[string]interface{} type).",
				ForProviderValidations: reservedEAKeyValidations(),
			},
			{
				Name:        "Cidr",
				JSONName:    "cidr",
				GoType:      goTypeString,
				Scope:       FieldScopeRequest,
				Description: "CIDR of the network from which to allocate the next available IP address (WAPI func:nextavailableip). Create-time-only — ignored on Update. Mutually exclusive with the static ipv4Addr/ipv6Addr field. When set, networkView is also required.",
			},
			{
				Name:        "NetworkView",
				JSONName:    "networkView",
				GoType:      goTypeString,
				Scope:       FieldScopeRequest,
				Description: "Network view to scope the CIDR for next-available-IP allocation. Create-time-only — ignored on Update. Required when cidr is set; ignored otherwise.",
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
				Description: "Zone in which the record resides, e.g. \"zone.com\". Derived from name/view by WAPI — not a CreatePTRRecord parameter, so it has no ForProvider counterpart.",
			},
			{
				Name:        "DNSName",
				JSONName:    "dnsName",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Record name in punycode format (derived from name).",
			},
			{
				Name:        "DNSPtrdname",
				JSONName:    "dnsPtrdname",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Target domain name in punycode format.",
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
				GoType:      "*PTRRecordAwsRte53RecordInfo",
				Scope:       FieldScopeResponse,
				Description: "AWS Route 53 record information (cloud-managed records only).",
			},
			{
				Name:        "CloudInfo",
				JSONName:    "cloudInfo",
				GoType:      "*PTRRecordCloudInfo",
				Scope:       FieldScopeResponse,
				Description: "Cloud API related information for this object (cloud-managed records only).",
			},
			{
				Name:        "MsAdUserData",
				JSONName:    "msAdUserData",
				GoType:      "*PTRRecordMsAdUserData",
				Scope:       FieldScopeResponse,
				Description: "Microsoft Active Directory user information (MS-managed records only).",
			},
			{
				Name:        "DiscoveredData",
				JSONName:    "discoveredData",
				GoType:      "*PTRRecordDiscoveredData",
				Scope:       FieldScopeResponse,
				Description: "Discovered data for this PTR record (DNS discovery feature).",
			},
		},
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "PTRRecordAwsRte53RecordInfo",
				Description: "carries AWS Route 53 record information for a cloud-managed PTRRecord (mirrors the SDK's Awsrte53recordinfo struct).",
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
				TypeName:    "PTRRecordCloudInfo",
				Description: "carries Cloud API delegation/ownership information for a cloud-managed PTRRecord (mirrors the SDK's GridCloudapiInfo struct).",
				Fields: []FieldDef{
					{Name: "DelegatedMember", JSONName: "delegatedMember", GoType: "*PTRRecordCloudInfoDelegatedMember", Description: "The Cloud Platform Appliance to which authority of the object is delegated."},
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
				TypeName:    "PTRRecordCloudInfoDelegatedMember",
				Description: "identifies the Grid member a cloud-managed PTRRecord's authority is delegated to (mirrors the SDK's Dhcpmember struct).",
				Fields: []FieldDef{
					{Name: "Ipv4Addr", JSONName: "ipv4addr", GoType: goTypeString, Description: "The IPv4 Address of the Grid Member."},
					{Name: "Ipv6Addr", JSONName: "ipv6addr", GoType: goTypeString, Description: "The IPv6 Address of the Grid Member."},
					{Name: "Name", JSONName: "name", GoType: goTypeString, Description: "The Grid member name"},
				},
			},
			{
				TypeName:    "PTRRecordMsAdUserData",
				Description: "carries Microsoft Active Directory user information for an MS-managed PTRRecord (mirrors the SDK's MsserverAduserData struct).",
				Fields: []FieldDef{
					{Name: "ActiveUsersCount", JSONName: "activeUsersCount", GoType: goTypeInt64, Description: "The number of active users."},
				},
			},
			{
				TypeName:    "PTRRecordDiscoveredData",
				Description: "carries DNS-discovery-feature data for a PTRRecord (mirrors the SDK's Discoverydata struct — 96 fields, all response-only; populated only when the NIOS Discovery feature is enabled and has scanned the host).",
				Fields: []FieldDef{
					{Name: "DeviceModel", JSONName: "deviceModel", GoType: goTypeString, Description: "The model name of the end device in the vendor terminology."},
					{Name: "DevicePortName", JSONName: "devicePortName", GoType: goTypeString, Description: "The system name of the interface associated with the discovered IP address."},
					{Name: "DevicePortType", JSONName: "devicePortType", GoType: goTypeString, Description: "The hardware type of the interface associated with the discovered IP address."},
					{Name: "DeviceType", JSONName: "deviceType", GoType: goTypeString, Description: "The type of end host in vendor terminology."},
					{Name: "DeviceVendor", JSONName: "deviceVendor", GoType: goTypeString, Description: "The vendor name of the end host."},
					{Name: "DiscoveredName", JSONName: "discoveredName", GoType: goTypeString, Description: "The name of the network device associated with the discovered IP address."},
					{Name: "Discoverer", JSONName: "discoverer", GoType: goTypeString, Description: "Specifies whether the IP address was discovered by a NetMRI or NIOS discovery process."},
					{Name: "Duid", JSONName: "duid", GoType: goTypeString, Description: "For IPv6 address only. The DHCP unique identifier of the discovered host. This is an optional field, and data might not be included."},
					{Name: "FirstDiscovered", JSONName: "firstDiscovered", GoType: goTypeInt64, Description: "The date and time the IP address was first discovered in Epoch seconds format."},
					{Name: "IprgNo", JSONName: "iprgNo", GoType: goTypeInt64, Description: "The port redundant group number."},
					{Name: "IprgState", JSONName: "iprgState", GoType: goTypeString, Description: "The status for the IP address within port redundant group."},
					{Name: "IprgType", JSONName: "iprgType", GoType: goTypeString, Description: "The port redundant group type."},
					{Name: "LastDiscovered", JSONName: "lastDiscovered", GoType: goTypeInt64, Description: "The date and time the IP address was last discovered in Epoch seconds format."},
					{Name: "MacAddress", JSONName: "macAddress", GoType: goTypeString, Description: "The discovered MAC address for the host. This is the unique identifier of a network device. The discovery acquires the MAC address for hosts that are located on the same network as the Grid member that is running the discovery. This can also be the MAC address of a virtual entity on a specified vSphere server."},
					{Name: "MgmtIpAddress", JSONName: "mgmtIpAddress", GoType: goTypeString, Description: "The management IP address of the end host that has more than one IP."},
					{Name: "NetbiosName", JSONName: "netbiosName", GoType: goTypeString, Description: "The name returned in the NetBIOS reply or the name you manually register for the discovered host."},
					{Name: "NetworkComponentDescription", JSONName: "networkComponentDescription", GoType: goTypeString, Description: "A textual description of the switch that is connected to the end device."},
					{Name: "NetworkComponentIp", JSONName: "networkComponentIp", GoType: goTypeString, Description: "The IPv4 Address or IPv6 Address of the switch that is connected to the end device."},
					{Name: "NetworkComponentModel", JSONName: "networkComponentModel", GoType: goTypeString, Description: "Model name of the switch port connected to the end host in vendor terminology."},
					{Name: "NetworkComponentName", JSONName: "networkComponentName", GoType: goTypeString, Description: "If a reverse lookup was successful for the IP address associated with this switch, the host name is displayed in this field."},
					{Name: "NetworkComponentPortDescription", JSONName: "networkComponentPortDescription", GoType: goTypeString, Description: "A textual description of the switch port that is connected to the end device."},
					{Name: "NetworkComponentPortName", JSONName: "networkComponentPortName", GoType: goTypeString, Description: "The name of the switch port connected to the end device."},
					{Name: "NetworkComponentPortNumber", JSONName: "networkComponentPortNumber", GoType: goTypeString, Description: "The number of the switch port connected to the end device."},
					{Name: "NetworkComponentType", JSONName: "networkComponentType", GoType: goTypeString, Description: "Identifies the switch that is connected to the end device."},
					{Name: "NetworkComponentVendor", JSONName: "networkComponentVendor", GoType: goTypeString, Description: "The vendor name of the switch port connected to the end host."},
					{Name: "OpenPorts", JSONName: "openPorts", GoType: goTypeString, Description: "The list of opened ports on the IP address, represented as: \"TCP: 21,22,23 UDP: 137,139\". Limited to max total 1000 ports."},
					{Name: "Os", JSONName: "os", GoType: goTypeString, Description: "The operating system of the detected host or virtual entity. The OS can be one of the following: * Microsoft for all discovered hosts that have a non-null value in the MAC addresses using the NetBIOS discovery method. * A value that a TCP discovery returns. * The OS of a virtual entity on a vSphere server."},
					{Name: "PortDuplex", JSONName: "portDuplex", GoType: goTypeString, Description: "The negotiated or operational duplex setting of the switch port connected to the end device."},
					{Name: "PortLinkStatus", JSONName: "portLinkStatus", GoType: goTypeString, Description: "The link status of the switch port connected to the end device. Indicates whether it is connected."},
					{Name: "PortSpeed", JSONName: "portSpeed", GoType: goTypeString, Description: "The interface speed, in Mbps, of the switch port."},
					{Name: "PortStatus", JSONName: "portStatus", GoType: goTypeString, Description: "The operational status of the switch port. Indicates whether the port is up or down."},
					{Name: "PortType", JSONName: "portType", GoType: goTypeString, Description: "The type of switch port."},
					{Name: "PortVlanDescription", JSONName: "portVlanDescription", GoType: goTypeString, Description: "The description of the VLAN of the switch port that is connected to the end device."},
					{Name: "PortVlanName", JSONName: "portVlanName", GoType: goTypeString, Description: "The name of the VLAN of the switch port."},
					{Name: "PortVlanNumber", JSONName: "portVlanNumber", GoType: goTypeString, Description: "The ID of the VLAN of the switch port."},
					{Name: "VAdapter", JSONName: "vAdapter", GoType: goTypeString, Description: "The name of the physical network adapter through which the virtual entity is connected to the appliance."},
					{Name: "VCluster", JSONName: "vCluster", GoType: goTypeString, Description: "The name of the VMware cluster to which the virtual entity belongs."},
					{Name: "VDatacenter", JSONName: "vDatacenter", GoType: goTypeString, Description: "The name of the vSphere datacenter or container to which the virtual entity belongs."},
					{Name: "VEntityName", JSONName: "vEntityName", GoType: goTypeString, Description: "The name of the virtual entity."},
					{Name: "VEntityType", JSONName: "vEntityType", GoType: goTypeString, Description: "The virtual entity type. This can be blank or one of the following: Virtual Machine, Virtual Host, or Virtual Center. Virtual Center represents a VMware vCenter server."},
					{Name: "VHost", JSONName: "vHost", GoType: goTypeString, Description: "The name of the VMware server on which the virtual entity was discovered."},
					{Name: "VSwitch", JSONName: "vSwitch", GoType: goTypeString, Description: "The name of the switch to which the virtual entity is connected."},
					{Name: "VmiName", JSONName: "vmiName", GoType: goTypeString, Description: "Name of the virtual machine."},
					{Name: "VmiId", JSONName: "vmiId", GoType: goTypeString, Description: "ID of the virtual machine."},
					{Name: "VlanPortGroup", JSONName: "vlanPortGroup", GoType: goTypeString, Description: "Port group which the virtual machine belongs to."},
					{Name: "VswitchName", JSONName: "vswitchName", GoType: goTypeString, Description: "Name of the virtual switch."},
					{Name: "VswitchId", JSONName: "vswitchId", GoType: goTypeString, Description: "ID of the virtual switch."},
					{Name: "VswitchType", JSONName: "vswitchType", GoType: goTypeString, Description: "Type of the virtual switch: standard or distributed."},
					{Name: "VswitchIpv6Enabled", JSONName: "vswitchIpv6Enabled", GoType: goTypeBool, Description: "Indicates the virtual switch has IPV6 enabled."},
					{Name: "VportName", JSONName: "vportName", GoType: goTypeString, Description: "Name of the network adapter on the virtual switch connected with the virtual machine."},
					{Name: "VportMacAddress", JSONName: "vportMacAddress", GoType: goTypeString, Description: "MAC address of the network adapter on the virtual switch where the virtual machine connected to."},
					{Name: "VportLinkStatus", JSONName: "vportLinkStatus", GoType: goTypeString, Description: "Link status of the network adapter on the virtual switch where the virtual machine connected to."},
					{Name: "VportConfSpeed", JSONName: "vportConfSpeed", GoType: goTypeString, Description: "Configured speed of the network adapter on the virtual switch where the virtual machine connected to. Unit is kb."},
					{Name: "VportConfMode", JSONName: "vportConfMode", GoType: goTypeString, Description: "Configured mode of the network adapter on the virtual switch where the virtual machine connected to."},
					{Name: "VportSpeed", JSONName: "vportSpeed", GoType: goTypeString, Description: "Actual speed of the network adapter on the virtual switch where the virtual machine connected to. Unit is kb."},
					{Name: "VportMode", JSONName: "vportMode", GoType: goTypeString, Description: "Actual mode of the network adapter on the virtual switch where the virtual machine connected to."},
					{Name: "VswitchSegmentType", JSONName: "vswitchSegmentType", GoType: goTypeString, Description: "Type of the network segment on which the current virtual machine/vport connected to."},
					{Name: "VswitchSegmentName", JSONName: "vswitchSegmentName", GoType: goTypeString, Description: "Name of the network segment on which the current virtual machine/vport connected to."},
					{Name: "VswitchSegmentId", JSONName: "vswitchSegmentId", GoType: goTypeString, Description: "ID of the network segment on which the current virtual machine/vport connected to."},
					{Name: "VswitchSegmentPortGroup", JSONName: "vswitchSegmentPortGroup", GoType: goTypeString, Description: "Port group of the network segment on which the current virtual machine/vport connected to."},
					{Name: "VswitchAvailablePortsCount", JSONName: "vswitchAvailablePortsCount", GoType: goTypeInt64, Description: "Numer of available ports reported by the virtual switch on which the virtual machine/vport connected to."},
					{Name: "VswitchTepType", JSONName: "vswitchTepType", GoType: goTypeString, Description: "Type of virtual tunnel endpoint (VTEP) in the virtual switch."},
					{Name: "VswitchTepIp", JSONName: "vswitchTepIp", GoType: goTypeString, Description: "IP address of the virtual tunnel endpoint (VTEP) in the virtual switch."},
					{Name: "VswitchTepPortGroup", JSONName: "vswitchTepPortGroup", GoType: goTypeString, Description: "Port group of the virtual tunnel endpoint (VTEP) in the virtual switch."},
					{Name: "VswitchTepVlan", JSONName: "vswitchTepVlan", GoType: goTypeString, Description: "VLAN of the virtual tunnel endpoint (VTEP) in the virtual switch."},
					{Name: "VswitchTepDhcpServer", JSONName: "vswitchTepDhcpServer", GoType: goTypeString, Description: "DHCP server of the virtual tunnel endpoint (VTEP) in the virtual switch."},
					{Name: "VswitchTepMulticast", JSONName: "vswitchTepMulticast", GoType: goTypeString, Description: "Muticast address of the virtual tunnel endpoint (VTEP) in the virtual swtich."},
					{Name: "VmhostIpAddress", JSONName: "vmhostIpAddress", GoType: goTypeString, Description: "IP address of the physical node on which the virtual machine is hosted."},
					{Name: "VmhostName", JSONName: "vmhostName", GoType: goTypeString, Description: "Name of the physical node on which the virtual machine is hosted."},
					{Name: "VmhostMacAddress", JSONName: "vmhostMacAddress", GoType: goTypeString, Description: "MAC address of the physical node on which the virtual machine is hosted."},
					{Name: "VmhostSubnetCidr", JSONName: "vmhostSubnetCidr", GoType: goTypeInt64, Description: "CIDR subnet of the physical node on which the virtual machine is hosted."},
					{Name: "VmhostNicNames", JSONName: "vmhostNicNames", GoType: goTypeString, Description: "List of all physical port names used by the virtual switch on the physical node on which the virtual machine is hosted. Represented as: \"eth1,eth2,eth3\"."},
					{Name: "VmiTenantId", JSONName: "vmiTenantId", GoType: goTypeString, Description: "ID of the tenant which virtual machine belongs to."},
					{Name: "CmpType", JSONName: "cmpType", GoType: goTypeString, Description: "If the IP is coming from a Cloud environment, the Cloud Management Platform type."},
					{Name: "VmiIpType", JSONName: "vmiIpType", GoType: goTypeString, Description: "Discovered IP address type."},
					{Name: "VmiPrivateAddress", JSONName: "vmiPrivateAddress", GoType: goTypeString, Description: "Private IP address of the virtual machine."},
					{Name: "VmiIsPublicAddress", JSONName: "vmiIsPublicAddress", GoType: goTypeBool, Description: "Indicates whether the IP address is a public address."},
					{Name: "CiscoIseSsid", JSONName: "ciscoIseSsid", GoType: goTypeString, Description: "The Cisco ISE SSID."},
					{Name: "CiscoIseEndpointProfile", JSONName: "ciscoIseEndpointProfile", GoType: goTypeString, Description: "The Endpoint Profile created in Cisco ISE."},
					{Name: "CiscoIseSessionState", JSONName: "ciscoIseSessionState", GoType: goTypeString, Description: "The Cisco ISE connection session state."},
					{Name: "CiscoIseSecurityGroup", JSONName: "ciscoIseSecurityGroup", GoType: goTypeString, Description: "The Cisco ISE security group name."},
					{Name: "TaskName", JSONName: "taskName", GoType: goTypeString, Description: "The name of the discovery task."},
					{Name: "NetworkComponentLocation", JSONName: "networkComponentLocation", GoType: goTypeString, Description: "Location of the network component on which the IP address was discovered."},
					{Name: "NetworkComponentContact", JSONName: "networkComponentContact", GoType: goTypeString, Description: "Contact information from the network component on which the IP address was discovered."},
					{Name: "DeviceLocation", JSONName: "deviceLocation", GoType: goTypeString, Description: "Location of device on which the IP address was discovered."},
					{Name: "DeviceContact", JSONName: "deviceContact", GoType: goTypeString, Description: "Contact information from device on which the IP address was discovered."},
					{Name: "ApName", JSONName: "apName", GoType: goTypeString, Description: "Discovered name of Wireless Access Point."},
					{Name: "ApIpAddress", JSONName: "apIpAddress", GoType: goTypeString, Description: "Discovered IP address of Wireless Access Point."},
					{Name: "ApSsid", JSONName: "apSsid", GoType: goTypeString, Description: "Service set identifier (SSID) associated with Wireless Access Point."},
					{Name: "BridgeDomain", JSONName: "bridgeDomain", GoType: goTypeString, Description: "Discovered bridge domain."},
					{Name: "EndpointGroups", JSONName: "endpointGroups", GoType: goTypeString, Description: "A comma-separated list of the discovered endpoint groups."},
					{Name: "Tenant", JSONName: "tenant", GoType: goTypeString, Description: "Discovered tenant."},
					{Name: "VrfName", JSONName: "vrfName", GoType: goTypeString, Description: "The name of the VRF."},
					{Name: "VrfDescription", JSONName: "vrfDescription", GoType: goTypeString, Description: "Description of the VRF."},
					{Name: "VrfRd", JSONName: "vrfRd", GoType: goTypeString, Description: "Route distinguisher of the VRF."},
					{Name: "BgpAs", JSONName: "bgpAs", GoType: goTypeInt64, Description: "The BGP autonomous system number."},
				},
			},
		},
	}
}
