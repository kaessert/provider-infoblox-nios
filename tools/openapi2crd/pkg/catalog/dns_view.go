package catalog

// dnsView returns the DNSView resource descriptor.
//
// DNSView was missing from the original inventory (tools/openapi/inventory.md)
// — it was discovered during Phase 6 live probing against a real NIOS Grid
// Manager appliance (WAPI v2.9.7) and is authoritative in the phase-6
// live-verification decision record. It has full CRUD support via direct
// WAPI calls: the pinned infoblox-go-client/v2 SDK's ObjectManager wrapper
// only exposes a GetDNSView helper, so this descriptor is built directly
// from the SDK's `View` struct (tools/openapi/specs/infobloxopen/, the
// `objects_generated.go` wire-format struct) rather than from Create/Update
// method signatures, and live-verified 2026-07-28.
//
// WAPI object: `view`. CRUD: Create (POST -> 201 bare _ref), Read (GET ->
// JSON), Update (PUT -> 200 bare _ref), Delete (DELETE -> 200).
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// POST).
//
// Well-known-default pattern: three views always exist (`default` with
// is_default=true, `External`, `Internal`); additional views can be
// created and deleted freely, same pattern as NetworkView.
//
// _ref format: `view/<base64>:<name>/<is_default>` — UNSTABLE. Renaming
// `name` changes the record's _ref; the controller must re-read _ref from
// the PUT response and update the external-name annotation.
//
// Immutable fields: `is_default` is WAPI `supports=sr` (search + read
// only) — it is never a POST/PUT parameter, so it has no ForProvider
// representation and carries Scope: FieldScopeResponse. Per the
// FieldDef.Immutable doc, a Response-scope field marked Immutable carries
// its CEL `self == oldSelf` rule on the AtProvider (status) mirror field
// instead of a ForProvider field — the same pattern already established by
// ARecord's `zone` field.
//
// Almost every other field is FieldScopeBoth: the WAPI View object accepts
// these fields on POST/PUT and echoes them back on GET, so they are both
// user-settable (ForProvider) and observable (AtProvider, full mirror per
// convention). `cloud_info` is the sole exception — like ARecord's
// `cloudInfo`, it is populated only for cloud-managed objects and is not a
// documented POST/PUT parameter, so it is Scope: FieldScopeResponse
// (AtProvider only).
//
// Cross-resource references: none. DNSView.network_view is a plain string
// field (not a NetworkView Ref/Selector pair) — retrofitting Ref/Selector
// support onto DNSView's network_view, and onto DNS record types' `view`
// field pointing at DNSView, is explicitly deferred by the phase-6
// live-verification decision record to avoid retroactive changes during
// wave expansion.
//
// Excluded fields: edns_udp_size, use_edns_udp_size, last_queried_acl, and
// max_udp_size/use_max_udp_size are NOT part of this descriptor. They are
// documented in newer WAPI schema versions but do not exist at all on the
// pinned WAPI 2.9.7 baseline — requesting any of them returns a live 400
// from a real Grid Manager appliance ("Unknown argument/field").
// edns_udp_size/use_edns_udp_size first appear starting WAPI 2.13.1;
// last_queried_acl and max_udp_size/use_max_udp_size were confirmed
// unsupported by the same live-probing method during the DNSView Phase 6
// wave. Re-add them (as FieldScopeBoth, matching the rest of the View
// object) only after the pinned WAPI baseline is upgraded past the version
// that introduces each field.
func dnsView() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "DNSView",
		Slug:                 "dnsview",
		ClusterGroup:         clusterGroup("dnsview"),
		NamespacedGroup:      namespacedGroup("dnsview"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "Name",
				JSONName:    "name",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Name of the DNS view. Renaming an existing view changes its _ref (DNSView is in the _ref-unstable resource group — the controller re-reads _ref from the PUT response and updates the external-name annotation).",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the DNS view; maximum 64 characters.",
			},
			{
				Name:        "NetworkView",
				JSONName:    "networkView",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "The name of the network view object associated with this DNS view.",
			},
			{
				Name:        "Disable",
				JSONName:    "disable",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Determines if the DNS view is disabled or not. When this is set to false, the DNS view is enabled.",
			},
			{
				Name:        "BlacklistAction",
				JSONName:    "blacklistAction",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Enum:        []string{"REDIRECT", "REFUSE"},
				Description: "The action to perform when a domain name matches the pattern defined in a rule specified by the blacklist ruleset. The default value is \"REFUSE\".",
			},
			{
				Name:        "BlacklistLogQuery",
				JSONName:    "blacklistLogQuery",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "The flag that indicates whether blacklist redirection queries are logged. The default value is false.",
			},
			{
				Name:        "BlacklistRedirectAddresses",
				JSONName:    "blacklistRedirectAddresses",
				GoType:      "[]string",
				Scope:       FieldScopeBoth,
				Description: "The array of IP addresses the appliance includes in the response it sends in place of a blacklisted IP address.",
			},
			{
				Name:        "BlacklistRedirectTTL",
				JSONName:    "blacklistRedirectTtl",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Minimum:     int64Ptr(ttlMinimumSeconds),
				Maximum:     int64Ptr(ttlMaximumSeconds),
				Description: "The Time To Live (TTL), in seconds, of the synthetic DNS responses resulting from blacklist redirection. Must be non-negative (0-2147483647); NIOS has no zone/grid-default sentinel for this field, so out-of-range values are rejected outright.",
			},
			{
				Name:        "BlacklistRulesets",
				JSONName:    "blacklistRulesets",
				GoType:      "[]string",
				Scope:       FieldScopeBoth,
				Description: "The name of the Ruleset object assigned at the Grid level for blacklist redirection.",
			},
			{
				Name:        "UseBlacklist",
				JSONName:    "useBlacklist",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: blacklist_action, blacklist_log_query, blacklist_redirect_addresses, blacklist_redirect_ttl, blacklist_rulesets, enable_blacklist.",
			},
			{
				Name:        "EnableBlacklist",
				JSONName:    "enableBlacklist",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Determines if the blacklist in a DNS view is enabled or not.",
			},
			{
				Name:        "CustomRootNameServers",
				JSONName:    "customRootNameServers",
				GoType:      "[]DNSViewNameServer",
				Scope:       FieldScopeBoth,
				Description: "The list of customized root name servers. Select and use Internet root name servers, or specify custom root name servers by providing a host name and IP address to which the appliance can send queries.",
			},
			{
				Name:        "RootNameServerType",
				JSONName:    "rootNameServerType",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Determines the type of root name servers.",
			},
			{
				Name:        "UseRootNameServer",
				JSONName:    "useRootNameServer",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: custom_root_name_servers, root_name_server_type.",
			},
			{
				Name:        "DdnsForceCreationTimestampUpdate",
				JSONName:    "ddnsForceCreationTimestampUpdate",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Defines whether the creation timestamp of a resource record is updated when a DDNS update happens, even if there is no change to the resource record.",
			},
			{
				Name:        "UseDdnsForceCreationTimestampUpdate",
				JSONName:    "useDdnsForceCreationTimestampUpdate",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: ddns_force_creation_timestamp_update.",
			},
			{
				Name:        "DdnsPrincipalGroup",
				JSONName:    "ddnsPrincipalGroup",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "The DDNS Principal cluster group name.",
			},
			{
				Name:        "DdnsPrincipalTracking",
				JSONName:    "ddnsPrincipalTracking",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "The flag that indicates whether the DDNS principal track is enabled or disabled.",
			},
			{
				Name:        "UseDdnsPrincipalSecurity",
				JSONName:    "useDdnsPrincipalSecurity",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: ddns_restrict_secure, ddns_principal_tracking, ddns_principal_group.",
			},
			{
				Name:        "DdnsRestrictPatterns",
				JSONName:    "ddnsRestrictPatterns",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "The flag that indicates whether an option to restrict DDNS update requests based on FQDN patterns is enabled or disabled.",
			},
			{
				Name:        "DdnsRestrictPatternsList",
				JSONName:    "ddnsRestrictPatternsList",
				GoType:      "[]string",
				Scope:       FieldScopeBoth,
				Description: "The unordered list of restriction patterns for the option to restrict DDNS updates based on FQDN patterns.",
			},
			{
				Name:        "UseDdnsPatternsRestriction",
				JSONName:    "useDdnsPatternsRestriction",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: ddns_restrict_patterns_list, ddns_restrict_patterns.",
			},
			{
				Name:        "DdnsRestrictProtected",
				JSONName:    "ddnsRestrictProtected",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "The flag that indicates whether an option to restrict DDNS update requests to protected resource records is enabled or disabled.",
			},
			{
				Name:        "UseDdnsRestrictProtected",
				JSONName:    "useDdnsRestrictProtected",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: ddns_restrict_protected.",
			},
			{
				Name:        "DdnsRestrictSecure",
				JSONName:    "ddnsRestrictSecure",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "The flag that indicates whether DDNS update requests for a principal other than the target resource record's principal are restricted.",
			},
			{
				Name:        "DdnsRestrictStatic",
				JSONName:    "ddnsRestrictStatic",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "The flag that indicates whether an option to restrict DDNS update requests to resource records marked as 'STATIC' is enabled or disabled.",
			},
			{
				Name:        "UseDdnsRestrictStatic",
				JSONName:    "useDdnsRestrictStatic",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: ddns_restrict_static.",
			},
			{
				Name:        "Dns64Enabled",
				JSONName:    "dns64Enabled",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Determines if DNS64 is enabled or not.",
			},
			{
				Name:        "Dns64Groups",
				JSONName:    "dns64Groups",
				GoType:      "[]string",
				Scope:       FieldScopeBoth,
				Description: "The list of DNS64 synthesis groups associated with this DNS view.",
			},
			{
				Name:        "UseDns64",
				JSONName:    "useDns64",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: dns64_enabled, dns64_groups.",
			},
			{
				Name:        "DnssecEnabled",
				JSONName:    "dnssecEnabled",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Determines if the DNS security extension is enabled or not.",
			},
			{
				Name:        "DnssecExpiredSignaturesEnabled",
				JSONName:    "dnssecExpiredSignaturesEnabled",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Determines if the DNS security extension accepts expired signatures or not.",
			},
			{
				Name:        "DnssecNegativeTrustAnchors",
				JSONName:    "dnssecNegativeTrustAnchors",
				GoType:      "[]string",
				Scope:       FieldScopeBoth,
				Description: "A list of zones for which the server does not perform DNSSEC validation.",
			},
			{
				Name:        "DnssecTrustedKeys",
				JSONName:    "dnssecTrustedKeys",
				GoType:      "[]*DNSViewDnssecTrustedKey",
				Scope:       FieldScopeBoth,
				Description: "The list of trusted keys for the DNS security extension.",
			},
			{
				Name:        "DnssecValidationEnabled",
				JSONName:    "dnssecValidationEnabled",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Determines if the DNS security validation is enabled or not.",
			},
			{
				Name:        "UseDnssec",
				JSONName:    "useDnssec",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: dnssec_enabled, dnssec_expired_signatures_enabled, dnssec_validation_enabled, dnssec_trusted_keys.",
			},
			// EdnsUDPSize and UseEdnsUDPSize (WAPI edns_udp_size /
			// use_edns_udp_size) are intentionally excluded from this
			// descriptor: they don't exist at all on the pinned WAPI 2.9.7
			// baseline (confirmed by a live 400 response against a real
			// Grid Manager appliance) and first appear starting WAPI
			// 2.13.1. Advertising them in the CRD schema would let a user
			// set a field the controller can never read back or persist.
			{
				Name:        "EnableFixedRrsetOrderFqdns",
				JSONName:    "enableFixedRrsetOrderFqdns",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Determines if the fixed RRset order FQDN is enabled or not.",
			},
			{
				Name:        "FixedRrsetOrderFqdns",
				JSONName:    "fixedRrsetOrderFqdns",
				GoType:      "[]*DNSViewFixedRrsetOrderFqdn",
				Scope:       FieldScopeBoth,
				Description: "The fixed RRset order FQDN list. If this field is non-empty, the appliance automatically sets enable_fixed_rrset_order_fqdns to true, unless the same request also sets it to false.",
			},
			{
				Name:        "UseFixedRrsetOrderFqdns",
				JSONName:    "useFixedRrsetOrderFqdns",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: fixed_rrset_order_fqdns, enable_fixed_rrset_order_fqdns.",
			},
			{
				Name:        "EnableMatchRecursiveOnly",
				JSONName:    "enableMatchRecursiveOnly",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Determines if the 'match-recursive-only' option in a DNS view is enabled or not.",
			},
			{
				Name:        "ExtAttrs",
				JSONName:    "extAttrs",
				GoType:      goTypeStringMap,
				Scope:       FieldScopeBoth,
				Description: "Extensible attributes (arbitrary key/value metadata defined in Grid Manager). The WAPI wire format wraps each value as {\"value\": ...}; this map is the simplified string-valued CRD representation (the controller translates to/from the SDK's EA map[string]interface{} type).",
			},
			{
				Name:        "FilterAaaa",
				JSONName:    "filterAaaa",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "The type of AAAA filtering for this DNS view object.",
			},
			{
				Name:        "FilterAaaaList",
				JSONName:    "filterAaaaList",
				GoType:      "[]*DNSViewAddressAc",
				Scope:       FieldScopeBoth,
				Description: "Applies AAAA filtering to a named ACL, or to a list of IPv4/IPv6 addresses and networks from which queries are received. This field does not allow TSIG keys.",
			},
			{
				Name:        "UseFilterAaaa",
				JSONName:    "useFilterAaaa",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: filter_aaaa, filter_aaaa_list.",
			},
			{
				Name:        "ForwardOnly",
				JSONName:    "forwardOnly",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Determines if this DNS view sends queries to forwarders only or not. When true, queries are sent to forwarders only, not to other internal or Internet root servers.",
			},
			{
				Name:        "Forwarders",
				JSONName:    "forwarders",
				GoType:      "[]string",
				Scope:       FieldScopeBoth,
				Description: "The list of forwarders for the DNS view. A forwarder is a name server to which other name servers first send their off-site queries.",
			},
			{
				Name:        "UseForwarders",
				JSONName:    "useForwarders",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: forwarders, forward_only.",
			},
			{
				Name:        "LameTTL",
				JSONName:    "lameTtl",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Minimum:     int64Ptr(ttlMinimumSeconds),
				Maximum:     int64Ptr(ttlMaximumSeconds),
				Description: "The number of seconds to cache lame delegations or lame servers. Must be non-negative (0-2147483647); to inherit the grid default, set useLameTtl to false rather than passing a negative sentinel value.",
			},
			{
				Name:        "UseLameTTL",
				JSONName:    "useLameTtl",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: lame_ttl.",
			},
			// LastQueriedAcl (WAPI last_queried_acl) is intentionally excluded
			// from this descriptor: it doesn't exist at all on the pinned WAPI
			// 2.9.7 baseline (confirmed by a live 400 response against a real
			// Grid Manager appliance) — the same class of gap as
			// edns_udp_size/use_edns_udp_size. Advertising it in the CRD schema
			// would let a user set a field the controller can never read back
			// or persist.
			{
				Name:        "MatchClients",
				JSONName:    "matchClients",
				GoType:      "[]*DNSViewAddressAc",
				Scope:       FieldScopeBoth,
				Description: "A named ACL, or a list of IPv4/IPv6 addresses, networks, or TSIG keys of clients that are allowed or denied access to the DNS view.",
			},
			{
				Name:        "MatchDestinations",
				JSONName:    "matchDestinations",
				GoType:      "[]*DNSViewAddressAc",
				Scope:       FieldScopeBoth,
				Description: "A named ACL, or a list of IPv4/IPv6 addresses, networks, or TSIG keys of destinations that are allowed or denied access to the DNS view.",
			},
			{
				Name:        "MaxCacheTTL",
				JSONName:    "maxCacheTtl",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Minimum:     int64Ptr(ttlMinimumSeconds),
				Maximum:     int64Ptr(ttlMaximumSeconds),
				Description: "The maximum number of seconds to cache ordinary (positive) answers. Must be non-negative (0-2147483647); to inherit the grid default, set useMaxCacheTtl to false rather than passing a negative sentinel value.",
			},
			{
				Name:        "UseMaxCacheTTL",
				JSONName:    "useMaxCacheTtl",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: max_cache_ttl.",
			},
			{
				Name:        "MaxNcacheTTL",
				JSONName:    "maxNcacheTtl",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Minimum:     int64Ptr(ttlMinimumSeconds),
				Maximum:     int64Ptr(ttlMaximumSeconds),
				Description: "The maximum number of seconds to cache negative (NXDOMAIN) answers. Must be non-negative (0-2147483647); to inherit the grid default, set useMaxNcacheTtl to false rather than passing a negative sentinel value.",
			},
			{
				Name:        "UseMaxNcacheTTL",
				JSONName:    "useMaxNcacheTtl",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: max_ncache_ttl.",
			},
			// MaxUDPSize and UseMaxUDPSize (WAPI max_udp_size /
			// use_max_udp_size) are intentionally excluded from this
			// descriptor: they don't exist at all on the pinned WAPI 2.9.7
			// baseline (confirmed by a live 400 response against a real
			// Grid Manager appliance) and first appear in a later WAPI
			// version. Advertising them in the CRD schema would let a
			// user set a field the controller can never read back or
			// persist.
			{
				Name:        "NotifyDelay",
				JSONName:    "notifyDelay",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Description: "The number of seconds of delay before notify messages are sent to secondaries.",
			},
			{
				Name:        "NxdomainLogQuery",
				JSONName:    "nxdomainLogQuery",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "The flag that indicates whether NXDOMAIN redirection queries are logged. The default value is false.",
			},
			{
				Name:        "NxdomainRedirect",
				JSONName:    "nxdomainRedirect",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Determines if NXDOMAIN redirection in a DNS view is enabled or not.",
			},
			{
				Name:        "NxdomainRedirectAddresses",
				JSONName:    "nxdomainRedirectAddresses",
				GoType:      "[]string",
				Scope:       FieldScopeBoth,
				Description: "The array with IPv4 addresses the appliance includes in the response it sends in place of an NXDOMAIN response.",
			},
			{
				Name:        "NxdomainRedirectAddressesV6",
				JSONName:    "nxdomainRedirectAddressesV6",
				GoType:      "[]string",
				Scope:       FieldScopeBoth,
				Description: "The array with IPv6 addresses the appliance includes in the response it sends in place of an NXDOMAIN response.",
			},
			{
				Name:        "NxdomainRedirectTTL",
				JSONName:    "nxdomainRedirectTtl",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Minimum:     int64Ptr(ttlMinimumSeconds),
				Maximum:     int64Ptr(ttlMaximumSeconds),
				Description: "The Time To Live (TTL), in seconds, of the synthetic DNS responses resulting from NXDOMAIN redirection. Must be non-negative (0-2147483647); NIOS has no zone/grid-default sentinel for this field, so out-of-range values are rejected outright.",
			},
			{
				Name:        "NxdomainRulesets",
				JSONName:    "nxdomainRulesets",
				GoType:      "[]string",
				Scope:       FieldScopeBoth,
				Description: "The names of the Ruleset objects assigned at the grid level for NXDOMAIN redirection.",
			},
			{
				Name:        "UseNxdomainRedirect",
				JSONName:    "useNxdomainRedirect",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: nxdomain_redirect, nxdomain_redirect_addresses, nxdomain_redirect_addresses_v6, nxdomain_redirect_ttl, nxdomain_log_query, nxdomain_rulesets.",
			},
			{
				Name:        "Recursion",
				JSONName:    "recursion",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Determines if recursion is enabled or not.",
			},
			{
				Name:        "UseRecursion",
				JSONName:    "useRecursion",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: recursion.",
			},
			{
				Name:        "ResponseRateLimiting",
				JSONName:    "responseRateLimiting",
				GoType:      "*DNSViewResponseRateLimiting",
				Scope:       FieldScopeBoth,
				Description: "The response rate limiting settings for the DNS view.",
			},
			{
				Name:        "UseResponseRateLimiting",
				JSONName:    "useResponseRateLimiting",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: response_rate_limiting.",
			},
			{
				Name:        "RpzDropIPRuleEnabled",
				JSONName:    "rpzDropIpRuleEnabled",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Enables the appliance to ignore RPZ-IP triggers with prefix lengths less than the specified minimum prefix length.",
			},
			{
				Name:        "RpzDropIPRuleMinPrefixLengthIPv4",
				JSONName:    "rpzDropIpRuleMinPrefixLengthIpv4",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Description: "The minimum prefix length for IPv4 RPZ-IP triggers. The appliance ignores RPZ-IP triggers with prefix lengths less than this value.",
			},
			{
				Name:        "RpzDropIPRuleMinPrefixLengthIPv6",
				JSONName:    "rpzDropIpRuleMinPrefixLengthIpv6",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Description: "The minimum prefix length for IPv6 RPZ-IP triggers. The appliance ignores RPZ-IP triggers with prefix lengths less than this value.",
			},
			{
				Name:        "UseRpzDropIPRule",
				JSONName:    "useRpzDropIpRule",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: rpz_drop_ip_rule_enabled, rpz_drop_ip_rule_min_prefix_length_ipv4, rpz_drop_ip_rule_min_prefix_length_ipv6.",
			},
			{
				Name:        "RpzQnameWaitRecurse",
				JSONName:    "rpzQnameWaitRecurse",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "The flag that indicates whether recursive RPZ lookups are enabled.",
			},
			{
				Name:        "UseRpzQnameWaitRecurse",
				JSONName:    "useRpzQnameWaitRecurse",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: rpz_qname_wait_recurse.",
			},
			{
				Name:        "ScavengingSettings",
				JSONName:    "scavengingSettings",
				GoType:      "*DNSViewScavengingSettings",
				Scope:       FieldScopeBoth,
				Description: "The scavenging settings for the DNS view.",
			},
			{
				Name:        "UseScavengingSettings",
				JSONName:    "useScavengingSettings",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: scavenging_settings.",
			},
			{
				Name:        "Sortlist",
				JSONName:    "sortlist",
				GoType:      "[]*DNSViewSortlistEntry",
				Scope:       FieldScopeBoth,
				Description: "A sort list that determines the order of IP addresses in responses sent to DNS queries.",
			},
			{
				Name:        "UseSortlist",
				JSONName:    "useSortlist",
				GoType:      goTypeBool,
				Scope:       FieldScopeBoth,
				Description: "Use flag for: sortlist.",
			},
			{
				Name:        "Ref",
				JSONName:    "ref",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Server-assigned opaque object reference (WAPI `_ref`). Mirrors the crossplane.io/external-name annotation for observability and uptest import verification. UNSTABLE — renaming the view changes this value; the controller re-reads _ref from the PUT response and updates the external-name annotation.",
			},
			{
				Name:        "IsDefault",
				JSONName:    "isDefault",
				GoType:      goTypeBool,
				Scope:       FieldScopeResponse,
				Immutable:   true,
				Description: "The NIOS appliance always provides one default DNS view (name \"default\") plus two other well-known views (\"External\", \"Internal\"). You can rename the default view and change its settings, but its is_default flag never changes after creation. WAPI marks this field `supports=sr` (search + read only) — it is never a POST/PUT parameter, so it has no ForProvider representation; the CEL `self == oldSelf` rule is instead emitted on this AtProvider (status) mirror field (see FieldDef.Immutable doc, same pattern as ARecord's `zone` field).",
			},
			{
				Name:        "CloudInfo",
				JSONName:    "cloudInfo",
				GoType:      "*DNSViewCloudInfo",
				Scope:       FieldScopeResponse,
				Description: "Cloud API related information for this object (cloud-managed views only).",
			},
		},
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "DNSViewNameServer",
				Description: "describes one customized root name server entry (mirrors the SDK's NameServer struct).",
				Fields: []FieldDef{
					{Name: "Address", JSONName: "address", GoType: goTypeString, Description: "The IPv4 address or IPv6 address of the server."},
					{Name: "Name", JSONName: "name", GoType: goTypeString, Description: "A resolvable domain name for the external DNS server."},
					{Name: "SharedWithMsParentDelegation", JSONName: "sharedWithMsParentDelegation", GoType: goTypeBool, Description: "Whether the name server is shared with the parent Microsoft primary zone's delegation server."},
					{Name: "Stealth", JSONName: "stealth", GoType: goTypeBool, Description: "Hide the NS record for the primary name server from DNS queries."},
					{Name: "TsigKey", JSONName: "tsigKey", GoType: goTypeString, Description: "A generated TSIG key."},
					{Name: "TsigKeyAlg", JSONName: "tsigKeyAlg", GoType: goTypeString, Description: "The TSIG key algorithm."},
					{Name: "TsigKeyName", JSONName: "tsigKeyName", GoType: goTypeString, Description: "The TSIG key name."},
					{Name: "UseTsigKeyName", JSONName: "useTsigKeyName", GoType: goTypeBool, Description: "Use flag for: tsig_key_name."},
				},
			},
			{
				TypeName:    "DNSViewDnssecTrustedKey",
				Description: "describes one DNSSEC trusted key entry (mirrors the SDK's Dnssectrustedkey struct).",
				Fields: []FieldDef{
					{Name: "Fqdn", JSONName: "fqdn", GoType: goTypeString, Description: "The FQDN of the domain for which the member validates responses to recursive queries."},
					{Name: "Algorithm", JSONName: "algorithm", GoType: goTypeString, Description: "The DNSSEC algorithm used to generate the key."},
					{Name: "Key", JSONName: "key", GoType: goTypeString, Description: "The DNSSEC key."},
					{Name: "SecureEntryPoint", JSONName: "secureEntryPoint", GoType: goTypeBool, Description: "The secure entry point flag; if set, this is a KSK configuration."},
					{Name: "DnssecMustBeSecure", JSONName: "dnssecMustBeSecure", GoType: goTypeBool, Description: "Responses must be DNSSEC secure for this hierarchy/domain."},
				},
			},
			{
				TypeName:    "DNSViewAddressAc",
				Description: "describes one address/ACL access-control entry (mirrors the SDK's Addressac struct). Reused across filterAaaaList, matchClients, and matchDestinations.",
				Fields: []FieldDef{
					{Name: "Address", JSONName: "address", GoType: goTypeString, Description: "The address this rule applies to, or \"Any\"."},
					{Name: "Permission", JSONName: "permission", GoType: goTypeString, Description: "The permission to use for this address."},
					{Name: "TsigKey", JSONName: "tsigKey", GoType: goTypeString, Description: "A generated TSIG key."},
					{Name: "TsigKeyAlg", JSONName: "tsigKeyAlg", GoType: goTypeString, Description: "The TSIG key algorithm."},
					{Name: "TsigKeyName", JSONName: "tsigKeyName", GoType: goTypeString, Description: "The name of the TSIG key."},
					{Name: "UseTsigKeyName", JSONName: "useTsigKeyName", GoType: goTypeBool, Description: "Use flag for: tsig_key_name."},
				},
			},
			{
				TypeName:    "DNSViewFixedRrsetOrderFqdn",
				Description: "describes one fixed RRset order FQDN entry (mirrors the SDK's GridDnsFixedrrsetorderfqdn struct).",
				Fields: []FieldDef{
					{Name: "Fqdn", JSONName: "fqdn", GoType: goTypeString, Description: "The FQDN of the fixed RRset configuration item."},
					{Name: "RecordType", JSONName: "recordType", GoType: goTypeString, Description: "The record type for the specified FQDN in the fixed RRset configuration."},
				},
			},
			{
				TypeName:    "DNSViewResponseRateLimiting",
				Description: "carries response rate limiting settings for the DNS view (mirrors the SDK's GridResponseratelimiting struct).",
				Fields: []FieldDef{
					{Name: "EnableRrl", JSONName: "enableRrl", GoType: goTypeBool, Description: "Determines if response rate limiting is enabled or not."},
					{Name: "LogOnly", JSONName: "logOnly", GoType: goTypeBool, Description: "Determines if logging for response rate limiting without dropping any requests is enabled or not."},
					{Name: "ResponsesPerSecond", JSONName: "responsesPerSecond", GoType: goTypeUint32, Description: "The number of responses per client per second."},
					{Name: "Window", JSONName: "window", GoType: goTypeUint32, Description: "The time interval in seconds over which responses are tracked."},
					{Name: "Slip", JSONName: "slip", GoType: goTypeUint32, Description: "The response rate limiting slip. If slip is not 0, every n-th rate-limited UDP request is sent a truncated response instead of being dropped."},
				},
			},
			{
				TypeName:    "DNSViewScavengingSettings",
				Description: "carries resource-record scavenging settings for the DNS view (mirrors the SDK's SettingScavenging struct).",
				Fields: []FieldDef{
					{Name: "EnableScavenging", JSONName: "enableScavenging", GoType: goTypeBool, Description: "Indicates if resource record scavenging is enabled or not."},
					{Name: "EnableRecurrentScavenging", JSONName: "enableRecurrentScavenging", GoType: goTypeBool, Description: "Indicates if recurrent resource record scavenging is enabled or not."},
					{Name: "EnableAutoReclamation", JSONName: "enableAutoReclamation", GoType: goTypeBool, Description: "Indicates if automatic resource record scavenging is enabled or not."},
					{Name: "EnableRrLastQueried", JSONName: "enableRrLastQueried", GoType: goTypeBool, Description: "Indicates if resource record last-queried monitoring in affected zones is enabled or not."},
					{Name: "EnableZoneLastQueried", JSONName: "enableZoneLastQueried", GoType: goTypeBool, Description: "Indicates if last-queried monitoring for affected zones is enabled or not."},
					{Name: "ReclaimAssociatedRecords", JSONName: "reclaimAssociatedRecords", GoType: goTypeBool, Description: "Indicates if associated resource record scavenging is enabled or not."},
					{Name: "ScavengingSchedule", JSONName: "scavengingSchedule", GoType: "*DNSViewScavengingSchedule", Description: "Schedule setting for the scavenging task."},
					{Name: "ExpressionList", JSONName: "expressionList", GoType: "[]*DNSViewExpressionOp", Description: "The expression list. A record is treated as reclaimable if the expression evaluates to 'true' for it, unless scavenging has been manually disabled on that record."},
					{Name: "EaExpressionList", JSONName: "eaExpressionList", GoType: "[]*DNSViewEaExpressionOp", Description: "The extensible attributes expression list. A record is treated as reclaimable if the extensible-attributes expression evaluates to 'true' for it, unless scavenging has been manually disabled on that record."},
				},
			},
			{
				TypeName:    "DNSViewScavengingSchedule",
				Description: "carries schedule settings for a scavenging task (mirrors the SDK's SettingSchedule struct).",
				Fields: []FieldDef{
					{Name: "Weekdays", JSONName: "weekdays", GoType: "[]string", Description: "Days of the week when scheduling is triggered."},
					{Name: "TimeZone", JSONName: "timeZone", GoType: goTypeString, Description: "The time zone for the schedule."},
					{Name: "RecurringTime", JSONName: "recurringTime", GoType: goTypeInt64, Description: "The recurring time for the schedule, in Epoch seconds. Obsolete — preserved for backward compatibility; prefer year/month/day_of_month/hour_of_day/minutes_past_hour."},
					{Name: "Frequency", JSONName: "frequency", GoType: goTypeString, Description: "The frequency for the scheduled task."},
					{Name: "Every", JSONName: "every", GoType: goTypeUint32, Description: "The number of frequency units to wait before repeating the scheduled task."},
					{Name: "MinutesPastHour", JSONName: "minutesPastHour", GoType: goTypeUint32, Description: "The minutes past the hour for the scheduled task."},
					{Name: "HourOfDay", JSONName: "hourOfDay", GoType: goTypeUint32, Description: "The hour of day for the scheduled task."},
					{Name: "Year", JSONName: "year", GoType: goTypeUint32, Description: "The year for the scheduled task."},
					{Name: "Month", JSONName: "month", GoType: goTypeUint32, Description: "The month for the scheduled task."},
					{Name: "DayOfMonth", JSONName: "dayOfMonth", GoType: goTypeUint32, Description: "The day of the month for the scheduled task."},
					{Name: "Repeat", JSONName: "repeat", GoType: goTypeString, Description: "Indicates if the scheduled task repeats or runs only once."},
					{Name: "Disable", JSONName: "disable", GoType: goTypeBool, Description: "If set to true, the scheduled task is disabled."},
				},
			},
			{
				TypeName:    "DNSViewExpressionOp",
				Description: "describes one scavenging expression operand pair (mirrors the SDK's Expressionop struct).",
				Fields: []FieldDef{
					{Name: "Op", JSONName: "op", GoType: goTypeString, Description: "The operation name."},
					{Name: "Op1", JSONName: "op1", GoType: goTypeString, Description: "The first operand value."},
					{Name: "Op1Type", JSONName: "op1Type", GoType: goTypeString, Description: "The first operand type."},
					{Name: "Op2", JSONName: "op2", GoType: goTypeString, Description: "The second operand value."},
					{Name: "Op2Type", JSONName: "op2Type", GoType: goTypeString, Description: "The second operand type."},
				},
			},
			{
				TypeName:    "DNSViewEaExpressionOp",
				Description: "describes one scavenging extensible-attributes expression operand pair (mirrors the SDK's Eaexpressionop struct).",
				Fields: []FieldDef{
					{Name: "Op", JSONName: "op", GoType: goTypeString, Description: "The operation name."},
					{Name: "Op1", JSONName: "op1", GoType: goTypeString, Description: "The name of the Extensible Attribute Definition object used as the first operand value."},
					{Name: "Op1Type", JSONName: "op1Type", GoType: goTypeString, Description: "The first operand type."},
					{Name: "Op2", JSONName: "op2", GoType: goTypeString, Description: "The second operand value."},
					{Name: "Op2Type", JSONName: "op2Type", GoType: goTypeString, Description: "The second operand type."},
				},
			},
			{
				TypeName:    "DNSViewSortlistEntry",
				Description: "describes one sortlist entry (mirrors the SDK's Sortlist struct; renamed to avoid a name clash with the Sortlist field).",
				Fields: []FieldDef{
					{Name: "Address", JSONName: "address", GoType: goTypeString, Description: "The source address of a sortlist entry."},
					{Name: "MatchList", JSONName: "matchList", GoType: "[]string", Description: "The match list of a sortlist entry."},
				},
			},
			{
				TypeName:    "DNSViewCloudInfo",
				Description: "carries Cloud API delegation/ownership information for a cloud-managed DNSView (mirrors the SDK's GridCloudapiInfo struct).",
				Fields: []FieldDef{
					{Name: "DelegatedMember", JSONName: "delegatedMember", GoType: "*DNSViewCloudInfoDelegatedMember", Description: "The Cloud Platform Appliance to which authority of the object is delegated."},
					{Name: "DelegatedScope", JSONName: "delegatedScope", GoType: goTypeString, Description: "Indicates the scope of delegation for the object: NONE (outside any delegation), ROOT (the delegation point), SUBTREE (within the scope of a delegation), or RECLAIMING (within the scope of a delegation being reclaimed)."},
					{Name: "DelegatedRoot", JSONName: "delegatedRoot", GoType: goTypeString, Description: "Indicates the root of the delegation if delegated_scope is SUBTREE or RECLAIMING. Not set otherwise."},
					{Name: "OwnedByAdaptor", JSONName: "ownedByAdaptor", GoType: goTypeBool, Description: "Determines whether the object was created by the cloud adapter or not."},
					{Name: "Usage", JSONName: "usage", GoType: goTypeString, Description: "Indicates the cloud origin of the object."},
					{Name: "Tenant", JSONName: "tenant", GoType: goTypeString, Description: "Reference to the tenant object associated with the object, if any."},
					{Name: "MgmtPlatform", JSONName: "mgmtPlatform", GoType: goTypeString, Description: "Indicates the specified cloud management platform."},
					{Name: "AuthorityType", JSONName: "authorityType", GoType: goTypeString, Description: "Type of authority over the object."},
				},
			},
			{
				TypeName:    "DNSViewCloudInfoDelegatedMember",
				Description: "identifies the Grid member a cloud-managed DNSView's authority is delegated to (mirrors the SDK's Dhcpmember struct).",
				Fields: []FieldDef{
					{Name: "Ipv4Addr", JSONName: "ipv4Addr", GoType: goTypeString, Description: "The IPv4 Address of the Grid Member."},
					{Name: "Ipv6Addr", JSONName: "ipv6Addr", GoType: goTypeString, Description: "The IPv6 Address of the Grid Member."},
					{Name: "Name", JSONName: "name", GoType: goTypeString, Description: "The Grid member name."},
				},
			},
		},
	}
}
