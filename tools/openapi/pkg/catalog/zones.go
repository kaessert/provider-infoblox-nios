package catalog

import "github.com/crossplane-contrib/provider-infoblox-nios/tools/openapi/pkg/model"

func zoneAuth() model.Resource {
	return model.Resource{
		Slug:           "zone_auth",
		Kind:           "ZoneAuth",
		WAPIObjectType: "zone_auth",
		Pattern:        model.PatternCreateReadDelete,
		GoStructName:   "ZoneAuth",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateZoneAuth", Receiver: "ObjectManager", Notes: "Minimal signature: CreateZoneAuth(fqdn, ea) only \u2014 no view/zoneFormat/comment parameter. New zones are created in the default DNS view; non-default-view zones require the generic Connector.CreateObject."},
			{Operation: "ReadByRef", Method: "GetZoneAuthByRef", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteZoneAuth", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST (e.g. `zone_auth/ZG5z...:example.com/Internal`).",
		ExternalNameSourcePath: "_ref",
		Fields: []model.Field{
			f("_ref", "string", model.FieldScopeResponse, false, false, "Server-assigned opaque object reference. This is the Crossplane external-name."),
			f("fqdn", "string", model.FieldScopeBoth, true, true, "Fully-qualified domain name of the zone, e.g. \"example.com\". Immutable \u2014 there is no UpdateZoneAuth method at all."),
			f("view", "string", model.FieldScopeResponse, false, true, "DNS view the zone resides in. Not a CreateZoneAuth parameter (defaults to the Grid's default view); immutable once created."),
			f("extattrs", "EA", model.FieldScopeBoth, false, false, "Extensible attributes."),
			f("comment", "*string", model.FieldScopeResponse, false, false, "Comment for the zone. Not settable via CreateZoneAuth \u2014 requires the generic Connector to set at creation, or a follow-up UpdateObject call."),
			f("disable", "*bool", model.FieldScopeResponse, false, false, "Whether the zone is disabled. Not settable via ObjectManager."),
			f("zone_format", "string", model.FieldScopeResponse, false, true, "Zone format (FORWARD, IPV4, IPV6). Not settable via CreateZoneAuth; immutable."),
			f("ns_group", "*string", model.FieldScopeResponse, false, false, "Name server group associated with the zone. Not settable via ObjectManager."),
			f("soa_default_ttl", "*uint32", model.FieldScopeResponse, false, false, "Default TTL of the SOA record."),
			f("soa_expire", "*uint32", model.FieldScopeResponse, false, false, "SOA expire value."),
			f("soa_negative_ttl", "*uint32", model.FieldScopeResponse, false, false, "SOA negative-caching TTL."),
			f("soa_refresh", "*uint32", model.FieldScopeResponse, false, false, "SOA refresh interval."),
			f("soa_retry", "*uint32", model.FieldScopeResponse, false, false, "SOA retry interval."),
			f("soa_serial_number", "uint32", model.FieldScopeResponse, false, false, "Current SOA serial number."),
		},
		FullSchemaNotes: "ZoneAuth's full WAPI object (objects_generated.go) has well over 100 fields (DNSSEC, GSS-TSIG, RPZ, notify/transfer ACLs, grid primaries/secondaries, etc.). NONE of these beyond fqdn/extattrs are settable through the ObjectManager wrapper \u2014 a controller that needs to configure zone behavior beyond bare creation must use the generic Connector (CreateObject/UpdateObject) with a hand-built field map, a materially different implementation path than every other resource in this catalog.",
		ImmutableFields: []string{"fqdn", "view", "zone_format"},
		MutableFields:   []string{"extattrs"},
		DeleteBehavior:  "hard-delete (404 on subsequent GET) — inferred from RecordA behavior; not independently live-verified (avoided deleting real zones peatestinglab.com/example.com/foo.com/bar.com already present on the shared test Grid).",
		Notes: "NON-STANDARD CRUD: no Update method exists in ObjectManager at all \u2014 CRUD is effectively Create+Read+Delete only through the SDK wrapper; any post-creation change (comment, disable, view assignment, NS group, SOA tuning) requires the generic Connector. " +
			"Live-observed 2026-07-28: GET https://<host>/wapi/v2.9.7/zone_auth returned 4 existing zones in the \"Internal\" view (peatestinglab.com, example.com, foo.com, bar.com), each with only _ref/fqdn/view populated by default (matches ReturnFields() default).",
		LiveVerified: true,
	}
}

func zoneForward() model.Resource {
	return model.Resource{
		Slug:           "zone_forward",
		Kind:           "ZoneForward",
		WAPIObjectType: "zone_forward",
		Pattern:        model.PatternCRUD,
		GoStructName:   "ZoneForward",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateZoneForward", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetZoneForwardByRef", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetZoneForwardFilters", Receiver: "ObjectManager", Notes: "List/query only \u2014 no single-object GetZoneForward(fqdn) exists."},
			{Operation: "Update", Method: "UpdateZoneForward", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteZoneForward", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST.",
		ExternalNameSourcePath: "_ref",
		Fields: []model.Field{
			f("_ref", "string", model.FieldScopeResponse, false, false, "Server-assigned opaque object reference. This is the Crossplane external-name."),
			f("fqdn", "string", model.FieldScopeBoth, true, true, "FQDN of the forward zone. Immutable \u2014 absent from UpdateZoneForward."),
			f("view", "string", model.FieldScopeBoth, false, true, "DNS view the zone resides in. Immutable \u2014 absent from UpdateZoneForward."),
			f("zone_format", "string", model.FieldScopeBoth, false, true, "Zone format (FORWARD, IPV4, IPV6). Immutable \u2014 absent from UpdateZoneForward."),
			f("comment", "*string", model.FieldScopeBoth, false, false, "Comment for the zone."),
			f("disable", "*bool", model.FieldScopeBoth, false, false, "Whether the zone is disabled."),
			f("extattrs", "EA", model.FieldScopeBoth, false, false, "Extensible attributes."),
			f("forward_to", "NullableNameServers", model.FieldScopeBoth, true, false, "List of name servers this zone forwards queries to."),
			f("forwarders_only", "*bool", model.FieldScopeBoth, false, false, "Whether to forward only to the servers in forward_to, ignoring the Grid's default forwarders."),
			f("forwarding_servers", "*NullableForwardingServers", model.FieldScopeBoth, false, false, "Per-Grid-member forwarding server overrides."),
			f("ns_group", "*string", model.FieldScopeBoth, false, false, "Forward stub server group name."),
			f("external_ns_group", "*string", model.FieldScopeBoth, false, false, "External name server group name."),
		},
		ImmutableFields: []string{"fqdn", "view", "zone_format"},
		MutableFields:   []string{"comment", "disable", "extattrs", "forward_to", "forwarders_only", "forwarding_servers", "ns_group", "external_ns_group"},
		DeleteBehavior:  "hard-delete (404 on subsequent GET) — inferred; not independently live-verified.",
	}
}

func zoneDelegated() model.Resource {
	return model.Resource{
		Slug:           "zone_delegated",
		Kind:           "ZoneDelegated",
		WAPIObjectType: "zone_delegated",
		Pattern:        model.PatternCRUD,
		GoStructName:   "ZoneDelegated",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateZoneDelegated", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetZoneDelegated", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetZoneDelegatedByRef", Receiver: "ObjectManager"},
			{Operation: "Update", Method: "UpdateZoneDelegated", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteZoneDelegated", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST.",
		ExternalNameSourcePath: "_ref",
		Fields: []model.Field{
			f("_ref", "string", model.FieldScopeResponse, false, false, "Server-assigned opaque object reference. This is the Crossplane external-name."),
			f("fqdn", "string", model.FieldScopeBoth, true, true, "FQDN of the delegated zone. Immutable \u2014 absent from UpdateZoneDelegated."),
			f("view", "*string", model.FieldScopeBoth, false, true, "DNS view the zone resides in. Immutable \u2014 absent from UpdateZoneDelegated."),
			f("zone_format", "string", model.FieldScopeBoth, false, true, "Zone format (FORWARD, IPV4, IPV6). Immutable \u2014 absent from UpdateZoneDelegated."),
			f("delegate_to", "NullableNameServers", model.FieldScopeBoth, true, false, "List of name servers the zone is delegated to."),
			f("comment", "*string", model.FieldScopeBoth, false, false, "Comment for the zone."),
			f("disable", "*bool", model.FieldScopeBoth, false, false, "Whether the zone is disabled."),
			f("locked", "*bool", model.FieldScopeBoth, false, false, "Whether the zone is locked against modification by other administrators."),
			f("ns_group", "*string", model.FieldScopeBoth, false, false, "Delegation name server group name."),
			f("delegated_ttl", "*uint32", model.FieldScopeBoth, false, false, "TTL of the auto-generated NS/glue records for the delegation."),
			f("use_delegated_ttl", "*bool", model.FieldScopeBoth, false, false, "Use flag for delegated_ttl."),
			f("extattrs", "EA", model.FieldScopeBoth, false, false, "Extensible attributes."),
		},
		ImmutableFields: []string{"fqdn", "view", "zone_format"},
		MutableFields:   []string{"delegate_to", "comment", "disable", "locked", "ns_group", "delegated_ttl", "use_delegated_ttl", "extattrs"},
		DeleteBehavior:  "hard-delete (404 on subsequent GET) — inferred; not independently live-verified.",
	}
}
