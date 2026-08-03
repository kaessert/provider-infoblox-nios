package catalog

import "github.com/crossplane-contrib/provider-infoblox-nios/tools/openapi/pkg/model"

// commonRecordFields returns the doc-comment-derived fields shared by nearly
// every simple DNS record type (A/AAAA/CNAME/MX/NS/PTR/SRV/TXT). Each
// resource function starts from this list and adds/overrides
// resource-specific fields (identity fields, disable exposure, etc.).
func commonRecordFields() []model.Field {
	return []model.Field{
		f("_ref", "string", model.FieldScopeResponse, false, false, "Server-assigned opaque object reference. This is the Crossplane external-name."),
		f("aws_rte53_record_info", "*Awsrte53recordinfo", model.FieldScopeResponse, false, false, "AWS Route 53 record information (cloud-managed records only)."),
		f("cloud_info", "*GridCloudapiInfo", model.FieldScopeResponse, false, false, "Cloud API related information for this object (cloud-managed records only)."),
		f("comment", "*string", model.FieldScopeBoth, false, false, "Comment for the record; maximum 256 characters."),
		f("creation_time", "*UnixTime", model.FieldScopeResponse, false, false, "Record creation time (Epoch seconds)."),
		f("creator", "string", model.FieldScopeResponse, false, false, "Record creator. Changing to/from 'SYSTEM' is not allowed."),
		f("ddns_principal", "*string", model.FieldScopeResponse, false, false, "GSS-TSIG principal that owns this record (DDNS-created records only)."),
		f("ddns_protected", "*bool", model.FieldScopeResponse, false, false, "Whether DDNS updates for this record are protected."),
		f("dns_name", "string", model.FieldScopeResponse, false, false, "Record name in punycode format (derived from name)."),
		f("extattrs", "EA", model.FieldScopeBoth, false, false, "Extensible attributes (arbitrary key/value metadata defined in Grid Manager)."),
		f("forbid_reclamation", "*bool", model.FieldScopeResponse, false, false, "Whether reclamation is forbidden for the record (DNS discovery feature)."),
		f("last_queried", "*UnixTime", model.FieldScopeResponse, false, false, "Time of the last DNS query for this record (Epoch seconds)."),
		f("ms_ad_user_data", "*MsserverAduserData", model.FieldScopeResponse, false, false, "Microsoft Active Directory user information (MS-managed records only)."),
		f("reclaimable", "bool", model.FieldScopeResponse, false, false, "Whether the record is reclaimable (DNS discovery feature)."),
		f("shared_record_group", "string", model.FieldScopeResponse, false, false, "Name of the shared record group, if this is a shared record."),
		f("ttl", "*uint32", model.FieldScopeBoth, false, false, "Time-to-live in seconds. Zero means the record is not cached."),
		f("use_ttl", "*bool", model.FieldScopeBoth, false, false, "Use flag for ttl — when false the zone/grid default TTL applies."),
		f("view", "string", model.FieldScopeBoth, true, true, "DNS view in which the record resides, e.g. \"external\". Fixed at creation — WAPI ties the record's _ref to (view, zone, name)."),
		f("zone", "string", model.FieldScopeResponse, false, true, "Zone in which the record resides, e.g. \"zone.com\". Derived from name/view; immutable."),
	}
}

func aRecord() model.Resource {
	fields := append(commonRecordFields(),
		f("name", "*string", model.FieldScopeBoth, true, false, "Owner name in FQDN format for the A record. Renaming changes the record's _ref."),
		f("ipv4addr", "*string", model.FieldScopeBoth, true, false, "IPv4 address of the record. May be set statically or allocated dynamically from a CIDR at create time."),
		f("discovered_data", "*Discoverydata", model.FieldScopeResponse, false, false, "Discovered data for this A record (DNS discovery feature)."),
		f("remove_associated_ptr", "bool", model.FieldScopeRequest, false, false, "Delete option: also remove the associated PTR record. Write-only (Update/Delete parameter, never echoed back)."),
	)
	return model.Resource{
		Slug:           "record_a",
		Kind:           "ARecord",
		WAPIObjectType: "record:a",
		Pattern:        model.PatternCRUD,
		GoStructName:   "RecordA",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateARecord", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetARecord", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetARecordByRef", Receiver: "ObjectManager"},
			{Operation: "Update", Method: "UpdateARecord", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteARecord", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST (e.g. `record:a/ZG5z...:name/view`); no user-controlled key exists that survives a rename.",
		ExternalNameSourcePath: "_ref",
		Fields:                 fields,
		FullSchemaNotes:        "disable is present on the WAPI object (objects_generated.go) but is NOT a parameter of CreateARecord/UpdateARecord — the SDK wrapper cannot enable/disable an A record; only the generic Connector can.",
		ImmutableFields:        []string{"view", "zone"},
		MutableFields:          []string{"name", "ipv4addr", "comment", "ttl", "use_ttl", "extattrs"},
		DeleteBehavior:         "hard-delete (404 on subsequent GET) — live-verified",
		Notes:                  "Recommended pilot resource: simplest full-CRUD DNS record, server-assigned identity, live-verified end-to-end.",
		LiveVerified:           true,
		LiveNotes: "Live-verified 2026-07-28 against a real NIOS Grid Manager: created a throwaway A record " +
			"(name=apigen-probe-test.peatestinglab.com, view=Internal), confirmed GET reflects comment/creator/disable/extattrs/view, " +
			"confirmed PUT is a partial/merge update (an update touching only ttl/use_ttl left comment untouched), confirmed renaming " +
			"(PUT {\"name\": ...}) changes _ref and the old _ref immediately 404s, and confirmed DELETE returns 200 with the deleted " +
			"_ref followed by 404 on GET. Test resource was deleted; no residue left on the Grid.",
	}
}

func aaaaRecord() model.Resource {
	fields := append(commonRecordFields(),
		f("name", "*string", model.FieldScopeBoth, true, false, "Owner name in FQDN format for the AAAA record. Renaming changes the record's _ref."),
		f("ipv6addr", "*string", model.FieldScopeBoth, true, false, "IPv6 address of the record. May be set statically or allocated dynamically from a CIDR at create time."),
		f("discovered_data", "*Discoverydata", model.FieldScopeResponse, false, false, "Discovered data for this AAAA record (DNS discovery feature)."),
		f("remove_associated_ptr", "bool", model.FieldScopeRequest, false, false, "Delete option: also remove the associated PTR record. Write-only."),
	)
	return model.Resource{
		Slug:           "record_aaaa",
		Kind:           "AAAARecord",
		WAPIObjectType: "record:aaaa",
		Pattern:        model.PatternCRUD,
		GoStructName:   "RecordAAAA",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateAAAARecord", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetAAAARecord", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetAAAARecordByRef", Receiver: "ObjectManager"},
			{Operation: "Update", Method: "UpdateAAAARecord", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteAAAARecord", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST; no user-controlled key survives a rename.",
		ExternalNameSourcePath: "_ref",
		Fields:                 fields,
		FullSchemaNotes:        "disable is on the WAPI object but not exposed by CreateAAAARecord/UpdateAAAARecord.",
		ImmutableFields:        []string{"view", "zone"},
		MutableFields:          []string{"name", "ipv6addr", "comment", "ttl", "use_ttl", "extattrs"},
		DeleteBehavior:         "hard-delete (404 on subsequent GET) — inferred from RecordA behavior (identical WAPI record family; not independently live-verified)",
		Notes:                  "IPv6 counterpart of ARecord; same shape and lifecycle.",
	}
}

func cnameRecord() model.Resource {
	fields := append(commonRecordFields(),
		f("name", "*string", model.FieldScopeBoth, true, false, "Alias name in FQDN format. Renaming changes the record's _ref."),
		f("canonical", "*string", model.FieldScopeBoth, true, false, "Canonical (target) name in FQDN format."),
		f("dns_canonical", "string", model.FieldScopeResponse, false, false, "Canonical name in punycode format."),
	)
	return model.Resource{
		Slug:           "record_cname",
		Kind:           "CNAMERecord",
		WAPIObjectType: "record:cname",
		Pattern:        model.PatternCRUD,
		GoStructName:   "RecordCNAME",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateCNAMERecord", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetCNAMERecord", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetCNAMERecordByRef", Receiver: "ObjectManager"},
			{Operation: "Update", Method: "UpdateCNAMERecord", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteCNAMERecord", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST; no user-controlled key survives a rename.",
		ExternalNameSourcePath: "_ref",
		Fields:                 fields,
		FullSchemaNotes:        "disable is on the WAPI object but not exposed by CreateCNAMERecord/UpdateCNAMERecord.",
		ImmutableFields:        []string{"view", "zone"},
		MutableFields:          []string{"name", "canonical", "comment", "ttl", "use_ttl", "extattrs"},
		DeleteBehavior:         "hard-delete (404 on subsequent GET) — inferred from RecordA behavior (identical WAPI record family)",
		Notes:                  "Alternate pilot candidate — simple alias record, fewer fields than ARecord.",
		CrossReferences: []model.CrossReference{
			{FieldPath: "canonical", TargetKind: "ARecord", TargetScope: "namespaced", Extractor: "external-name", Compound: false, Notes: "Canonical target is commonly another DNS record's FQDN, but WAPI accepts any string (including names outside NIOS-managed zones) — reference resolution should be optional/best-effort, not required."},
		},
	}
}

func ptrRecord() model.Resource {
	fields := append(commonRecordFields(),
		f("name", "*string", model.FieldScopeBoth, false, false, "PTR record name in FQDN (in-addr.arpa/ip6.arpa) format. Usually derived from ipv4addr/ipv6addr; renaming changes _ref."),
		f("ptrdname", "*string", model.FieldScopeBoth, true, false, "Domain name this PTR record points to, in FQDN format."),
		f("dns_ptrdname", "string", model.FieldScopeResponse, false, false, "Target domain name in punycode format."),
		f("ipv4addr", "*string", model.FieldScopeBoth, false, false, "IPv4 address the PTR record is keyed by (mutually exclusive with ipv6addr)."),
		f("ipv6addr", "*string", model.FieldScopeBoth, false, false, "IPv6 address the PTR record is keyed by (mutually exclusive with ipv4addr)."),
		f("discovered_data", "*Discoverydata", model.FieldScopeResponse, false, false, "Discovered data for this PTR record (DNS discovery feature)."),
	)
	return model.Resource{
		Slug:           "record_ptr",
		Kind:           "PTRRecord",
		WAPIObjectType: "record:ptr",
		Pattern:        model.PatternCRUD,
		GoStructName:   "RecordPTR",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreatePTRRecord", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetPTRRecord", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetPTRRecordByRef", Receiver: "ObjectManager"},
			{Operation: "Update", Method: "UpdatePTRRecord", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeletePTRRecord", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST; no user-controlled key survives a rename.",
		ExternalNameSourcePath: "_ref",
		Fields:                 fields,
		FullSchemaNotes:        "disable is on the WAPI object but not exposed by CreatePTRRecord/UpdatePTRRecord.",
		ImmutableFields:        []string{"view", "zone"},
		MutableFields:          []string{"name", "ptrdname", "ipv4addr", "ipv6addr", "comment", "ttl", "use_ttl", "extattrs"},
		DeleteBehavior:         "hard-delete (404 on subsequent GET) — inferred from RecordA behavior",
		CrossReferences: []model.CrossReference{
			{FieldPath: "ptrdname", TargetKind: "ARecord", TargetScope: "namespaced", Extractor: "external-name", Compound: false, Notes: "PTR commonly points back at the FQDN of an A/AAAA record created for the same host, but WAPI does not enforce that the target exists."},
		},
	}
}

func mxRecord() model.Resource {
	fields := append(commonRecordFields(),
		f("name", "*string", model.FieldScopeBoth, true, false, "Owner name (FQDN) the MX record applies to."),
		f("mail_exchanger", "*string", model.FieldScopeBoth, true, false, "Mail exchanger hostname in FQDN format."),
		f("dns_mail_exchanger", "string", model.FieldScopeResponse, false, false, "Mail exchanger name in punycode format."),
		f("preference", "*uint32", model.FieldScopeBoth, true, false, "Preference value, 0-65535 — lower values are preferred."),
	)
	return model.Resource{
		Slug:           "record_mx",
		Kind:           "MXRecord",
		WAPIObjectType: "record:mx",
		Pattern:        model.PatternCRUD,
		GoStructName:   "RecordMX",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateMXRecord", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetMXRecord", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetMXRecordByRef", Receiver: "ObjectManager"},
			{Operation: "Update", Method: "UpdateMXRecord", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteMXRecord", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST; no user-controlled key survives a rename.",
		ExternalNameSourcePath: "_ref",
		Fields:                 fields,
		FullSchemaNotes:        "disable is on the WAPI object but not exposed by CreateMXRecord/UpdateMXRecord.",
		ImmutableFields:        []string{"zone"},
		MutableFields:          []string{"name", "mail_exchanger", "preference", "comment", "ttl", "use_ttl", "extattrs", "view"},
		DeleteBehavior:         "hard-delete (404 on subsequent GET) — inferred from RecordA behavior",
		Notes:                  "UpdateMXRecord's SDK signature retains a `dnsView` parameter (unlike Create/Update for A/AAAA/CNAME/PTR/SRV/TXT, where the view parameter is dropped from Update). Treat `view` as mutable-via-wrapper but flag for live verification in Phase 4 — NIOS record identity is conventionally tied to (view, zone, name), so this may be a no-op in practice.",
	}
}

func nsRecord() model.Resource {
	fields := append(commonRecordFields(),
		f("name", "string", model.FieldScopeBoth, true, false, "Name of the NS record in FQDN format (the delegated zone/subdomain)."),
		f("nameserver", "*string", model.FieldScopeBoth, true, true, "FQDN of the authoritative server for the redirected zone. WAPI allows this field to be updated in place, but the provider marks it immutable: this object's server-assigned handle is derived from (view, name, nameserver), it is the only one of those three components WAPI does not already reject an update for, and NSRecord has no extattrs field available to carry a stable identity stamp instead. Freezing it makes the natural key the full, provably-stable identity of the handle."),
		f("addresses", "[]*ZoneNameServer", model.FieldScopeBoth, false, false, "Glue address records for the name server."),
		f("ms_delegation_name", "*string", model.FieldScopeBoth, false, false, "MS delegation point name."),
		f("policy", "string", model.FieldScopeResponse, false, false, "Host name policy for the record."),
	)
	// NS records have no comment/disable/ttl/extattrs fields in the SDK
	// struct — drop the inherited common fields that don't apply.
	fields = dropFields(fields, "comment", "creator", "ddns_principal", "ddns_protected", "extattrs",
		"forbid_reclamation", "reclaimable", "shared_record_group", "ttl", "use_ttl",
		"aws_rte53_record_info")
	return model.Resource{
		Slug:           "record_ns",
		Kind:           "NSRecord",
		WAPIObjectType: "record:ns",
		Pattern:        model.PatternCRUD,
		GoStructName:   "RecordNS",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateNSRecord", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetNSRecordByRef", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetAllRecordNS", Receiver: "ObjectManager", Notes: "List/query only — no single-object GetNSRecord(name) exists; the wrapper only exposes GetAllRecordNS(queryParams) and GetNSRecordByRef(ref)."},
			{Operation: "Update", Method: "UpdateNSRecord", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteNSRecord", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST; no user-controlled key survives a rename.",
		ExternalNameSourcePath: "_ref",
		Fields:                 fields,
		ImmutableFields:        []string{"zone", "nameserver"},
		MutableFields:          []string{"name", "addresses", "ms_delegation_name", "view"},
		DeleteBehavior:         "hard-delete (404 on subsequent GET) — inferred from RecordA behavior",
		Notes:                  "Delegation NS record (not a zone's own apex NS set). UpdateNSRecord retains `dnsView` — same view-mutability caveat as MXRecord. `nameserver` is provider-imposed immutable (not a WAPI restriction): it is a component of this object's server-assigned handle, and NSRecord has no extattrs field to carry a stable identity stamp instead.",
	}
}

func srvRecord() model.Resource {
	fields := append(commonRecordFields(),
		f("name", "*string", model.FieldScopeBoth, true, false, "Owner name (FQDN) the SRV record applies to."),
		f("target", "*string", model.FieldScopeBoth, true, false, "Target host in FQDN format."),
		f("dns_target", "string", model.FieldScopeResponse, false, false, "Target host in punycode format."),
		f("priority", "*uint32", model.FieldScopeBoth, true, false, "Priority, 0-65535 — lower values are preferred."),
		f("weight", "*uint32", model.FieldScopeBoth, true, false, "Relative weight for records with the same priority, 0-65535."),
		f("port", "*uint32", model.FieldScopeBoth, true, false, "TCP/UDP port on the target host, 0-65535."),
	)
	return model.Resource{
		Slug:           "record_srv",
		Kind:           "SRVRecord",
		WAPIObjectType: "record:srv",
		Pattern:        model.PatternCRUD,
		GoStructName:   "RecordSRV",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateSRVRecord", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetSRVRecord", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetSRVRecordByRef", Receiver: "ObjectManager"},
			{Operation: "Update", Method: "UpdateSRVRecord", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteSRVRecord", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST; no user-controlled key survives a rename.",
		ExternalNameSourcePath: "_ref",
		Fields:                 fields,
		FullSchemaNotes:        "disable is on the WAPI object but not exposed by CreateSRVRecord/UpdateSRVRecord.",
		ImmutableFields:        []string{"view", "zone"},
		MutableFields:          []string{"name", "target", "priority", "weight", "port", "comment", "ttl", "use_ttl", "extattrs"},
		DeleteBehavior:         "hard-delete (404 on subsequent GET) — inferred from RecordA behavior",
	}
}

func txtRecord() model.Resource {
	fields := append(commonRecordFields(),
		f("name", "*string", model.FieldScopeBoth, true, false, "Owner name (FQDN) the TXT record applies to."),
		f("text", "*string", model.FieldScopeBoth, true, false, "Text content, up to 255 bytes/substring, 512 bytes total. Quote to preserve leading/trailing/embedded spaces."),
	)
	return model.Resource{
		Slug:           "record_txt",
		Kind:           "TXTRecord",
		WAPIObjectType: "record:txt",
		Pattern:        model.PatternCRUD,
		GoStructName:   "RecordTXT",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateTXTRecord", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetTXTRecord", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetTXTRecordByRef", Receiver: "ObjectManager"},
			{Operation: "Update", Method: "UpdateTXTRecord", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteTXTRecord", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST; no user-controlled key survives a rename.",
		ExternalNameSourcePath: "_ref",
		Fields:                 fields,
		FullSchemaNotes:        "disable is on the WAPI object but not exposed by CreateTXTRecord/UpdateTXTRecord.",
		ImmutableFields:        []string{"view", "zone"},
		MutableFields:          []string{"name", "text", "comment", "ttl", "use_ttl", "extattrs"},
		DeleteBehavior:         "hard-delete (404 on subsequent GET) — inferred from RecordA behavior",
	}
}

func aliasRecord() model.Resource {
	fields := append(commonRecordFields(),
		f("name", "*string", model.FieldScopeBoth, true, false, "Alias name in FQDN format. Renaming changes the record's _ref."),
		f("target_name", "*string", model.FieldScopeBoth, true, false, "Target name in FQDN format."),
		f("dns_target_name", "string", model.FieldScopeResponse, false, false, "Target name in punycode format."),
		fEnum("target_type", "string", model.FieldScopeBoth, true, false, "Record type the alias resolves to.", "A", "AAAA", "MX", "NAPTR", "PTR", "SPF", "TXT", "SRV"),
		f("disable", "*bool", model.FieldScopeBoth, false, false, "Whether the record is disabled. Unlike most other record types, Alias exposes this via the SDK wrapper."),
	)
	fields = dropFields(fields, "ddns_principal", "ddns_protected") // Alias struct has no ddns_* fields
	return model.Resource{
		Slug:           "record_alias",
		Kind:           "AliasRecord",
		WAPIObjectType: "record:alias",
		Pattern:        model.PatternCRUD,
		GoStructName:   "RecordAlias",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateAliasRecord", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetAliasRecordByRef", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetAllAliasRecord", Receiver: "ObjectManager", Notes: "List/query only — no single-object GetAliasRecord(name) exists."},
			{Operation: "Update", Method: "UpdateAliasRecord", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteAliasRecord", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST; no user-controlled key survives a rename.",
		ExternalNameSourcePath: "_ref",
		Fields:                 fields,
		ImmutableFields:        []string{"zone"},
		MutableFields:          []string{"name", "target_name", "target_type", "disable", "comment", "ttl", "use_ttl", "extattrs", "view"},
		DeleteBehavior:         "hard-delete (404 on subsequent GET) — inferred from RecordA behavior",
		Notes:                  "Alternate pilot candidate. Unlike other simple record types, UpdateAliasRecord retains `dnsView` and exposes `disable` — treat `view` as tentatively mutable-via-wrapper pending Phase 4 live verification.",
	}
}

func httpsRecord() model.Resource {
	fields := dropFields(commonRecordFields(), "creator", "forbid_reclamation", "ddns_principal", "ddns_protected")
	fields = append(fields,
		f("name", "*string", model.FieldScopeBoth, true, false, "Owner name (FQDN) the HTTPS record applies to."),
		f("target_name", "*string", model.FieldScopeBoth, true, false, "Target FQDN for the HTTPS RR."),
		f("priority", "*uint32", model.FieldScopeBoth, true, false, "SvcPriority value."),
		f("svc_params", "[]SVCParams", model.FieldScopeBoth, false, false, "Service binding parameters (SvcParams), the same structure used by SVCBRecord."),
		f("disable", "*bool", model.FieldScopeBoth, false, false, "Whether the record is disabled."),
		f("forbid_reclamation", "*bool", model.FieldScopeBoth, false, false, "Whether reclamation is forbidden (overrides the response-only field from commonRecordFields — HTTPS exposes it as a Create/Update parameter)."),
		f("ddns_principal", "*string", model.FieldScopeBoth, false, false, "GSS-TSIG principal (overrides the response-only field from commonRecordFields — HTTPS exposes it as a Create/Update parameter)."),
		f("ddns_protected", "*bool", model.FieldScopeBoth, false, false, "Whether DDNS updates are protected (overrides the response-only field from commonRecordFields — HTTPS exposes it as a Create/Update parameter)."),
		f("creator", "string", model.FieldScopeBoth, false, false, "Record creator. HTTPS/SVCB expose this as a Create/Update parameter, unlike other record types."),
	)
	return model.Resource{
		Slug:           "record_https",
		Kind:           "HTTPSRecord",
		WAPIObjectType: "record:https",
		Pattern:        model.PatternCRUD,
		GoStructName:   "RecordHttps",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateHTTPSRecord", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetHTTPSRecordByRef", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetAllHTTPSRecord", Receiver: "ObjectManager", Notes: "List/query only — no single-object GetHTTPSRecord(name) exists."},
			{Operation: "Update", Method: "UpdateHTTPSRecord", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteHTTPSRecord", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST; no user-controlled key survives a rename.",
		ExternalNameSourcePath: "_ref",
		Fields:                 fields,
		ImmutableFields:        []string{"view", "zone"},
		MutableFields:          []string{"name", "target_name", "priority", "svc_params", "disable", "forbid_reclamation", "ddns_principal", "ddns_protected", "creator", "comment", "ttl", "use_ttl", "extattrs"},
		DeleteBehavior:         "hard-delete (404 on subsequent GET) — inferred from RecordA behavior",
		Notes:                  "`view` is present in CreateHTTPSRecord but absent from UpdateHTTPSRecord — immutable, consistent with the majority pattern.",
	}
}

func svcbRecord() model.Resource {
	fields := dropFields(commonRecordFields(), "creator", "forbid_reclamation", "ddns_principal", "ddns_protected")
	fields = append(fields,
		f("name", "*string", model.FieldScopeBoth, true, false, "Owner name (FQDN) the SVCB record applies to."),
		f("target_name", "*string", model.FieldScopeBoth, true, false, "Target FQDN for the SVCB RR."),
		f("priority", "*uint32", model.FieldScopeBoth, true, false, "SvcPriority value."),
		f("svc_params", "[]SVCParams", model.FieldScopeBoth, false, false, "Service binding parameters (SvcParams) — SvcValue must always be an array, even for valueless params (e.g. ohttp); omitting it causes a WAPI 'NoneType has no len()' server error (fixed upstream by removing omitempty on SVCParams.SvcValue)."),
		f("disable", "*bool", model.FieldScopeBoth, false, false, "Whether the record is disabled."),
		f("forbid_reclamation", "*bool", model.FieldScopeBoth, false, false, "Whether reclamation is forbidden (Create/Update parameter for SVCB, unlike most other record types)."),
		f("ddns_principal", "*string", model.FieldScopeBoth, false, false, "GSS-TSIG principal (Create/Update parameter for SVCB)."),
		f("ddns_protected", "*bool", model.FieldScopeBoth, false, false, "Whether DDNS updates are protected (Create/Update parameter for SVCB)."),
		f("creator", "string", model.FieldScopeBoth, false, false, "Record creator — Create/Update parameter for SVCB, unlike most other record types."),
	)
	return model.Resource{
		Slug:           "record_svcb",
		Kind:           "SVCBRecord",
		WAPIObjectType: "record:svcb",
		Pattern:        model.PatternCRUD,
		GoStructName:   "RecordSVCB",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateSVCBRecord", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetSVCBRecordByRef", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetAllSVCBRecords", Receiver: "ObjectManager", Notes: "List/query only — no single-object GetSVCBRecord(name) exists."},
			{Operation: "Update", Method: "UpdateSVCBRecord", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteSVCBRecord", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST; no user-controlled key survives a rename.",
		ExternalNameSourcePath: "_ref",
		Fields:                 fields,
		ImmutableFields:        []string{"view", "zone"},
		MutableFields:          []string{"name", "target_name", "priority", "svc_params", "disable", "forbid_reclamation", "ddns_principal", "ddns_protected", "creator", "comment", "ttl", "use_ttl", "extattrs"},
		DeleteBehavior:         "hard-delete (404 on subsequent GET) — inferred from RecordA behavior",
	}
}

func hostRecord() model.Resource {
	return model.Resource{
		Slug:           "host_record",
		Kind:           "HostRecord",
		WAPIObjectType: "record:host",
		Pattern:        model.PatternCRUD,
		GoStructName:   "HostRecord",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateHostRecord", Receiver: "ObjectManager"},
			{Operation: "Read", Method: "GetHostRecord", Receiver: "ObjectManager"},
			{Operation: "ReadByRef", Method: "GetHostRecordByRef", Receiver: "ObjectManager"},
			{Operation: "Update", Method: "UpdateHostRecord", Receiver: "ObjectManager"},
			{Operation: "Delete", Method: "DeleteHostRecord", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST; no user-controlled key survives a rename.",
		ExternalNameSourcePath: "_ref",
		Fields: []model.Field{
			f("_ref", "string", model.FieldScopeResponse, false, false, "Server-assigned opaque object reference. This is the Crossplane external-name."),
			f("name", "*string", model.FieldScopeBoth, true, false, "Host name in FQDN format."),
			f("network_view", "string", model.FieldScopeBoth, true, false, "Network view the host record resides in. Both Create and Update accept this parameter (unlike Network/NetworkContainer, where it is immutable) — treat as mutable-via-wrapper pending live verification."),
			f("view", "*string", model.FieldScopeBoth, true, false, "DNS view. Both Create and Update accept this parameter — same caveat as network_view."),
			f("ipv4addrs", "[]HostRecordIpv4Addr", model.FieldScopeBoth, false, false, "List of IPv4 addresses for the host (static or CIDR-allocated)."),
			f("ipv6addrs", "[]HostRecordIpv6Addr", model.FieldScopeBoth, false, false, "List of IPv6 addresses for the host."),
			f("aliases", "[]string", model.FieldScopeBoth, false, false, "List of alias FQDNs for the host."),
			f("configure_for_dns", "*bool", model.FieldScopeBoth, false, false, "enable_dns — whether the host has DNS (parent zone) information. When false, no DNS records are created."),
			f("mac_address", "string", model.FieldScopeRequest, false, false, "MAC address for DHCP association (write-only SDK parameter; persisted on the nested ipv4addrs/ipv6addrs entries, not a top-level response field)."),
			f("duid", "string", model.FieldScopeRequest, false, false, "DHCP unique identifier for IPv6 DHCP association (write-only SDK parameter)."),
			f("comment", "*string", model.FieldScopeBoth, false, false, "Comment for the record; maximum 256 characters."),
			f("disable", "*bool", model.FieldScopeBoth, false, false, "Whether the record is disabled."),
			f("extattrs", "EA", model.FieldScopeBoth, false, false, "Extensible attributes."),
			f("ttl", "*uint32", model.FieldScopeBoth, false, false, "Time-to-live in seconds."),
			f("use_ttl", "*bool", model.FieldScopeBoth, false, false, "Use flag for ttl."),
			f("zone", "string", model.FieldScopeResponse, false, true, "Zone the record resides in. Derived from name/view; immutable."),
			f("dns_name", "string", model.FieldScopeResponse, false, false, "Host name in punycode format."),
			f("dns_aliases", "[]string", model.FieldScopeResponse, false, false, "Alias FQDNs in punycode format."),
			f("cloud_info", "*GridCloudapiInfo", model.FieldScopeResponse, false, false, "Cloud API related information."),
			f("creation_time", "*UnixTime", model.FieldScopeResponse, false, false, "Record creation time (Epoch seconds)."),
			f("ddns_protected", "*bool", model.FieldScopeResponse, false, false, "Whether DDNS updates are protected."),
			f("last_queried", "*UnixTime", model.FieldScopeResponse, false, false, "Time of the last DNS query."),
			f("ms_ad_user_data", "*MsserverAduserData", model.FieldScopeResponse, false, false, "Microsoft Active Directory user information."),
			f("device_description", "*string", model.FieldScopeResponse, false, false, "Discovered device description (DNS/network discovery feature)."),
			f("device_location", "*string", model.FieldScopeResponse, false, false, "Discovered device location."),
			f("device_type", "*string", model.FieldScopeResponse, false, false, "Discovered device type."),
			f("device_vendor", "*string", model.FieldScopeResponse, false, false, "Discovered device vendor."),
			fEnum("rrset_order", "*string", model.FieldScopeResponse, false, false, "Order in which resource record sets are returned.", "cyclic", "random", "fixed"),
		},
		FullSchemaNotes: "HostRecord also has discovery/credential fields (allow_telnet, cli_credentials, snmp_credential, snmp3_credential, restart_if_needed) not exposed by CreateHostRecord/UpdateHostRecord — these require the generic Connector.",
		ImmutableFields: []string{"zone"},
		MutableFields:   []string{"name", "network_view", "view", "ipv4addrs", "ipv6addrs", "aliases", "configure_for_dns", "comment", "disable", "extattrs", "ttl", "use_ttl"},
		DeleteBehavior:  "hard-delete (404 on subsequent GET) — inferred from RecordA behavior",
		Notes:           "Complex composite record: holds a list of IPv4/IPv6 addresses (each with its own MAC/DUID) rather than a single address. Both network_view and view are accepted by Update (unlike simple record types) — a real behavioral difference worth live-verifying before relying on it.",
	}
}

// dropFields removes fields with the given JSON names from fs, returning a
// new slice. Used when a resource-specific override needs to replace a
// field inherited from commonRecordFields with a different Scope/Required
// value, or when a common field genuinely does not apply to one resource
// (e.g. NS records have no comment/ttl/extattrs).
func dropFields(fs []model.Field, names ...string) []model.Field {
	drop := make(map[string]bool, len(names))
	for _, n := range names {
		drop[n] = true
	}
	out := make([]model.Field, 0, len(fs))
	for _, field := range fs {
		if drop[field.JSONName] {
			continue
		}
		out = append(out, field)
	}
	return out
}
