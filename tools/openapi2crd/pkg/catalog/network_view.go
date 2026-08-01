package catalog

// networkView returns the NetworkView resource descriptor.
//
// Source: tools/openapi/inventory.md, "### NetworkView" section (fields
// request=0, response=12, both=3) — derived from the pinned
// infoblox-go-client/v2 SDK (tools/openapi/specs/infobloxopen/) and
// live-verified 2026-07-28 against a real NIOS Grid Manager appliance
// (GET networkview returned exactly one entry, "default", with
// is_default=true).
//
// Pattern: well-known-default — a "default" network view always exists on
// the Grid and cannot be deleted, but additional custom views can be
// created, updated, and deleted through the ObjectManager wrapper like any
// other resource.
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateNetworkView).
//
// Cataloged fields are intentionally limited to what the ObjectManager
// wrapper's CreateNetworkView/UpdateNetworkView methods can actually set —
// name, comment, and extattrs — plus the two response-only fields the
// controller needs to observe (`_ref` for the external name and
// `is_default` for the well-known-default invariant). The raw NetworkView
// WAPI object also carries several Grid-administrative, response-only
// fields (associated_dns_views, associated_members, cloud_info,
// ddns_dns_view, ddns_zone_primaries, internal_forward_zones, mgm_private,
// ms_ad_user_data, remote_forward_zones, remote_reverse_zones) that are
// never accepted as create/update parameters by the wrapper; they are
// omitted here rather than cataloged as fields with no working setter (same
// precedent as ARecord's `disable` field — see dns_records.go).
//
// Immutable fields: `is_default` is response-only (Scope
// FieldScopeResponse) and has no ForProvider representation — WAPI decides
// which view is the Grid's default, it is never a create/update parameter.
// It still gets a CEL `self == oldSelf` rule because the field is
// server-controlled and must never appear to change once observed.
//
// No cross-resource references: NetworkView is a reference TARGET for
// FixedAddress, Network, NetworkContainer, and Range (via their
// `networkView` field) but does not itself reference any other cataloged
// resource.
func networkView() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 kindNetworkView,
		Slug:                 slugNetworkView,
		ClusterGroup:         clusterGroup(slugNetworkView),
		NamespacedGroup:      namespacedGroup(slugNetworkView),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "Name",
				JSONName:    "name",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Name of the network view. Mutable — UpdateNetworkView accepts a new name, but renaming changes the record's _ref.",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the network view; maximum 256 characters.",
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
				Description: "Server-assigned opaque object reference (WAPI `_ref`, e.g. `networkview/ZG5z...:default/true`). Mirrors the crossplane.io/external-name annotation for observability and uptest import verification.",
			},
			{
				Name:        "IsDefault",
				JSONName:    "isDefault",
				GoType:      goTypeBool,
				Scope:       FieldScopeResponse,
				Immutable:   true,
				Description: "Whether this is the Grid's default network view. Exactly one exists per Grid and it cannot be deleted (live-verified: GET networkview always returns a `default` entry with is_default=true). Server-controlled — never a create/update parameter.",
			},
		},
	}
}
