package catalog

// extensibleAttributeDef returns the ExtensibleAttributeDef resource
// descriptor.
//
// Source: tools/openapi/inventory.md, "### ExtensibleAttributeDef" section
// (fields request=0, response=1, both=10) — itself derived from the pinned
// infoblox-go-client/v2 SDK (tools/openapi/specs/infobloxopen/) and
// corrected by live probing against a real NIOS Grid Manager appliance.
//
// Pattern: CRUD. The SDK's ObjectManager wrapper only exposes
// CreateEADefinition/GetEADefinition (no Update/Delete wrappers), but the
// underlying WAPI object supports PUT and DELETE directly — a controller
// needing to update or remove a definition falls back to a direct WAPI call
// with a hand-built field map, the same pattern already required for
// ZoneAuth updates.
//
// External-name strategy: server-assigned (the WAPI `_ref` returned by
// CreateEADefinition).
//
// Immutable fields: `type` is rejected by WAPI on update ("cannot be
// modified"). `min` is rejected by WAPI on update ("cannot be modified").
// `max` is inferred immutable (same family as `min`, both apply only when
// type=INTEGER). `name`, `comment`, and `default_value` are mutable
// (confirmed against a live Grid Manager appliance, correcting the Phase 1
// inventory which listed every field as immutable due to the SDK wrapper's
// missing Update method). `flags`, `list_values`, and `allowed_object_types`
// are mutable per the WAPI field schema's `supports=rwu` flag.
//
// `descendants_action` is write-only: the WAPI field schema reports
// `supports=wu` and a GET requesting it in `_return_fields` fails with
// "Field is not readable". It is cataloged as request-only
// (FieldScopeRequest) with OmitFromObservation set, so it appears in
// ForProvider but is never mirrored into AtProvider — a controller must
// never include it in a `_return_fields` list.
//
// `flags` is a combination of order-sensitive single-letter codes (e.g.
// "CR" for Cloud API + Read Only), not a single enumerated value, so no
// kubebuilder Enum marker is emitted for it — the valid letters are
// documented in its field description instead.
//
// Nested type: EADefListValue mirrors the SDK's list_values entries
// (applicable when type=ENUM). It is user-settable (unlike ARecord's
// response-only DiscoveredData) — it appears in ForProvider and is mirrored
// into AtProvider via the parent field's FieldScopeBoth scope.
//
// No cross-resource references: ExtensibleAttributeDef does not reference
// any other cataloged resource.
func extensibleAttributeDef() ResourceDescriptor {
	return ResourceDescriptor{
		Kind:                 "ExtensibleAttributeDef",
		Slug:                 "extensibleattributedef",
		ClusterGroup:         clusterGroup("extensibleattributedef"),
		NamespacedGroup:      namespacedGroup("extensibleattributedef"),
		ExternalNameStrategy: StrategyServerAssigned,
		Fields: []FieldDef{
			{
				Name:        "Name",
				JSONName:    "name",
				GoType:      "string",
				Scope:       FieldScopeBoth,
				Required:    true,
				Description: "Name of the Extensible Attribute Definition. Mutable — renaming changes the record's _ref.",
			},
			{
				Name:        "Type",
				JSONName:    "type",
				GoType:      "string",
				Scope:       FieldScopeBoth,
				Required:    true,
				Immutable:   true,
				Enum:        []string{"STRING", "INTEGER", "ENUM", "DATE", "EMAIL", "URL"},
				Description: "Data type of the extensible attribute's value. Immutable — WAPI rejects a change with \"Type of extensible attribute definition cannot be modified\".",
			},
			{
				Name:        "Comment",
				JSONName:    "comment",
				GoType:      goTypeString,
				Scope:       FieldScopeBoth,
				Description: "Comment for the Extensible Attribute Definition; maximum 256 characters. Mutable.",
			},
			{
				Name:     "DefaultValue",
				JSONName: "defaultValue",
				GoType:   goTypeString,
				Scope:    FieldScopeBoth,
				Description: "Default value used to pre-populate the attribute value in the GUI. For email, URL, " +
					"and string types, a string with a maximum of 256 characters. For an integer, an integer " +
					"from -2147483648 through 2147483647. For a date, the number of seconds elapsed since " +
					"January 1st, 1970 UTC. Mutable.",
			},
			{
				Name:        "Min",
				JSONName:    "min",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Immutable:   true,
				Description: "Minimum allowed value of the extensible attribute. Applicable if type=INTEGER. Immutable — WAPI rejects a change with \"Minimum value cannot be modified\".",
			},
			{
				Name:        "Max",
				JSONName:    "max",
				GoType:      goTypeUint32,
				Scope:       FieldScopeBoth,
				Immutable:   true,
				Description: "Maximum allowed value of the extensible attribute. Applicable if type=INTEGER. Immutable — same family as min, rejected by WAPI on update.",
			},
			{
				Name:     "Flags",
				JSONName: "flags",
				GoType:   goTypeString,
				Scope:    FieldScopeBoth,
				Description: "Extensible attribute flags. Possible letters, most-significant first: " +
					"(A)udited, (C)loud API, Cloud (G)master, (I)nheritable, (L)isted, (M)andatory value, " +
					"MGM (P)rivate, (R)ead Only, (S)ort enum values, Multiple (V)alues. If there are two or " +
					"more flags they must be listed in the order shown above (e.g. \"CR\" is valid, \"RC\" is " +
					"not). Mutable.",
			},
			{
				Name:        "ListValues",
				JSONName:    "listValues",
				GoType:      "[]EADefListValue",
				Scope:       FieldScopeBoth,
				Description: "Enumerated valid values. Applicable if type=ENUM. Mutable.",
			},
			{
				Name:        "AllowedObjectTypes",
				JSONName:    "allowedObjectTypes",
				GoType:      "[]string",
				Scope:       FieldScopeBoth,
				Description: "WAPI object types this attribute is allowed to associate with (empty means all types). Mutable.",
			},
			{
				Name:     "DescendantsAction",
				JSONName: "descendantsAction",
				GoType:   goTypeString,
				Scope:    FieldScopeRequest,
				Description: "Action taken on descendant objects carrying this attribute when it is Inheritable. " +
					"Write-only — the WAPI field schema marks it not readable, so it is never mirrored into " +
					"the observed state; a controller must never request it in _return_fields.",
				OmitFromObservation: true,
			},
			// Response-only fields
			{
				Name:        "Ref",
				JSONName:    "ref",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Server-assigned opaque object reference (WAPI `_ref`). Mirrors the crossplane.io/external-name annotation for observability and uptest import verification.",
			},
			{
				Name:        "Namespace",
				JSONName:    "namespace",
				GoType:      goTypeString,
				Scope:       FieldScopeResponse,
				Description: "Namespace for the Extensible Attribute Definition. Read-only.",
			},
		},
		NestedTypes: []NestedTypeDef{
			{
				TypeName:    "EADefListValue",
				Description: "holds one enumerated value entry for an ExtensibleAttributeDef with type=ENUM.",
				Fields: []FieldDef{
					{Name: "Value", JSONName: "value", GoType: "string", Required: true, Description: "The enumerated value."},
				},
			},
		},
	}
}
