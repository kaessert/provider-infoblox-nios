package catalog

import "github.com/crossplane-contrib/provider-infoblox-nios/tools/openapi/pkg/model"

func eaDefinition() model.Resource {
	return model.Resource{
		Slug:           "extensible_attribute_def",
		Kind:           "ExtensibleAttributeDef",
		WAPIObjectType: "extensibleattributedef",
		Pattern:        model.PatternCreateReadDelete,
		GoStructName:   "EADefinition",
		CRUD: []model.CRUDMethod{
			{Operation: "Create", Method: "CreateEADefinition", Receiver: "ObjectManager", Notes: "NON-STANDARD: takes a single EADefinition struct parameter rather than individual scalar parameters like every other Create<Resource> method."},
			{Operation: "Read", Method: "GetEADefinition", Receiver: "ObjectManager"},
		},
		ExternalNameStrategy:   model.StrategyServerAssigned,
		ExternalNameRationale:  "WAPI assigns an opaque `_ref` on POST.",
		ExternalNameSourcePath: "_ref",
		Fields: []model.Field{
			f("_ref", "string", model.FieldScopeResponse, false, false, "Server-assigned opaque object reference. This is the Crossplane external-name."),
			f("name", "*string", model.FieldScopeBoth, true, true, "Name of the Extensible Attribute Definition. No Update method exists at all \u2014 treat as immutable through the wrapper."),
			fEnum("type", "string", model.FieldScopeBoth, true, true, "Data type of the extensible attribute's value.", "STRING", "INTEGER", "ENUM", "DATE", "EMAIL", "URL"),
			f("comment", "*string", model.FieldScopeBoth, false, true, "Comment for the definition; maximum 256 characters."),
			f("default_value", "*string", model.FieldScopeBoth, false, true, "Default value pre-populated in the GUI."),
			f("allowed_object_types", "[]string", model.FieldScopeBoth, false, true, "WAPI object types this attribute may be associated with (empty = all types)."),
			f("list_values", "[]*EADefListValue", model.FieldScopeBoth, false, true, "Enumerated valid values (type=ENUM)."),
			f("min", "*uint32", model.FieldScopeBoth, false, true, "Minimum allowed value (type=INTEGER)."),
			f("max", "*uint32", model.FieldScopeBoth, false, true, "Maximum allowed value (type=INTEGER)."),
			fEnum("flags", "*string", model.FieldScopeBoth, false, true, "Extensible attribute flags. Order-sensitive letter codes.", "A", "C", "G", "I", "L", "M", "P", "R", "S", "V"),
			f("descendants_action", "*ExtensibleattributedefDescendants", model.FieldScopeBoth, false, true, "Action taken on descendants of objects carrying this attribute when it is Inheritable."),
		},
		ImmutableFields: []string{"name", "type", "comment", "default_value", "allowed_object_types", "list_values", "min", "max", "flags", "descendants_action"},
		MutableFields:   []string{},
		DeleteBehavior:  "not exercised via ObjectManager \u2014 no DeleteEADefinition method exists; deleting an EA definition (or changing any field after creation) requires the generic Connector.",
		Notes: "Grid-wide extensible attribute schema (defines the key/type/constraints for the `extattrs`/Ea maps every other resource in this catalog carries). " +
			"NON-STANDARD CRUD: the ObjectManager wrapper supports Create+Read ONLY \u2014 there is no Update or Delete method. Every field is therefore effectively immutable through the wrapper; a controller needing to edit or remove a definition must fall back to the generic Connector (UpdateObject/DeleteObject) with a hand-built field map, the same pattern already required for ZoneAuth updates.",
	}
}
