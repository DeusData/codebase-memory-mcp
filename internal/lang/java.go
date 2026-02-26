package lang

func init() {
	Register(&LanguageSpec{
		Language:          Java,
		FileExtensions:    []string{".java"},
		FunctionNodeTypes: []string{"method_declaration", "constructor_declaration"},
		ClassNodeTypes: []string{
			"class_declaration",
			"interface_declaration",
			"enum_declaration",
			"annotation_type_declaration",
			"record_declaration",
		},
		FieldNodeTypes: []string{"field_declaration"},
		ModuleNodeTypes: []string{"program"},
		CallNodeTypes:   []string{"method_invocation"},
		ImportNodeTypes: []string{"import_declaration"},
		ImportFromTypes: []string{"import_declaration"},
	})
}
