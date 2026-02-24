package lang

func init() {
	Register(&LanguageSpec{
		Language:       Scala,
		FileExtensions: []string{".scala", ".sc"},
		FunctionNodeTypes: []string{"function_definition", "function_declaration"},
		ClassNodeTypes: []string{
			"class_definition",
			"object_definition",
			"trait_definition",
		},
		ModuleNodeTypes: []string{"compilation_unit"},
		CallNodeTypes: []string{
			"call_expression",
			"generic_function",
			"field_expression",
			"infix_expression",
		},
		ImportNodeTypes: []string{"import_declaration"},
		ImportFromTypes: []string{"import_declaration"},
	})
}
