package lang

func init() {
	Register(&LanguageSpec{
		Language:       TypeScript,
		FileExtensions: []string{".ts"},
		FunctionNodeTypes: []string{
			"function_declaration",
			"generator_function_declaration",
			"function_expression",
			"arrow_function",
			"method_definition",
			"function_signature",
		},
		ClassNodeTypes: []string{
			"class_declaration",
			"class",
			"abstract_class_declaration",
			"enum_declaration",
			"interface_declaration",
			"type_alias_declaration",
			"internal_module",
		},
		ModuleNodeTypes: []string{"program"},
		CallNodeTypes:   []string{"call_expression"},
		ImportNodeTypes: []string{"import_statement", "lexical_declaration", "export_statement"},
		ImportFromTypes: []string{"import_statement", "lexical_declaration", "export_statement"},
	})
}
