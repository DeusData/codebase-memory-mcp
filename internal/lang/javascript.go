package lang

func init() {
	Register(&LanguageSpec{
		Language:       JavaScript,
		FileExtensions: []string{".js", ".jsx"},
		FunctionNodeTypes: []string{
			"function_declaration",
			"generator_function_declaration",
			"function_expression",
			"arrow_function",
			"method_definition",
		},
		ClassNodeTypes:  []string{"class_declaration", "class"},
		ModuleNodeTypes: []string{"program"},
		CallNodeTypes:   []string{"call_expression"},
		ImportNodeTypes: []string{"import_statement", "lexical_declaration", "export_statement"},
		ImportFromTypes: []string{"import_statement", "lexical_declaration", "export_statement"},
	})
}
