package lang

func init() {
	Register(&LanguageSpec{
		Language:       PHP,
		FileExtensions: []string{".php"},
		FunctionNodeTypes: []string{
			"function_static_declaration",
			"anonymous_function",
			"function_definition",
			"arrow_function",
		},
		ClassNodeTypes: []string{
			"trait_declaration",
			"enum_declaration",
			"interface_declaration",
			"class_declaration",
		},
		ModuleNodeTypes: []string{"program"},
		CallNodeTypes: []string{
			"member_call_expression",
			"scoped_call_expression",
			"function_call_expression",
			"nullsafe_member_call_expression",
		},
	})
}
