package lang

func init() {
	Register(&LanguageSpec{
		Language:       TSX,
		FileExtensions: []string{".tsx"},
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

		BranchingNodeTypes:      []string{"if_statement", "for_statement", "for_in_statement", "while_statement", "switch_statement", "case_clause", "try_statement", "catch_clause"},
		VariableNodeTypes:       []string{"lexical_declaration", "variable_declaration"},
		AssignmentNodeTypes:     []string{"assignment_expression", "augmented_assignment_expression"},
		ThrowNodeTypes:          []string{"throw_statement"},
		DecoratorNodeTypes:      []string{"decorator"},
		EnvAccessMemberPatterns: []string{"process.env"},
	})
}
