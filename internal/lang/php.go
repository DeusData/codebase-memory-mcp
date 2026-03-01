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
			"method_declaration",
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

		BranchingNodeTypes:  []string{"if_statement", "for_statement", "foreach_statement", "while_statement", "switch_statement", "case_statement", "try_statement", "catch_clause"},
		VariableNodeTypes:   []string{"expression_statement"},
		AssignmentNodeTypes: []string{"assignment_expression"},
		ThrowNodeTypes:      []string{"throw_expression"},
		DecoratorNodeTypes:  []string{"attribute_group"},
		EnvAccessFunctions:  []string{"getenv", "env"},
	})
}
