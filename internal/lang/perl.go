package lang

func init() {
	Register(&LanguageSpec{
		Language:       Perl,
		FileExtensions: []string{".pl", ".pm"},
		FunctionNodeTypes: []string{
			"subroutine_declaration_statement",
		},
		ClassNodeTypes:  []string{},
		FieldNodeTypes:  []string{},
		ModuleNodeTypes: []string{"source_file"},
		CallNodeTypes:   []string{"ambiguous_function_call_expression", "function_call_expression"},
		ImportNodeTypes: []string{"use_statement", "require_statement"},
		BranchingNodeTypes: []string{
			"if_statement", "unless_statement", "for_statement",
			"foreach_statement", "while_statement",
		},
		VariableNodeTypes:   []string{"variable_declaration"},
		AssignmentNodeTypes: []string{"assignment_expression"},
		EnvAccessFunctions:  []string{"$ENV"},
	})
}
