package lang

func init() {
	Register(&LanguageSpec{
		Language:       C,
		FileExtensions: []string{".c"},
		FunctionNodeTypes: []string{
			"function_definition",
		},
		ClassNodeTypes:  []string{"struct_specifier", "enum_specifier", "union_specifier"},
		FieldNodeTypes:  []string{"field_declaration"},
		ModuleNodeTypes: []string{"translation_unit"},
		CallNodeTypes:   []string{"call_expression"},
		ImportNodeTypes: []string{"preproc_include"},

		BranchingNodeTypes:  []string{"if_statement", "for_statement", "while_statement", "do_statement", "switch_statement", "case_statement"},
		VariableNodeTypes:   []string{"declaration"},
		AssignmentNodeTypes: []string{"assignment_expression"},
		EnvAccessFunctions:  []string{"getenv"},
	})
}
