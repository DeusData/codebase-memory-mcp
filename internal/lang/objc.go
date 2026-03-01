package lang

func init() {
	Register(&LanguageSpec{
		Language:       ObjectiveC,
		FileExtensions: []string{".m"},
		FunctionNodeTypes: []string{
			"function_definition",
			"method_definition",
		},
		ClassNodeTypes: []string{
			"class_interface",
			"class_implementation",
			"protocol_declaration",
		},
		FieldNodeTypes:  []string{"property_declaration"},
		ModuleNodeTypes: []string{"translation_unit"},
		CallNodeTypes:   []string{"call_expression", "message_expression"},
		ImportNodeTypes: []string{"preproc_import"},

		BranchingNodeTypes:  []string{"if_statement", "for_statement", "while_statement", "switch_statement"},
		VariableNodeTypes:   []string{"declaration"},
		AssignmentNodeTypes: []string{"assignment_expression"},
	})
}
