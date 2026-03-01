package lang

func init() {
	Register(&LanguageSpec{
		Language:       Swift,
		FileExtensions: []string{".swift"},
		FunctionNodeTypes: []string{
			"function_declaration",
		},
		ClassNodeTypes: []string{
			"class_declaration",
			"protocol_declaration",
			"struct_declaration",
			"enum_declaration",
		},
		FieldNodeTypes:  []string{"property_declaration"},
		ModuleNodeTypes: []string{"source_file"},
		CallNodeTypes:   []string{"call_expression"},
		ImportNodeTypes: []string{"import_declaration"},
		BranchingNodeTypes: []string{
			"if_statement", "guard_statement", "for_statement",
			"while_statement", "switch_statement",
		},
		VariableNodeTypes:   []string{"property_declaration"},
		AssignmentNodeTypes: []string{"assignment"},
		ThrowNodeTypes:      []string{"throw_statement"},
		DecoratorNodeTypes:  []string{"attribute"},
	})
}
