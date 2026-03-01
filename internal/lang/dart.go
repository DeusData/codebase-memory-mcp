package lang

func init() {
	Register(&LanguageSpec{
		Language:       Dart,
		FileExtensions: []string{".dart"},
		FunctionNodeTypes: []string{
			"function_signature", // top-level function: return_type name(params)
			"method_signature",   // class method: wraps function_signature
		},
		ClassNodeTypes: []string{
			"class_definition",
			"enum_declaration",
			"mixin_declaration",
		},
		FieldNodeTypes:  []string{"declaration"},
		ModuleNodeTypes: []string{"program"},
		CallNodeTypes:   []string{"selector"},
		ImportNodeTypes: []string{"import_or_export"},
		BranchingNodeTypes: []string{
			"if_statement", "for_statement", "while_statement",
			"switch_statement",
		},
		VariableNodeTypes:   []string{"declaration"},
		AssignmentNodeTypes: []string{"assignment_expression"},
		ThrowNodeTypes:      []string{"throw_expression"},
		DecoratorNodeTypes:  []string{"annotation"},
	})
}
