package lang

func init() {
	Register(&LanguageSpec{
		Language:       Kotlin,
		FileExtensions: []string{".kt", ".kts"},
		FunctionNodeTypes: []string{
			"function_declaration",
			"secondary_constructor",
			"anonymous_function",
		},
		ClassNodeTypes: []string{
			"class_declaration",
			"object_declaration",
			"companion_object",
		},
		ModuleNodeTypes: []string{"source_file"},
		CallNodeTypes: []string{
			"call_expression",
			"navigation_expression",
		},
		ImportNodeTypes: []string{"import"},
		ImportFromTypes: []string{"import"},

		BranchingNodeTypes:  []string{"if_expression", "for_statement", "while_statement", "when_expression", "when_entry", "try_expression", "catch_block"},
		VariableNodeTypes:   []string{"property_declaration"},
		AssignmentNodeTypes: []string{"assignment", "directly_assignable_expression"},
		ThrowNodeTypes:      []string{"throw_expression"},
		DecoratorNodeTypes:  []string{"annotation"},
		EnvAccessFunctions:  []string{"System.getenv", "System.getProperty"},
	})
}
