package lang

func init() {
	Register(&LanguageSpec{
		Language:       Groovy,
		FileExtensions: []string{".groovy", ".gradle"},
		FunctionNodeTypes: []string{
			"function_definition", // def greet() {} or void greet() {}
		},
		ClassNodeTypes: []string{
			"class_definition", // class Foo extends Bar {}
		},
		FieldNodeTypes:  []string{},
		ModuleNodeTypes: []string{"source_file"},
		CallNodeTypes:   []string{"function_call", "juxt_function_call"},
		ImportNodeTypes: []string{"groovy_import"},
		BranchingNodeTypes: []string{
			"if_statement", "for_statement", "while_statement",
			"switch_statement",
		},
		VariableNodeTypes:   []string{"declaration"},
		AssignmentNodeTypes: []string{"assignment"},
		ThrowNodeTypes:      []string{"throw_statement"},
		DecoratorNodeTypes:  []string{"annotation"},
	})
}
