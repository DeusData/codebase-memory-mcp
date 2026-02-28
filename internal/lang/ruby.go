package lang

func init() {
	Register(&LanguageSpec{
		Language:          Ruby,
		FileExtensions:    []string{".rb"},
		ClassNodeTypes:    []string{"class", "module"},
		ModuleNodeTypes:   []string{"program"},
		FunctionNodeTypes: []string{"method"},
		FieldNodeTypes:    []string{"assignment", "var_field", "instance_variable", "class_variable"},
		CallNodeTypes:     []string{"call", "command", "command_call"},
		ImportNodeTypes:   []string{"require", "require_relative"},
		ImportFromTypes:   []string{"require", "require_relative"},
		BranchingNodeTypes: []string{
			"if", "unless", "while", "until", "for", "case", "when", "rescue",
		},
		AssignmentNodeTypes: []string{"assignment", "var_field", "instance_variable", "class_variable"},
		ThrowNodeTypes:      []string{"raise"},
		ThrowsClauseField:   "",
		DecoratorNodeTypes:  []string{},
		EnvAccessFunctions:  []string{"ENV[]", "ENV.fetch"},
	})
}
