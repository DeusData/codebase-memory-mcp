package lang

func init() {
	Register(&LanguageSpec{
		Language:            MATLAB,
		FileExtensions:      []string{".matlab", ".mlx"},
		FunctionNodeTypes:   []string{"function_definition"},
		ClassNodeTypes:      []string{"class_definition"},
		ModuleNodeTypes:     []string{"source_file"},
		CallNodeTypes:       []string{"function_call", "command"},
		ImportNodeTypes:     []string{},
		BranchingNodeTypes:  []string{"if_statement", "for_statement", "while_statement", "switch_statement", "try_statement"},
		VariableNodeTypes:   []string{"assignment"},
		AssignmentNodeTypes: []string{"assignment"},
	})
}
