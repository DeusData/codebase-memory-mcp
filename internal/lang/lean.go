package lang

func init() {
	Register(&LanguageSpec{
		Language:            Lean,
		FileExtensions:      []string{".lean"},
		FunctionNodeTypes:   []string{"def", "theorem", "instance", "abbrev"},
		ClassNodeTypes:      []string{"structure", "class_inductive", "inductive"},
		ModuleNodeTypes:     []string{"module"},
		CallNodeTypes:       []string{"apply"},
		ImportNodeTypes:     []string{"import"},
		BranchingNodeTypes:  []string{"if", "match", "do"},
		VariableNodeTypes:   []string{},
		AssignmentNodeTypes: []string{},
	})
}
