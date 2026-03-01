package lang

func init() {
	Register(&LanguageSpec{
		Language:       Ruby,
		FileExtensions: []string{".rb", ".rake", ".gemspec"},
		FunctionNodeTypes: []string{
			"method",
			"singleton_method",
		},
		ClassNodeTypes:  []string{"class", "module"},
		ModuleNodeTypes: []string{"program"},
		CallNodeTypes:   []string{"call", "command_call"},
		ImportNodeTypes: []string{"call"},

		BranchingNodeTypes:      []string{"if", "unless", "while", "until", "for", "case", "when", "rescue", "elsif"},
		VariableNodeTypes:       []string{"assignment"},
		AssignmentNodeTypes:     []string{"assignment", "operator_assignment"},
		DecoratorNodeTypes:      []string{},
		EnvAccessMemberPatterns: []string{"ENV"},
	})
}
