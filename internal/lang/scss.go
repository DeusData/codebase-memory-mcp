package lang

func init() {
	Register(&LanguageSpec{
		Language:        SCSS,
		FileExtensions:  []string{".scss"},
		ModuleNodeTypes: []string{"stylesheet"},
		FunctionNodeTypes: []string{
			"mixin_statement",
			"function_statement",
		},
		ClassNodeTypes:     []string{},
		FieldNodeTypes:     []string{},
		CallNodeTypes:      []string{"call_expression"},
		ImportNodeTypes:    []string{"import_statement", "use_statement"},
		VariableNodeTypes:  []string{"declaration"},
		BranchingNodeTypes: []string{"if_statement"},
	})
}
