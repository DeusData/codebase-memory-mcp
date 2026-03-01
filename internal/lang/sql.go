package lang

func init() {
	Register(&LanguageSpec{
		Language:        SQL,
		FileExtensions:  []string{".sql"},
		ModuleNodeTypes: []string{"program"},
		FunctionNodeTypes: []string{
			"create_function",
		},
		ClassNodeTypes:  []string{},
		FieldNodeTypes:  []string{"column_definition"},
		CallNodeTypes:   []string{"function_call"},
		ImportNodeTypes: []string{},
		VariableNodeTypes: []string{
			"create_table",
			"create_view",
		},
		BranchingNodeTypes: []string{"if_statement", "case_expression"},
	})
}
