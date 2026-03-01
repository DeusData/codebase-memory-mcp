package lang

func init() {
	Register(&LanguageSpec{
		Language:       Erlang,
		FileExtensions: []string{".erl"},
		FunctionNodeTypes: []string{
			"function_clause",
		},
		ClassNodeTypes:  []string{},
		FieldNodeTypes:  []string{},
		ModuleNodeTypes: []string{"source_file"},
		CallNodeTypes:   []string{"call"},
		ImportNodeTypes: []string{"module_attribute"},
		BranchingNodeTypes: []string{
			"if_expression", "case_expression", "receive_expression",
		},
		VariableNodeTypes:   []string{"pp_define", "record_decl"},
		AssignmentNodeTypes: []string{"match_expression"},
		ThrowNodeTypes:      []string{"call"},
	})
}
