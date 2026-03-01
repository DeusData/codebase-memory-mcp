package lang

func init() {
	Register(&LanguageSpec{
		Language:          Scala,
		FileExtensions:    []string{".scala", ".sc"},
		FunctionNodeTypes: []string{"function_definition", "function_declaration"},
		ClassNodeTypes: []string{
			"class_definition",
			"object_definition",
			"trait_definition",
		},
		ModuleNodeTypes: []string{"compilation_unit"},
		CallNodeTypes: []string{
			"call_expression",
			"generic_function",
			"field_expression",
			"infix_expression",
		},
		ImportNodeTypes: []string{"import_declaration"},
		ImportFromTypes: []string{"import_declaration"},

		BranchingNodeTypes:  []string{"if_expression", "for_expression", "while_expression", "match_expression", "case_clause", "try_expression", "catch_clause"},
		VariableNodeTypes:   []string{"val_definition", "var_definition", "val_declaration", "var_declaration"},
		AssignmentNodeTypes: []string{"assignment_expression"},
		ThrowNodeTypes:      []string{"throw_expression"},
		EnvAccessFunctions:  []string{"sys.env", "System.getenv", "System.getProperty"},
	})
}
