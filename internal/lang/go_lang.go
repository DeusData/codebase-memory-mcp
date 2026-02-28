package lang

func init() {
	Register(&LanguageSpec{
		Language:          Go,
		FileExtensions:    []string{".go"},
		FunctionNodeTypes: []string{"function_declaration", "method_declaration"},
		ClassNodeTypes:    []string{"type_spec", "type_alias"},
		FieldNodeTypes:    []string{"field_declaration"},
		ModuleNodeTypes:   []string{"source_file"},
		CallNodeTypes:     []string{"call_expression"},
		ImportNodeTypes:   []string{"import_declaration"},
		ImportFromTypes:   []string{"import_declaration"},

		BranchingNodeTypes:  []string{"if_statement", "for_statement", "switch_expression", "select_statement", "case_clause", "default_clause"},
		VariableNodeTypes:   []string{"var_declaration", "const_declaration"},
		AssignmentNodeTypes: []string{"assignment_statement", "short_var_declaration"},
		EnvAccessFunctions:  []string{"os.Getenv", "os.LookupEnv"},
	})
}
