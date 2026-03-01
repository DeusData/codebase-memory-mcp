package lang

func init() {
	Register(&LanguageSpec{
		Language:          Bash,
		FileExtensions:    []string{".sh", ".bash"},
		FunctionNodeTypes: []string{"function_definition"},
		ClassNodeTypes:    []string{},
		ModuleNodeTypes:   []string{"program"},
		CallNodeTypes:     []string{"command"},
		ImportNodeTypes:   []string{"command"},

		BranchingNodeTypes:  []string{"if_statement", "while_statement", "for_statement", "case_statement", "elif_clause"},
		VariableNodeTypes:   []string{"variable_assignment"},
		AssignmentNodeTypes: []string{"variable_assignment"},
	})
}
