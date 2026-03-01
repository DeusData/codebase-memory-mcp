package lang

func init() {
	Register(&LanguageSpec{
		Language:       R,
		FileExtensions: []string{".r", ".R"},
		FunctionNodeTypes: []string{
			"function_definition",
		},
		ClassNodeTypes:  []string{},
		FieldNodeTypes:  []string{},
		ModuleNodeTypes: []string{"program"},
		CallNodeTypes:   []string{"call"},
		ImportNodeTypes: []string{"call"},
		BranchingNodeTypes: []string{
			"if_statement", "for_statement", "while_statement",
		},
		VariableNodeTypes:   []string{"binary_operator"},
		AssignmentNodeTypes: []string{"binary_operator"},
		EnvAccessFunctions:  []string{"Sys.getenv"},
	})
}
