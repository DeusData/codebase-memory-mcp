package lang

func init() {
	Register(&LanguageSpec{
		Language:          Python,
		FileExtensions:    []string{".py"},
		FunctionNodeTypes: []string{"function_definition"},
		ClassNodeTypes:    []string{"class_definition"},
		ModuleNodeTypes:   []string{"module"},
		CallNodeTypes:     []string{"call", "with_statement"},
		ImportNodeTypes:   []string{"import_statement"},
		ImportFromTypes:   []string{"import_from_statement"},
		PackageIndicators: []string{"__init__.py"},

		BranchingNodeTypes:      []string{"if_statement", "for_statement", "while_statement", "try_statement", "except_clause", "with_statement", "elif_clause"},
		VariableNodeTypes:       []string{"assignment", "augmented_assignment"},
		AssignmentNodeTypes:     []string{"assignment", "augmented_assignment"},
		ThrowNodeTypes:          []string{"raise_statement"},
		DecoratorNodeTypes:      []string{"decorator"},
		EnvAccessFunctions:      []string{"os.getenv", "os.environ.get"},
		EnvAccessMemberPatterns: []string{"os.environ"},
	})
}
