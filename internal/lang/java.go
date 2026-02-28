package lang

func init() {
	Register(&LanguageSpec{
		Language:          Java,
		FileExtensions:    []string{".java"},
		FunctionNodeTypes: []string{"method_declaration", "constructor_declaration"},
		ClassNodeTypes: []string{
			"class_declaration",
			"interface_declaration",
			"enum_declaration",
			"annotation_type_declaration",
			"record_declaration",
		},
		FieldNodeTypes:  []string{"field_declaration"},
		ModuleNodeTypes: []string{"program"},
		CallNodeTypes:   []string{"method_invocation"},
		ImportNodeTypes: []string{"import_declaration"},
		ImportFromTypes: []string{"import_declaration"},

		BranchingNodeTypes:  []string{"if_statement", "for_statement", "enhanced_for_statement", "while_statement", "switch_expression", "switch_block_statement_group", "try_statement", "catch_clause"},
		AssignmentNodeTypes: []string{"assignment_expression"},
		ThrowNodeTypes:      []string{"throw_statement"},
		ThrowsClauseField:   "throws",
		DecoratorNodeTypes:  []string{"marker_annotation", "annotation"},
		EnvAccessFunctions:  []string{"System.getenv", "System.getProperty"},
	})
}
