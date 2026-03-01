package lang

func init() {
	Register(&LanguageSpec{
		Language:       Zig,
		FileExtensions: []string{".zig"},
		FunctionNodeTypes: []string{
			"function_declaration",
			"test_declaration", // test "description" { ... }
		},
		ClassNodeTypes:  []string{"struct_declaration", "enum_declaration", "union_declaration"},
		FieldNodeTypes:  []string{"container_field"},
		ModuleNodeTypes: []string{"source_file"},
		CallNodeTypes:   []string{"call_expression", "builtin_function"},
		ImportNodeTypes: []string{"builtin_function"},

		BranchingNodeTypes:  []string{"if_statement", "for_statement", "while_statement", "switch_expression"},
		VariableNodeTypes:   []string{"variable_declaration"},
		AssignmentNodeTypes: []string{"assignment_expression"},
		EnvAccessFunctions:  []string{"std.os.getenv"},
	})
}
