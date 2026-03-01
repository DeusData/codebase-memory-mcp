package lang

func init() {
	Register(&LanguageSpec{
		Language:       Rust,
		FileExtensions: []string{".rs"},
		FunctionNodeTypes: []string{
			"function_item",
			"function_signature_item",
			"closure_expression",
		},
		ClassNodeTypes: []string{
			"struct_item",
			"enum_item",
			"union_item",
			"trait_item",
			"type_item",
		},
		FieldNodeTypes:    []string{"field_declaration"},
		ModuleNodeTypes:   []string{"source_file", "mod_item"},
		CallNodeTypes:     []string{"call_expression", "macro_invocation"},
		ImportNodeTypes:   []string{"use_declaration", "extern_crate_declaration"},
		ImportFromTypes:   []string{"use_declaration"},
		PackageIndicators: []string{"Cargo.toml"},

		BranchingNodeTypes:  []string{"if_expression", "for_expression", "while_expression", "loop_expression", "match_expression", "match_arm"},
		VariableNodeTypes:   []string{"static_item", "const_item"},
		AssignmentNodeTypes: []string{"assignment_expression", "compound_assignment_expr"},
		EnvAccessFunctions:  []string{"env::var", "std::env::var"},
	})
}
