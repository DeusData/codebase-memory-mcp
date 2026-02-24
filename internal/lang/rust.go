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
			"impl_item",
			"type_item",
		},
		ModuleNodeTypes:   []string{"source_file", "mod_item"},
		CallNodeTypes:     []string{"call_expression", "macro_invocation"},
		ImportNodeTypes:   []string{"use_declaration", "extern_crate_declaration"},
		ImportFromTypes:   []string{"use_declaration"},
		PackageIndicators: []string{"Cargo.toml"},
	})
}
