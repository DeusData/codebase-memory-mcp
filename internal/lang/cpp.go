package lang

func init() {
	Register(&LanguageSpec{
		Language:       CPP,
		FileExtensions: []string{".cpp", ".h", ".hpp", ".cc", ".cxx", ".hxx", ".hh", ".ixx", ".cppm", ".ccm"},
		FunctionNodeTypes: []string{
			"function_definition",
			"declaration",
			"field_declaration",
			"template_declaration",
			"lambda_expression",
		},
		ClassNodeTypes: []string{
			"class_specifier",
			"struct_specifier",
			"union_specifier",
			"enum_specifier",
		},
		FieldNodeTypes: []string{"field_declaration"},
		ModuleNodeTypes: []string{
			"translation_unit",
			"namespace_definition",
			"linkage_specification",
			"declaration",
		},
		CallNodeTypes: []string{
			"call_expression",
			"field_expression",
			"subscript_expression",
			"new_expression",
			"delete_expression",
			"binary_expression",
			"unary_expression",
			"update_expression",
		},
		ImportNodeTypes:   []string{"preproc_include", "template_function", "declaration"},
		ImportFromTypes:   []string{"preproc_include", "template_function", "declaration"},
		PackageIndicators: []string{"CMakeLists.txt", "Makefile", "*.vcxproj", "conanfile.txt"},

		BranchingNodeTypes: []string{"if_statement", "for_statement", "for_range_loop", "while_statement", "switch_statement", "case_statement", "try_statement", "catch_clause"},
	})
}
