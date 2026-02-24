package lang

func init() {
	Register(&LanguageSpec{
		Language:       CSharp,
		FileExtensions: []string{".cs"},
		FunctionNodeTypes: []string{
			"destructor_declaration",
			"local_function_statement",
			"function_pointer_type",
			"constructor_declaration",
			"anonymous_method_expression",
			"lambda_expression",
			"method_declaration",
		},
		ClassNodeTypes: []string{
			"class_declaration",
			"struct_declaration",
			"enum_declaration",
			"interface_declaration",
		},
		ModuleNodeTypes: []string{"compilation_unit"},
		CallNodeTypes:   []string{"invocation_expression"},
		ImportNodeTypes: []string{"using_directive"},
		ImportFromTypes: []string{"using_directive"},
	})
}
