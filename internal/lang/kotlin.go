package lang

func init() {
	Register(&LanguageSpec{
		Language:       Kotlin,
		FileExtensions: []string{".kt", ".kts"},
		FunctionNodeTypes: []string{
			"function_declaration",
			"secondary_constructor",
			"anonymous_function",
		},
		ClassNodeTypes: []string{
			"class_declaration",
			"object_declaration",
			"companion_object",
		},
		ModuleNodeTypes: []string{"source_file"},
		CallNodeTypes: []string{
			"call_expression",
			"navigation_expression",
		},
		ImportNodeTypes: []string{"import"},
		ImportFromTypes: []string{"import"},
	})
}
