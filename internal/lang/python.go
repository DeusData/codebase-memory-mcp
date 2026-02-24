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
	})
}
