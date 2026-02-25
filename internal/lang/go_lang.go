package lang

func init() {
	Register(&LanguageSpec{
		Language:          Go,
		FileExtensions:    []string{".go"},
		FunctionNodeTypes: []string{"function_declaration", "method_declaration"},
		ClassNodeTypes:    []string{"type_spec", "type_alias"},
		ModuleNodeTypes:   []string{"source_file"},
		CallNodeTypes:     []string{"call_expression"},
		ImportNodeTypes:   []string{"import_declaration"},
		ImportFromTypes:   []string{"import_declaration"},
	})
}
