package lang

func init() {
	Register(&LanguageSpec{
		Language:          CSS,
		FileExtensions:    []string{".css"},
		FunctionNodeTypes: []string{},
		ClassNodeTypes:    []string{},
		ModuleNodeTypes:   []string{"stylesheet"},
		CallNodeTypes:     []string{},
		ImportNodeTypes:   []string{"import_statement"},

		VariableNodeTypes: []string{},
	})
}
