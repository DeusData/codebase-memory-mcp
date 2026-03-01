package lang

func init() {
	Register(&LanguageSpec{
		Language:          HTML,
		FileExtensions:    []string{".html", ".htm"},
		FunctionNodeTypes: []string{},
		ClassNodeTypes:    []string{},
		ModuleNodeTypes:   []string{"document"},
		CallNodeTypes:     []string{},
		ImportNodeTypes:   []string{},

		VariableNodeTypes: []string{},
	})
}
