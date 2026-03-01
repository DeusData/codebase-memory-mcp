package lang

func init() {
	Register(&LanguageSpec{
		Language:          TOML,
		FileExtensions:    []string{".toml"},
		FunctionNodeTypes: []string{},
		ClassNodeTypes:    []string{},
		ModuleNodeTypes:   []string{"document"},
		CallNodeTypes:     []string{},
		ImportNodeTypes:   []string{},

		VariableNodeTypes: []string{},
	})
}
