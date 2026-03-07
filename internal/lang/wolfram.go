package lang

func init() {
	Register(&LanguageSpec{
		Language:          Wolfram,
		FileExtensions:    []string{".wl", ".wls"},
		FunctionNodeTypes: []string{"set_delayed_top", "set_top", "set_delayed", "set"},
		ModuleNodeTypes:   []string{"source_file"},
		CallNodeTypes:     []string{"apply"},
		ImportNodeTypes:   []string{"get_top"},
	})
}
