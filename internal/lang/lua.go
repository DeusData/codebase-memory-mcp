package lang

func init() {
	Register(&LanguageSpec{
		Language:          Lua,
		FileExtensions:    []string{".lua"},
		FunctionNodeTypes: []string{"function_declaration", "function_definition"},
		ClassNodeTypes:    []string{},
		ModuleNodeTypes:   []string{"chunk"},
		CallNodeTypes:     []string{"function_call"},
		ImportNodeTypes:   []string{"function_call"},
	})
}
