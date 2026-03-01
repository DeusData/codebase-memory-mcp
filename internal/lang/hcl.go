package lang

func init() {
	Register(&LanguageSpec{
		Language:          HCL,
		FileExtensions:    []string{".tf", ".hcl"},
		FunctionNodeTypes: []string{},
		ClassNodeTypes: []string{
			"block", // resource, variable, output, data, module blocks
		},
		ModuleNodeTypes: []string{"config_file"},
		CallNodeTypes:   []string{"function_call"},
		ImportNodeTypes: []string{},

		VariableNodeTypes: []string{"attribute"},
	})
}
