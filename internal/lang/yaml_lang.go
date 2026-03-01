package lang

func init() {
	Register(&LanguageSpec{
		Language:          YAML,
		FileExtensions:    []string{".yml", ".yaml"},
		FunctionNodeTypes: []string{},
		ClassNodeTypes:    []string{},
		ModuleNodeTypes:   []string{"stream"},
		CallNodeTypes:     []string{},
		ImportNodeTypes:   []string{},

		VariableNodeTypes: []string{"block_mapping_pair"},
	})
}
