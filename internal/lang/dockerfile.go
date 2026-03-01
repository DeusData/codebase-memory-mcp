package lang

func init() {
	Register(&LanguageSpec{
		Language:          Dockerfile,
		FileExtensions:    []string{".dockerfile", "Dockerfile"},
		ModuleNodeTypes:   []string{"source_file"},
		FunctionNodeTypes: []string{},
		ClassNodeTypes:    []string{},
		FieldNodeTypes:    []string{},
		CallNodeTypes:     []string{},
		ImportNodeTypes:   []string{},
		VariableNodeTypes: []string{
			"env_instruction",
			"arg_instruction",
		},
	})
}
