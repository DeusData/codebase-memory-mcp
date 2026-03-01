package lang

func init() {
	Register(&LanguageSpec{
		Language:       Haskell,
		FileExtensions: []string{".hs"},
		FunctionNodeTypes: []string{
			"function",  // function binding with name, patterns, match
			"signature", // type signature (name :: Type -> Type)
		},
		ClassNodeTypes: []string{
			"class",     // type class declaration
			"data_type", // algebraic data type
			"newtype",   // newtype declaration
		},
		ModuleNodeTypes: []string{"haskell"},
		CallNodeTypes:   []string{"infix", "apply"},
		ImportNodeTypes: []string{"import"},

		BranchingNodeTypes: []string{
			"match",   // pattern match clause
			"guards",  // guard expressions
			"if",      // if expression
			"case",    // case expression
			"do",      // do notation
			"boolean", // guard condition
		},
		VariableNodeTypes:   []string{"function"},
		AssignmentNodeTypes: []string{"function"},
		EnvAccessFunctions:  []string{"lookupEnv", "getEnv"},
	})
}
