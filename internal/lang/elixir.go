package lang

func init() {
	Register(&LanguageSpec{
		Language:       Elixir,
		FileExtensions: []string{".ex", ".exs"},
		// Elixir uses "call" for everything (homoiconic), but we only want
		// definition forms (def, defp, defmacro) in FunctionNodeTypes.
		// The pipeline has custom Elixir handling to classify call nodes.
		FunctionNodeTypes: []string{
			"call", // classified by pipeline: def/defp → Function, defmodule → Class
		},
		ClassNodeTypes:  []string{}, // handled by custom extraction (defmodule)
		ModuleNodeTypes: []string{"source"},
		CallNodeTypes:   []string{"call", "dot"},
		ImportNodeTypes: []string{"call"},

		BranchingNodeTypes: []string{
			"call", // if/cond/case are macro calls in Elixir
		},
		VariableNodeTypes:   []string{"binary_operator"},
		AssignmentNodeTypes: []string{"binary_operator"},
		EnvAccessFunctions:  []string{"System.get_env"},
	})
}
