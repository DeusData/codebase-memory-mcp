package lang

func init() {
	Register(&LanguageSpec{
		Language:       OCaml,
		FileExtensions: []string{".ml", ".mli"},
		FunctionNodeTypes: []string{
			"value_definition", // let name args = body (contains let_binding)
		},
		ClassNodeTypes: []string{
			"type_definition",   // type color = Red | Green
			"class_definition",  // class foo = object ... end
			"module_definition", // module M = struct ... end
		},
		ModuleNodeTypes: []string{"compilation_unit"},
		CallNodeTypes:   []string{"application", "infix_expression"},
		ImportNodeTypes: []string{"open_module"},

		BranchingNodeTypes: []string{
			"match_expression", // match ... with
			"if_expression",    // if ... then ... else
			"match_case",       // individual pattern match case
		},
		VariableNodeTypes:   []string{"value_definition"},
		AssignmentNodeTypes: []string{"value_definition"},
		EnvAccessFunctions:  []string{"Sys.getenv"},
	})
}
