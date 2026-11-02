const PRECEDENCE = {
  MEMBER_ACCESS: 20,
  INVOCATION: 19,
  ARRAY_ACCESS: 18,
  UNARY: 15,
  EXPONENTIATION: 14,
  MULTIPLICATIVE: 13,
  INTEGER_DIVISION: 12,
  MODULO: 11,
  ADDITIVE: 10,
  CONCATENATION: 9,
  SHIFT: 8,
  RELATIONAL: 7,
  EQUALITY: 6,
  LOGICAL_AND: 5,
  LOGICAL_OR: 4,
  LOGICAL_XOR: 3,
  CONDITIONAL: 2,
  ASSIGNMENT: 1,
};

/* Based on govindbanura/tree-sitter-vbnet (MIT). The real-world VB.NET
 * coverage patch is derived from CodeGraph MCP (MIT) and retained in patches/. */
module.exports = grammar({
  name: "vbnet",
  word: ($) => $._identifier_token,

  externals: ($) => [$._query_clause_continuation, $.xml_literal],

  extras: $ => [
    $.comment,
    $.preprocessor_directive,
    /[ \t\v\f\uFEFF]+/,
    /\r?\n/,
    /_[ \t]*\r?\n/,
  ],

  supertypes: ($) => [
    $._declaration,
    $._type_member_declaration,
    $._interface_member_declaration,
    $._statement,
    $._expression,
    $._type,
  ],
  conflicts: ($) => [
    [$.binary_expression, $.assignment_expression, $.inline_assignment_statement],
    [$.simple_name, $.generic_name],
    [$.await_expression, $.with_expression],
    [$._name_reference, $._type],
    [$.object_creation_expression, $.array_creation_expression, $.array_type],
    [$._primary_expression, $.variable_declarator],
    [$.binary_expression, $.assignment_expression],
    [$._primary_expression, $.select_element],
    [$.select_clause],
    [$._primary_expression, $.group_element],
    [$._terminator, $._block_terminator],
    [$.labeled_statement],
    [$.argument_list, $.array_rank_specifier],
    [$._argument, $.array_creation_expression],
    [$.object_creation_expression],
    [$.mybase_expression, $.constructor_initializer],
    [$.myclass_expression, $.constructor_initializer],
    [$.lambda_expression, $.expression_statement],
    [$._primary_expression, $.by_element],
    [$.catch_block],
    [$.finally_block],
    [$.aggregate_function],
    [$.aggregate_element, $.into_element],
    [$.group_by_clause],
    [$.else_clause],
    [$.case_else_clause],
    [$.case_clause],
    [$.elseif_clause],
    [$._primary_expression, $.labeled_statement],
    [$._primary_expression, $.simple_name],
    [$._primary_expression, $.simple_name, $.generic_name],
    [$._primary_expression, $.variable_declarator, $.labeled_statement],
    [$._statement, $.with_statement],
    [
      $.class_declaration,
      $.structure_declaration,
      $.interface_declaration,
      $.enum_declaration,
      $.delegate_declaration,
      $.interface_method_declaration,
      $.interface_property_declaration,
      $.interface_event_declaration,

    ],
  ],


  rules: {
    source_file: ($) =>
      seq(
        optional($.option_statements),
        repeat(choice(
          prec(10, $.imports_statement),
          prec(10, $.global_attribute_section),
          prec(10, $.file_attribute_section),
          $._declaration,
          $._statement,
          $._block_terminator
        ))
      ),

    file_attribute_section: ($) =>
      seq(
        $.attribute_list,
        $._terminator
      ),


    option_statements: ($) => repeat1($.option_statement),

    option_statement: ($) =>
      seq(
        ci("Option"),
        choice(
          seq(ci("Explicit"), optional(choice(ci("On"), ci("Off")))),
          seq(ci("Strict"), optional(choice(ci("On"), ci("Off")))),
          seq(ci("Compare"), choice(ci("Binary"), ci("Text"))),
          seq(ci("Infer"), optional(choice(ci("On"), ci("Off"))))
        ),
        $._terminator
      ),

    imports_statement: ($) =>
      seq(
        ci("Imports"),
        choice(
          field("namespace", $._name_reference),
          seq(
            field("alias", $.identifier),
            "=",
            field("namespace", $._name_reference)
          )
        ),
        $._terminator
      ),

    global_attribute_section: ($) =>
      seq(
        "<",
        field("target", $.attribute_target),
        ":",
        commaSep1($.attribute),
        ">",
        $._terminator
      ),

    attribute_target: ($) =>
      choice(
        ci("Assembly"),
        ci("Module")
      ),

    _terminator: ($) =>
      prec.right(choice(seq(optional(":"), choice("\n", "\r\n")), prec(-1, ":"), $._eof)),

    _block_terminator: ($) => prec.right(repeat1(choice("\n", "\r\n", ":", $._eof))),

    _eof: ($) => token(prec(-10, "$")),

    _identifier_token: ($) =>
      token(prec(-1, /[\p{L}\p{Nl}_][\p{L}\p{Nl}\p{Nd}\p{Mn}\p{Pc}]*[%$&!#@]?/u)),

    identifier: ($) =>
      choice(
        $._identifier_token,
        seq("[", /[^\]]+/, "]") // Bracketed identifier
      ),

    _implements_keyword: ($) => token(prec(10, ci("Implements"))),
    _inherits_keyword: ($) => token(prec(10, ci("Inherits"))),

    _sub_new: ($) => token(prec(100, seq(ci("Sub"), /\s+/, ci("New")))),

    member_modifier: ($) =>
      token(
        prec(
          10,
          choice(
            ci("Public"),
            ci("Private"),
            ci("Protected"),
            ci("Friend"),
            ci("Protected Friend"),
            ci("Private Protected"),
            ci("ReadOnly"),
            ci("WriteOnly"),
            ci("Shared"),
            ci("Shadows"),
            ci("MustInherit"),
            ci("NotInheritable"),
            ci("Overrides"),
            ci("NotOverridable"),
            ci("Overridable"),
            ci("Overloads"),
            ci("WithEvents"),
            ci("Widening"),
            ci("Narrowing"),
            ci("Partial"),
            ci("Async"),
            ci("Iterator"),
            ci("Dim"),
            ci("Const")
          )
        )
      ),

    _method_body_statement: ($) =>
      choice(
        prec(10, $.try_statement),  // High precedence for try
        prec(10, $.select_statement),  // High precedence for select
        prec(10, $.if_statement),
        prec(10, $.while_statement),
        prec(10, $.do_statement),
        prec(10, $.for_statement),
        prec(10, $.for_each_statement),
        prec(10, $.using_statement),
        prec(10, $.with_statement),
        prec(1, $.declaration_statement),  // Lower precedence for declarations
        $._statement  // Fallback to regular statements
      ),

    local_declaration_modifier: ($) =>
      choice(ci("Dim"), ci("Const"), ci("Static")),

    attribute_list: ($) => seq("<", commaSep1($.attribute), ">"),

    attribute: ($) =>
      seq(
        field("name", $._name_reference),
        optional(field("arguments", $.attribute_argument_list))
      ),

    attribute_argument_list: ($) =>
      seq("(", commaSep($._attribute_argument), ")"),

    _attribute_argument: ($) => choice($._expression, $.named_argument),

    named_argument: ($) =>
      seq(field("name", $.identifier), ":=", field("value", $._expression)),

    // Expressions
    _expression: ($) =>
      choice(
        // $.aggregate_function,
        $._primary_expression,
        $.binary_expression,
        $.unary_expression,
        $.assignment_expression,
        $.conditional_expression,
        $.lambda_expression,
        $.await_expression,
        $.query_expression,
        $.typeof_is_expression
      ),


    _primary_expression: ($) =>
      choice(
        $._literal,
        $.array_literal,
        $.tuple_expression,
        $.identifier,
        $.parenthesized_expression,
        $.generic_invocation_expression,  // Add this before regular invocation
        $.invocation_expression,
        $.member_access_expression,
        $.array_access_expression,
        $.object_creation_expression,
        $.array_creation_expression,
        $.typeof_expression,
        $.cast_expression,
        $.me_expression,
        $.mybase_expression,
        $.myclass_expression,
        $.if_expression,
        $.with_expression,
        $.anonymous_object_creation_expression,
        $.with_member_access_expression
      ),

    generic_invocation_expression: ($) =>
      prec.right(
        PRECEDENCE.INVOCATION + 1,
        seq(
          field("function", $._expression),
          field("type_arguments", $.type_argument_list),
          optional(field("arguments", $.argument_list))
        )
      ),

    parenthesized_expression: ($) => seq("(", $._expression, ")"),

    tuple_expression: ($) =>
      seq(
        "(",
        choice($._expression, seq(field("name", $.identifier), ":=", $._expression)),
        ",",
        commaSep1(choice($._expression, seq(field("name", $.identifier), ":=", $._expression))),
        ")"
      ),

    member_access_expression: ($) =>
      prec.left(
        PRECEDENCE.MEMBER_ACCESS,
        seq(
          field("object", $._expression),
          choice(".", "?."),
          field("member", $.identifier)
        )
      ),

    invocation_expression: ($) =>
      prec(
        PRECEDENCE.INVOCATION,
        seq(
          field("function", $._expression),
          field("arguments", $.argument_list)
        )
      ),

    argument_list: ($) => seq("(", optional($._arguments), ")"),

    // Argument slots may be omitted between commas (`f(a,, b)`, `f(, x)`) —
    // VB passes Optional parameters positionally by leaving the slot empty.
    _arguments: ($) =>
      seq($._argument, repeat(seq(",", optional($._argument)))),

    _argument: ($) => choice($._expression, $.named_argument),

    array_access_expression: ($) =>
      prec(
        PRECEDENCE.ARRAY_ACCESS,
        seq(
          field("array", $._expression),
          optional("?"),
          "(",
          field("indices", $._arguments),
          ")"
        )
      ),

    binary_expression: ($) =>
      choice(
        // Special handling for "Is Not" as two tokens
        prec.left(
          PRECEDENCE.EQUALITY,
          seq(
            field("left", $._expression),
            field("operator", seq(ci("Is"), ci("Not"))),
            field("right", $._expression)
          )
        ),
        ...[
          ["^", PRECEDENCE.EXPONENTIATION],
          ["*", PRECEDENCE.MULTIPLICATIVE],
          ["/", PRECEDENCE.MULTIPLICATIVE],
          ["\\", PRECEDENCE.INTEGER_DIVISION],
          [ci("Mod"), PRECEDENCE.MODULO],
          ["+", PRECEDENCE.ADDITIVE],
          ["-", PRECEDENCE.ADDITIVE],
          ["&", PRECEDENCE.CONCATENATION],
          ["<<", PRECEDENCE.SHIFT],
          [">>", PRECEDENCE.SHIFT],
          ["<", PRECEDENCE.RELATIONAL],
          [">", PRECEDENCE.RELATIONAL],
          ["<=", PRECEDENCE.RELATIONAL],
          [">=", PRECEDENCE.RELATIONAL],
          ["=", PRECEDENCE.EQUALITY],
          ["<>", PRECEDENCE.EQUALITY],
          [ci("Is"), PRECEDENCE.EQUALITY],
          [ci("IsNot"), PRECEDENCE.EQUALITY],
          [ci("Like"), PRECEDENCE.RELATIONAL],
          [ci("And"), PRECEDENCE.LOGICAL_AND],
          [ci("AndAlso"), PRECEDENCE.LOGICAL_AND],
          [ci("Or"), PRECEDENCE.LOGICAL_OR],
          [ci("OrElse"), PRECEDENCE.LOGICAL_OR],
          [ci("Xor"), PRECEDENCE.LOGICAL_XOR],
        ].map(([op, precedence]) =>
          prec.left(
            precedence,
            seq(
              field("left", $._expression),
              field("operator", seq(op, optional(/_[ \t]*\r?\n/))),
              // Accept optional extras (comments, line continuation) here
              field("right", $._expression)
            )
          )
        )
      ),


    unary_expression: ($) =>
      prec.left(
        PRECEDENCE.UNARY,
        choice(
          seq("+", $._expression),
          seq("-", $._expression),
          seq(ci("Not"), $._expression),
          seq(ci("AddressOf"), $._expression)
        )
      ),

    assignment_expression: ($) =>
      prec.right(
        PRECEDENCE.ASSIGNMENT,
        seq(
          field("left", $._expression),
          field(
            "operator",
            choice("=", "+=", "-=", "*=", "/=", "\\=", "^=", "&=", "<<=", ">>=")
          ),
          field("right", $._expression)
        )
      ),

    conditional_expression: ($) =>
      prec.right(
        PRECEDENCE.CONDITIONAL,
        seq(
          ci("If"),
          "(",
          field("condition", $._expression),
          ",",
          field("true_expression", $._expression),
          ",",
          field("false_expression", $._expression),
          ")"
        )
      ),

    if_expression: ($) =>
      prec.right(
        PRECEDENCE.CONDITIONAL,
        seq(
          ci("If"),
          "(",
          field("condition", $._expression),
          ",",
          field("true_expression", $._expression),
          ")"
        )
      ),

    lambda_expression: ($) =>
      choice(
        prec.right(
          seq(
            optional(choice(ci("Async"), ci("Iterator"))),
            choice(ci("Function"), ci("Sub")),
            field("parameters", $.lambda_parameter_list),
            field(
              "body",
              choice(
                $._expression,
                alias($.inline_if_statement, $.if_statement),
                alias($.inline_throw_statement, $.throw_statement)
              )
            )
          )
        ),
        seq(
          optional(choice(ci("Async"), ci("Iterator"))),
          ci("Function"),
          field("parameters", $.lambda_parameter_list),
          optional($.as_clause),
          $._block_terminator,
          repeat($._statement),
          ci("End"), ci("Function")
        ),
        seq(
          optional(choice(ci("Async"), ci("Iterator"))),
          ci("Sub"),
          field("parameters", $.lambda_parameter_list),
          $._block_terminator,
          repeat($._statement),
          ci("End"), ci("Sub")
        )
      ),

    lambda_parameter_list: ($) => seq("(", commaSep($.lambda_parameter), ")"),

    lambda_parameter: ($) =>
      seq(
        optional(field("modifiers", repeat1(choice(ci("ByVal"), ci("ByRef"))))),
        field("name", $.identifier),
        optional(choice(
          $.array_rank_specifier,
          seq("(", field("bounds", commaSep1($._expression)), ")")
        )),
        optional($.as_clause)
      ),

    typeof_is_expression: ($) =>
      prec.left(
        PRECEDENCE.RELATIONAL,
        seq(
          ci("TypeOf"),
          field("expression", $._expression),
          choice(seq(ci("Is"), ci("Not")), token(prec(12, ci("IsNot"))), ci("Is")),
          field("type", $._type)
        )
      ),
    await_expression: ($) => seq(ci("Await"), $._expression),

    object_creation_expression: ($) =>
      seq(
        ci("New"),
        field("type", $._type),
        optional(field("arguments", $.argument_list)),
        optional(
          field(
            "initializer",
            choice(
              $.object_initializer,
              $.collection_initializer
            )
          )
        )
      ),



    object_initializer: ($) =>
      seq(ci("With"), "{", commaSep($.member_initializer), "}"),
    member_initializer: ($) =>
      choice(
        seq(
          ".",
          field("name", $.identifier),
          "=",
          field("value", $._expression)
        ),
        seq(
          ci("Key"),
          ".",
          field("name", $.identifier),
          "=",
          field("value", $._expression)
        )
      ),

    collection_initializer: ($) =>
      seq(ci("From"), "{", commaSep($._expression), "}"),

    array_literal: ($) => seq("{", commaSep($._expression), "}"),


    array_creation_expression: ($) =>
      seq(
        ci("New"),
        field("type", $._type),
        choice(
          seq(
            "(",
            field("bounds", commaSep1($._expression)),
            ")",
            optional(field("initializer", $.array_initializer))
          ),
          field("initializer", $.array_initializer)
        )
      ),
    array_initializer: ($) => seq("{", commaSep($._expression), "}"),
    anonymous_object_creation_expression: ($) =>
      seq(
        ci("New"),
        ci("With"),
        "{",
        commaSep($.anonymous_object_member_declarator),
        "}"
      ),
    anonymous_object_member_declarator: ($) =>
      choice(
        seq(
          ".",
          field("name", $.identifier),
          "=",
          field("value", $._expression)
        ),
        seq(
          ci("Key"),
          ".",
          field("name", $.identifier),
          "=",
          field("value", $._expression)
        ),
        $._expression
      ),
    typeof_expression: ($) =>
      seq(ci("GetType"), "(", field("type", $._type), ")"),
    cast_expression: ($) =>
      choice(
        seq(
          ci("CType"),
          "(",
          field("expression", $._expression),
          ",",
          field("type", $._type),
          ")"
        ),
        seq(
          ci("DirectCast"),
          "(",
          field("expression", $._expression),
          ",",
          field("type", $._type),
          ")"
        ),
        seq(
          ci("TryCast"),
          "(",
          field("expression", $._expression),
          ",",
          field("type", $._type),
          ")"
        ),
        seq(ci("CInt"), "(", field("expression", $._expression), ")"),
        seq(ci("CLng"), "(", field("expression", $._expression), ")"),
        seq(ci("CDbl"), "(", field("expression", $._expression), ")"),
        seq(ci("CDec"), "(", field("expression", $._expression), ")"),
        seq(ci("CStr"), "(", field("expression", $._expression), ")"),
        seq(ci("CBool"), "(", field("expression", $._expression), ")"),
        seq(ci("CDate"), "(", field("expression", $._expression), ")"),
        seq(ci("CObj"), "(", field("expression", $._expression), ")"),
        seq(ci("CByte"), "(", field("expression", $._expression), ")"),
        seq(ci("CSByte"), "(", field("expression", $._expression), ")"),
        seq(ci("CShort"), "(", field("expression", $._expression), ")"),
        seq(ci("CUShort"), "(", field("expression", $._expression), ")"),
        seq(ci("CUInt"), "(", field("expression", $._expression), ")"),
        seq(ci("CULng"), "(", field("expression", $._expression), ")"),
        seq(ci("CSng"), "(", field("expression", $._expression), ")"),
        seq(ci("CChar"), "(", field("expression", $._expression), ")")
      ),
    me_expression: ($) => ci("Me"),
    mybase_expression: ($) => ci("MyBase"),
    myclass_expression: ($) => ci("MyClass"),
    with_member_access_expression: ($) =>
      prec(PRECEDENCE.MEMBER_ACCESS, seq(".", $.identifier)),
    with_expression: ($) =>
      seq(field("object", $._expression), "!", field("member", $.identifier)),
    _literal: ($) =>
      choice(
        $.xml_literal,
        $.interpolated_string_literal,
        $.date_literal,
        $.string_literal,
        $.character_literal,
        $.integer_literal,
        $.floating_point_literal,
        $.boolean_literal,
        $.nothing_literal,
        $.date_literal
      ),
    string_literal: ($) => token(seq('"', repeat(choice(/[^"]/, '""')), '"')),
    interpolated_string_literal: ($) =>
      seq(
        '$"',
        repeat(
          choice(
            token.immediate(prec(101, /[^"{}]+/)),
            token.immediate(prec(101, '""')),
            token.immediate(prec(101, "{{")),
            token.immediate(prec(101, "}}")),
            $.interpolation
          )
        ),
        token.immediate('"')
      ),
    interpolation: ($) =>
      seq(
        token.immediate(prec(101, "{")),
        $._expression,
        optional(seq(",", /-?\d+/)),
        optional(seq(":", /[^}"\r\n]+/)),
        "}"
      ),
    date_literal: ($) => token(prec(2, /#[ \t]*\d[^#\r\n]*#/)),
    character_literal: ($) => token(seq('"', choice(/[^"\r\n]/, '""'), '"', /[cC]/)),
    integer_literal: ($) =>
      token(
        seq(
          choice(
            /\d+(_+\d+)*/,
            /&[Hh][0-9A-Fa-f]+(_+[0-9A-Fa-f]+)*/,
            /&[Oo][0-7]+(_+[0-7]+)*/,
            /&[Bb][01]+(_+[01]+)*/
          ),
          optional(choice(/[Uu][SsIiLl]/, /[SsIiLl]/, "%", "&"))
        )
      ),
    floating_point_literal: ($) =>
      token(
        seq(
          choice(seq(/\d+/, ".", /\d+/), seq(".", /\d+/), /\d+/),
          optional(/[eE][+-]?\d+/),
          optional(/[FfRrDd!#@]/)
        )
      ),
    boolean_literal: ($) => choice(ci("True"), ci("False")),
    nothing_literal: ($) => ci("Nothing"),
    date_literal: ($) => seq("#", /[^#]+/, "#"),

    // Names and types
    _name_reference: ($) =>
      choice($.simple_name, $.generic_name, $.qualified_name, $.global_qualified_name),

    simple_name: ($) => $.identifier,

    qualified_name: ($) =>
      prec.left(
        PRECEDENCE.MEMBER_ACCESS + 1,
        seq(
          field("qualifier", $._name_reference),
          ".",
          field("name", choice($.simple_name, $.generic_name))
        )
      ),
    global_qualified_name: ($) =>
      seq(ci("Global"), ".", field("name", $._name_reference)),

    _type: ($) =>
      choice(
        prec(2, $.nullable_type),  // Give nullable_type higher precedence
        $.simple_name,
        $.qualified_name,
        $.generic_name,
        $.global_qualified_name,
        $.predefined_type,
        $.array_type,
        $.tuple_type
      ),

    predefined_type: ($) =>
      choice(
        ci("Boolean"),
        ci("Byte"),
        ci("SByte"),
        ci("Char"),
        ci("Date"),
        ci("Decimal"),
        ci("Double"),
        ci("Integer"),
        ci("Long"),
        ci("Object"),
        ci("Short"),
        ci("Single"),
        ci("String"),
        ci("UInteger"),
        ci("ULong"),
        ci("UShort")
      ),

    generic_name: ($) => seq(field("name", $.identifier), $.type_argument_list),
    type_argument_list: ($) =>
      seq("(", ci("Of"), optional($._type), repeat(seq(",", optional($._type))), ")"),
    array_type: ($) =>
      seq(field("element_type", $._type), $.array_rank_specifier),
    array_rank_specifier: ($) => seq("(", repeat(","), ")"),

    nullable_type: ($) => prec.left(20, seq(
      field("type", choice(
        $.predefined_type,
        $.simple_name,
        $.qualified_name,
        $.generic_name  // Also allow generic types to be nullable
      )),
      token.immediate("?")
    )),

    tuple_type: ($) => seq("(", commaSep2($.tuple_element), ")"),
    tuple_element: ($) =>
      seq(
        optional(seq(field("name", $.identifier), ci("As"))),
        field("type", $._type)
      ),

    as_clause: ($) =>
      seq(
        ci("As"),
        choice(
          $.object_creation_expression,
          field("declared_type", $._type)
        )
      ),

    implements_member_clause: ($) =>
      seq(ci("Implements"), commaSep1($._name_reference)),

    implements_statement: ($) =>
      prec.right(
        10,
        seq($._implements_keyword, commaSep1($._name_reference), $._terminator)
      ),

    inherits_statement: ($) =>
      prec.right(
        10,
        seq($._inherits_keyword, commaSep1($._name_reference), $._terminator)
      ),

    // Declarations
    _declaration: ($) =>
      choice(
        $.namespace_declaration,
        $.class_declaration,
        $.structure_declaration,
        $.interface_declaration,
        $.module_declaration,
        $.enum_declaration,
        $.delegate_declaration
      ),

    namespace_declaration: ($) =>
      seq(
        ci("Namespace"),
        field("name", $._name_reference),
        $._block_terminator,
        repeat(choice($.imports_statement, $._declaration)),
        ci("End"),
        ci("Namespace"),
        $._terminator
      ),

    class_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),  // Allow multiple attribute lists
        optional(field("modifiers", repeat1($.member_modifier))),
        ci("Class"),
        field("name", $.identifier),
        optional(field("type_parameters", $.type_parameter_list)),
        $._block_terminator,
        field("body", repeat($._type_member_declaration)),
        ci("End"),
        ci("Class"),
        $._terminator
      ),

    structure_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),  // Change from optional to repeat
        optional(field("modifiers", repeat1($.member_modifier))),
        ci("Structure"),
        field("name", $.identifier),
        optional(field("type_parameters", $.type_parameter_list)),
        $._block_terminator,
        field("body", repeat($._type_member_declaration)),
        ci("End"),
        ci("Structure"),
        $._terminator
      ),

    interface_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),  // Change from optional to repeat
        optional(field("modifiers", repeat1($.member_modifier))),
        ci("Interface"),
        field("name", $.identifier),
        optional(field("type_parameters", $.type_parameter_list)),
        $._block_terminator,
        field("body", repeat($._interface_member_declaration)),
        ci("End"),
        ci("Interface"),
        $._terminator
      ),

    module_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),  // Change from optional to repeat
        optional(field("modifiers", repeat1($.member_modifier))),
        ci("Module"),
        field("name", $.identifier),
        $._block_terminator,
        field("body", repeat($._type_member_declaration)),
        ci("End"),
        ci("Module"),
        $._terminator
      ),

    enum_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),  // Change from optional to repeat
        optional(field("modifiers", repeat1($.member_modifier))),
        ci("Enum"),
        field("name", $.identifier),
        optional($.as_clause),
        $._block_terminator,
        repeat($.enum_member_declaration),
        ci("End"),
        ci("Enum"),
        $._terminator
      ),

    enum_member_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),
        field("name", $.identifier),
        optional(seq("=", field("value", $._expression))),
        $._terminator
      ),

    delegate_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),  // Change from optional to repeat
        optional(field("modifiers", repeat1($.member_modifier))),
        ci("Delegate"),
        choice(ci("Sub"), ci("Function")),
        field("name", $.identifier),
        optional(field("type_parameters", $.type_parameter_list)),
        optional(field("parameters", $.parameter_list)),
        optional($.as_clause),
        $._terminator
      ),

    // Type members
    _type_member_declaration: ($) =>
      choice(
        prec(-1, $.empty_statement),
        prec(10, $.inherits_statement),
        prec(10, $.implements_statement),
        prec(100, $.constructor_declaration),
        prec(150, $.external_method_declaration),
        prec(160, $.custom_event_declaration),
        prec(170, $.abstract_method_declaration),
        prec(171, $.abstract_property_declaration),
        // Explicitly add statements that can appear at class level
        prec(200, $.try_statement),      // Very high precedence
        prec(200, $.select_statement),   // Very high precedence
        prec(200, $.if_statement),
        prec(200, $.while_statement),
        prec(200, $.do_statement),
        prec(200, $.for_statement),
        prec(200, $.for_each_statement),
        prec(200, $.using_statement),
        prec(200, $.with_statement),
        prec(200, $.synclock_statement),
        prec(200, $.throw_statement),
        prec(200, $.return_statement),
        prec(200, $.exit_statement),
        prec(200, $.continue_statement),
        prec(200, $.stop_statement),
        prec(200, $.end_statement),
        prec(200, $.goto_statement),
        prec(200, $.resume_statement),
        prec(200, $.error_statement),
        prec(200, $.on_error_statement),
        prec(200, $.redim_statement),
        prec(200, $.erase_statement),
        prec(200, $.raiseevent_statement),
        prec(200, $.add_handler_statement),
        prec(200, $.remove_handler_statement),
        prec(200, $.call_statement),
        prec(200, $.yield_statement),
        prec(200, $.assignment_statement),
        prec(200, $.expression_statement),
        prec(200, $.declaration_statement),
        prec(200, $.labeled_statement),
        prec(1, $.method_declaration),
        prec(1, $.property_declaration),
        prec(1, $.event_declaration),
        prec(1, $.operator_declaration),
        $.type_declaration_in_type,
        prec.dynamic(-100, $.field_declaration)  // Very low precedence
      ),

    type_declaration_in_type: ($) =>
      choice(
        $.class_declaration,
        $.structure_declaration,
        $.interface_declaration,
        $.enum_declaration,
        $.delegate_declaration
      ),

    _field_modifiers: ($) => repeat1($.member_modifier),

    variable_declarator: ($) =>
      seq(
        field("name", $.identifier),
        optional("?"),
        optional(choice(
          // Array bounds specification
          seq("(", field("bounds", commaSep1($._expression)), ")"),
          // Array rank specification
          $.array_rank_specifier
        )),
        optional($.as_clause),
        optional(seq("=", field("initializer", $._expression)))
      ),


    field_declaration: ($) =>
      prec.dynamic(-100, seq(
        repeat(field("attributes", $.attribute_list)),
        optional(field("modifiers", repeat1($.member_modifier))),
        commaSep1($.variable_declarator),
        $._terminator
      )),

    field_variable_declarator: ($) =>
      seq(
        field("name", $.identifier),
        optional("?"),
        optional($.array_rank_specifier),
        optional($.as_clause),
        optional(seq("=", field("initializer", $._expression)))
      ),

    property_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),
        optional(field("modifiers", repeat1($.member_modifier))), // Changed
        ci("Property"),
        field("name", $.identifier),
        optional(field("parameters", $.parameter_list)),
        optional($.as_clause),
        optional(seq("=", field("initializer", $._expression))),
        optional($.implements_member_clause),
        optional(seq("=", field("initializer", $._expression))),
        choice(
          $._terminator, // Auto-implemented property
          seq(
            $._block_terminator,
            optional($.property_getter),
            optional($.property_setter),
            ci("End"),
            ci("Property"),
            $._terminator
          )
        )
      ),

    property_getter: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),
        optional(field("modifiers", repeat1($.member_modifier))),
        ci("Get"),
        $._block_terminator,
        repeat($._method_body_statement),  // Use the new rule here
        ci("End"),
        ci("Get"),
        $._terminator
      ),

    property_setter: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),
        optional(field("modifiers", repeat1($.member_modifier))),
        ci("Set"),
        optional(seq("(", field("parameter", $.parameter), ")")),
        $._block_terminator,
        repeat($._method_body_statement),  // Use the new rule here
        ci("End"),
        ci("Set"),
        $._terminator
      ),

    method_declaration: ($) =>
      prec(
        1,
        seq(
          repeat(field("attributes", $.attribute_list)),
          optional(field("modifiers", repeat1($.member_modifier))),
          choice(
            seq(
              ci("Sub"),
              field("name", $.identifier)
            ),
            seq(ci("Function"), field("name", $.identifier))
          ),
          optional(field("type_parameters", $.type_parameter_list)),
          optional(field("parameters", $.parameter_list)),
          optional($.as_clause),
          optional($.handles_clause),
          optional($.implements_member_clause),
          $._block_terminator,
          repeat($._method_body_statement),
          ci("End"),
          choice(ci("Sub"), ci("Function")),
          $._terminator
        )
      ),

    handles_clause: ($) => seq(ci("Handles"), commaSep1($.event_member)),

    event_member: ($) =>
      seq(
        field("object", $._name_reference),
        ".",
        field("event", $.identifier)
      ),

    constructor_declaration: ($) =>
      prec(
        100,
        seq(
          repeat(field("attributes", $.attribute_list)),
          optional(field("modifiers", repeat1($.member_modifier))),
          $._sub_new,
          optional(field("parameters", $.parameter_list)),
          $._block_terminator,
          optional($.constructor_initializer),
          repeat($._method_body_statement),  // Use the new rule here
          ci("End"),
          ci("Sub"),
          $._terminator
        )
      ),

    constructor_initializer: ($) =>
      seq(
        choice(ci("MyBase"), ci("MyClass")),
        ".",
        ci("New"),
        $.argument_list,
        $._terminator
      ),

    event_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),
        optional(field("modifiers", repeat1($.member_modifier))), // Changed
        ci("Event"),
        field("name", $.identifier),
        choice($.as_clause, field("parameters", $.parameter_list)),
        optional($.implements_member_clause),
        $._terminator
      ),

    operator_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),
        optional(field("modifiers", repeat1($.member_modifier))), // Changed
        ci("Operator"),
        field(
          "operator",
          choice(
            "+",
            "-",
            "*",
            "/",
            "\\",
            "^",
            "&",
            ci("Mod"),
            ci("And"),
            ci("Or"),
            ci("Xor"),
            ci("Not"),
            "<<",
            ">>",
            "=",
            "<>",
            "<",
            ">",
            "<=",
            ">=",
            ci("IsTrue"),
            ci("IsFalse"),
            ci("Like"),
            ci("CType")
          )
        ),
        field("parameters", $.parameter_list),
        $.as_clause,
        $._block_terminator,
        repeat($._statement),
        ci("End"),
        ci("Operator"),
        $._terminator
      ),

    // Interface members
    _interface_member_declaration: ($) =>
      choice(
        prec(-1, $.empty_statement),
        $.inherits_statement,
        $.interface_method_declaration,
        $.interface_property_declaration,
        $.interface_event_declaration,
        $.type_declaration_in_type
      ),

    interface_method_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),
        optional(field("modifiers", repeat1($.member_modifier))), // Changed
        choice(ci("Sub"), ci("Function")),
        field("name", $.identifier),
        optional(field("type_parameters", $.type_parameter_list)),
        optional(field("parameters", $.parameter_list)),
        optional($.as_clause),
        $._terminator
      ),

    interface_property_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),
        optional(field("modifiers", repeat1($.member_modifier))), // Changed
        ci("Property"),
        field("name", $.identifier),
        optional(field("parameters", $.parameter_list)),
        $.as_clause,
        $._terminator
      ),

    interface_event_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),
        optional(field("modifiers", repeat1($.member_modifier))), // Changed
        ci("Event"),
        field("name", $.identifier),
        choice($.as_clause, field("parameters", $.parameter_list)),
        $._terminator
      ),

    // Parameters
    type_parameter_list: ($) =>
      seq("(", ci("Of"), commaSep1($.type_parameter), ")"),

    type_parameter: ($) =>
      seq(
        optional($.variance_annotation),
        field("name", $.identifier),
        optional($.type_parameter_constraints_clause)
      ),
    variance_annotation: ($) => choice(ci("In"), ci("Out")),
    type_parameter_constraints_clause: ($) =>
      seq(
        ci("As"),
        choice(
          $.type_parameter_constraint,
          seq("{", commaSep1($.type_parameter_constraint), "}")
        )
      ),
    type_parameter_constraint: ($) =>
      choice($._type, ci("New"), ci("Class"), ci("Structure")),

    parameter_list: ($) => seq("(", commaSep($.parameter), ")"),

    parameter: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),
        optional(
          field(
            "modifiers",
            repeat1(choice(ci("Optional"), ci("ByVal"), ci("ByRef"), ci("ParamArray")))
          )
        ),
        field("name", $.identifier),
        optional(choice(
          // Array parameter specification (e.g., values())
          $.array_rank_specifier,
          // Array bounds specification (e.g., values(10))
          seq("(", field("bounds", commaSep1($._expression)), ")")
        )),
        optional($.as_clause),
        optional(seq("=", field("default_value", $._expression)))
      ),

    // Statements
    _statement: ($) =>
      choice(
        $.empty_statement,
        $.labeled_statement,
        prec(2, $.select_statement),
        prec(2, $.try_statement),  // Give try_statement higher precedence
        $.declaration_statement,
        $.expression_statement,
        $.assignment_statement,
        $.call_statement,
        $.if_statement,
        $.while_statement,
        $.do_statement,
        $.for_statement,
        $.for_each_statement,
        $.using_statement,
        $.with_statement,
        $.throw_statement,
        $.return_statement,
        $.yield_statement,
        $.exit_statement,
        $.continue_statement,
        $.stop_statement,
        $.end_statement,
        $.goto_statement,
        $.resume_statement,
        $.error_statement,
        $.on_error_statement,
        $.redim_statement,
        $.erase_statement,
        $.synclock_statement,
        $.raiseevent_statement,
        $.add_handler_statement,
        $.remove_handler_statement
      ),

    _statement_block: ($) => repeat1($._statement),

    empty_statement: ($) => $._terminator,

    labeled_statement: ($) =>
      seq(field("label", $.identifier), ":", optional($._statement)),

    declaration_statement: ($) =>
      prec(1, seq(  // Add precedence
        repeat(field("attributes", $.attribute_list)),
        field("modifiers", choice(
          $.local_declaration_modifier,
          repeat1($.local_declaration_modifier)
        )),
        commaSep1($.variable_declarator),
        $._terminator
      )),

    _inline_statements: ($) =>
      prec.right(seq($._inline_statement, repeat(seq(":", $._inline_statement)))),

    _inline_statement: ($) =>
      choice(
        alias($.inline_if_statement, $.if_statement),
        alias($.inline_assignment_statement, $.assignment_statement),
        alias($.inline_return_statement, $.return_statement),
        alias($.inline_throw_statement, $.throw_statement),
        alias($.inline_goto_statement, $.goto_statement),
        alias($.inline_exit_statement, $.exit_statement),
        alias($.inline_continue_statement, $.continue_statement),
        alias($.inline_call_statement, $.call_statement),
        alias($.inline_raiseevent_statement, $.raiseevent_statement),
        alias($.inline_expression_statement, $.expression_statement)
      ),

    inline_assignment_statement: ($) =>
      prec.right(
        PRECEDENCE.ASSIGNMENT + 1,
        seq(
          field("left", choice(
            $.identifier,
            $.member_access_expression,
            $.array_access_expression
          )),
          field(
            "operator",
            choice("=", "+=", "-=", "*=", "/=", "\\=", "^=", "&=", "<<=", ">>=")
          ),
          field("right", $._expression)
        )
      ),
    inline_return_statement: ($) => prec.right(1, seq(ci("Return"), optional($._expression))),
    inline_throw_statement: ($) => prec.right(1, seq(ci("Throw"), optional($._expression))),
    inline_goto_statement: ($) => seq(ci("GoTo"), field("label", $.identifier)),
    inline_exit_statement: ($) =>
      seq(
        ci("Exit"),
        choice(ci("Do"), ci("For"), ci("While"), ci("Select"), ci("Sub"), ci("Function"), ci("Property"), ci("Try"))
      ),
    inline_continue_statement: ($) =>
      seq(ci("Continue"), choice(ci("Do"), ci("For"), ci("While"))),
    inline_call_statement: ($) => prec.right(1, seq(ci("Call"), $._expression)),
    inline_raiseevent_statement: ($) =>
      prec.right(1, seq(ci("RaiseEvent"), field("event", $.identifier), optional(field("arguments", $.argument_list)))),
    inline_expression_statement: ($) => prec.right(1, $._expression),

    inline_if_statement: ($) =>
      prec.right(
        seq(
          ci("If"),
          field("condition", $._expression),
          ci("Then"),
          field("then_statement", $._inline_statements),
          optional(seq(ci("Else"), field("else_statement", $._inline_statements)))
        )
      ),

    expression_statement: ($) => seq($._expression, $._terminator),

    assignment_statement: ($) =>
      prec.right(
        PRECEDENCE.ASSIGNMENT,
        seq(
          field("left", choice(
            $.identifier,
            $.member_access_expression,
            $.array_access_expression
          )),
          field(
            "operator",
            choice("=", "+=", "-=", "*=", "/=", "\\=", "^=", "&=", "<<=", ">>=")
          ),
          field("right", $._expression),
          $._terminator
        )
      ),
    custom_event_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),
        optional(field("modifiers", repeat1($.member_modifier))),
        ci("Custom"),
        ci("Event"),
        field("name", $.identifier),
        $.as_clause,
        optional($.implements_member_clause),
        $._block_terminator,
        repeat($.event_accessor),
        ci("End"), ci("Event"),
        $._terminator
      ),

    event_accessor: ($) =>
      seq(
        choice(ci("AddHandler"), ci("RemoveHandler"), ci("RaiseEvent")),
        optional(field("parameters", $.parameter_list)),
        $._block_terminator,
        repeat($._statement),
        ci("End"),
        choice(ci("AddHandler"), ci("RemoveHandler"), ci("RaiseEvent")),
        $._terminator
      ),

    _mustoverride_token: ($) => token(prec(11, ci("MustOverride"))),

    abstract_method_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),
        optional(field("modifiers", repeat1($.member_modifier))),
        $._mustoverride_token,
        optional(field("modifiers", repeat1($.member_modifier))),
        choice(ci("Sub"), ci("Function")),
        field("name", $.identifier),
        optional(field("type_parameters", $.type_parameter_list)),
        optional(field("parameters", $.parameter_list)),
        optional($.as_clause),
        optional($.implements_member_clause),
        $._terminator
      ),

    abstract_property_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),
        optional(field("modifiers", repeat1($.member_modifier))),
        $._mustoverride_token,
        optional(field("modifiers", repeat1($.member_modifier))),
        ci("Property"),
        field("name", $.identifier),
        optional(field("parameters", $.parameter_list)),
        optional($.as_clause),
        optional($.implements_member_clause),
        $._terminator
      ),

    external_method_declaration: ($) =>
      seq(
        repeat(field("attributes", $.attribute_list)),
        optional(field("modifiers", repeat1($.member_modifier))),
        ci("Declare"),
        optional(choice(ci("Auto"), ci("Ansi"), ci("Unicode"))),
        choice(ci("Sub"), ci("Function")),
        field("name", $.identifier),
        ci("Lib"),
        field("library", $.string_literal),
        optional(seq(ci("Alias"), field("alias", $.string_literal))),
        optional(field("parameters", $.parameter_list)),
        optional($.as_clause),
        $._terminator
      ),

    call_statement: ($) => seq(ci("Call"), $._expression, $._terminator),

    if_statement: ($) =>
      seq(
        ci("If"),
        field("condition", $._expression),
        choice(
          // Single-line if: inline statements, optional Else, one terminator
          seq(
            ci("Then"),
            field("then_statement", $._inline_statements),
            optional(seq(ci("Else"), field("else_statement", $._inline_statements))),
            $._terminator
          ),
          // Multi-line if
          seq(
            optional(ci("Then")),
            $._block_terminator,
            field("then_branch", repeat($._statement)),
            repeat($.elseif_clause),
            optional($.else_clause),
            ci("End"),
            ci("If"),
            $._terminator
          )
        )
      ),

    elseif_clause: ($) =>
      seq(
        ci("ElseIf"),
        field("condition", $._expression),
        optional(ci("Then")),
        $._block_terminator,
        field("body", repeat($._statement))
      ),

    else_clause: ($) =>
      seq(ci("Else"), $._block_terminator, field("body", repeat($._statement))),

    select_statement: $ => prec(2, seq(
      ci('Select'),
      ci('Case'),
      field('expression', $._expression),
      $._block_terminator,
      repeat($.case_clause),
      optional($.case_else_clause),
      ci('End'),
      ci('Select'),
      $._terminator
    )),

    case_clause: ($) =>
      seq(
        ci("Case"),
        field("values", commaSep1($.case_value)),
        $._block_terminator,
        field("body", repeat($._statement))
      ),

    case_value: ($) =>
      choice(
        $._expression,
        seq(field("from", $._expression), ci("To"), field("to", $._expression)),
        seq(
          ci("Is"),
          field("operator", choice("<", ">", "<=", ">=", "=", "<>")),
          field("value", $._expression)
        )
      ),

    case_else_clause: ($) =>
      seq(
        ci("Case"),
        ci("Else"),
        $._block_terminator,
        field("body", repeat($._statement))
      ),

    while_statement: ($) =>
      seq(
        ci("While"),
        field("condition", $._expression),
        $._block_terminator,
        field("body", repeat($._statement)),
        ci("End"),
        ci("While"),
        $._terminator
      ),

    do_statement: ($) =>
      seq(
        ci("Do"),
        optional(
          choice(
            seq(ci("While"), field("while_condition", $._expression)),
            seq(ci("Until"), field("until_condition", $._expression))
          )
        ),
        $._block_terminator,
        field("body", repeat($._statement)),
        ci("Loop"),
        optional(
          choice(
            seq(ci("While"), field("while_condition", $._expression)),
            seq(ci("Until"), field("until_condition", $._expression))
          )
        ),
        $._terminator
      ),

    for_statement: ($) =>
      seq(
        ci("For"),
        field("variable", $.identifier),
        optional($.as_clause),
        "=",
        field("from", $._expression),
        ci("To"),
        field("to", $._expression),
        optional(seq(ci("Step"), field("step", $._expression))),
        $._block_terminator,
        field("body", repeat($._statement)),
        ci("Next"),
        optional(field("counter", $.identifier)),
        $._terminator
      ),

    for_each_statement: ($) =>
      seq(
        ci("For"),
        ci("Each"),
        field("variable", $.identifier),
        optional($.as_clause),
        ci("In"),
        field("collection", $._expression),
        $._block_terminator,
        field("body", repeat($._statement)),
        ci("Next"),
        optional(field("counter", $.identifier)),
        $._terminator
      ),

    using_statement: ($) =>
      seq(
        ci("Using"),
        commaSep1(field("resource", choice($.variable_declarator, $._expression))),
        $._block_terminator,
        field("body", repeat($._statement)),
        ci("End"),
        ci("Using"),
        $._terminator
      ),

    with_statement: ($) =>
      seq(
        ci("With"),
        field("expression", $._expression),
        $._block_terminator,
        field("body", repeat(choice(
          $.declaration_statement,  // Allow declarations in With blocks
          $._statement
        ))),
        ci("End"),
        ci("With"),
        $._terminator
      ),

    try_statement: ($) =>
      prec(2, seq(
        ci("Try"),
        $._block_terminator,
        field("body", repeat($._statement)),
        repeat($.catch_block),
        optional($.finally_block),
        ci("End"),
        ci("Try"),
        $._terminator
      )),

    catch_block: ($) =>
      seq(
        ci("Catch"),
        optional(
          seq(
            field("exception", $.identifier),
            optional(seq(ci("As"), field("type", $._type))),
            optional(seq(ci("When"), field("filter", $._expression)))
          )
        ),
        $._block_terminator,
        field("body", repeat($._statement))
      ),
    finally_block: ($) =>
      seq(
        ci("Finally"),
        $._block_terminator,
        field("body", repeat($._statement))
      ),
    throw_statement: ($) =>
      seq(ci("Throw"), optional($._expression), $._terminator),
    return_statement: ($) =>
      seq(ci("Return"), optional($._expression), $._terminator),
    yield_statement: ($) => seq(ci("Yield"), $._expression, $._terminator),
    exit_statement: ($) =>
      seq(
        ci("Exit"),
        choice(
          ci("Do"),
          ci("For"),
          ci("While"),
          ci("Select"),
          ci("Sub"),
          ci("Function"),
          ci("Property"),
          ci("Try")
        ),
        $._terminator
      ),
    continue_statement: ($) =>
      seq(
        ci("Continue"),
        choice(ci("Do"), ci("For"), ci("While")),
        $._terminator
      ),
    stop_statement: ($) => seq(ci("Stop"), $._terminator),
    end_statement: ($) => seq(ci("End"), $._terminator),
    goto_statement: ($) =>
      seq(ci("GoTo"), field("label", $.identifier), $._terminator),
    resume_statement: ($) =>
      seq(
        ci("Resume"),
        optional(choice(ci("Next"), field("label", $.identifier))),
        $._terminator
      ),
    error_statement: ($) => seq(ci("Error"), $._expression, $._terminator),
    on_error_statement: ($) =>
      seq(
        ci("On"),
        ci("Error"),
        choice(
          seq(ci("GoTo"), field("label", choice($.identifier, "0"))),
          seq(ci("Resume"), ci("Next"))
        ),
        $._terminator
      ),
    redim_statement: ($) =>
      seq(
        ci("ReDim"),
        optional(ci("Preserve")),
        commaSep1($.redim_clause),
        $._terminator
      ),
    redim_clause: ($) =>
      seq(field("array", $.identifier), "(", commaSep1($._expression), ")"),
    erase_statement: ($) =>
      seq(ci("Erase"), commaSep1($.identifier), $._terminator),
    synclock_statement: ($) =>
      seq(
        ci("SyncLock"),
        field("expression", $._expression),
        $._block_terminator,
        field("body", repeat($._statement)),
        ci("End"),
        ci("SyncLock"),
        $._terminator
      ),
    raiseevent_statement: ($) =>
      seq(
        ci("RaiseEvent"),
        field("event", $.identifier),
        optional(field("arguments", $.argument_list)),
        $._terminator
      ),
    add_handler_statement: ($) =>
      seq(
        ci("AddHandler"),
        field("event", $._expression),
        ",",
        field("handler", $._expression),
        $._terminator
      ),
    remove_handler_statement: ($) =>
      seq(
        ci("RemoveHandler"),
        field("event", $._expression),
        ",",
        field("handler", $._expression),
        $._terminator
      ), // LINQ Query expressions
    query_expression: ($) =>
      prec.right(
        choice(
          seq(
            $.from_clause,
            repeat(seq(optional($._query_clause_continuation), $.query_body_clause)),
            optional(seq(optional($._query_clause_continuation), $.select_or_group_clause))
          ),
          $.aggregate_clause
        )
      ),
    from_clause: ($) => prec.right(seq(ci("From"), commaSep1($.collection_range_variable))),
    collection_range_variable: ($) =>
      prec.right(
        seq(
          field("variable", $.identifier),
          optional($.as_clause),
          ci("In"),
          field("collection", $._expression)
        )
      ),
    query_body_clause: ($) =>
      choice(
        $.from_clause,
        $.let_clause,
        $.where_clause,
        $.join_clause,
        $.order_by_clause,
        $.aggregate_clause,
        $.distinct_clause,
        $.skip_take_clause
      ),
    distinct_clause: ($) => ci("Distinct"),
    skip_take_clause: ($) =>
      prec.right(seq(choice(ci("Skip"), ci("Take")), optional(ci("While")), $._expression)),
    let_clause: ($) => prec.right(seq(ci("Let"), commaSep1($.expression_range_variable))),
    expression_range_variable: ($) =>
      prec.right(seq(
        field("variable", $.identifier),
        "=",
        field("expression", $._expression)
      )),
    where_clause: ($) =>
      prec.right(seq(ci("Where"), field("condition", $._expression))),
    join_clause: ($) =>
      prec.right(
        seq(
          ci("Join"),
          $.collection_range_variable,
          optional($._query_clause_continuation),
          ci("On"),
          commaSep1($.join_condition)
        )
      ),
    join_condition: ($) =>
      prec.right(seq(
        field("left", $._expression),
        ci("Equals"),
        field("right", $._expression)
      )),
    order_by_clause: ($) => prec.right(seq(ci("Order"), ci("By"), commaSep1($.ordering))),
    ordering: ($) =>
      prec.right(
        seq(
          field("expression", $._expression),
          optional(choice(ci("Ascending"), ci("Descending")))
        )
      ),
    aggregate_clause: ($) =>
      prec.right(
        seq(
          ci("Aggregate"),
          $.collection_range_variable,
          repeat(seq(optional($._query_clause_continuation), $.query_body_clause)),
          optional($._query_clause_continuation),
          ci("Into"),
          commaSep1($.aggregate_element)
        )
      ),
    aggregate_element: ($) =>
      prec.right(seq(
        optional(seq(field("name", $.identifier), "=")),
        field("function", $.aggregate_function)
      )),
    aggregate_function: ($) =>
      seq(
        field(
          "name",
          choice(
            ci("Count"),
            ci("Sum"),
            ci("Min"),
            ci("Max"),
            ci("Average"),
            ci("All"),
            ci("Any"),
            ci("Group")
          )
        ),
        optional(seq("(", field("argument", $._expression), ")"))
      ),
    select_or_group_clause: ($) => choice($.select_clause, $.group_by_clause),
    select_clause: ($) => seq(ci("Select"), commaSep1($.select_element)),
    select_element: ($) =>
      prec.right(choice(
        $._expression,
        seq(
          field("name", $.identifier),
          "=",
          field("expression", $._expression)
        )
      )),
    group_by_clause: ($) =>
      prec.right(
        seq(
          ci("Group"),
          commaSep1($.group_element),
          ci("By"),
          commaSep1($.by_element),
          ci("Into"),
          commaSep1($.into_element)
        )
      ),
    group_element: ($) =>
      prec.right(choice(
        $._expression,
        seq(
          field("name", $.identifier),
          "=",
          field("expression", $._expression)
        )
      )),
    by_element: ($) =>
      prec.right(choice(
        $._expression,
        seq(
          field("name", $.identifier),
          "=",
          field("expression", $._expression)
        )
      )),
    into_element: ($) => prec.right(choice($.identifier, $.aggregate_element)),

    // Comments
    comment: ($) => token(prec(100, choice(
      // Single quote comment (including multiple quotes) - don't consume newline
      /'+[^\n\r]*/,
      // REM followed by whitespace - don't consume newline
      /[Rr][Ee][Mm][ \t][^\n\r]*/
    ))),

    // Preprocessor Directives
    preprocessor_directive: ($) => token(seq("#", /[ \t]*[A-Za-z][^\r\n]*/)),
  },
});

// Helper functions
function ci(keyword) {
  return new RegExp(
    keyword
      .split("")
      .map((letter) => `[${letter.toLowerCase()}${letter.toUpperCase()}]`)
      .join("")
  );
}

// Helper function for negative lookahead
function negative_lookahead(pattern) {
  return seq(not(pattern), '');
}

function not(pattern) {
  return token(prec(-1, seq('', pattern)));
}

function commaSep1(rule) {
  return seq(rule, repeat(seq(",", rule)));
}

function commaSep(rule) {
  return optional(commaSep1(rule));
}

function commaSep2(rule) {
  return seq(rule, ",", commaSep1(rule));
}
