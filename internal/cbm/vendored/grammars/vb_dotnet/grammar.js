/**
 * @file Tree sitter grammar for VB.NET
 * @author CodeAnt AI <chinmay@codeant.ai>
 * @license MIT
 */

/// <reference types="tree-sitter-cli/dsl" />
// @ts-check

// Tree-sitter grammar for Visual Basic .NET, based on the official VB.NET language specification.
module.exports = grammar({
  name: 'vb_dotnet',


  extras: $ => [
    $.comment,
    /[ \t\f\u00A0]+/,        // whitespace except newlines
    $._line_continuation    
  ],

  
  conflicts: $ => [
    [$.type, $.invocation],
    [$.type] ,
    [$.new_expression] ,
    [$.type_argument_list] ,
    [$.property_declaration] ,
    [$.constructor_declaration],
    [$.method_declaration],
    [$.left_hand_side, $.expression],
    [$.label_statement, $.expression],
    [$.event_declaration],
    [$.if_statement],
    [$.type, $.array_type],
    [$.if_statement, $.binary_expression],
    [$.empty_statement, $.if_statement],
    [$.namespace_name, $.attribute],
    [$.namespace_name],
  ],

  rules: {
    
    source_file: $ => seq(
    optional($.option_statements),
    repeat($.imports_statement),
    repeat(choice(
      $.attribute_block,
      $.namespace_block,
      $.type_declaration,
      $.preprocessor_directive,          // #Region around top-level types
      alias($._terminator, $.blank_line) //
    ))
  ),

    
    option_statements: $ => repeat1(
      seq(
        kw('Option'),
        choice(
          seq(kw('Explicit'), choice(kw('On'), kw('Off'))),
          seq(kw('Strict'), choice(kw('On'), kw('Off'))),
          seq(kw('Infer'), choice(kw('On'), kw('Off'))),
          seq(kw('Compare'), choice(kw('Binary'), kw('Text')))
        ),
        $._terminator
      )
    ),

    // Imports statement for namespace imports (can import multiple namespaces in one line)
    imports_statement: $ => seq(
      kw('Imports'),
      commaSep1(field('namespace', $.namespace_name)),
      $._terminator
    ),

    // A dot-separated name (for namespaces or qualified types)
    namespace_name: $ => seq($.identifier, repeat(seq('.', $.identifier))),

    // Any top-level type or namespace declaration
    type_declaration: $ => choice(
      $.class_block,
      $.module_block,
      $.structure_block,
      $.interface_block,
      $.enum_block,
      $.delegate_declaration
    ),

    // Namespace block: Namespace Name ... End Namespace
    // namespace_block: $ => seq(
    //   kw('Namespace'),
    //   field('name', $.namespace_name),
    //   $._terminator,
    //   repeat(choice($.attribute_block, $.type_declaration, $.namespace_block)),
    //   kw('End'), kw('Namespace'), $._terminator
    // ),
    namespace_block: $ => seq(
      kw('Namespace'),
      field('name', $.namespace_name),
      $._terminator,
      repeat(choice(
        $.attribute_block,
        $.type_declaration,
        $.namespace_block,
        $.imports_statement,  // Allow imports inside namespace
        $.preprocessor_directive,  // #Region around types in a namespace
        alias($._terminator, $.blank_line)  // Allow blank lines
      )),
      kw('End'), kw('Namespace'), $._terminator
    ),

    // Class definition block
    // Inherits/Implements normally appear on their own lines right after the
    // header (the standard VB.NET form); the inline header position is kept
    // for compatibility.
    class_block: $ => seq(
      field('modifiers', optional($.modifiers)),
      kw('Class'),
      field('name', $.identifier),
      optional($.type_parameters),
      optional(field('inherits', $.inherits_clause)),
      optional(field('implements', $.implements_clause)),
      $._terminator,
      repeat(choice(
        seq(field('inherits', $.inherits_clause), $._terminator),
        seq(field('implements', $.implements_clause), $._terminator)
      )),
      repeat($._member_declaration),
      kw('End'), kw('Class'), $._terminator
    ),

    // Module definition block
    module_block: $ => seq(
      field('modifiers', optional($.modifiers)),
      kw('Module'),
      field('name', $.identifier),
      $._terminator,
      repeat($._member_declaration),
      kw('End'), kw('Module'), $._terminator
    ),

    // Structure definition block
    structure_block: $ => seq(
      field('modifiers', optional($.modifiers)),
      kw('Structure'),
      field('name', $.identifier),
      optional($.type_parameters),
      optional(field('implements', $.implements_clause)),
      $._terminator,
      repeat(seq(field('implements', $.implements_clause), $._terminator)),
      repeat($._member_declaration),
      kw('End'), kw('Structure'), $._terminator
    ),

    // Interface definition block
    interface_block: $ => seq(
      field('modifiers', optional($.modifiers)),
      kw('Interface'),
      field('name', $.identifier),
      optional($.type_parameters),
      optional(field('inherits', $.inherits_clause)), // interfaces can inherit multiple interfaces
      $._terminator,
      repeat(seq(field('inherits', $.inherits_clause), $._terminator)),
      repeat($._member_declaration),
      kw('End'), kw('Interface'), $._terminator
    ),

    // Enum definition block
    enum_block: $ => seq(
      field('modifiers', optional($.modifiers)),
      kw('Enum'),
      field('name', $.identifier),
      $._terminator,
      repeat($.enum_member),
      kw('End'), kw('Enum'), $._terminator
    ),
    enum_member: $ => seq(
      field('name', $.identifier),
      optional(seq('=', field('value', $.expression))),
      $._terminator
    ),

    // Delegate declaration (treated as a type declaration, no End block since it's a single line)
    delegate_declaration: $ => seq(
      field('modifiers', optional($.modifiers)),
      kw('Delegate'),
      choice(kw('Sub'), kw('Function')),
      field('name', $.identifier),
      optional($.type_parameters),
      field('parameters', $.parameter_list),
      optional(seq(kw('As'), field('return_type', $.type))),  // only for Function delegate
      $._terminator
    ),

    // Inheritance clause (for classes or interfaces)
    inherits_clause: $ => seq(kw('Inherits'), commaSep1($.type)),
    // Implements clause (for classes or structures implementing interfaces)
    implements_clause: $ => seq(kw('Implements'), commaSep1($.type)),

    // `Handles Button1.Click, Me.Load` on a Sub/Function header
    handles_clause: $ => seq(kw('Handles'), commaSep1($.handles_target)),
    handles_target: $ => seq($.identifier, repeat1(seq('.', $.identifier))),
    // `Implements IFace.Member` on a Sub/Function/Property header
    member_implements_clause: $ => seq(kw('Implements'), commaSep1($.handles_target)),

    // Generic type parameter definitions: e.g., (Of T As {Constraint})
    type_parameters: $ => seq(
      kw('Of'),
      commaSep1($.type_parameter)
    ),
    type_parameter: $ => seq(
      field('name', $.identifier),
      optional(seq(kw('As'), field('constraint', $.type_constraint)))
    ),
    type_constraint: $ => choice(
      $.type,
      kw('Structure'),
      kw('Class'),
      kw('New')  // type must have a public parameterless constructor
    ),

    // Attributes: <...> blocks attached to declarations.  The terminator is
    // optional: `<Attr()> _` continuation lines (Designer files) join the
    // attribute and the declaration into one logical line with no terminator
    // in between.
    attribute_block: $ => prec.right(seq(
      '<',
      commaSep1($.attribute),
      '>',
      optional($._terminator)
    )),
    // attribute: $ => seq(
    //   optional(seq(field('target', $.identifier), ':')),  // e.g., Assembly: or Module: target
    //   field('name', $.identifier),
    //   optional($.argument_list)  // arguments in parentheses
    // ),
    attribute: $ => seq(
      optional(seq(field('target', $.identifier), ':')),
      field('name', choice($.identifier, $.namespace_name)),
      optional($.argument_list)
    ),

    // Modifiers (public/private/etc.). Multiple modifiers can appear in sequence.
    modifiers: $ => repeat1($.modifier),
    modifier: $ => token(choice(
      kw('Public'), kw('Private'), kw('Protected'), kw('Friend'),
      kw('Shared'), kw('Shadows'), kw('Static'),
      kw('Overloads'), kw('Overrides'), kw('Overridable'), kw('NotOverridable'), kw('MustOverride'),
      kw('MustInherit'), kw('NotInheritable'),
      kw('Partial'), kw('Narrowing'), kw('Widening'),
      kw('Default'),              // for default properties
      kw('ReadOnly'), kw('WriteOnly'),
      kw('WithEvents'), kw('Async'), kw('Iterator')
    )),

    // Type member declarations inside class/module/etc.
    _member_declaration: $ => choice(
      alias($._terminator, $.blank_line),
      $.const_declaration,
      $.field_declaration,
      $.method_declaration,
      $.constructor_declaration,
      $.property_declaration,
      $.event_declaration,
      $.delegate_declaration,   // nested delegate type
      $.preprocessor_directive, // #Region / #End Region between members
      // nested types (Partial Class bodies, Designer files); delegate_declaration
      // is already listed above, so name the block types directly rather than
      // $.type_declaration to avoid an LR ambiguity on delegates
      $.class_block,
      $.structure_block,
      $.interface_block,
      $.enum_block
    ),

    // Constant definitions (inside classes or procedures)
    const_declaration: $ => seq(
      optional(field('attributes', $.attribute_block)),
      field('modifiers', optional($.modifiers)),
      kw('Const'),
      commaSep1(seq(
        field('name', $.identifier),
        optional($.as_clause),
        '=', 
        field('value', $.expression)
      )),
      $._terminator
    ),

    // Field (variable) declarations (inside classes/modules or as locals with Dim)
    field_declaration: $ => seq(
      optional(field('attributes', $.attribute_block)),
      field('modifiers', optional($.modifiers)),
      optional(kw('Dim')),  
      commaSep1($.variable_declarator),
      $._terminator
    ),
    variable_declarator: $ => seq(
      field('name', $.identifier),
      optional($.array_rank_specifier),
      optional($.as_clause),
      optional(seq('=', field('initializer', $.expression)))
    ),
    array_rank_specifier: $ => seq('(', optional(repeat(',')), ')'),  // e.g. "()" or "(,)" for array dimensions
    // `As Type` or the `As New Type(args)` initializer shorthand
    as_clause: $ => seq(kw('As'), choice(field('type', $.type), field('new', $.new_expression))),

    // Type (for variables, parameters, return types, etc.)
    type: $ => choice(
      $.primitive_type,
      $.array_type,
      $.generic_type,
      seq($.namespace_name, optional($.type_argument_list), optional($.array_rank_specifier))
    ),

    generic_type: $ => prec.left(3, seq(
      $.namespace_name,
      $.type_argument_list,
      optional($.array_rank_specifier)
    )),

    array_type: $ => seq(
      choice($.primitive_type, $.namespace_name),
      optional($.type_argument_list),
      $.array_rank_specifier
    ),

    primitive_type: $ => token(choice(  // built-in types
      kw('Boolean'), kw('Byte'), kw('Short'), kw('Integer'), kw('Long'),
      kw('Single'), kw('Double'), kw('Decimal'),
      kw('Char'), kw('String'),
      kw('Object'), kw('Date')
    )),
    type_argument_list: $ => seq(kw('Of'), commaSep1($.type)),

    // Method (Sub/Function) declaration inside a class/module or as a procedure in a module
    method_declaration: $ => seq(
      optional(field('attributes', $.attribute_block)),
      field('modifiers', optional($.modifiers)),
      choice(kw('Sub'), kw('Function')),
      field('name', $.identifier),
      optional($.type_parameters),
      field('parameters', $.parameter_list),
      optional(seq(kw('As'), field('return_type', $.type))),  // only for Function
      // `Handles obj.Event` (WinForms wiring) or `Implements IFace.Member`
      optional(choice(
        field('handles', $.handles_clause),
        field('implements', $.member_implements_clause)
      )),
      choice(
        // With body:
        seq($._terminator, repeat($.statement), kw('End'), choice(kw('Sub'), kw('Function')), $._terminator),
        // Without body (e.g., abstract method in MustInherit class, or interface method):
        $._terminator
      )
    ),

    // Constructor (Sub New) declaration
    constructor_declaration: $ => seq(
      optional(field('attributes', $.attribute_block)),
      field('modifiers', optional($.modifiers)),
      kw('Sub'), kw('New'),
      field('parameters', $.parameter_list),
      choice(
        seq($._terminator, repeat($.statement), kw('End'), kw('Sub'), $._terminator),
        $._terminator
      )
    ),

    // Property declaration (auto or with getters/setters)
    property_declaration: $ => seq(
      optional(field('attributes', $.attribute_block)),
      field('modifiers', optional($.modifiers)),
      kw('Property'),
      field('name', $.identifier),
      optional(field('parameters', $.parameter_list)),  // indexed properties
      optional($.as_clause),
      optional(field('implements', $.member_implements_clause)),
      choice(
        // Auto-property or declaration without body:
        seq(optional(seq('=', field('initializer', $.expression))), $._terminator),
        // Property with Get/Set accessors:
        seq(
          $._terminator,
          optional($.get_accessor),
          optional($.set_accessor),
          kw('End'), kw('Property'), $._terminator
        )
      )
    ),
    get_accessor: $ => seq(
      optional(field('modifiers', optional($.modifiers))),
      kw('Get'), $._terminator,
      repeat($.statement),
      kw('End'), kw('Get'), $._terminator
    ),
    set_accessor: $ => seq(
      optional(field('modifiers', optional($.modifiers))),
      kw('Set'),
      optional(field('parameters', $.parameter_list)),  // Set can have a Value parameter
      $._terminator,
      repeat($.statement),
      kw('End'), kw('Set'), $._terminator
    ),

    // Event declaration (regular or custom event with add/remove/raise handlers)
    event_declaration: $ => seq(
      optional(field('attributes', $.attribute_block)),
      field('modifiers', optional($.modifiers)),
      kw('Event'),
      field('name', $.identifier),
      choice(
        // Standard event declaration
        seq(optional($.parameter_list), optional($.as_clause), $._terminator),
        // Custom event with handlers
        seq(
          $.parameter_list, $.as_clause, $._terminator,
          repeat(choice($.add_handler_block, $.remove_handler_block, $.raise_event_block)),
          kw('End'), kw('Event'), $._terminator
        )
      )
    ),
    add_handler_block: $ => seq(
      optional(field('modifiers', optional($.modifiers))),
      kw('AddHandler'), $._terminator,
      repeat($.statement),
      kw('End'), kw('AddHandler'), $._terminator
    ),
    remove_handler_block: $ => seq(
      optional(field('modifiers', optional($.modifiers))),
      kw('RemoveHandler'), $._terminator,
      repeat($.statement),
      kw('End'), kw('RemoveHandler'), $._terminator
    ),
    raise_event_block: $ => seq(
      optional(field('modifiers', optional($.modifiers))),
      kw('RaiseEvent'), $._terminator,
      repeat($.statement),
      kw('End'), kw('RaiseEvent'), $._terminator
    ),

    // Parameter list for methods/delegates (Parentheses with comma-separated parameters)
    parameter_list: $ => seq('(', optional(commaSep($.parameter)), ')'),
    // parameter: $ => seq(
    //   optional(choice(kw('ByVal'), kw('ByRef'), kw('ParamArray'))),
    //   field('name', $.identifier),
    //   optional($.array_rank_specifier),
    //   optional($.as_clause),
    //   optional(seq('=', field('default_value', $.expression)))  // default parameter value
    // ),
    parameter: $ => seq(
      optional($.attribute_block),  // Add support for attributes on parameters
      optional(choice(kw('Optional'), kw('ByVal'), kw('ByRef'), kw('ParamArray'))),
      field('name', $.identifier),
      optional($.array_rank_specifier),
      optional($.as_clause),
      optional(seq('=', field('default_value', $.expression)))
    ),

    // *** Statements (inside procedures or blocks) ***

    statement: $ => choice(
      $.empty_statement,
      $.label_statement,
      $.dim_statement,
      $.const_declaration,      // local constant
      prec(1, $.assignment_statement),
      $.call_statement,
      $.if_statement,
      $.select_case_statement,
      $.while_statement,
      $.do_statement,
      $.for_statement,
      $.for_each_statement,
      $.try_statement,
      $.with_statement,
      $.using_statement,
      $.sync_lock_statement,
      $.return_statement,
      $.exit_statement,
      $.continue_statement,
      $.throw_statement,
      $.goto_statement,
      $.redim_statement,
      $.preprocessor_directive,  // allow preprocessor directives in code flow
      $.compound_assignment_statement,
      $.add_remove_handler_statement,
      $.raise_event_statement,
    ),

    // `AddHandler obj.Event, AddressOf Handler` / `RemoveHandler ...`
    add_remove_handler_statement: $ => seq(
      choice(kw('AddHandler'), kw('RemoveHandler')),
      field('event', $.expression),
      token(seq(',', optional(/\r?\n/))),
      field('handler', $.expression),
      $._terminator
    ),

    // `RaiseEvent SomethingHappened(args)`
    raise_event_statement: $ => seq(
      kw('RaiseEvent'),
      $.expression,
      $._terminator
    ),

    empty_statement: $ => prec(1, $._terminator),  // a standalone line break (no-op statement)

    label_statement: $ => seq(field('label', $.identifier), ':'),

    dim_statement: $ => seq(
      kw('Dim'),
      commaSep1(seq(
        field('name', $.identifier),
        optional($.as_clause),
        optional(seq('=', field('initializer', $.expression)))
      )),
      $._terminator
    ),

    compound_assignment_statement: $ => seq(
      field('left', $.left_hand_side),
      field('operator', choice('+=', '-=', '*=', '/=', '\\=', '^=', '&=', '<<=', '>>=')),
      field('right', $.expression),
      $._terminator
    ),

    assignment_statement: $ => seq(
      field('left', $.left_hand_side),
      '=',
      field('right', $.expression),
      $._terminator
    ),
    left_hand_side: $ => choice(
      $.identifier,
      $.member_access,
      $.element_access
    ),

    call_statement: $ => seq(
      choice(seq(kw('Call'), $.expression), $.expression),
      $._terminator
    ),

    // if_statement: $ => choice(
    //   // Single-line If
    //   seq(
    //     kw('If'), field('condition', $.expression), kw('Then'),
    //     field('then_branch', $.statement),
    //     optional(seq(kw('Else'), field('else_branch', $.statement)))
    //   ),
    //   // Block If/ElseIf/Else
    //   seq(
    //     kw('If'), field('condition', $.expression), kw('Then'), $._terminator,
    //     repeat($.statement),
    //     repeat(seq(kw('ElseIf'), $.expression, kw('Then'), $._terminator, repeat($.statement))),
    //     optional(seq(kw('Else'), $._terminator, repeat($.statement))),
    //     kw('End'), kw('If'), $._terminator
    //   )
    // ),
    if_statement: $ => choice(
      // Single-line If
      prec(2, seq(
        kw('If'), field('condition', $.expression), kw('Then'),
        field('then_branch', choice($.statement, $.expression)),
        optional(seq(kw('Else'), field('else_branch', choice($.statement, $.expression))))
      )),
      // Block If/ElseIf/Else
      prec(1, seq(
        kw('If'), field('condition', $.expression), kw('Then'), $._terminator,
        repeat($.statement),
        repeat($.elseif_clause),
        optional($.else_clause),
        kw('End'), kw('If'), $._terminator
      ))
    ),

    elseif_clause: $ => seq(
      kw('ElseIf'), field('condition', $.expression), kw('Then'), $._terminator,
      repeat($.statement)
    ),

    else_clause: $ => seq(
      kw('Else'), $._terminator,
      repeat($.statement)
    ),

    select_case_statement: $ => seq(
      kw('Select'), kw('Case'), field('selector', $.expression), $._terminator,
      repeat($.case_block),
      optional($.case_else_block),
      kw('End'), kw('Select'), $._terminator
    ),
    case_block: $ => seq(
      kw('Case'),
      commaSep1($.case_clause),
      $._terminator,
      repeat($.statement)
    ),
    case_else_block: $ => seq(
      kw('Case'), kw('Else'), $._terminator,
      repeat($.statement)
    ),
    case_clause: $ => choice(
      $.expression,
      seq($.expression, kw('To'), $.expression),                // range: low To high
      seq(kw('Is'), $.relational_operator, $.expression)        // relational form: e.g. Is < 5
    ),
    relational_operator: $ => token(choice('=', '<>', '<', '>', '<=', '>=')),

    while_statement: $ => seq(
      kw('While'), field('condition', $.expression), $._terminator,
      repeat($.statement),
      kw('End'), kw('While'), $._terminator
    ),

    do_statement: $ => choice(
      // Do ... Loop [While/Until condition] (exit condition at bottom)
      seq(kw('Do'), $._terminator,
          repeat($.statement),
          kw('Loop'), optional(choice(seq(kw('While'), $.expression), seq(kw('Until'), $.expression))), $._terminator),
      // Do [While/Until condition] ... Loop (check at top)
      seq(kw('Do'), choice(seq(kw('While'), $.expression), seq(kw('Until'), $.expression)), $._terminator,
          repeat($.statement),
          kw('Loop'), $._terminator)
    ),

    // for_statement: $ => seq(
    //   kw('For'),
    //   field('variable', $.identifier), '=', field('start', $.expression),
    //   kw('To'), field('end', $.expression),
    //   optional(seq(kw('Step'), field('step', $.expression))),
    //   $._terminator,
    //   repeat($.statement),
    //   kw('Next'), optional(alias($.identifier, $.variable)), $._terminator
    // ),
    for_statement: $ => seq(
      kw('For'),
      choice(
        // With type declaration
        seq(
          field('variable', $.identifier),
          $.as_clause,
          '=',
          field('start', $.expression)
        ),
        // Without type declaration
        seq(
          field('variable', $.identifier),
          '=',
          field('start', $.expression)
        )
      ),
      kw('To'), field('end', $.expression),
      optional(seq(kw('Step'), field('step', $.expression))),
      $._terminator,
      repeat($.statement),
      // kw('Next'), optional(field('variable', $.identifier)), $._terminator
      kw('Next'), optional(field('variable', $.identifier)), $._terminator
    ),

    // for_each_statement: $ => seq(
    //   kw('For'), kw('Each'),
    //   field('variable', $.identifier),
    //   kw('In'),
    //   field('collection', $.expression),
    //   $._terminator,
    //   repeat($.statement),
    //   kw('Next'), optional(alias($.identifier, $.variable)), $._terminator
    // ),
    for_each_statement: $ => seq(
      kw('For'), kw('Each'),
      field('variable', $.identifier),
      kw('In'),
      field('collection', $.expression),
      $._terminator,
      repeat($.statement),
      kw('Next'), optional(field('variable', $.identifier)), $._terminator
    ),

    try_statement: $ => seq(
      kw('Try'), $._terminator,
      repeat($.statement),
      repeat($.catch_block),
      optional($.finally_block),
      kw('End'), kw('Try'), $._terminator
    ),
    catch_block: $ => seq(
      kw('Catch'),
      optional(seq(field('exception', $.identifier), optional(seq(kw('As'), field('type', $.type))))),
      optional(seq(kw('When'), field('filter', $.expression))),
      $._terminator,
      repeat($.statement)
    ),
    finally_block: $ => seq(
      kw('Finally'), $._terminator,
      repeat($.statement)
    ),

    with_statement: $ => seq(
      kw('With'), field('target', $.expression), $._terminator,
      repeat($.statement),
      kw('End'), kw('With'), $._terminator
    ),

    using_statement: $ => seq(
      kw('Using'),
      choice(
        // Using resourceVar As Type = expr
        seq(field('resource', $.identifier), $.as_clause, '=', field('value', $.expression)),
        // Using <expression>
        field('value', $.expression)
      ),
      $._terminator,
      repeat($.statement),
      kw('End'), kw('Using'), $._terminator
    ),

    sync_lock_statement: $ => seq(
      kw('SyncLock'), field('lock', $.expression), $._terminator,
      repeat($.statement),
      kw('End'), kw('SyncLock'), $._terminator
    ),

    return_statement: $ => seq(kw('Return'), optional($.expression), $._terminator),

    exit_statement: $ => seq(kw('Exit'), choice(kw('Sub'), kw('Function'), kw('Property'), kw('Do'), kw('For'), kw('While'), kw('Select'), kw('Try')), $._terminator),

    continue_statement: $ => seq(kw('Continue'), choice(kw('Do'), kw('For'), kw('While')), $._terminator),

    throw_statement: $ => seq(kw('Throw'), optional($.expression), $._terminator),

    goto_statement: $ => seq(kw('GoTo'), field('label', $.identifier), $._terminator),

    redim_statement: $ => seq(
      kw('ReDim'),
      optional(kw('Preserve')),
      commaSep1(seq(field('array', $.identifier), $.re_dim_clause)),
      $._terminator
    ),
    re_dim_clause: $ => seq(
      '(', field('upper_bound', $.expression), optional(seq(kw('To'), $.expression)), ')'
    ),

    // Preprocessor directive (treated as a standalone statement)
    preprocessor_directive: $ => token(seq('#', /[^\r\n]*/)),

    // *** Expressions ***

    expression: $ => choice(
      $.literal,
      $.identifier,
      $.parenthesized_expression,
      $.member_access,
      $.element_access,
      $.invocation,
      $.unary_expression,
      $.binary_expression,
      $.ternary_expression,
      $.new_expression,
      $.lambda_expression,
      $.array_literal,
    ),

    array_literal: $ => seq(
      '{',
      optional(commaSep($.expression)),
      '}'
    ),

    parenthesized_expression: $ => seq('(', $.expression, ')'),

    lambda_expression: $ => prec.right(seq(
      choice(kw('Function'), kw('Sub')),
      '(',
      optional(commaSep($.lambda_parameter)),
      ')',
      choice(
        $.expression,  // Single expression lambda
        seq(           // Multi-line lambda
          $._terminator,
          repeat($.statement),
          kw('End'), choice(kw('Function'), kw('Sub'))
        )
      )
    )),

    lambda_parameter: $ => seq(
      field('name', $.identifier),
      optional($.as_clause)
    ),
    
    invocation: $ => prec.left(1, seq(
      field('target', choice($.member_access, $.identifier)),
      field('arguments', $.argument_list)
    )),

    argument_list: $ => seq(
      '(',
      optional(commaSep($.argument)),
      ')'
    ),
    argument: $ => choice(
      $.expression,
      seq(field('name', $.identifier), ':', '=', $.expression)  // named argument (Name:=Expr)
    ),

    // Member access (object.member) possibly spanning lines after the dot
    // member_access: $ => seq(
    //   field('object', $.expression),
    //   token(seq('.', optional(/\r?\n/))),  // allow newline after dot (implicit line continuation)
    //   field('member', $.identifier)
    // ),
    member_access: $ => prec.left(10, seq(
      field('object', $.expression),
      '.',
      field('member', $.identifier)
    )),

    // Element/array index access: expr(index, index, ...)
    element_access: $ => seq(
      field('object', $.expression),
      '(',
      commaSep(field('index', $.expression)),
      ')'
    ),

    // Object creation: New Type[(args)] [ with initializers ]
    // new_expression: $ => seq(
    //   kw('New'),
    //   field('type', $.type),
    //   optional($.argument_list),
    //   optional($.object_initializers)
    // ),
    new_expression: $ => seq(
      kw('New'),
      field('type', $.type),
      optional($.argument_list),
      optional(choice(
        $.object_initializers,
        $.with_initializer
      ))
    ),

    with_initializer: $ => seq(
      kw('With'),
      '{',
      optional(commaSep($.member_initializer)),
      '}'
    ),

    member_initializer: $ => seq(
      '.',
      field('member', $.identifier),
      '=',
      field('value', $.expression)
    ),
    object_initializers: $ => seq(
      '{',
      optional(commaSep($.object_initializer)),
      '}'
    ),
    object_initializer: $ => choice(
      seq('.', $.identifier, '=', $.expression),  // assignment to member
      $.expression                               // value for collection initializer
    ),

    // Unary operators: e.g. -x, +x, Not x, AddressOf x
    unary_expression: $ => prec(8, seq(
      field('operator', choice(kw('Not'), kw('AddressOf'), '-', '+')),
      field('operand', $.expression)
    )),

    // Binary operators with precedence (higher number = higher precedence binding)
    binary_expression: $ => {
      const table = [
        [7, choice('^')],                              // exponentiation (right-associative in VB)
        [6, choice('*', '/', '\\', kw('Mod'))],        // multiplication, division, integer division, modulo
        [5, choice('+', '-')],                         // addition and subtraction
        [4, kw('&')],                                  // string concatenation
        [3, choice('<<', '>>')],                       // bit shifts
        [2, choice('=', '<>', '<', '>', '<=', '>=', kw('Is'), kw('IsNot'), kw('Like'))],  
        [1, choice(kw('TypeOf'))],                     // TypeOf ... Is ... (treated separately if needed)
        [0, choice(kw('And'), kw('Or'), kw('Xor'))],   // boolean/bitwise AND/OR/XOR
        [-1, choice(kw('AndAlso'), kw('OrElse'))]      // short-circuit logical operators (lowest precedence)
      ];
      // Build binary expression rules for each operator with correct precedence and associativity
      return choice(...table.map(([precedence, operator]) =>
        prec.left(precedence, seq(field('left', $.expression), field('operator', operator), field('right', $.expression)))
      ));
    },

    // Ternary conditional (IIf-like or If operator: If(condition, trueExpr, falseExpr))
    ternary_expression: $ => prec.right(seq(
      kw('If'),
      '(',
      field('condition', $.expression), ',',
      field('true_branch', $.expression), ',',
      field('false_branch', $.expression),
      ')'
    )),

    // Literals
    literal: $ => choice(
      $.boolean_literal,
      $.integer_literal,
      $.floating_point_literal,
      $.string_literal,
      $.character_literal,
      $.date_literal,
      kw('Nothing')
    ),

    boolean_literal: $ => token(choice(kw('True'), kw('False'))),

    integer_literal: $ => token(choice(
      // Decimal literal (optional type suffix)
      /\d+(?:US|UI|UL|S|I|L|%|&)?/i,
      // Hexadecimal literal (prefix &H)
      /&H[0-9A-F]+(?:US|UI|UL|S|I|L|%|&)?/i,
      // Octal literal (prefix &O)
      /&O[0-7]+(?:US|UI|UL|S|I|L|%|&)?/i
    )),  

    floating_point_literal: $ => token(choice(
      // Formats: D (integer) . D (fraction) E? exponent, etc., with optional FP type suffix (F, R, D, !, #, @)
      /\d+\.\d+([Ee][+-]?\d+)?[FfRrDd!#@]?/,
      /\d+\.([Ee][+-]?\d+)?[FfRrDd!#@]?/,
      /\.\d+([Ee][+-]?\d+)?[FfRrDd!#@]?/,
      /\d+([Ee][+-]?\d+)[FfRrDd!#@]?/,
      /\d+[FfRrDd!#@]/
    )),

    // string_literal: $ => token(seq(
    //   '"',
    //   repeat(choice(/[^"\r\n]/, /""/)),  // double "" inside represents a quote
    //   '"'
    // )),
    string_literal: $ => choice(
      // Regular string
      token(seq(
        '"',
        repeat(choice(/[^"\r\n]/, /""/)),
        '"'
      )),
      // Interpolated string
      $.interpolated_string_literal
    ),

    interpolated_string_literal: $ => seq(
      '$"',
      repeat(choice(
        /[^"{}\r\n]+/,  // Regular text
        /""/,           // Escaped quote
        $.interpolation // Interpolated expression
      )),
      '"'
    ),

    interpolation: $ => seq(
      '{',
      $.expression,
      optional(seq(':', /[^}]+/)), // Format specifier
      '}'
    ),

    character_literal: $ => token(seq(
      '"',
      choice(/[^"\r\n]/, /""/),  // exactly one character (or an escaped quote)
      '"',
      /[cC]/                    // 'C' suffix (case-insensitive)
    )),

    // Date literal: #MM/dd/yyyy [hh:mm[:ss] [AM|PM]]#
    date_literal: $ => token(seq(
      '#',
      /[0-9/\-:\sAPMapm]+/,  // simplified pattern: digits, '/', '-', ':', whitespace, AM/PM
      '#'
    )),

    // Identifiers (case-insensitive, including escaped [bracketed] identifiers and type-char suffix)
    identifier: $ => token(choice(
      // Escaped identifier in [] (can be a keyword inside)
      /\[(?:[^\]\r\n]+)\]/,
      // Unescaped identifier (start with letter/underscore, followed by letters/digits/underscore, optionally with type character)
      /[A-Za-z_][A-Za-z_0-9]*[$%&@#!]?/   // allow type-declaration suffix in identifier (e.g., foo$, bar!)
    )),

    // Comment: `'` or `REM` to end of line
    comment: $ => token(choice(
      seq("'", /[^\r\n]*/),
      seq(kw('REM'), /[^\r\n]*/)
    )),

    // Line break (statement terminator)
    _newline: $ => /\r?\n/,

    _line_continuation: $ => token(seq('_', /[ \t]*/, /\r?\n/)),

    // Statement terminator: newline or colon (for multiple statements on one line)
    _terminator: $ => choice($._newline, ':')
  }
});

// Helper: comma-separated list (zero or more)
function commaSep(rule) {
  return optional(commaSep1(rule));
}

function kw(word) {
  // token + positive precedence guarantees the keyword wins ties
  return token(prec(1, ci(word)));
}

// Helper: comma-separated list (one or more), allowing a newline after commas
function commaSep1(rule) {
  return seq(rule, repeat(seq(token(seq(',', optional(/\r?\n/))), rule)));
}

// Case-insensitive literal helper: creates a regex that matches the given keyword in any case
function ci(keyword) {
  const pattern = keyword.split('').map(ch => {
    if (/[A-Za-z]/.test(ch)) {
      return `[${ch.toLowerCase()}${ch.toUpperCase()}]`;
    }
    // Escape any regex special characters (though keywords are alphabetic in VB)
    return ch.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
  }).join('');
  return new RegExp(pattern);
}

