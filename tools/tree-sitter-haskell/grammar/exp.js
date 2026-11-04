const {
  sep1,
  sep,
  parens,
  braces,
  brackets,
  layout_sort,
  layout,
  unboxed_tuple_nonempty,
  unboxed_sum_single,
  qualified,
} = require('./util.js')

module.exports = {

  // ------------------------------------------------------------------------
  // names
  // ------------------------------------------------------------------------

  _exp_name: $ => choice(
    $._cons,
    $._vars,
    $.variable,
    $.implicit_variable,
    $.label,
  ),

  _exp_th_quoted_name: $ => choice(
    seq('\'', field('name', choice($._vars, $._cons))),
    prec('prefix', seq('\'\'', field('type', $.type))),
  ),

  // ------------------------------------------------------------------------
  // tuples and parens
  // ------------------------------------------------------------------------

  _exp_parens: $ => parens($, field('expression', $._exp)),

  // Having this separate reduces size by ~15kB
  _exp_tuple_elems: $ => seq(
    choice(
      seq(repeat1(','), field('element', $._exp)),
      seq(field('element', $._exp), ',', optional(field('element', $._exp))),
    ),
    repeat(seq(',', optional(field('element', $._exp))))
  ),

  /**
    * This needs to be disambiguated from `prefix_tuple`, which is a constructor with _only_ commas.
    * Tuple sections aren't allowed in patterns.
    *
    * Since tuple expressions can contain singular expressions in sections like `(a,)` and `(,a)`, it has to be ensured
    * that there is _at least_ each one comma and one expression in there, but the comma may be on either side and be
    * preceded by any number of further commas, like `(,,,a)`.
    *
    * The final `repeat` is simpler, it just has to ensure that no two `_exp`s can be successive, but this encoding
    * means that the optional `_exp` after `(5,)` needs to be included in the `choice`, otherwise a simple pair
    * would be impossible.
    */
  _exp_tuple: $ => parens($, $._exp_tuple_elems),

  /**
   * Unlike their boxed variants, unboxed tuples may be nullary and unary, making it simpler to parse them.
   */
  _exp_unboxed_tuple: $ => unboxed_tuple_nonempty($, $._exp),

  /**
   * Unboxed sums must have at least one separating `|`, otherwise the expression would be a unary or nullary tuple.
   * This is a lenient parser that allows multiple variants to be filled in, for simplicity.
   */
  _exp_unboxed_sum: $ => unboxed_sum_single($, $._exp),

  // ------------------------------------------------------------------------
  // lists
  // ------------------------------------------------------------------------

  _exp_list: $ => brackets($, sep1(',', field('element', $._exp)), optional(',')),

  /**
    * An expression like `[1,2..20]`.
    */
  _exp_arithmetic_sequence: $ => brackets(
    $,
    field('from', $._exp),
    optional(seq(',', field('step', $._exp))),
    $._dotdot,
    optional(field('to', $._exp)),
  ),

  /**
   * `TransformListComp`, group style.
   *
   * Inlining `_exp` here is necessary to avoid a conflict.
   */
  group: $ => seq(
    'then',
    'group',
    optional(seq('by', field('key', choice(alias($._exp_signature, $.signature), $.expression)))),
    'using',
    field('classifier', $._exp),
  ),

  /**
   * `TransformListComp`, simple transformation style.
   */
  transform: $ => seq(
    'then',
    field('transformation', $._exp),
    optional(seq('by', field('key', $._exp))),
  ),

  qualifier: $ => choice(
    $.generator,
    $.let,
    $.transform,
    $.group,
    alias($._exp, $.boolean),
  ),

  /**
   * This is a supertype.
   */
  qualifiers: $ => seq(sep1(',', field('qualifier', $.qualifier))),

  _exp_list_comprehension: $ => brackets(
    $,
    field('expression', $._exp),
    repeat1(seq('|', field('qualifiers', $.qualifiers))),
  ),

  // ------------------------------------------------------------------------
  // greedy block args
  // ------------------------------------------------------------------------

  _exp_lambda: $ => seq(
    '\\',
    field('patterns', $.patterns),
    $._arrow,
    field('expression', $._exp),
  ),

  _exp_let_in: $ => seq($._let, optional($._phantom_in), 'in', field('expression', $._exp)),

  _exp_conditional: $ => seq(
    'if',
    field('if', $._exp),
    repeat(';'),
    'then',
    field('then', $._exp),
    repeat(';'),
    'else',
    field('else', $._exp),
  ),

  /**
   * These block arguments don't end in a layout, so they all range over all following block arguments and will
   * therefore always be the last argument in an application or infix chain.
   * They also pull a trailing type annotation into their body.
   */
  _exp_greedy: $ => choice(
    alias($._exp_lambda, $.lambda),
    alias($._exp_let_in, $.let_in),
    alias($._exp_conditional, $.conditional),
  ),

  // ------------------------------------------------------------------------
  // do
  // ------------------------------------------------------------------------

  _exp_statement: $ => $._exp,

  /**
   * This is a supertype.
   */
  statement: $ => choice(
    alias($._exp_statement, $.exp),
    alias($.generator, $.bind),
    $.let,
    $.rec,
  ),

  _statements: $ => layout_sort($, $._cmd_layout_start_do, field('statement', $.statement)),

  rec: $ => seq('rec', $._statements),

  _do_keyword: _ => choice('mdo', 'do'),

  do_module: $ => field('qualified_do', qualified($, $._do_keyword)),

  _do: $ => choice(
    $.do_module,
    $._do_keyword
  ),

  _exp_do: $ => seq($._do, $._statements),

  // ------------------------------------------------------------------------
  // case
  // ------------------------------------------------------------------------

  match: $ => seq(
    $._guards,
    optional($._phantom_arrow),
    $._arrow,
    $._cmd_texp_end,
    field('expression', $._exp),
  ),

  _simple_match: $ => seq($._arrow, field('expression', $._exp)),

  _matches: $ => field('match', choice(
    alias($._simple_match, $.match),
    repeat1($.match),
  )),

  alternative: $ => seq(
    field('pattern', $._pat),
    $._matches,
    optional($._where_binds),
  ),

  _nalt: $ => seq(
    field('patterns', $.patterns),
    $._matches,
    optional($._where_binds),
  ),

  alternatives: $ => layout_sort($, $._cmd_layout_start_case, field('alternative', $.alternative)),
  _nalts: $ => layout_sort($, $._cmd_layout_start_case, field('alternative', alias($._nalt, $.alternative))),

  _exp_case: $ => seq('case', $._exp, 'of', optional(field('alternatives', $.alternatives))),

  _exp_lambda_case: $ => seq(
    '\\',
    'case',
    optional(field('alternatives', $.alternatives)),
  ),

  /**
   * alternatives are not optional in a `\cases` expression, but we're lenient.
   */
  _exp_lambda_cases: $ => seq(
    '\\',
    'cases',
    optional(field('alternatives', alias($._nalts, $.alternatives))),
  ),

  _exp_multi_way_if: $ => seq(
    'if',
    $._cmd_layout_start_if,
    repeat(field('match', $.match)),
    $._cond_layout_end,
  ),

  // ------------------------------------------------------------------------
  // record
  // ------------------------------------------------------------------------

  field_update: $ => choice(
    alias('..', $.wildcard),
    seq(
      field('field', $._field_spec),
      optional(seq('=', field('expression', $._exp)))
    ),
  ),

  _exp_record: $ => prec('record', seq(
    field('expression', $.expression),
    braces($, sep(',', field('field', $.field_update)))),
  ),

  _exp_projection_selector: $ => parens(
    $,
    $._any_prefix_dot,
    field('field', $.variable),
    repeat(seq($._tight_dot, field('field', $.variable))),
  ),

  /**
   * A dot-syntax field projection like `var.name.othername`.
   */
  _exp_projection: $ => seq(
    prec('projection', seq(
      field('expression', $.expression),
      $._tight_dot,
    )),
    field('field', $.field_name),
  ),

  // ------------------------------------------------------------------------
  // application
  // ------------------------------------------------------------------------

  explicit_type: $ => parens($, 'type', field('type', $.type)),

  _exp_apply: $ => prec.left('apply', seq(
    field('function', $.expression),
    field('argument', choice(
      $.expression,
      alias($._at_type, $.type_application),
      $.explicit_type,
    )),
  )),

  // ------------------------------------------------------------------------
  // operators
  // ------------------------------------------------------------------------

  _exp_op: $ => choice(
    $._sym,
    $._op_ticked,
    alias($._prefix_dot, $.operator),
  ),

  _exp_section_left: $ => parens(
    $,
    field('left_operand', $.expression),
    $._cond_left_section_op,
    field('operator', choice(
      $._exp_op,
      $._operator_minus,
      $._qsym,
    )),
  ),

  _exp_section_right: $ => parens(
    $,
    choice(
      alias($._operator_qual_dot_head, $.operator),
      $._ops,
    ),
    field('right_operand', $.expression),
  ),

  _exp_negation: $ => seq(
    field('minus', '-'),
    prec('negation', field('expression', $.expression)),
  ),

  /**
   * Infix expressions have severe conflicts with several structures:
   *
   * - Negation is supposed to bind less tight than application and tighter than infix, which requires an unsolvable
   *   precedence configuration
   * - Qualified operators cannot be identified with single-token lookahead, which causes ambiguity with function
   *   application
   * - Left operator sections require infix expressions in their operand to reduce before the section operator, but
   *   single-token lookahead also makes this decision impossible
   *
   * All of these are solved with external symbols.
   * Consult `grammar/externals.js` for more information.
   */
  _exp_infix: $ => prec.right('infix', seq(
    field('left_operand', $.expression),
    optional($._cond_no_section_op),
    field('operator', choice(
      seq($._cond_minus, $._operator_minus),
      $._exp_op,
      seq($._cond_qualified_op, $._qsym),
    )),
    field('right_operand', $.expression),
  )),

  // ------------------------------------------------------------------------
  // top level
  // ------------------------------------------------------------------------

  /**
   * This is a supertype.
   */
  expression: $ => choice(
    alias($._exp_infix, $.infix),
    alias($._exp_negation, $.negation),
    alias($._exp_apply, $.apply),
    alias($._exp_record, $.record),
    alias($._exp_projection, $.projection),
    alias($._exp_arithmetic_sequence, $.arithmetic_sequence),
    alias($._exp_list_comprehension, $.list_comprehension),
    alias($._exp_unboxed_tuple, $.unboxed_tuple),
    alias($._exp_unboxed_sum, $.unboxed_sum),
    alias($._exp_projection_selector, $.projection_selector),
    alias($._exp_quote, $.quote),
    alias($._exp_typed_quote, $.typed_quote),
    alias($._exp_th_quoted_name, $.th_quoted_name),
    alias($._exp_lambda_case, $.lambda_case),
    alias($._exp_lambda_cases, $.lambda_cases),
    alias($._exp_do, $.do),
    alias($._exp_parens, $.parens),
    alias($._exp_tuple, $.tuple),
    alias($._exp_list, $.list),
    // Not using a name like "empty list" here because it's only really special in types.
    alias($._plist, $.list),
    alias($._exp_section_left, $.left_section),
    alias($._exp_section_right, $.right_section),
    $._exp_greedy,
    alias($._exp_case, $.case),
    alias($._exp_multi_way_if, $.multi_way_if),
    $._exp_name,
    $._universal,
  ),

  _exp_signature: $ => prec.right('annotated', seq(
    field('expression', $.expression),
    $._type_annotation,
  )),

  _exp: $ => choice(
    alias($._exp_signature, $.signature),
    // Right-associative means that the reduction of `expression` to `_exp` loses against any shift.
    prec.right($.expression),
  ),

}
