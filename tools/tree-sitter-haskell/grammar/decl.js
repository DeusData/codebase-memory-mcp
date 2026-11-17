const {
  sep1,
  sep2,
  parens,
  layout,
} = require('./util.js')

module.exports = {

  // ------------------------------------------------------------------------
  // fixity
  // ------------------------------------------------------------------------

  _fun_arrow_prec: _ => seq('-', '1'),

  // GHC.Types special decl
  _fun_arrow_fixity: $ => seq(
    field('associativity', 'infixr'),
    field('precedence', alias($._fun_arrow_prec, $.integer)),
    field('operator', alias('->', $.operator)),
  ),

  fixity: $ => choice(
    $._fun_arrow_fixity,
    seq(
      field('associativity', choice('infixl', 'infixr', 'infix')),
      field('precedence', optional($.integer)),
      field('operator', sep1(',', choice($._operator_minus, $._varop, $._conop))),
    ),
  ),

  // ------------------------------------------------------------------------
  // signature
  // ------------------------------------------------------------------------

  _con_binding_list: $ => sep2(',', field('name', $._con)),

  _var_binding_list: $ => sep2(',', field('name', $._var)),

  signature: $ => seq(
    choice(
      field('name', $._var),
      field('names', alias($._var_binding_list, $.binding_list)),
    ),
    $._type_annotation,
  ),

  // ------------------------------------------------------------------------
  // function and pattern bind
  // ------------------------------------------------------------------------

  _simple_bind_match: $ => seq('=', field('expression', $._exp)),

  _bind_match: $ => seq(
    $._guards,
    '=',
    $._cmd_texp_end,
    field('expression', $._exp),
  ),

  _bind_matches: $ => seq(
    choice(
      field('match', alias($._simple_bind_match, $.match)),
      repeat1(field('match', alias($._bind_match, $.match))),
    ),
    optional($._where_binds),
  ),

  _function_name: $ => field('name', $._var),

  function_head_parens: $ => parens(
    $,
    choice(
      $._function_head,
      $._function_head_patterns,
    ),
  ),

  _function_head_patterns: $ => choice(
    $._function_name,
    field('parens', $.function_head_parens),
  ),

  /**
   * The difference between a `function` with an `infix` head and a `bind` with `pat_infix` is that the former is for
   * _declaring_ a `varop` and the latter uses a `conop` to pattern match on the rhs expression.
   * The former may not have a type annotation, while the latter may.
   *
   * > a <> b = undefined
   * > h : t :: [Int] = undefined
   */
  _function_head_infix: $ => seq(
    field('left_operand', $.pattern),
    optional($._cond_no_section_op),
    field('operator', choice(seq($._cond_minus, $._operator_minus), $._varop)),
    field('right_operand', $.pattern),
  ),

  _function_head: $ => choice(
    seq($._function_head_patterns, field('patterns', $.patterns)),
    alias($._function_head_infix, $.infix),
  ),

  function: $ => seq(
    $._function_head,
    $._bind_matches,
  ),

  /**
   * The `implicit_variable` here is for:
   * g = let ?par = Impy 5 in f
   */
  bind: $ => prec('bind', seq(
    choice(
      field('pattern', $._pat),
      field('name', $._var),
      field('implicit', $.implicit_variable),
    ),
    $._bind_matches,
  )),

  /**
   * This is a supertype.
   */
  decl: $ => choice(
    $.signature,
    $.function,
    $.bind,
  ),

  _local_decl: $ => choice(
    $.fixity,
    $.decl,
  ),

  local_binds: $ => layout($, field('decl', $._local_decl)),

  _where_binds: $ => seq($._where, optional(field('binds', $.local_binds))),

  // ------------------------------------------------------------------------
  // foreign
  // ------------------------------------------------------------------------

  calling_convention: _ => token(choice(
    'ccall',
    'stdcall',
    'capi',
    'prim',
    'javascript',
    /[A-Z_]+/, // It's common in GHC to use a cpp #define for this
  )),

  safety: _ => token(choice(
    'unsafe',
    'safe',
    'interruptible',
  )),

  entity: $ => $.string,

  foreign_import: $ => seq(
    'foreign',
    'import',
    field('calling_convention', $.calling_convention),
    optional(field('safety', $.safety)),
    optional(field('entity', $.entity)),
    field('signature', $.signature),
  ),

  foreign_export: $ => seq(
    'foreign',
    'export',
    field('calling_convention', $.calling_convention),
    optional(field('entity', $.entity)),
    field('signature', $.signature),
  ),

  // ------------------------------------------------------------------------
  // default
  // ------------------------------------------------------------------------

  default_types: $ => seq('default', parens($, optional(sep1(',', field('type', $._ktype))))),

}
