const {
  sep1,
  layout_sort,
} = require('./util.js')

module.exports = {

  // ------------------------------------------------------------------------
  // guard
  // ------------------------------------------------------------------------

  generator: $ => seq(
    field('pattern', $._pat),
    field('arrow', $._larrow),
    field('expression', $._exp),
  ),

  _let_binds: $ => layout_sort($, $._cmd_layout_start_let, field('decl', $.decl)),

  _let: $ => seq('let', optional(field('binds', alias($._let_binds, $.local_binds)))),

  let: $ => $._let,

  /**
   * This is a supertype.
   */
  guard: $ => choice(
    // Cannot be named `pattern` because name clash.
    alias($.generator, $.pattern_guard),
    $.let,
    alias($._exp, $.boolean),
  ),

  guards: $ => sep1(',', field('guard', $.guard)),

  _guards: $ => seq(
    $._bar,
    $._cmd_texp_start,
    field('guards', $.guards),
  ),

  // ------------------------------------------------------------------------
  // rules shared by expression, pattern, type
  // ------------------------------------------------------------------------

  _universal: $ => choice(
    $.splice,
    $.quasiquote,
    $.literal,
    $._unit_cons,
    $._tuple_cons,
  ),

}
