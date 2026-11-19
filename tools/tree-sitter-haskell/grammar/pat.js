const {
  sep1,
  sep2,
  sep,
  parens,
  braces,
  brackets,
  unboxed_tuple_full,
  unboxed_sum_single,
} = require('./util.js')

module.exports = {

  // ------------------------------------------------------------------------
  // tuples and parens
  // ------------------------------------------------------------------------

  _pat_parens: $ => parens($, field('pattern', $._pat_texp)),

  _pat_tuple_elems: $ => sep2(',', field('element', $._pat_texp)),

  _pat_tuple: $ => parens($, $._pat_tuple_elems),

  _pat_unboxed_tuple: $ => unboxed_tuple_full($, $._pat_texp),

  _pat_unboxed_sum: $ => unboxed_sum_single($, $._pat_texp),

  _pat_list: $ => brackets($, sep1(',', field('element', $._pat_texp)), optional(',')),

  // ------------------------------------------------------------------------
  // record
  // ------------------------------------------------------------------------

  field_pattern: $ => choice(
    alias('..', $.wildcard),
    seq(field('field', $._field_names), optional(seq('=', field('pattern', $._pat_texp)))),
  ),

  _pat_record: $ => prec('record', seq(
    field('constructor', $.pattern),
    braces($, sep(',', field('field', $.field_pattern))),
  )),

  // ------------------------------------------------------------------------
  // misc
  // ------------------------------------------------------------------------

  /**
   * This dynamic precedence penalty is relevant for the conflict between `function` and `bind`.
   * Consider:
   *
   * > f (A a) = exp
   *
   * Because of the "single choice disambiguated with named precedences" approach used for `pattern`, the left node in
   * `pat_apply` can be a variable, even though it's not valid Haskell.
   * While the static prec 'pat-name' covers this at generation time, Haskell's ambiguity requires us to use a runtime
   * conflict for `function`/`bind`, where static prec is ineffective.
   * Giving the reduction of `_var` to `pattern` a strong negative dynamic prec ensures that the runtime branch for
   * `bind` has lower precedence because of `f`, so `function` always wins.
   *
   * While `bind` usually has a lower score than `function` anyway in this situation because it is slightly more
   * complex, there are never any guarantees for runtime conflicts.
   * In particular, the presence of minor parse errors later in the declaration can tip the scales randomly.
   */
  _pat_name: $ => choice(
    prec('pat-name', prec.dynamic(-1000, $._var)),
    $._cons,
  ),

  _pat_as: $ => prec('prefix', seq(field('bind', $.variable), $._tight_at, field('pattern', $.pattern))),

  _pat_wildcard: _ => '_',

  _pat_strict: $ => prec('prefix', seq($._any_prefix_bang, field('pattern', $.pattern))),

  _pat_irrefutable: $ => prec('prefix', seq($._any_prefix_tilde, field('pattern', $.pattern))),

  // ------------------------------------------------------------------------
  // application
  // ------------------------------------------------------------------------

  _pat_apply_arg: $ => choice(
    $.pattern,
    alias($._at_type, $.type_binder),
    $.explicit_type,
  ),

  _pat_apply: $ => prec.left('apply', seq(
    field('function', $.pattern),
    field('argument', $._pat_apply_arg),
  )),

  // ------------------------------------------------------------------------
  // operators
  // ------------------------------------------------------------------------

  _pat_negation: $ => seq('-', field('number', $._number)),

  _pat_infix: $ => prec.right('infix', seq(
    field('left_operand', $.pattern),
    optional($._cond_no_section_op),
    field('operator', choice(
      $.constructor_operator,
      $._conids_ticked,
      seq($._cond_qualified_op, $._qconsym),
    )),
    field('right_operand', $.pattern),
  )),

  // ------------------------------------------------------------------------
  // top level
  // ------------------------------------------------------------------------

  pattern: $ => choice(
    alias($._pat_infix, $.infix),
    alias($._pat_negation, $.negation),
    alias($._pat_apply, $.apply),
    $._pat_name,
    alias($._pat_as, $.as),
    alias($._pat_record, $.record),
    alias($._pat_wildcard, $.wildcard),
    alias($._pat_parens, $.parens),
    alias($._pat_tuple, $.tuple),
    alias($._pat_unboxed_tuple, $.unboxed_tuple),
    alias($._pat_unboxed_sum, $.unboxed_sum),
    alias($._pat_list, $.list),
    alias($._plist, $.list),
    alias($._pat_strict, $.strict),
    alias($._pat_irrefutable, $.irrefutable),
    $._universal,
  ),

  patterns: $ => repeat1(prec('patterns', $._pat_apply_arg)),

  _pat_signature: $ => prec.right('annotated', seq(
    field('pattern', $.pattern),
    $._type_annotation,
  )),

  _pat: $ => choice(
    alias($._pat_signature, $.signature),
    prec.right($.pattern),
  ),

  view_pattern: $ => seq(field('expression', $._exp), $._arrow, field('pattern', $._pat_texp)),

  _pat_texp: $ => choice($.view_pattern, $._pat),

}
