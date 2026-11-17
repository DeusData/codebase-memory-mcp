const {
  parens,
  sep2,
} = require('./util.js')

module.exports = {

  // ------------------------------------------------------------------------
  // context
  // ------------------------------------------------------------------------

  _class_apply: $ => prec.left('apply', seq(
    field('constructor', $.constraint),
    field('argument', $._type_apply_arg),
  )),

  _class_infix: $ => prec.right('infix', seq(
    $._cond_infix,
    field('left_operand', $.type),
    field('operator', $._tyops),
    field('right_operand', $.type),
  )),

  _ctr_parens: $ => parens($, $.constraints),

  _ctr_tuple: $ => parens($, sep2(',', $.constraints)),

  /**
   * Implicit parameters have an annotation with `::` but bind tighter than `_type_signature`, with the same precedence
   * as foralls, contexts and arrows.
   *
   * > A => ?a :: A   | associates as |   A => (?a :: A)
   * > ?a :: A -> A   | associates as |   ?a :: (A -> A)
   */
  implicit_parameter: $ => prec.left(seq(field('name', $.implicit_variable), $._type_annotation)),

  constraint: $ => choice(
    $._type_name,
    alias($._class_infix, $.infix),
    alias($._class_apply, $.apply),
    alias($._ctr_parens, $.parens),
    alias($._ctr_tuple, $.tuple),
    alias($._type_wildcard, $.wildcard),
    $._universal,
  ),

  _ctr_forall: $ => prec('fun', seq($._forall_body, '.', field('constraint', $.constraints))),

  _ctr_context: $ => prec('fun', seq($._context_inline, field('constraint', $.constraints))),

  _ctr_signature: $ => prec('annotated', seq(field('constraint', $.constraints), $._kind_annotation)),

  constraints: $ => choice(
    $.constraint,
    alias($._ctr_context, $.context),
    alias($._ctr_forall, $.forall),
    $.implicit_parameter,
    alias($._ctr_signature, $.signature),
  ),

  _context_inline: $ => seq(
    $._cond_context,
    field('context', $.constraint),
    field('arrow', $._carrow),
  ),

  context: $ => prec('qtype-single', $._context_inline),

}
