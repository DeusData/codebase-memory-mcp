const {
  layout,
  optional_where,
} = require('./util.js')

module.exports = {
  // ------------------------------------------------------------------------
  // pattern synonym
  // ------------------------------------------------------------------------

  _patsyn_signature: $ => seq(
    field('synonym', choice($._con, alias($._con_binding_list, $.binding_list))),
    $._colon2,
    field('type', $.quantified_type),
  ),

  _patsyn_cons: $ => layout($, alias($.bind, $.constructor_synonym)),

  /**
   * This allows a `where` after any equation for parsing resilience, even though it's only valid for the arrow variant
   * (explicitly bidirectional patterns).
   * The `where` may also be empty.
   */
  _patsyn_equation: $ => seq(
    field('synonym', $.pattern),
    choice(
      '=',
      $._larrow,
    ),
    field('pattern', $._pat),
    optional_where($, field('constructors', alias($._patsyn_cons, $.constructor_synonyms))),
  ),

  pattern_synonym: $ => seq(
    'pattern',
    choice(
      alias($._patsyn_signature, $.signature),
      alias($._patsyn_equation, $.equation),
    ),
  ),

}
