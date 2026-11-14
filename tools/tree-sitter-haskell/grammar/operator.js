const {
  parens,
  qualified,
} = require('./util.js')

module.exports = {

  // ------------------------------------------------------------------------
  // var
  // ------------------------------------------------------------------------

  _operator_qual_dot_head: $ => seq($._cond_qual_dot, $._varsym),

  _operator_hash_head: _ => seq(
    choice('#', token.immediate('#')),
    optional(choice(token.immediate('#'), token.immediate('|'))),
  ),

  operator: $ => choice(
    seq(optional($._cond_prefix_dot), $._varsym),
    $._operator_hash_head,
    '*',
  ),

  _operator_alias: $ => $.operator,

  _operator_minus: $ => alias('-', $.operator),

  _varsym_prefix: $ => parens(
    $,
    choice(
      $.operator,
      $._operator_minus,
      alias($._operator_qual_dot_head, $.operator),
    ),
  ),

  _pvarsym: $ => alias($._varsym_prefix, $.prefix_id),

  _qualified_varsym: $ => qualified($, choice($.operator, $._operator_minus)),
  _qvarsym: $ => alias($._qualified_varsym, $.qualified),

  _qvarsym_prefix: $ => parens($, $._qvarsym),
  _pqvarsym: $ => alias($._qvarsym_prefix, $.prefix_id),

  // ------------------------------------------------------------------------
  // con
  // ------------------------------------------------------------------------

  constructor_operator: $ => $._consym,

  _constructor_operator_alias: $ => $.constructor_operator,

  _consym_prefix: $ => parens($, $.constructor_operator),
  _pconsym: $ => alias($._consym_prefix, $.prefix_id),

  _qualified_consym: $ => qualified($, $.constructor_operator),
  _qconsym: $ => alias($._qualified_consym, $.qualified),

  _qconsym_prefix: $ => parens($, $._qconsym),
  _pqconsym: $ => alias($._qconsym_prefix, $.prefix_id),

  // ------------------------------------------------------------------------
  // op
  // ------------------------------------------------------------------------

  _sym: $ => choice($._operator_alias, $._constructor_operator_alias),

  _sym_prefix: $ => choice($._pvarsym, $._pconsym),

  _qsym: $ => choice($._qvarsym, $._qconsym),

  _pqsym: $ => choice($._pqvarsym, $._pqconsym),

}
