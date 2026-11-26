const {
  parens,
  ticked,
  promoted,
  qualified,
} = require('./util.js')

module.exports = {
  // ------------------------------------------------------------------------
  // var
  // ------------------------------------------------------------------------

  _qualified_variable: $ => qualified($, $.variable),
  _qvarid: $ => alias($._qualified_variable, $.qualified),
  _varids: $ => choice($._qvarid, $.variable),

  _var: $ => choice($.variable, $._pvarsym),
  _qvar: $ => choice($._qvarid, $._pqvarsym),
  _vars: $ => choice($._var, $._qvar),

  _variable_ticked: $ => ticked($.variable),

  _varop: $ => choice($.operator, alias($._variable_ticked, $.infix_id)),

  _qvariable_ticked: $ => ticked($._qvarid),

  _varids_ticked: $ => alias(
    choice(
      $._variable_ticked,
      $._qvariable_ticked,
    ),
    $.infix_id,
  ),

  // ------------------------------------------------------------------------
  // con
  // ------------------------------------------------------------------------

  _constructor: $ => alias($.name, $.constructor),
  _qualified_constructor: $ => qualified($, $._constructor),
  _qconid: $ => alias($._qualified_constructor, $.qualified),
  _conids: $ => choice($._qconid, $._constructor),

  _con: $ => choice($._constructor, $._pconsym),
  _qcon: $ => choice($._qconid, $._pqconsym),
  _cons: $ => choice(prec('con', $._con), $._qcon),

  _constructor_ticked: $ => ticked($._constructor),
  _conop: $ => choice($._constructor_operator_alias, alias($._constructor_ticked, $.infix_id)),

  _qconstructor_ticked: $ => ticked($._qconid),

  _conids_ticked: $ => alias(
    choice(
      $._constructor_ticked,
      $._qconstructor_ticked,
    ),
    $.infix_id,
  ),

  // ------------------------------------------------------------------------
  // tycon
  // ------------------------------------------------------------------------

  _tyconid: $ => $.name,
  _qualified_type: $ => qualified($, $._tyconid),
  _qtyconid: $ => alias($._qualified_type, $.qualified),
  _tyconids: $ => choice($._qtyconid, $._tyconid),

  _tycon_arrow: $ => parens($, alias($._arrow, $.operator)),
  _qualified_arrow: $ => qualified($, alias($._arrow, $.operator)),
  _qtycon_arrow: $ => parens($, alias($._qualified_arrow, $.qualified)),

  _tycon: $ => choice($._tyconid, $._pvarsym, alias($._tycon_arrow, $.prefix_id), $._pconsym),
  _qtycon: $ => choice($._qtyconid, alias($._qtycon_arrow, $.prefix_id), $._pqsym),
  _tycons: $ => choice($._tycon, $._qtycon),

  _promoted_tycons_alias: $ => seq('\'', $._cons),

  _promoted_tycons: $ => alias($._promoted_tycons_alias, $.promoted),

  _tycon_ticked: $ => ticked($._tyconid),
  _qtycon_ticked: $ => ticked($._qtyconid),

  _tyconids_ticked: $ => alias(
    choice(
      $._tycon_ticked,
      $._qtycon_ticked,
    ),
    $.infix_id,
  ),

  _tyconops: $ => choice(
    $._sym,
    $._qsym,
    $._operator_minus,
    $._tyconids_ticked,
  ),

  /**
   * Lenient parsing: `varsym` is not legal (like `'++`).
   */
  _promoted_tyconops_alias: $ => promoted($._tyconops),

  _promoted_tyconops: $ => alias($._promoted_tyconops_alias, $.promoted),

  _tyops: $ => choice(
    $._tyconops,
    $._promoted_tyconops,
  ),

  // ------------------------------------------------------------------------
  // op
  // ------------------------------------------------------------------------

  _op_ticked: $ => choice(
    $._varids_ticked,
    $._conids_ticked,
  ),

  _ops: $ => choice(
    $.operator,
    $._qvarsym,
    $.constructor_operator,
    $._qconsym,
    $._op_ticked,
  ),

  _name: $ => choice($._var, $._con),
  _qname: $ => choice($._vars, $._cons),

}
