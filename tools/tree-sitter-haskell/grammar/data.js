const {
  sep1,
  sep,
  braces,
  layout,
  unboxed_sum_single,
  qualified,
  context,
  forall,
} = require('./util.js')

module.exports = {

  // ------------------------------------------------------------------------
  // fields
  // ------------------------------------------------------------------------

  field_name: $ => $.variable,
  _qfield_name: $ => qualified($, $.field_name),
  _field_names: $ => choice($.field_name, alias($._qfield_name, $.qualified)),

  field_path: $ => seq(
    field('field', $._field_names),
    repeat1(seq($._tight_dot, field('subfield', $.field_name))),
  ),

  _field_spec: $ => choice(
    $._field_names,
    $.field_path,
  ),

  field: $ => prec('annotated', seq(
    sep1(',', field('name', $.field_name)),
    $._colon2,
    field('type', $._parameter_type),
  )),

  _record_fields: $ => braces($, sep(',', field('field', $.field)), optional(',')),

  // ------------------------------------------------------------------------
  // deriving
  // ------------------------------------------------------------------------

  via: $ => seq('via', field('type', $.quantified_type)),

  deriving_strategy: _ => choice('stock', 'newtype', 'anyclass'),

  deriving: $ => seq(
    optional($._phantom_deriving),
    'deriving',
    optional(field('strategy', $.deriving_strategy)),
    field('classes', $.constraint),
    optional(field('via', $.via)),
  ),

  // ------------------------------------------------------------------------
  // gadt
  // ------------------------------------------------------------------------

  _gadt_con_prefix: $ => field('type', $.quantified_type),

  _gadt_con_record: $ => seq(
    field('fields', alias($._record_fields, $.fields)),
    field('arrow', $._fun_arrow),
    field('type', $.quantified_type),
  ),

  /**
   * GADT constructors only allow single foralls and contexts
   */
  gadt_constructor: $ => seq(
    choice(
      field('name', $._con),
      field('names', alias($._con_binding_list, $.binding_list)),
    ),
    $._colon2,
    forall($),
    context($),
    field('type', choice(
      alias($._gadt_con_prefix, $.prefix),
      alias($._gadt_con_record, $.record),
    )),
  ),

  gadt_constructors: $ => layout($, field('constructor', $.gadt_constructor)),

  _gadt: $ => seq(
    optional($._kind_annotation),
    $._where,
    optional(field('constructors', $.gadt_constructors)),
  ),

  // ------------------------------------------------------------------------
  // data type
  // ------------------------------------------------------------------------

  _field_type: $ => choice($.strict_field, $.lazy_field, $.type),

  _datacon_prefix: $ => seq(
    field('name', $._con),
    repeat(prec('patterns', field('field', $._field_type))),
  ),

  _datacon_infix: $ => prec('infix', seq(
    $._cond_data_infix,
    field('left_operand', $._field_type),
    field('operator', $._conop),
    field('right_operand', $._field_type),
  )),

  _datacon_record: $ => seq(
    field('name', $._constructor),
    field('fields', alias($._record_fields, $.fields)),
  ),

  _datacon_unboxed_sum: $ => unboxed_sum_single($, $.quantified_type),

  /**
   * Special constructors occurring in GHC code
   */
  _datacon_special: $ => choice(
    $.unit,
    $.unboxed_unit,
    alias($._plist, $.empty_list),
    alias($._type_tuple, $.tuple),
    alias($._type_unboxed_tuple, $.unboxed_tuple),
    alias($._datacon_unboxed_sum, $.unboxed_sum),
  ),

  /**
   * data constructors only allow single foralls and contexts
   */
  data_constructor: $ => seq(
    forall($),
    context($),
    field('constructor', choice(
      alias($._datacon_prefix, $.prefix),
      alias($._datacon_infix, $.infix),
      alias($._datacon_record, $.record),
      alias($._datacon_special, $.special),
    )),
  ),

  data_constructors: $ => sep1($._bar, field('constructor', $.data_constructor)),

  _data_rhs: $ => choice(
    $._kind_annotation,
    seq('=', field('constructors', $.data_constructors)),
    $._gadt,
  ),

  _data: $ => seq(
    context($),
    $._type_head,
    optional($._data_rhs),
    repeat(field('deriving', $.deriving)),
  ),

  data_type: $ => seq(
    optional('type'),
    'data',
    $._data,
  ),

  // ------------------------------------------------------------------------
  // newtype
  // ------------------------------------------------------------------------

  _newtype_con_field: $ =>  $.type,

  newtype_constructor: $ => seq(
    field('name', $._con),
    field('field', choice(
      alias($._newtype_con_field, $.field),
      alias($._record_fields, $.record),
    )),
  ),

  _newtype: $ => seq(
    choice(
      seq('=', field('constructor', $.newtype_constructor)),
      $._gadt,
    ),
    repeat(field('deriving', $.deriving)),
  ),

  newtype: $ => seq(
    'newtype',
    context($),
    $._type_head,
    $._newtype,
  ),

  // ------------------------------------------------------------------------
  // data family
  // ------------------------------------------------------------------------

  _datafam: $ => seq(
    $._type_head,
    optional($._kind_annotation),
  ),

  data_family: $ => seq(
    'data',
    'family',
    $._datafam,
  ),

  _inst_adt: $ => seq(
    forall($),
    context($),
    $._type_instance_head,
    optional($._data_rhs),
    repeat(field('deriving', $.deriving)),
  ),

  decl_inst_adt: $ => seq(
    'data',
    'instance',
    $._inst_adt,
  ),

  _inst_newtype: $ => seq(
    forall($),
    context($),
    $._type_instance_head,
    $._newtype,
  ),

  decl_inst_newtype: $ => seq(
    'newtype',
    'instance',
    $._inst_newtype,
  ),

  data_instance: $ => choice(
    alias($.decl_inst_adt, $.data_type),
    alias($.decl_inst_newtype, $.newtype),
  ),

}
