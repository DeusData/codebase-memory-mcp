const {
  sep1,
  layout,
  context,
  forall,
} = require('./util.js')

module.exports = {

  // ------------------------------------------------------------------------
  // associated families
  // ------------------------------------------------------------------------

  /**
   * In associated family declarations, result type aliasing without injectivity is invalid, since that syntax is taken
   * by type instance declarations.
   */
  _assoc_tyfam: $ => seq(
    'type',
    optional('family'),
    $._type_head,
    optional(choice(
      $._kind_annotation,
      seq(
        $.type_family_result,
        $.type_family_injectivity,
      ),
    )),
  ),

  _assoc_tyinst: $ => seq(
    'type',
    optional('instance'),
    forall($),
    $._cond_assoc_tyinst,
    $._type_instance_common,
  ),

  _assoc_datafam: $ => seq(
    'data',
    optional('family'),
    $._datafam,
  ),

  _assoc_datainst_adt: $ => seq(
    'data',
    optional('instance'),
    $._inst_adt,
  ),

  _assoc_datainst_newtype: $ => seq(
    'newtype',
    optional('instance'),
    $._inst_newtype,
  ),

  _assoc_datainst: $ => choice(
    alias($._assoc_datainst_adt, $.data_type),
    alias($._assoc_datainst_newtype, $.newtype),
  ),

  // ------------------------------------------------------------------------
  // class
  // ------------------------------------------------------------------------

  default_signature: $ => seq('default', field('signature', $.signature)),

  /**
   * Classes can have both type families and instances, but only data families.
   */
  class_decl: $ => choice(
    $._local_decl,
    $.default_signature,
    alias($._assoc_tyfam, $.type_family),
    alias($._assoc_tyinst, $.type_instance),
    alias($._assoc_datafam, $.data_family),
  ),

  fundep: $ => seq(
    field('matched', repeat1($.variable)),
    $._arrow,
    field('determined', repeat1($.variable)),
  ),

  fundeps: $ => seq($._bar, sep1(',', field('fundep', $.fundep))),

  class_declarations: $ => layout($, field('declaration', $.class_decl)),

  class: $ => seq(
    'class',
    context($),
    $._type_head,
    field('fundeps', optional($.fundeps)),
    optional(seq($._where, optional(field('declarations', $.class_declarations)))),
  ),

  // ------------------------------------------------------------------------
  // instance
  // ------------------------------------------------------------------------

  instance_decl: $ => choice(
    $.decl,
    alias($._assoc_datainst, $.data_instance),
    alias($._assoc_tyinst, $.type_instance),
  ),

  instance_declarations: $ => layout($, field('declaration', $.instance_decl)),

  /**
   * instances only allow single foralls and contexts
   */
  _instance: $ => seq(
    'instance',
    forall($),
    context($),
    $._type_instance_head,
  ),

  instance: $ => seq(
    $._instance,
    optional(seq($._where, optional(field('declarations', $.instance_declarations)))),
  ),

  deriving_instance: $ => seq(
    optional($._phantom_deriving),
    'deriving',
    optional(choice(field('strategy', $.deriving_strategy), field('via', $.via))),
    $._instance,
  ),
}
