const {
  parens,
  braces,
  brackets,
  prefix_at,
  sep1,
  sep2,
  unboxed_tuple_full,
  unboxed_sum_full,
  forall,
  layout,
  layout_single,
  optional_where,
} = require('./util.js')

module.exports = {

  // ------------------------------------------------------------------------
  // type parameters
  // ------------------------------------------------------------------------

  _inferred_tyvar: $ => braces($, $._ktype_param),

  _type_param_parens: $ => parens($, $._ktype_param),

  _type_param_wildcard: _ => '_',

  _type_param_annotated: $ => prec('annotated', seq($.type_param, $._kind_annotation)),

  _type_param_invisible: $ => prefix_at($, field('bind', $.type_param)),

  /**
   * This is a supertype.
   */
  type_param: $ => choice(
    alias($._type_param_wildcard, $.wildcard),
    alias($._type_param_invisible, $.invisible),
    alias($._type_param_parens, $.parens),
    field('bind', $.variable),
  ),

  _ktype_param: $ => choice(
    $.type_param,
    alias($._type_param_annotated, $.annotated),
  ),

  type_params: $ => repeat1(prec('patterns', $.type_param)),

  quantified_variables: $ => repeat1(choice($.type_param, alias($._inferred_tyvar, $.inferred))),

  // ------------------------------------------------------------------------
  // tuples and parens
  // ------------------------------------------------------------------------

  _type_parens: $ => parens($, field('type', $._ktype)),

  _type_tuple_elems: $ => sep2(',', field('element', $._ktype)),

  /**
   * Tuple types must either be saturated or empty, sections aren't legal.
   * We could be lenient here, but it seems useful to have a different node name for the prefix variant.
   */
  _type_tuple: $ => parens($, $._type_tuple_elems),

  _type_unboxed_tuple: $ => unboxed_tuple_full($, $._ktype),

  _type_unboxed_sum: $ => unboxed_sum_full($, $._ktype),

  _type_list: $ => brackets($, sep1(',', field('element', $._ktype))),

  // ------------------------------------------------------------------------
  // names etc
  // ------------------------------------------------------------------------

  _type_promoted: $ => seq(
    '\'',
    choice(
      alias($._plist, $.empty_list),
      alias($._type_tuple, $.tuple),
      alias($._type_list, $.list),
      $.prefix_tuple,
      $.unit,
    ),
  ),

  _type_name: $ => choice(
    $.variable,
    $._promoted_tycons,
    prec('type-name', $._tycons),
  ),

  _type_star: _ => choice('*', '★'),

  _type_wildcard: _ => '_',

  // ------------------------------------------------------------------------
  // application
  // ------------------------------------------------------------------------

  _at_type: $ => prefix_at($, field('type', $.type)),

  _type_apply_arg: $ => choice($.type, alias($._at_type, $.kind_application)),

  /**
   * Type application, as in `Either e (Int, Text)` or `TypeRep @Int`.
   */
  _type_apply: $ => prec.left('apply', seq(
    field('constructor', $.type),
    field('argument', $._type_apply_arg),
  )),

  // ------------------------------------------------------------------------
  // infix
  // ------------------------------------------------------------------------

  _type_infix: $ => prec.right('infix', seq(
    field('left_operand', $.type),
    field('operator', $._tyops),
    field('right_operand', $.type),
  )),

  // ------------------------------------------------------------------------
  // unquantified type
  // ------------------------------------------------------------------------

  /**
   * This is a supertype.
   */
  type: $ => choice(
    $._type_name,
    alias($._type_star, $.star),
    alias($._type_wildcard, $.wildcard),
    alias($._type_parens, $.parens),
    alias($._type_promoted, $.promoted),
    alias($._type_list, $.list),
    alias($._plist, $.prefix_list),
    alias($._type_unboxed_tuple, $.unboxed_tuple),
    alias($._type_unboxed_sum, $.unboxed_sum),
    alias($._type_tuple, $.tuple),
    alias($._type_infix, $.infix),
    alias($._type_apply, $.apply),
    $._universal,
  ),

  // ------------------------------------------------------------------------
  // forall
  // ------------------------------------------------------------------------

  _forall_keyword: _ => choice('forall', '∀'),

  _forall_body: $ => seq(
    field('quantifier', $._forall_keyword),
    optional(field('variables', $.quantified_variables)),
  ),

  forall: $ => prec('qtype-single', seq(
    $._forall_body,
    '.',
  )),

  forall_required: $ => prec('qtype-single', seq(
    $._forall_body,
    $._arrow,
  )),

  _forall: $ => choice(
    $.forall,
    $.forall_required,
  ),

  _qtype_forall: $ => prec.right('qtype-curried', seq(
    $._forall_body,
    '.',
    field('type', $.quantified_type),
  )),

  _qtype_forall_required: $ => prec.right('qtype-curried', seq(
    $._forall_body,
    $._arrow,
    field('type', $.quantified_type),
  )),

  // ------------------------------------------------------------------------
  // function
  // ------------------------------------------------------------------------

  _fun_arrow: $ => seq(
    optional($._phantom_arrow),
    field('arrow', $._arrow),
  ),

  modifier: $ => prec('prefix', seq($._prefix_percent, $.type)),

  _linear_fun_arrow: $ => choice(
    seq(
      field('multiplicity', $.modifier),
      $._fun_arrow,
    ),
    seq(
      optional($._phantom_arrow),
      field('arrow', $._linear_arrow),
    ),
  ),

  /**
   * These also allow tight infix because unpack pragmas can precede them without space.
   * Technically pragmas aren't considered for tight infix, but it's simpler to do it this way than to track that in the
   * scanner.
   */
  strict_field: $ => prec('prefix', seq($._any_prefix_bang, field('type', $.type))),
  lazy_field: $ => prec('prefix', seq($._any_prefix_tilde, field('type', $.type))),

  _parameter_type: $ => field('parameter', choice($.strict_field, $.lazy_field, $.quantified_type)),

  /**
   * We allow strict and lazy field types in function types so that GADTs don't need a separate rule tree.
   */
  _qtype_function: $ => prec.right(seq(
    $._parameter_type,
    $._fun_arrow,
    field('result', $.quantified_type),
  )),

  _qtype_linear_function: $ => prec.right(seq(
    $._parameter_type,
    $._linear_fun_arrow,
    field('result', $.quantified_type),
  )),

  // ------------------------------------------------------------------------
  // context
  // ------------------------------------------------------------------------

  _qtype_context: $ => prec.right('qtype-curried', seq(
    $._context_inline,
    field('type', $.quantified_type),
  )),

  // ------------------------------------------------------------------------
  // top level
  // ------------------------------------------------------------------------

  /**
   * This is a supertype.
   */
  quantified_type: $ => choice(
    alias($._qtype_function, $.function),
    alias($._qtype_linear_function, $.linear_function),
    alias($._qtype_forall, $.forall),
    alias($._qtype_forall_required, $.forall_required),
    alias($._qtype_context, $.context),
    $.implicit_parameter,
    prec.right($.type),
  ),

  _type_annotation: $ => seq(
    $._colon2,
    field('type', $.quantified_type),
  ),

  _kind_annotation: $ => seq(
    $._colon2,
    field('kind', $.quantified_type),
  ),

  _type_signature: $ => prec.right('annotated', seq(
    field('type', $.quantified_type),
    $._kind_annotation,
  )),

  _ktype: $ => choice(
    alias($._type_signature, $.signature),
    $.quantified_type,
  ),

  // ------------------------------------------------------------------------
  // type head
  // ------------------------------------------------------------------------

  _type_head_name: $ => field('name', choice(
    $._tycon,
    $.unit,
    alias($._plist, $.prefix_list),
  )),

  _type_head_parens: $ => parens(
    $,
    choice(
      $._type_head,
      $._type_head_params,
    ),
  ),

  _type_head_params: $ => choice(
    $._type_head_name,
    alias($._type_head_parens, $.parens),
  ),

  _type_head_infix: $ => prec('infix', seq(
    field('left_operand', $.type_param),
    field('operator', $._tyconops),
    field('right_operand', $.type_param),
  )),

  /**
  * A type head introduces the name and parameters in the declaration of a data type/family, type synonym/family, or
  * class.
  *
  * It can be in prefix or infix form:
  *
  * > A a b
  * > a +++ b
  *
  * Parameters can be visible or invisible, the latter marked by a prefix `@`.
  * They can be plain or parenthesized variable names, the latter with an optional kind signature.
  * They can be wildcards.
  *
  * Examples: `a`, `@a`, `(a :: Type)`, `@(_ :: Type -> Type)`
  *
  * The rules are slightly relaxed compared to GHC.
  */
  _type_head: $ => choice(
    seq($._type_head_params, optional(field('patterns', $.type_params))),
    alias($._type_head_infix, $.infix),
  ),

  // ------------------------------------------------------------------------
  // type instance head
  // ------------------------------------------------------------------------

  _type_instance_head_parens: $ => parens(
    $,
    choice(
      $._type_instance_head,
      $._type_instance_head_params,
    ),
    optional($._kind_annotation),
  ),

  _type_instance_head_params: $ => choice(
    field('name', $._tycons),
    alias($._type_instance_head_parens, $.parens),
  ),

  type_patterns: $ => repeat1(prec('patterns', $._type_apply_arg)),

  /**
   * The equivalent of a type head, for type instances, which can contain full type patterns rather than just variable
   * binders.
   */
  _type_instance_head: $ => choice(
    seq($._type_instance_head_params, optional(field('patterns', $.type_patterns))),
    seq($._cond_infix, alias($._type_infix, $.infix)),
  ),

  // ------------------------------------------------------------------------
  // type decl
  // ------------------------------------------------------------------------

  type_synonym: $ => seq(
    'type',
    $._type_head,
    '=',
    field('type', $._ktype),
  ),

  kind_signature: $ => seq(
    'type',
    $._type_head,
    $._kind_annotation,
  ),

  // ------------------------------------------------------------------------
  // type instance
  // ------------------------------------------------------------------------

  _type_instance_common: $ => seq(
    $._type_instance_head,
    '=',
    $.quantified_type,
  ),

  _type_instance: $ => seq(
    forall($),
    $._type_instance_common,
  ),

  type_instance: $ => seq(
    'type',
    'instance',
    $._type_instance,
  ),

  // ------------------------------------------------------------------------
  // type family
  // ------------------------------------------------------------------------

  type_family_result: $ => seq('=', field('result', $.quantified_type)),

  type_family_injectivity: $ => seq(
    $._bar,
    field('result', $.variable),
    $._arrow,
    field('determined', repeat1($.variable)),
  ),

  _tyfam_inj: $ => seq(
    $.type_family_result,
    optional($.type_family_injectivity),
  ),

  _tyfam: $ => seq(
    $._type_head,
    optional(choice($._kind_annotation, $._tyfam_inj)),
  ),

  _tyfam_equations: $ => layout($, field('equation', alias($._type_instance, $.equation))),

  /**
   * This syntax is valid in `.hs-boot` files.
   */
  abstract_family: $ => layout_single($, '..'),

  type_family: $ => seq(
    'type',
    'family',
    $._tyfam,
    optional_where($, field('closed_family', choice(
      alias($._tyfam_equations, $.equations),
      $.abstract_family,
    ))),
  ),

  // ------------------------------------------------------------------------
  // role
  // ------------------------------------------------------------------------

  type_role: _ => choice(
    'representational',
    'nominal',
    'phantom',
    '_',
  ),

  role_annotation: $ => seq(
    'type',
    'role',
    field('type', $._tycons),
    repeat1(field('role', $.type_role)),
  )
}
