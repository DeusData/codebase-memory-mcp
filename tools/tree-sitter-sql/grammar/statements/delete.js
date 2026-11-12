export default {

  _delete_statement: $ => seq(
    $.delete,
    alias($._delete_from, $.from),
    optional($.returning),
  ),

  _delete_from: $ => seq(
    $.keyword_from,
    optional(
      $.keyword_only,
    ),
    $.object_reference,
    optional($.where),
    optional($.order_by),
    optional($.limit),
  ),

  delete: $ => seq(
    $.keyword_delete,
    optional($.index_hint),
  ),

};
