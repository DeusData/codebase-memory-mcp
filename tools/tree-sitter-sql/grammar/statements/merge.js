import { optional_parenthesis } from "../helpers.js";

export default {

  _merge_statement: $=> seq(
    $.keyword_merge,
    $.keyword_into,
    $.object_reference,
    optional($._alias),
    $.keyword_using,
    choice(
      $.subquery,
      $.object_reference
    ),
    optional($._alias),
    $.keyword_on,
    optional_parenthesis(field("predicate", $._expression)),
    repeat1($.when_clause)
  ),

  when_clause: $ => prec.left(seq(
    $.keyword_when,
    optional($.keyword_not),
    $.keyword_matched,
    optional(
      seq(
        $.keyword_and,
        optional_parenthesis(field("predicate", $._expression))
      )
    ),
    $.keyword_then,
    choice(
      $.keyword_delete,
      seq(
        $.keyword_update,
        $._set_values,
      ),
      seq(
        $.keyword_insert,
        $._insert_values
      ),
      optional($.where)
    )
  )),

};
