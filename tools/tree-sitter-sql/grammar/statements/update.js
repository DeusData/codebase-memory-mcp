import { comma_list } from "../helpers.js";

export default {

  _update_statement: $ => seq(
    $.update,
    optional($.returning),
  ),

  update: $ => seq(
    $.keyword_update,
    optional($.keyword_only),
    choice(
      $._mysql_update_statement,
      $._postgres_update_statement,
    ),
  ),

  _mysql_update_statement: $ => prec(0,
    seq(
      comma_list($.relation, true),
      repeat($.join),
      $._set_values,
      optional($.where),
    ),
  ),

  _postgres_update_statement: $ => prec(1,
    seq(
      $.relation,
      $._set_values,
      optional($.from),
    ),
  ),

};
