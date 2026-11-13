import { wrapped_in_parenthesis } from "../helpers.js";

export default {

  _copy_statement: $ => seq(
    $.keyword_copy,
    $.object_reference,
    $._column_list,
    $.keyword_from,
    choice(
      $.keyword_stdin,
      alias($._literal_string, "filename"),
      seq($.keyword_program, alias($._literal_string, "command")),
    ),
    optional($.keyword_with),
    wrapped_in_parenthesis(
      repeat1(
        choice(
          seq(
            $.keyword_format,
            choice(
              $.keyword_csv,
              $.keyword_binary,
              $.keyword_text,
            ),
          ),
          seq(
            $.keyword_freeze,
            choice(
              $.keyword_true,
              $.keyword_false
            )
          ),
          seq(
            $.keyword_header,
            choice(
              $.keyword_true,
              $.keyword_false,
              $.keyword_match
            ),
          ),
          seq(
            choice(
              $.keyword_delimiter,
              $.keyword_null,
              $.keyword_default,
              $.keyword_escape,
              $.keyword_quote,
              $.keyword_encoding,
            ),
            alias($._literal_string, $.identifier)
          ),
          seq(
            choice(
              $.keyword_force_null,
              $.keyword_force_not_null,
              $.keyword_force_quote,
            ),
            $._column_list
          ),
        ),
      ),
    ),
    optional($.where),
  ),

};
