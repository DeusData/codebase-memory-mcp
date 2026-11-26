export default {

  create_procedure: $ => seq(
    $.keyword_create,
    optional($._or_replace),
    // mariadb/mysql
    optional(seq($.keyword_definer, '=', $.identifier)),
    $.keyword_procedure,
    optional($._if_not_exists),
    $.object_reference,
    optional($.function_arguments),
    repeat(
      choice(
        $.function_language,
        $.function_security,
        $.function_safety,
      ),
    ),
    $.procedure_body,
    repeat(
      choice(
        $.function_language,
        $.function_security,
        $.function_safety,
      ),
    ),
  ),

  procedure_body: $ => choice(
    // BEGIN ATOMIC block (SQL standard)
    seq(
      $.keyword_begin,
      $.keyword_atomic,
      repeat1(
        seq(
          $._function_body_statement,
          ';',
        ),
      ),
      $.keyword_end,
    ),
    // Dollar-quoted with optional DECLARE (PostgreSQL)
    seq(
      $.keyword_as,
      alias($._dollar_quoted_string_start_tag, $.dollar_quote),
      optional(
        seq(
          $.keyword_declare,
          repeat1(
            $.function_declaration,
          ),
        ),
      ),
      $.keyword_begin,
      repeat1(
        seq(
          $._function_body_statement,
          ';',
        ),
      ),
      $.keyword_end,
      optional(';'),
      alias($._dollar_quoted_string_end_tag, $.dollar_quote),
    ),
    // String literal body
    seq(
      $.keyword_as,
      alias(
        choice(
          $._single_quote_string,
          $._double_quote_string,
        ),
        $.literal
      ),
    ),
    // Dollar-quoted single statement (PostgreSQL)
    seq(
      $.keyword_as,
      alias($._dollar_quoted_string_start_tag, $.dollar_quote),
      $._function_body_statement,
      optional(';'),
      alias($._dollar_quoted_string_end_tag, $.dollar_quote),
    ),
    // T-SQL style (no required RETURN)
    $._tsql_procedure_body_statement,
  ),

  _tsql_procedure_body_statement: $ => seq(
    optional($.keyword_as),
    $.keyword_begin,
    optional($.var_declarations),
    choice(
      repeat($.statement),
      repeat1(seq(
        $.keyword_begin,
        repeat($.statement),
        $.keyword_end,
      )),
    ),
    $.keyword_end,
  ),

};
