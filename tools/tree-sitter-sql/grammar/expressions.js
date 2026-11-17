import { optional_parenthesis, paren_list, wrapped_in_parenthesis } from "./helpers.js";

export default {

  _expression: $ => prec(1,
    choice(
      $.literal,
      alias(
        $._qualified_field,
        $.field,
      ),
      $.parameter,
      $.list,
      $.case,
      $.window_function,
      $.subquery,
      $.cast,
      alias($.implicit_cast, $.cast),
      $.exists,
      $.invocation,
      $.binary_expression,
      $.subscript,
      $.unary_expression,
      $.array,
      $.interval,
      $.between_expression,
      $.parenthesized_expression,
      $.object_id,
    )
  ),

    object_reference: $ => choice(
      seq(
        field('database', $.identifier),
        '.',
        field('schema', $.identifier),
        '.',
        field('name', $.identifier),
      ),
      seq(
        field('schema', $.identifier),
        '.',
        field('name', $.identifier),
      ),
      field('name', $.identifier),
    ),

    field: $ => field('name', $.identifier),

    _qualified_field: $ => seq(
      optional(
        seq(
          optional_parenthesis($.object_reference),
          '.',
        ),
      ),
      field('name', $.identifier),
    ),

  parameter: $ => /\?|(\$[0-9]+)/,

  case: $ => seq(
    $.keyword_case,
    choice(
      // simplified CASE x WHEN
      seq(
        $._expression,
        $.keyword_when,
        $._expression,
        $.keyword_then,
        $._expression,
        repeat(
          seq(
            $.keyword_when,
            $._expression,
            $.keyword_then,
            $._expression,
          )
        ),
      ),
      // standard CASE WHEN x, where x must be a predicate
      seq(
        $.keyword_when,
        $._expression,
        $.keyword_then,
        $._expression,
        repeat(
          seq(
            $.keyword_when,
            $._expression,
            $.keyword_then,
            $._expression,
          )
        ),
      ),
    ),
    optional(
      seq(
        $.keyword_else,
        $._expression,
      )
    ),
    $.keyword_end,
  ),

  implicit_cast: $ => seq(
    $._expression,
    '::',
    $._type,
  ),

  cast: $ => seq(
    field('name', $.keyword_cast),
    wrapped_in_parenthesis(
      seq(
        field('parameter', $._expression),
        $.keyword_as,
        $._type,
      ),
    ),
  ),

  exists: $ => seq(
    $.keyword_exists,
    $.subquery,
  ),

  invocation: $ => prec(1,
    seq(
      $.object_reference,
      choice(
        // default invocation
        paren_list(
          seq(
            optional($.keyword_distinct),
            field(
              'parameter',
              $.term,
            ),
            optional($.order_by)
          )
        ),
        // https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_extract
        paren_list(
          seq(
            field(
              'unit',
              $.object_reference,
            ),
            $.keyword_from,
            $.term
          )
        ),
        // _aggregate_function, e.g. group_concat
        wrapped_in_parenthesis(
          seq(
            optional($.keyword_distinct),
            field('parameter', $.term),
            optional($.order_by),
            optional(seq(
              choice($.keyword_separator, ','),
              alias($._literal_string, $.literal)
            )),
            optional($.limit),
          ),
        ),
      ),
      optional(
        $.filter_expression
      )
    ),
  ),

  filter_expression : $ => seq(
    $.keyword_filter,
    wrapped_in_parenthesis($.where),
  ),

  parenthesized_expression: $ => prec(2,
    wrapped_in_parenthesis($._expression)
  ),

  subscript: $ => prec.left('binary_is',
    seq(
      field('expression', $._expression),
      "[",
      choice(
        field('subscript', $._expression),
        seq(
          field('lower', $._expression),
          ':',
          field('upper', $._expression),
        ),
      ),
      "]",
    ),
  ),

  op_other: $ => token(
    choice(
      '->',
      '->>',
      '#>',
      '#>>',
      '~',
      '!~',
      '~*',
      '!~*',
      '|',
      '&',
      '#',
      '<<',
      '>>',
      '<<=',
      '>>=',
      '##',
      '<->',
      '@>',
      '<@',
      '&<',
      '&>',
      '|>>',
      '<<|',
      '&<|',
      '|&>',
      '<^',
      '^>',
      '?#',
      '?-',
      '?|',
      '?-|',
      '?||',
      '@@',
      '@@@',
      '@?',
      '#-',
      '?&',
      '?',
      '-|-',
      '||',
      '^@',
    ),
  ),

  binary_expression: $ => choice(
    ...[
      ['+', 'binary_plus'],
      ['-', 'binary_plus'],
      ['*', 'binary_times'],
      ['/', 'binary_times'],
      ['%', 'binary_times'],
      ['^', 'binary_exp'],
      ['=', 'binary_relation'],
      ['<', 'binary_relation'],
      ['<=', 'binary_relation'],
      ['!=', 'binary_relation'],
      ['>=', 'binary_relation'],
      ['>', 'binary_relation'],
      ['<>', 'binary_relation'],
      [$.op_other, 'binary_other'],
      [$.keyword_is, 'binary_is'],
      [$.is_not, 'binary_is'],
      [$.keyword_like, 'pattern_matching'],
      [$.not_like, 'pattern_matching'],
      [$.keyword_rlike, 'pattern_matching'],
      [$.not_rlike, 'pattern_matching'],
      [$.similar_to, 'pattern_matching'],
      [$.not_similar_to, 'pattern_matching'],
      // binary_is precedence disambiguates `(is not distinct from)` from an
      // `is (not distinct from)` with a unary `not`
      [$.distinct_from, 'binary_is'],
      [$.not_distinct_from, 'binary_is'],
    ].map(([operator, precedence]) =>
      prec.left(precedence, seq(
        field('left', $._expression),
        field('operator', operator),
        field('right', $._expression)
      ))
    ),
    ...[
      [$.keyword_and, 'clause_connective'],
      [$.keyword_or, 'clause_disjunctive'],
    ].map(([operator, precedence]) =>
      prec.left(precedence, seq(
        field('left', $._expression),
        field('operator', operator),
        field('right', $._expression)
      ))
    ),
    ...[
      [$.keyword_in, 'binary_in'],
      [$.not_in, 'binary_in'],
    ].map(([operator, precedence]) =>
      prec.left(precedence, seq(
        field('left', $._expression),
        field('operator', operator),
        field('right', choice($.list, $.subquery))
      ))
    ),
  ),

  op_unary_other: $ => token(
    choice(
      '|/',
      '||/',
      '@',
      '~',
      '@-@',
      '@@',
      '#',
      '?-',
      '?|',
      '!!',
    ),
  ),

  unary_expression: $ => choice(
    ...[
      [$.keyword_not, 'unary_not'],
      [$.bang, 'unary_not'],
      [$.keyword_any, 'unary_not'],
      [$.keyword_some, 'unary_not'],
      [$.keyword_all, 'unary_not'],
      [$.op_unary_other, 'unary_other'],
    ].map(([operator, precedence]) =>
      prec.left(precedence, seq(
        field('operator', operator),
        field('operand', $._expression)
      ))
    ),
  ),

  // Postgres syntax for intervals
  interval: $ => seq(
      $.keyword_interval,
      $._literal_string,
  ),

  between_expression: $ => choice(
    ...[
          [$.keyword_between, 'between'],
          [seq($.keyword_not, $.keyword_between), 'between'],
      ].map(([operator, precedence]) =>
              prec.left(precedence, seq(
              field('left', $._expression),
              field('operator', operator),
              field('low', $._expression),
              $.keyword_and,
              field('high', $._expression)
          ))
      ),
  ),

  not_in: $ => seq(
    $.keyword_not,
    $.keyword_in,
  ),

  subquery: $ => wrapped_in_parenthesis(
    $._dml_read
  ),

  list: $ => paren_list($._expression),

  literal: $ => prec(2,
    choice(
      $._integer,
      $._decimal_number,
      $._literal_string,
      $._bit_string,
      $._string_casting,
      $.keyword_true,
      $.keyword_false,
      $.keyword_null,
    ),
  ),
  _double_quote_string: _ => /"[^"]*"/,
  _backtick_quoted_string: _ => /`[^`]*`/,
  // The norm specify that between two consecutive string must be a return,
  // but this is good enough.
  _single_quote_string: _ => seq(/([uU]&|[nN])?'([^']|'')*'/, repeat(/'([^']|'')*'/)),

  _postgres_escape_string: _ => /(e|E)'([^']|\\')*'/,

  _literal_string: $ => prec(
    1,
    choice(
      $._single_quote_string,
      $._double_quote_string,
      $._dollar_quoted_string,
      $._postgres_escape_string,
    ),
  ),
  _natural_number: _ => /\d+/,
  _integer: $ => seq(
    optional(choice("-", "+")),
    /(0[xX][0-9A-Fa-f]+(_[0-9A-Fa-f]+)*)|(0[oO][0-7]+(_[0-7]+)*)|(0[bB][01]+(_[01]+)*)|(\d+(_\d+)*(e[+-]?\d+(_\d+)*)?)/
  ),
  _decimal_number: $ => seq(
    optional(
      choice("-", "+")),
    /((\d+(_\d+)*)?[.]\d+(_\d+)*(e[+-]?\d+(_\d+)*)?)|(\d+(_\d+)*[.](e[+-]?\d+(_\d+)*)?)/
  ),
  _bit_string: $ => seq(/[bBxX]'([^']|'')*'/, repeat(/'([^']|'')*'/)),
  // The identifier should be followed by a string (no parenthesis allowed)
  _string_casting: $ => seq($.identifier, $._single_quote_string),

  bang: _ => '!',

  identifier: $ => choice(
    $._identifier,
    $._double_quote_string,
    $._backtick_quoted_string,
    $._tsql_parameter,
    seq("`", $._identifier, "`"),
  ),
  _tsql_parameter: $ => seq('@', $._identifier),
  // support nordic chars and umlaue
  _identifier: _ => /[A-Za-z_\u00C0-\u017F][0-9A-Za-z_\u00C0-\u017F]*/,

  object_id: $ => seq(
    $.keyword_object_id,
    wrapped_in_parenthesis(
      seq(
        alias($._literal_string, $.literal),
        optional(
          seq(
            ',',
            alias($._literal_string, $.literal),
          ),
        ),
      ),
    ),
  ),

};
