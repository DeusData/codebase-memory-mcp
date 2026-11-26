import { comma_list, optional_parenthesis, paren_list, wrapped_in_parenthesis } from "./helpers.js";

export default {

  _column_list: $ => paren_list(alias($._column, $.column), true),

  _column: $ => choice(
    $.identifier,
    alias($._literal_string, $.literal),
  ),

  _key_value_pair: $ => seq(
    field('key',$.identifier),
    '=',
    field('value', alias($._literal_string, $.literal)),
  ),

  column_definitions: $ => seq(
    '(',
    comma_list($.column_definition, true),
    optional($.constraints),
    ')',
  ),

  column_definition: $ => prec.left(seq(
    // cbm fork (#2246): SQLite does not reserve `key`, so it is a common column
    // name, but after the first column it lexes as the keyword of MySQL's
    // `KEY name (cols)` index. Accept that keyword as a column name too.
    field('name', choice($._column, alias($.keyword_key, $.identifier))),
    field('type', $._type),
    repeat($._column_constraint),
  )),

  _column_comment: $ => seq(
    $.keyword_comment,
    alias($._literal_string, $.literal)
  ),

  _column_constraint: $ => prec.left(choice(
    choice(
      $.keyword_null,
      $._not_null,
    ),
    seq(
      $.keyword_references,
      $.object_reference,
      paren_list($.identifier, true),
      repeat(
        seq(
          $.keyword_on,
          choice($.keyword_delete, $.keyword_update),
          choice(
            seq($.keyword_no, $.keyword_action),
            $.keyword_restrict,
            $.keyword_cascade,
            seq(
              $.keyword_set,
              choice($.keyword_null, $.keyword_default),
                optional(paren_list($.identifier, true))
            ),
          ),
        ),
      ),
    ),
    $._default_expression,
    $._primary_key,
    $.keyword_auto_increment,
    $.direction,
    $._column_comment,
    $._check_constraint,
    seq(
      optional(seq($.keyword_generated, $.keyword_always)),
      $.keyword_as,
      $._expression,
    ),
    choice(
      $.keyword_stored,
      $.keyword_virtual,
    ),
    $.keyword_unique
  )),

  _check_constraint: $ => seq(
    optional(
      seq(
        $.keyword_constraint,
        $.literal
      )
    ),
    $.keyword_check,
    wrapped_in_parenthesis($.binary_expression)
  ),

  _default_expression: $ => seq(
    $.keyword_default,
    optional_parenthesis($._inner_default_expression),
  ),
  _inner_default_expression: $ => choice(
      $.literal,
      $.list,
      $.cast,
      $.binary_expression,
      $.unary_expression,
      $.array,
      $.invocation,
      $.keyword_current_timestamp,
      alias($.implicit_cast, $.cast),
  ),

  constraints: $ => seq(
    ',',
    $.constraint,
    repeat(
      seq(',', $.constraint),
    ),
  ),

  constraint: $ => choice(
    $._constraint_literal,
    $._key_constraint,
    $._primary_key_constraint,
    $._check_constraint
  ),

  _constraint_literal: $ => seq(
    $.keyword_constraint,
    field('name', $.identifier),
    choice(
      seq(
        $._primary_key,
        $.ordered_columns,
      ),
      seq(
        $._check_constraint
      )
    )
  ),

  _primary_key_constraint: $ => seq(
    $._primary_key,
    $.ordered_columns,
  ),

  _key_constraint: $ => seq(
    choice(
      seq(
        $.keyword_unique,
        optional(
          choice(
            $.keyword_index,
            $.keyword_key,
            seq($.keyword_nulls, optional($.keyword_not), $.keyword_distinct),
          ),
        ),
      ),
      seq(optional($.keyword_foreign), $.keyword_key, optional($._if_not_exists)),
      $.keyword_index,
    ),
    // cbm fork (#2246): with `key` also a column name (column_definition), a
    // type keyword after KEY now lexes as that type, so accept the types that
    // take no arguments as index names too (MySQL `KEY name (name)`). A type
    // that takes arguments (int, varchar, time, ...) is not in the list.
    optional(field('name', choice(
      $.identifier,
      ...[
        'boolean', 'image', 'smallserial', 'serial', 'bigserial', 'money',
        'smallmoney', 'string', 'text', 'uuid', 'json', 'jsonb', 'xml', 'bytea',
        'inet', 'date', 'datetime', 'datetime2', 'smalldatetime', 'timestamptz',
        'interval', 'geometry', 'geography', 'box2d', 'box3d', 'oid', 'name',
        'regclass', 'regnamespace', 'regproc', 'regtype',
      ].map((type) => alias($['keyword_' + type], $.identifier)),
    ))),
    $.ordered_columns,
    optional(
      seq(
        $.keyword_references,
        $.object_reference,
        paren_list($.identifier, true),
        repeat(
          seq(
            $.keyword_on,
            choice($.keyword_delete, $.keyword_update),
            choice(
              seq($.keyword_no, $.keyword_action),
              $.keyword_restrict,
              $.keyword_cascade,
              seq(
                $.keyword_set,
                choice($.keyword_null, $.keyword_default),
                  optional(paren_list($.identifier, true))
              ),
            ),
          ),
        ),
      ),
    ),
  ),

  ordered_columns: $ => paren_list(alias($.ordered_column, $.column), true),

  ordered_column: $ => seq(
    field('name', $._column),
    optional($.direction),
  ),

};
