import { comma_list } from "../helpers.js";

export default {

  _insert_statement: $ => seq(
    $.insert,
    optional($.returning),
  ),

  insert: $ => seq(
    choice(
      $.keyword_insert,
      $.keyword_replace
    ),
    // cbm fork (#2246): SQLite INSERT OR {ABORT | FAIL | IGNORE | REPLACE | ROLLBACK}
    optional(
      seq(
        $.keyword_or,
        choice(
          $.keyword_abort,
          $.keyword_fail,
          $.keyword_ignore,
          $.keyword_replace,
          $.keyword_rollback,
        ),
      ),
    ),
    optional(
      choice(
        $.keyword_low_priority,
        $.keyword_delayed,
        $.keyword_high_priority,
      ),
    ),
    optional($.keyword_ignore),
    optional(
      choice(
        $.keyword_into,
        $.keyword_overwrite, // Spark SQL
      ),
    ),
    $.object_reference,
    optional($.table_partition), // Spark SQL
    optional(
      seq(
        $.keyword_as,
        field('alias', $.identifier)
      ),
    ),
    // TODO we need a test for `insert...set`
    choice(
      $._insert_values,
      $._set_values,
    ),
    optional(
      choice(
        $._on_conflict,
        $._on_duplicate_key_update,
      ),
    ),
  ),

  _on_conflict: $ => seq(
    $.keyword_on,
    $.keyword_conflict,
    seq(
      $.keyword_do,
      choice(
        $.keyword_nothing,
        seq(
          $.keyword_update,
          $._set_values,
          optional($.where),
        ),
      ),
    ),
  ),

  _on_duplicate_key_update: $ => seq(
    $.keyword_on,
    $.keyword_duplicate,
    $.keyword_key,
    $.keyword_update,
    $.assignment_list,
  ),

  assignment_list: $ => seq(
    $.assignment,
    repeat(seq(',', $.assignment)),
  ),

  assignment: $ => seq(
    field('left',
      alias(
        $._qualified_field,
        $.field,
      ),
    ),
    '=',
    field('right', $._expression),
  ),

  _insert_values: $ => seq(
    optional(alias($._column_list, $.list)),
    choice(
      seq(
        $.keyword_values,
        comma_list($.list, true),
      ),
      $._dml_read,
    ),
  ),

  _set_values: $ => seq(
    $.keyword_set,
    comma_list($.assignment, true),
  ),

}
