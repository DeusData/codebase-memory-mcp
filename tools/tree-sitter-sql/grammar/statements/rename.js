export default {

  _rename_statement: $ => seq(
    $.keyword_rename,
    choice(
      $.keyword_table,
      $.keyword_tables,
    ),
    optional($._if_exists),
    $.object_reference,
    optional(
      choice(
        $.keyword_nowait,
        seq(
          $.keyword_wait,
          field('timeout', alias($._natural_number, $.literal))
        )
      )
    ),
    $.keyword_to,
    $.object_reference,
    repeat(
      seq(
        ',',
        $._rename_table_names,
      )
    ),
  ),

  _rename_table_names: $ => seq(
    $.object_reference,
    $.keyword_to,
    $.object_reference,
  ),

};
