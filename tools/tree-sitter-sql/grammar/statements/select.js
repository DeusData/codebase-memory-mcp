import { comma_list, optional_parenthesis, paren_list, wrapped_in_parenthesis } from "../helpers.js";

export default {

  _cte: $ => seq(
      $.keyword_with,
      optional($.keyword_recursive),
      $.cte,
      repeat(
          seq(
            ',',
            $.cte,
          ),
      ),
  ),

  cte: $ => seq(
    $.identifier,
    optional(paren_list(field("argument", $.identifier))),
    $.keyword_as,
    optional(
      seq(
        optional($.keyword_not),
        $.keyword_materialized,
      ),
    ),
    wrapped_in_parenthesis(
      alias(
        choice($._dml_read, $._dml_write),
        $.statement,
      ),
    ),
  ),

  set_operation: $ => seq(
    $._select_statement,
    repeat1(
      seq(
        field(
          "operation",
          choice(
            seq($.keyword_union, optional($.keyword_all)),
            $.keyword_except,
            $.keyword_intersect,
          ),
        ),
        $._select_statement,
      ),
    ),
  ),

  _select_statement: $ => optional_parenthesis(
    seq(
      $.select,
      optional(
        seq(
          $.keyword_into,
          $.select_expression,
        ),
      ),
      optional($.from),
    ),
  ),

  select: $ => seq(
    $.keyword_select,
    seq(
      optional($.keyword_distinct),
      $.select_expression,
    ),
  ),

  select_expression: $ => seq(
    $.term,
    repeat(
      seq(
        ',',
        $.term,
      ),
    ),
  ),

  term: $ => seq(
    field(
      'value',
      choice(
        $.all_fields,
        $._expression,
      ),
    ),
    optional($._alias),
  ),

  all_fields: $ => seq(
    optional(
      seq(
        $.object_reference,
        '.',
      ),
    ),
    '*',
  ),

  partition_by: $ => seq(
      $.keyword_partition,
      $.keyword_by,
      comma_list($._expression, true),
  ),

  frame_definition: $ => seq(
      choice(
        seq(
          $.keyword_unbounded,
          $.keyword_preceding,
        ),
        seq(
            field("start",
              choice(
                $.identifier,
                $.binary_expression,
                alias($._literal_string, $.literal),
                alias($._integer, $.literal)
              )
            ),
            $.keyword_preceding,
        ),
        $._current_row,
        seq(
            field("end",
              choice(
                $.identifier,
                $.binary_expression,
                alias($._literal_string, $.literal),
                alias($._integer, $.literal)
              )
            ),
            $.keyword_following,
        ),
        seq(
          $.keyword_unbounded,
          $.keyword_following,
        ),
      ),
  ),

  window_frame: $ => seq(
      choice(
          $.keyword_range,
          $.keyword_rows,
          $.keyword_groups,
      ),

      choice(
          seq(
              $.keyword_between,
              $.frame_definition,
              optional(
                seq(
                  $.keyword_and,
                  $.frame_definition,
                )
              )
          ),
          seq(
              $.frame_definition,
          )
      ),
      optional(
          choice(
              $._exclude_current_row,
              $._exclude_group,
              $._exclude_ties,
              $._exclude_no_others,
          ),
      ),
  ),

  window_clause: $ => seq(
      $.keyword_window,
      $.identifier,
      $.keyword_as,
      $.window_specification,
  ),

  window_specification: $ => wrapped_in_parenthesis(
    seq(
      optional($.partition_by),
      optional($.order_by),
      optional($.window_frame),
    ),
  ),

  window_function: $ => seq(
      $.invocation,
      $.keyword_over,
      choice(
          $.identifier,
          $.window_specification,
      ),
  ),

  _alias: $ => seq(
    optional($.keyword_as),
    field('alias', $.identifier),
  ),

  from: $ => seq(
    $.keyword_from,
    optional(
      $.keyword_only,
    ),
    comma_list($.relation, true),
    optional($.index_hint),
    repeat(
      choice(
        $.join,
        $.cross_join,
        $.lateral_join,
        $.lateral_cross_join,
      ),
    ),
    optional($.where),
    optional($.group_by),
    optional($.having),
    optional($.window_clause),
    optional($.order_by),
    optional($.limit),
  ),

  relation: $ => prec.right(
    seq(
      choice(
        $.subquery,
        $.invocation,
        $.object_reference,
        wrapped_in_parenthesis($.values),
      ),
      optional(
        seq(
          $._alias,
          optional(alias($._column_list, $.list)),
        ),
      ),
    ),
  ),

  values: $ => seq(
    $.keyword_values,
    $.list,
    optional(
        repeat(
        seq(
          ',',
          $.list,
        ),
      ),
    ),
  ),

  index_hint: $ => seq(
    choice(
      $.keyword_force,
      $.keyword_use,
      $.keyword_ignore,
    ),
    $.keyword_index,
    optional(
      seq(
        $.keyword_for,
        $.keyword_join,
      ),
    ),
    wrapped_in_parenthesis(
      field('index_name', $.identifier),
    ),
  ),

  join: $ => seq(
    optional($.keyword_natural),
    optional(
      choice(
        $.keyword_left,
        seq($.keyword_full, $.keyword_outer),
        seq($.keyword_left, $.keyword_outer),
        $.keyword_right,
        seq($.keyword_right, $.keyword_outer),
        $.keyword_inner,
        $.keyword_full,
      ),
    ),
    $.keyword_join,
    $.relation,
    optional($.index_hint),
    optional($.join),
    choice(
      seq(
        $.keyword_on,
        field("predicate", $._expression),
      ),
      seq(
        $.keyword_using,
        alias($._column_list, $.list),
      )
    )
  ),

  cross_join: $ => prec.right(
    seq(
      $.keyword_cross,
      $.keyword_join,
      $.relation,
      optional(
        seq(
          $.keyword_with,
          $.keyword_ordinality,
          optional(
            seq(
              $.keyword_as,
              field("alias", $.identifier),
              paren_list($.identifier),
            )
          )
        )
      )
    )
  ),

  lateral_join: $ => seq(
    optional(
      choice(
        // lateral joins cannot be right!
        $.keyword_left,
        seq($.keyword_left, $.keyword_outer),
        $.keyword_inner,
      ),
    ),
    $.keyword_join,
    $.keyword_lateral,
    choice(
      $.invocation,
      $.subquery,
    ),
    optional(
      choice(
        seq(
          $.keyword_as,
          field('alias', $.identifier),
        ),
        field('alias', $.identifier),
      ),
    ),
    $.keyword_on,
    choice(
      $._expression,
      $.keyword_true,
      $.keyword_false,
    ),
  ),

  lateral_cross_join: $ => seq(
    $.keyword_cross,
    $.keyword_join,
    $.keyword_lateral,
    choice(
      $.invocation,
      $.subquery,
    ),
    optional(
      choice(
        seq(
          $.keyword_as,
          field('alias', $.identifier),
        ),
        field('alias', $.identifier),
      ),
    ),
  ),

  where: $ => seq(
    $.keyword_where,
    field("predicate", $._expression),
  ),

  group_by: $ => seq(
    $.keyword_group,
    $.keyword_by,
    comma_list($._expression, true),
  ),

  having: $ => seq(
    $.keyword_having,
    $._expression,
  ),

  order_by: $ => prec.right(seq(
    $.keyword_order,
    $.keyword_by,
    comma_list($.order_target, true),
  )),

  order_target: $ => seq(
    $._expression,
    optional(
      seq(
        choice(
          $.direction,
          seq(
            $.keyword_using,
            choice('<', '>', '<=', '>='),
          ),
        ),
        optional(
          seq(
            $.keyword_nulls,
            choice(
              $.keyword_first,
              $.keyword_last,
            ),
          ),
        ),
      ),
    ),
  ),

  limit: $ => seq(
    $.keyword_limit,
    $.literal,
    optional($.offset),
  ),

  offset: $ => seq(
    $.keyword_offset,
    $.literal,
  ),

  returning: $ => seq(
    $.keyword_returning,
    $.select_expression,
  ),

};
