import { comma_list, paren_list, wrapped_in_parenthesis } from "../helpers.js";

import create_function_rules from "./create-function.js";
import create_procedure_rules from "./create-procedure.js";

export default {

  _create_statement: $ => seq(
    choice(
      $.create_table,
      alias($._create_virtual_table, $.create_table), // cbm fork (#2246)
      $.create_view,
      $.create_materialized_view,
      $.create_index,
      $.create_function,
      $.create_procedure,
      $.create_type,
      $.create_database,
      $.create_role,
      $.create_sequence,
      $.create_extension,
      $.create_trigger,
      $.create_policy,
      prec.left(seq(
        $.create_schema,
        repeat($._create_statement),
      )),
    ),
  ),

  // left precedence because 'quoted' table options otherwise conflict with
  // `create function` string bodies; if you remove this precedence you will
  // have to also disable the `_literal_string` choice for the `name` field
  // in =-assigned `table_option`s
  create_table: $ => prec.left(
    seq(
      $.keyword_create,
      optional(
        choice(
          $._temporary,
          $.keyword_unlogged,
          $.keyword_external,
        )
      ),
      $.keyword_table,
      optional($._if_not_exists),
      $.object_reference,
      choice(
        seq(
          $.column_definitions,
          repeat($._table_settings),
          optional(
            seq(
              $.keyword_as,
              $._select_statement,
            ),
          )
        ),
        seq(
          repeat($._table_settings),
          seq(
            $.keyword_as,
            $.create_query,
          ),
        ),
      ),
    ),
  ),

  _table_settings: $ => choice(
    $.table_partition,
    $.stored_as,
    $.storage_location,
    $.table_sort,
    $.row_format,
    seq(
      $.keyword_tblproperties,
      paren_list($.table_option, true),
    ),
    seq($.keyword_without, $.keyword_oids),
    seq($.keyword_without, $.keyword_rowid), // cbm fork (#2246): SQLite
    $.storage_parameters,
    $.table_option,
  ),

  // cbm fork (#2246): SQLite CREATE VIRTUAL TABLE [IF NOT EXISTS] name
  // USING module[(arguments)], e.g. an fts5 full-text index. It is a table,
  // so it is aliased to create_table and extracted like any other.
  _create_virtual_table: $ => prec.right(seq(
    $.keyword_create,
    $.keyword_virtual,
    $.keyword_table,
    optional($._if_not_exists),
    $.object_reference,
    $.keyword_using,
    field('module', $.identifier),
    optional(paren_list($.module_argument, true)),
  )),

  // a column (fts5 `body`, `body UNINDEXED`, rtree `minX`) or an option
  // (fts5 `tokenize = 'unicode61'`, `content = ''`, `contentless_delete = 1`)
  module_argument: $ => choice(
    seq(field('name', $.identifier), repeat($.identifier)),
    seq(field('name', $.identifier), '=', field('value', choice($.literal, $.identifier))),
  ),

  stored_as: $ => seq(
      $.keyword_stored,
      $.keyword_as,
      choice(
          $.keyword_parquet,
          $.keyword_csv,
          $.keyword_sequencefile,
          $.keyword_textfile,
          $.keyword_rcfile,
          $.keyword_orc,
          $.keyword_avro,
          $.keyword_jsonfile,
      ),
  ),

  storage_parameters: $ => seq(
    $.keyword_with,
    paren_list(
      seq($.identifier, optional(seq('=', choice($.literal, $.array)))),
      true
    ),
  ),

  storage_location: $ => prec.right(
      seq(
          $.keyword_location,
          field('path', alias($._literal_string, $.literal)),
          optional(
              seq(
                  $.keyword_cached,
                  $.keyword_in,
                  field('pool', alias($._literal_string, $.literal)),
                  optional(
                      choice(
                          $.keyword_uncached,
                          seq(
                              $.keyword_with,
                              $.keyword_replication,
                              '=',
                              field('value', alias($._natural_number, $.literal)),
                          ),
                      ),
                  ),
              )
          )
      ),
  ),

  row_format: $ => seq(
      $.keyword_row,
      $.keyword_format,
      $.keyword_delimited,
      optional(
          seq(
              $.keyword_fields,
              $.keyword_terminated,
              $.keyword_by,
              field('fields_terminated_char', alias($._literal_string, $.literal)),
              optional(
                  seq(
                      $.keyword_escaped,
                      $.keyword_by,
                      field('escaped_char', alias($._literal_string, $.literal)),
                  )
              )
          )
      ),
      optional(
          seq(
              $.keyword_lines,
              $.keyword_terminated,
              $.keyword_by,
              field('row_terminated_char', alias($._literal_string, $.literal)),
          )
      )
  ),

  table_sort: $ => seq(
      $.keyword_sort,
      $.keyword_by,
      paren_list($.identifier, true),
  ),

  table_partition: $ => seq(
    choice(
      // Postgres/MySQL style
      seq(
        $.keyword_partition,
        $.keyword_by,
        choice(
          $.keyword_range,
          $.keyword_hash,
        )
      ),
      // Hive style
      seq(
        $.keyword_partitioned,
        $.keyword_by,
      ),
      // Spark SQL
      $.keyword_partition,
    ),
    choice(
      paren_list($.identifier),// postgres & Impala (CTAS)
      $.column_definitions, // impala/hive external tables
      paren_list($._key_value_pair, true), // Spark SQL
    )
  ),

  table_option: $ => choice(
    seq($.keyword_default, $.keyword_character, $.keyword_set, $.identifier),
    seq($.keyword_collate, $.identifier),
    field('name', $.keyword_default),
    seq(
      field('name', choice($.keyword_engine, $.identifier, $._literal_string)),
      '=',
      field('value', choice($.identifier, $._literal_string)),
    ),
  ),

  create_query: $ => $._dml_read,

  create_view: $ => prec.right(
    seq(
      $.keyword_create,
      optional($._or_replace),
      optional($._temporary),
      optional($.keyword_recursive),
      $.keyword_view,
      optional($._if_not_exists),
      $.object_reference,
      optional(paren_list($.identifier)),
      $.keyword_as,
      $.create_query,
      optional(
        seq(
          $.keyword_with,
          optional(
            choice(
              $.keyword_local,
              $.keyword_cascaded,
            )
          ),
          $._check_option,
        ),
      ),
    ),
  ),

  create_materialized_view: $ => prec.right(
    seq(
      $.keyword_create,
      optional($._or_replace),
      $.keyword_materialized,
      $.keyword_view,
      optional($._if_not_exists),
      $.object_reference,
      $.keyword_as,
      $.create_query,
      optional(
        choice(
          seq(
            $.keyword_with,
            $.keyword_data,
          ),
          seq(
            $.keyword_with,
            $.keyword_no,
            $.keyword_data,
          )
        )
      )
    ),
  ),

  _operator_class: $ => seq(
    field("opclass", $.identifier),
    optional(
      field("opclass_parameters", wrapped_in_parenthesis(comma_list($.term)))
    )
  ),

  composite_field: $ => seq(
    wrapped_in_parenthesis(
      comma_list(
        alias($._index_field, $.field),
        true,
      ),
    ),
    optional($.keyword_hash),
  ),

  _index_field: $ => seq(
    choice(
      field("expression", wrapped_in_parenthesis($._expression)),
      field("function", $.invocation),
      field("column", $._column),
    ),
    optional(seq($.keyword_collate, $.identifier)),
    optional($._operator_class),
    optional(choice($.keyword_hash, $.direction)),
    optional(
      seq(
        $.keyword_nulls,
        choice(
          $.keyword_first,
          $.keyword_last
        )
      )
    ),
  ),

  index_fields: $ => wrapped_in_parenthesis(
    comma_list(
      choice(
        $.composite_field,
        alias($._index_field, $.field),
      ),
      true,
    ),
  ),
  tablespace: $ => seq($.keyword_tablespace, $.identifier),
  tablet_split: $ => seq($.keyword_split, $.keyword_into, $._natural_number, $.keyword_tablets),

  covering_columns: $ => seq(
    $.keyword_include,
    $.index_fields,
  ),

  create_index: $ => seq(
    $.keyword_create,
    optional($.keyword_unique),
    $.keyword_index,
    optional($.keyword_concurrently),
    optional(
      seq(
        optional($._if_not_exists),
        field("column", $._column),
      ),
    ),
    $.keyword_on,
    optional($.keyword_only),
    seq(
      $.object_reference,
      optional(
        seq(
          $.keyword_using,
          choice(
            $.keyword_btree,
            $.keyword_hash,
            $.keyword_gist,
            $.keyword_spgist,
            $.keyword_gin,
            $.keyword_brin
          ),
        ),
      ),
      $.index_fields
    ),
    optional($.covering_columns),
    optional($.tablespace),
    optional($.tablet_split),
    optional(
      $.where,
    ),
  ),

  ...create_function_rules,
  ...create_procedure_rules,

  create_schema: $ => prec.left(seq(
    $.keyword_create,
    $.keyword_schema,
    choice(
      seq(
        optional($._if_not_exists),
        $.identifier,
        optional(seq($.keyword_authorization, $.identifier)),
      ),
      seq(
        $.keyword_authorization,
        $.identifier,
      ),
    ),
  )),

  _with_settings: $ => seq(
        field('name', $.identifier),
        optional('='),
        field('value', choice($.identifier, alias($._single_quote_string, $.literal))),
  ),

  create_database: $ => prec.left(seq(
    $.keyword_create,
    $.keyword_database,
    optional($._if_not_exists),
    $.identifier,
    optional($.keyword_with),
    repeat(
      $._with_settings
    ),
  )),

  create_role: $ => prec.left(seq(
    $.keyword_create,
    choice(
      $.keyword_user,
      $.keyword_role,
      $.keyword_group,
    ),
    $.identifier,
    optional($.keyword_with),
    repeat(
      choice(
        $._user_access_role_config,
        $._role_options,
      ),
    ),
  )),

  _role_options: $ => choice(
    field("option", $.identifier),
    seq(
      $.keyword_valid,
      $.keyword_until,
      field("valid_until", alias($._literal_string, $.literal))
    ),
    seq(
      $.keyword_connection,
      $.keyword_limit,
      field("connection_limit", alias($._integer, $.literal))
    ),
    seq(
      optional($.keyword_encrypted),
      $.keyword_password,
      choice(
        field("password", alias($._literal_string, $.literal)),
        $.keyword_null,
      ),
    ),
  ),

  _user_access_role_config: $ => seq(
    choice(
      seq(optional($.keyword_in), $.keyword_role),
      seq($.keyword_in, $.keyword_group),
      $.keyword_admin,
      $.keyword_user,
    ),
    comma_list($.identifier, true),
  ),

  create_sequence: $ => seq(
    $.keyword_create,
    optional(
      choice(
        choice($.keyword_temporary, $.keyword_temp),
        $.keyword_unlogged,
      )
    ),
    $.keyword_sequence,
    optional($._if_not_exists),
    $.object_reference,
    repeat(
      choice(
        seq($.keyword_as, $._type),
        seq($.keyword_increment, optional($.keyword_by), field("increment", alias($._integer, $.literal))),
        seq($.keyword_minvalue, choice($.literal, seq($.keyword_no, $.keyword_minvalue))),
        seq($.keyword_no, $.keyword_minvalue),
        seq($.keyword_maxvalue, choice($.literal, seq($.keyword_no, $.keyword_maxvalue))),
        seq($.keyword_no, $.keyword_maxvalue),
        seq($.keyword_start, optional($.keyword_with), field("start", alias($._integer, $.literal))),
        seq($.keyword_cache, field("cache", alias($._integer, $.literal))),
        seq(optional($.keyword_no), $.keyword_cycle),
        seq($.keyword_owned, $.keyword_by, choice($.keyword_none, $.object_reference)),
      )
    ),
  ),

  create_extension: $ => prec.left(seq(
    $.keyword_create,
    $.keyword_extension,
    optional($._if_not_exists),
    $.identifier,
    optional($.keyword_with),
    optional(seq($.keyword_schema, $.identifier)),
    optional(seq($.keyword_version, choice($.identifier, alias($._literal_string, $.literal)))),
    optional($.keyword_cascade),
  )),

  create_trigger: $ => seq(
    $.keyword_create,
    optional($._or_replace),
    // mariadb
    optional(seq($.keyword_definer, '=', $.identifier)),
    optional($.keyword_constraint),
    // sqlite
    optional($._temporary),
    $.keyword_trigger,
    // sqlite/mariadb
    optional($._if_not_exists),
    $.object_reference,
    choice(
      $.keyword_before,
      $.keyword_after,
      seq($.keyword_instead, $.keyword_of),
    ),
    $._create_trigger_event,
    repeat(seq($.keyword_or, $._create_trigger_event)),
    $.keyword_on,
    $.object_reference,
    repeat(
      choice(
        seq($.keyword_from, $.object_reference),
        choice(
          seq($.keyword_not, $.keyword_deferrable),
          $.keyword_deferrable,
          seq($.keyword_initially, $.keyword_immediate),
          seq($.keyword_initially, $.keyword_deferred),
        ),
        seq($.keyword_referencing, choice($.keyword_old, $.keyword_new), $.keyword_table, optional($.keyword_as), $.identifier),
        seq(
          $.keyword_for,
          optional($.keyword_each),
          choice($.keyword_row, $.keyword_statement),
          // mariadb
          optional(seq(choice($.keyword_follows, $.keyword_precedes), $.identifier)),
        ),
        seq($.keyword_when, wrapped_in_parenthesis($._expression)),
      ),
    ),
    choice(
      seq(
        $.keyword_execute,
        choice($.keyword_function, $.keyword_procedure),
        $.object_reference,
        paren_list(field('parameter', $.term)),
      ),
      // cbm fork (#2246): SQLite runs a BEGIN ... END body, after a WHEN
      // condition SQLite does not parenthesize
      seq(
        optional(seq($.keyword_when, $._expression)),
        $.block,
      ),
    ),
  ),

  _create_trigger_event: $ => choice(
    $.keyword_insert,
    seq(
      $.keyword_update,
      optional(
        seq(
          $.keyword_of,
          comma_list($.identifier, true),
        ),
      ),
    ),
    $.keyword_delete,
    $.keyword_truncate,
  ),

  create_type: $ => prec.left(seq(
    $.keyword_create,
    $.keyword_type,
    $.object_reference,
    optional(
      seq(
        choice(
          seq(
            $.keyword_as,
            $.column_definitions,
            optional(seq($.keyword_collate, $.identifier))
          ),
          seq(
            $.keyword_as,
            $.keyword_enum,
            $.enum_elements,
          ),
          seq(
            optional(
              seq(
                $.keyword_as,
                $.keyword_range,
              )
            ),
            paren_list(
              $._with_settings
            ),
          ),
        ),
      ),
    ),
  )),

  enum_elements: $ => seq(
    paren_list(field("enum_element", alias($._literal_string, $.literal))),
  ),

  // Postgres row level security
  create_policy: $ => prec.right(
    seq(
      $.keyword_create,
      $.keyword_policy,
      $.object_reference,
      $.keyword_on,
      $.object_reference,
      optional(
        seq(
          $.keyword_as,
          choice(
            $.keyword_permissive,
            $.keyword_restrictive,
          ),
        ),
      ),
      optional(
        seq(
          $.keyword_for,
          choice(
            $.keyword_all,
            $.keyword_select,
            $.keyword_insert,
            $.keyword_update,
            $.keyword_delete,
          ),
        ),
      ),
      optional(
        seq(
          $.keyword_to,
          choice(
            $.object_reference,
            $.keyword_public,
            $.keyword_current_role,
            $.keyword_current_user,
            $.keyword_session_user,
          ),
          repeat(
            seq(
              ',',
              choice(
                $.object_reference,
                $.keyword_public,
                $.keyword_current_role,
                $.keyword_current_user,
                $.keyword_session_user,
              ),
            ),
          ),
        ),
      ),
      optional(
        seq(
          $.keyword_using,
          $.parenthesized_expression,
        ),
      ),
      optional(
        seq(
          $.keyword_with,
          $.keyword_check,
          $.parenthesized_expression,
        ),
      ),
    ),
  ),

};
