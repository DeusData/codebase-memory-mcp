import { paren_list } from "../helpers.js";

export default {

  _optimize_statement: $ => choice(
    $._compute_stats,
    $._vacuum_table,
    $._optimize_table,
  ),

  // Compute stats for Impala and Hive
  _compute_stats: $ => prec.left(choice(
    // Hive
    seq(
      $.keyword_analyze,
      $.keyword_table,
      $.object_reference,
      optional($._partition_spec),
      $.keyword_compute,
      $.keyword_statistics,
      optional(
        seq(
          $.keyword_for,
          $.keyword_columns
        )
      ),
      optional(
        seq(
          $.keyword_cache,
          $.keyword_metadata
        )
      ),
      optional($.keyword_noscan),
    ),
    // Impala
    seq(
      $.keyword_compute,
      optional(
        $.keyword_incremental,
      ),
      $.keyword_stats,
      $.object_reference,
      optional(
        choice(
          paren_list(repeat1($.field)),
          $._partition_spec,
        )
      )
    ),
  )),

  _optimize_table: $ => choice(
    // Athena/Iceberg
    seq(
      $.keyword_optimize,
      $.object_reference,
      $.keyword_rewrite,
      $.keyword_data,
      $.keyword_using,
      $.keyword_bin_pack,
      optional(
        $.where,
      )
    ),
    // MariaDB Optimize
    seq(
      $.keyword_optimize,
      optional(
        choice(
          $.keyword_local,
          //$.keyword_no_write_to_binlog,
        )
      ),
      $.keyword_table,
      $.object_reference,
      repeat(seq(',', $.object_reference)),
    ),
  ),

  _vacuum_table: $ => prec.left(seq(
    $.keyword_vacuum,
    optional($._vacuum_option),
    $.object_reference,
    optional(
      paren_list($.field)
    ),
  )),

  _vacuum_option: $ => choice(
    seq($.keyword_full, optional(choice($.keyword_true, $.keyword_false))),
    seq($.keyword_parallel, optional(choice($.keyword_true, $.keyword_false))),
    seq($.keyword_analyze, optional(choice($.keyword_true, $.keyword_false))),
    // seq($.keyword_freeze, choice($.keyword_true, $.keyword_false)),
    // seq($.keyword_skip_locked, choice($.keyword_true, $.keyword_false)),
    // seq($.keyword_truncate, choice($.keyword_true, $.keyword_false)),
    // seq($.keyword_disable_page_skipping, choice($.keyword_true, $.keyword_false)),
    // seq($.keyword_process_toast, choice($.keyword_true, $.keyword_false)),
    // seq($.keyword_index_cleanup, choice($.keyword_auto, $.keyword_on, $.keyword_off)),
  ),

  // TODO: this does not account for partitions specs like
  // (partcol1='2022-01-01', hr=11)
  // the second argument is not a $.table_option
  _partition_spec: $ => seq(
    $.keyword_partition,
    paren_list($.table_option, true),
  ),

};
