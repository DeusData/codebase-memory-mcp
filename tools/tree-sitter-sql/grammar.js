/*
 * cbm self-maintained fork of tree-sitter-sql (SQLite migration syntax, #2246)
 *
 * Copyright (c) 2021 Derek Stride              (tree-sitter-sql, MIT)
 * Copyright (c) 2026 DeusData                  (the "cbm fork (#2246)" hunks in grammar/)
 *
 * The VENDORED parser ships upstream's MIT LICENSE byte-identical; our copyright
 * for the patch lives here, with the source, and in THIRD_PARTY.md.
 *
 * Fork provenance (pinned; the commit this repo vendored before the fork):
 *   DerekStride/tree-sitter-sql @ 851e9cb257ba7c66cc8c14214a31c44d2f1e954e
 *   (v0.3.11; upstream's gh-pages "deploy: 3c99273f9b32" commit, whose grammar
 *   sources are byte-identical to main @ 3c99273f9b32a00e38a737e8a0dd37397f931916)
 *     grammar.js     -> this file = upstream + this header
 *     grammar/*.js   -> upstream + patches/sqlite-dialect.patch
 *     src/scanner.c  -> upstream + the start_tag leak fix listed under "Local
 *                       source patches" in internal/cbm/vendored/grammars/MANIFEST.md
 *
 * The patch (and nothing else) differs from upstream. Upstream rejected forms
 * that valid SQLite migrations use, so those files became ERROR nodes and were
 * flagged parse_partial: PRAGMA; AUTOINCREMENT (upstream's own c042cde1e2a6,
 * after our pin); a column named `key` after the first column; WITHOUT ROWID;
 * a trigger's BEGIN ... END body and its unparenthesized WHEN; INSERT OR
 * <conflict>; CREATE VIRTUAL TABLE ... USING module(...), aliased to
 * create_table. Every upstream node keeps its type and shape; the new nodes
 * are pragma_statement and module_argument.
 * On an upstream bump: re-fetch the pinned grammar.js and grammar/, re-apply the
 * patch, regenerate, re-run test/corpus plus upstream's corpus -- and drop each
 * hunk upstream has absorbed.
 *
 * Regenerate with exactly this CLI version (ABI 15, the runtime ceiling; the
 * output is pinned by SHA-256 in scripts/vendored-checksums.txt):
 *   cd tools/tree-sitter-sql
 *   npx tree-sitter-cli@0.25.10 generate
 *   npx tree-sitter-cli@0.25.10 test
 *   cp src/parser.c ../../internal/cbm/vendored/grammars/sql/parser.c
 */

import keyword_rules from "./grammar/keywords.js";
import type_rules from "./grammar/types.js";
import column_list_rules from "./grammar/column-lists.js";
import expression_rules from "./grammar/expressions.js";
import transaction_rules from "./grammar/transactions.js";
import statement_rules from "./grammar/statements/index.js";

export default grammar({
  name: 'sql',

  extras: $ => [
    /\s\n/,
    /\s/,
    $.comment,
    $.marginalia,
  ],


  externals: $ => [
    $._dollar_quoted_string_start_tag,
    $._dollar_quoted_string_end_tag,
    $._dollar_quoted_string,
  ],

  conflicts: $ => [
    [$.object_reference, $._qualified_field],
    [$.field, $._qualified_field],
    [$._column, $._qualified_field],
    [$.object_reference],
    [$.between_expression, $.binary_expression],
    [$.time],
    [$.timestamp],
  ],

  precedences: $ => [
    [
      'binary_is',
      'unary_not',
      'binary_exp',
      'binary_times',
      'binary_plus',
      'unary_other',
      'binary_other',
      'binary_in',
      'binary_compare',
      'binary_relation',
      'pattern_matching',
      'between',
      'clause_connective',
      'clause_disjunctive',
    ],
  ],

  word: $ => $._identifier,

  rules: {
    program: $ => seq(
      // any number of transactions, statements, or blocks with a terminating ;
      repeat(
        seq(
          choice(
            $.transaction,
            $.statement,
            $.block,
          ),
          ';',
        ),
      ),
      // optionally, a single statement without a terminating ;
      optional(
        $.statement,
      ),
    ),

    comment: _ => /--.*/,
    // https://stackoverflow.com/questions/13014947/regex-to-match-a-c-style-multiline-comment
    marginalia: _ => /\/\*[^*]*\*+(?:[^/*][^*]*\*+)*\//,

    ...keyword_rules,
    ...type_rules,
    ...column_list_rules,
    ...expression_rules,
    ...transaction_rules,
    ...statement_rules,

  }

});
