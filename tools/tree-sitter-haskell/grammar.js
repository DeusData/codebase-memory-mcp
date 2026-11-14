/*
 * cbm self-maintained fork of tree-sitter-haskell (escaped char literals, #2439)
 *
 * Copyright (c) 2014 Max Brunsfeld                 (tree-sitter-haskell, MIT; maintained by tree-sitter/Torsten Schmits)
 * Copyright (c) 2026 DeusData                      (the "cbm fork (#2439)" hunk in grammar/literal.js)
 *
 * The VENDORED parser ships upstream's MIT LICENSE byte-identical; our copyright
 * for the patch lives here, with the source, and in THIRD_PARTY.md.
 *
 * Fork provenance (pinned; the commit this repo vendored before the fork):
 *   tree-sitter/tree-sitter-haskell @ 7fa19f195803a77855f036ee7f49e4b22856e338 (v0.23.1)
 *     grammar.js + grammar/*.js -> these files = upstream + this header
 *                                   + patches/escaped-char-literal.patch (grammar/literal.js only)
 *     src/scanner.c, src/unicode.h -> upstream bytes, unchanged (the external scanner is not involved)
 *
 * The patch (and nothing else) differs from upstream: upstream lexes an escaped
 * char literal as /'\\[^ ]*'/, and `[^ ]*` also matches newlines and quotes, so
 * the lexer's longest match ran on to the last `'` before the next space. In
 * `f = '\n'` followed by a blank line and `g' = 2`, the token ran from `'\n`
 * through `g'`: `f` spanned three lines, `g'` got no node and the valid module
 * reported an error. On one line `['\n','\t']` lexed as a single char token,
 * silently. The patched token is /'\\('|[^'\s]+)'/: a backslash, then either a
 * single quote (`'\''`) or a run of non-quote, non-whitespace characters, then
 * the closing quote. `'\\'`, `'\x41'`, `'\SOH'`, `'\^A'`, `'\1234'` and MagicHash
 * `'\n'#` lex as before; node types are unchanged.
 * On an upstream bump: re-fetch the pinned grammar.js + grammar/, re-apply the
 * patch, regenerate, re-run test/corpus -- and drop the fork if upstream fixes it.
 *
 * Regenerate (ABI 15, as the upstream pin was generated with the same CLI; NEVER
 * use a CLI >= 0.26, which emits ABI 16):
 *   cd tools/tree-sitter-haskell
 *   npx tree-sitter-cli@0.25.10 generate
 *   npx tree-sitter-cli@0.25.10 test
 *   cp src/parser.c ../../internal/cbm/vendored/grammars/haskell/parser.c
 */

const
  class_ = require('./grammar/class.js'),
  conflicts = require('./grammar/conflicts.js'),
  context = require('./grammar/context.js'),
  data = require('./grammar/data.js'),
  decl = require('./grammar/decl.js'),
  exp = require('./grammar/exp.js'),
  externals = require('./grammar/externals.js'),
  general = require('./grammar/general.js'),
  id = require('./grammar/id.js'),
  inline = require('./grammar/inline.js'),
  lexeme = require('./grammar/lexeme.js'),
  literal = require('./grammar/literal.js'),
  module_ = require('./grammar/module.js'),
  operator = require('./grammar/operator.js'),
  pat = require('./grammar/pat.js'),
  patsyn = require('./grammar/patsyn.js'),
  precedences = require('./grammar/precedences.js'),
  th = require('./grammar/th.js'),
  type = require('./grammar/type.js')

module.exports = grammar({
  name: 'haskell',

  rules: {
    haskell: $ => seq(
      optional($.header),
      optional($._body),
    ),

    ...general,
    ...type,
    ...context,
    ...exp,
    ...pat,
    ...module_,
    ...data,
    ...class_,
    ...decl,
    ...patsyn,
    ...th,
    ...literal,
    ...id,
    ...operator,
    ...lexeme,

  },

  ...externals,
  ...precedences,
  ...inline,
  ...conflicts,

  /**
   * These rules may occur anywhere in the grammar and don't have to be specified in productions.
   */
  extras: $ => [
    /\p{Zs}/,
    /\n/,
    /\r/,
    $.cpp,
    $.comment,
    $.haddock,
    $.pragma,
  ],

  /**
   * Rules with leading underscore are generally omitted from the AST, and can therefore not be used in queries.
   * The rules listed in this attribute are omitted from the AST, but their names can be used in queries in place of
   * their children; as well as in combination with them, using the syntax `expression/variable`.
   * This is most useful for choice rules that represent syntactic categories, like expressions, patterns, and types in
   * Haskell.
   *
   * See the readme for a detailed explanation.
   */
  supertypes: $ => [
    $.expression,
    $.pattern,
    $.type,
    $.quantified_type,
    $.constraint,
    $.constraints,
    $.type_param,
    $.declaration,
    $.decl,
    $.class_decl,
    $.instance_decl,
    $.statement,
    $.qualifier,
    $.guard,
  ],

  /**
   * This rule is used to detect that a reserved keyword is a prefix of an identifier.
   *
   * For example, if the identifier `ifM` occurs in a position where the keyword `if` is valid (so most expressions),
   * the fact that `ifM` matches `variable` prevents tree-sitter from lexing `if` followed by `M` as a `name`.
   */
  word: $ => $.variable,

})
