/*
 * cbm self-maintained fork of tree-sitter-typescript's `tsx` dialect
 * (JSX lone-ampersand fix, #1736)
 *
 * Copyright (c) 2017 Max Brunsfeld            (tree-sitter-typescript, MIT)
 * Copyright (c) 2014 Max Brunsfeld            (tree-sitter-javascript base, MIT)
 * Copyright (c) 2026 DeusData                 (the "cbm fork (#1736)" hunks in
 *                                              javascript-grammar.js)
 *
 * The VENDORED parser ships upstream's MIT LICENSE byte-identical; our copyright
 * for the patch lives here, with the source, and in THIRD_PARTY.md.
 *
 * Fork provenance (pinned; the same commits our vendored typescript/arkts use):
 *   tree-sitter/tree-sitter-typescript @ 75b3874edb2dc714fb1fd77a32013d0f8699989f (v0.23.2)
 *     tsx/grammar.js            -> this file (require path made local)
 *     common/define-grammar.js  -> ./define-grammar.js (only the
 *                                  tree-sitter-javascript require made local)
 *     common/scanner.h          -> src/_common_scanner.h (byte-identical)
 *     tsx/src/scanner.c         -> src/scanner.c (include path made local)
 *   tree-sitter/tree-sitter-javascript @ 3a837b6f3658ca3618f2022f8707e29739c91364
 *     (v0.23.1, the version tree-sitter-typescript v0.23.2's package-lock pins)
 *     grammar.js -> ./javascript-grammar.js = upstream +
 *                   ../tree-sitter-javascript/patches/jsx-lone-ampersand.patch
 *
 * Only the patch changes the grammar: without it this directory regenerates the
 * upstream tsx parser (identical state/symbol counts and node types).
 *
 * Regenerate (ABI 15 -- the vendored runtime ceiling; NEVER use a CLI >= 0.26,
 * which emits ABI 16):
 *   cd tools/tree-sitter-tsx
 *   npx tree-sitter-cli@0.25.10 generate
 *   npx tree-sitter-cli@0.25.10 test
 *   cp src/parser.c ../../internal/cbm/vendored/grammars/tsx/parser.c
 *   cp src/tree_sitter/*.h ../../internal/cbm/vendored/grammars/tsx/tree_sitter/
 */

const defineGrammar = require('./define-grammar');

module.exports = defineGrammar('tsx');
