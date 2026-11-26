/*
 * cbm fork of tree-sitter-vba (Visual Basic for Applications and VB6),
 * vba dialect
 *
 * Copyright (c) 2026 harumiWeb   (tree-sitter-vba, MIT)
 *
 * The VENDORED grammar ships upstream's MIT notice byte-identical:
 * internal/cbm/vendored/grammars/vba/LICENSE.
 *
 * Fork provenance (pinned):
 *   harumiWeb/tree-sitter-vba @ 83c5c3c3c7210468fb6745b585b5fb6532aa2376
 *     (tag v0.14.5, upstream main HEAD). Upstream checks in no generated
 *     parser.c (its docs/adr/0002), so the parser is generated here.
 *   common/define-grammar.js is upstream's file byte-identical
 *     (git blob 5e1f7d758ce1414dbe9ec01e5236bc1c29f62a9a).
 *   This file = this 40-line header + upstream's vba/grammar.js
 *     (`tail -n +41 vba/grammar.js | git hash-object --stdin` gives
 *     9247dbbfe166d139a28b10e9c960829ad6d265fc). No local patch.
 *
 * Why the vba dialect: upstream also generates a vb6 dialect from the same
 * core. On real VB6 code both expose the same procedures, but the vb6 dialect
 * rejects the VBA7 forms (PtrSafe, LongPtr) that dual-target VB6 sources keep
 * inside #If VBA7 blocks, and its error recovery then swallows whole files;
 * it is also 75.8 MB of parser.c against 48.3 MB here. The numbers are in
 * internal/cbm/vendored/grammars/MANIFEST.md.
 *
 * Regenerate with exactly tree-sitter-cli 0.25.10 (the generator every fork
 * here uses; the vendored runtime's ceiling is ABI 15):
 *   cd tools/tree-sitter-vba
 *   npx tree-sitter-cli@0.25.10 generate --abi 15 --output vba/src vba/grammar.js
 *   cp vba/src/parser.c ../../internal/cbm/vendored/grammars/vba/
 *   cp vba/src/tree_sitter/alloc.h vba/src/tree_sitter/array.h \
 *      vba/src/tree_sitter/parser.h \
 *      ../../internal/cbm/vendored/grammars/vba/tree_sitter/
 *
 * On an upstream bump: re-fetch common/define-grammar.js and vba/grammar.js
 * at the new tag, regenerate twice (the two parser.c must be byte-identical),
 * and re-run vba_vendored_grammar_* in tests/test_parse_coverage.c.
 */

/**
 * @file Tree-sitter grammar for Visual Basic for Applications (VBA) - entry point
 * @author harumiWeb
 * @license MIT
 */

/// <reference types="tree-sitter-cli/dsl" />
// @ts-check

module.exports = require("../common/define-grammar")("vba");
