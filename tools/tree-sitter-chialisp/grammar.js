/// <reference types="tree-sitter-cli/dsl" />
// tree-sitter grammar for Chialisp (Chia smart-coin s-expression language).
//
// Copyright (c) DeusData — MIT (the project's own license).
//
// Deliberately GENERIC. Chialisp has no reserved words at the reader level:
// `mod`, `defun`, `defconstant`, `include` and friends are ordinary head
// symbols of ordinary lists, resolved by the compiler, not by the tokenizer.
// Encoding them as grammar rules is how a Chialisp grammar breaks the moment
// the dialect gains a form (cl-21 -> cl-23 -> cl-26 each added several). So we
// model exactly what the clvm_tools reader models: nested s-expressions over
// five atom kinds. Definition/call/import shape is then decided in the
// extractor (internal/cbm/extract_defs.c and friends), where it can be revised
// without regenerating a parser.
//
// Reader model taken from clvm_tools' IR reader:
//   - a token ends at whitespace, `(`, `)`, `;` or a quote character;
//   - `;` starts a comment that runs to end of line (LF, CRLF or EOF);
//   - both `"` and `'` delimit strings;
//   - `.` standing alone is the dotted-pair marker, but a `.` INSIDE a token is
//     an ordinary character — which is why `(include foo.clib)` must parse.
//
// Atom kinds are ordered hex -> number -> symbol so that the lexer's
// equal-length tiebreak (match specificity, then rule order) prefers the
// specific reading: `0x17` is hex, `-5` is a number, and anything longer than
// either match — `0xdeadZZ`, `123abc` — falls through to `symbol` by longest
// match. Do not add token(prec(...)) here: lexical precedence outranks match
// length in tree-sitter, which would split `123abc` into `123` + `abc`.
module.exports = grammar({
  name: 'chialisp',

  extras: $ => [
    /\s/,
    $.comment,
  ],

  rules: {
    source_file: $ => repeat($._form),

    // `dot` is admitted anywhere a form is, rather than only in the one
    // grammatical dotted-pair slot. clvm_tools' reader is equally permissive,
    // and a stricter rule would turn a hand-written constant table into an
    // ERROR node — a mis-parse we would then have to explain rather than index.
    _form: $ => choice($._sexp, $.dot),

    _sexp: $ => choice(
      $.list,
      $.hex,
      $.number,
      $.string,
      $.symbol,
    ),

    list: $ => seq('(', repeat($._form), ')'),

    // `[^\r\n]*` (not `[^\n]*`) so a CRLF file does not carry the CR inside the
    // comment node; the CR is whitespace and is consumed by `extras`. Runs to
    // EOF when the last line has no terminator.
    comment: _ => token(seq(';', /[^\r\n]*/)),

    hex: _ => /0[xX][0-9a-fA-F]+/,

    // `-?` and not a separate sign token: a bare `-` is the subtraction
    // operator and must stay a `symbol`, which it does because this pattern
    // requires at least one digit.
    number: _ => /-?[0-9]+/,

    string: _ => token(choice(
      seq('"', repeat(choice(/[^"\\]/, /\\./)), '"'),
      seq("'", repeat(choice(/[^'\\]/, /\\./)), "'"),
    )),

    dot: _ => '.',

    symbol: _ => /[^\s()";']+/,
  },
});
