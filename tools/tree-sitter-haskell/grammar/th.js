const {
  layout_sort,
} = require('./util.js')

const quote_bracket = ($, quoter) => seq(
  $._cond_quote_start,
  '[',
  field('quoter', quoter),
    '|',
)

module.exports = {

  // ------------------------------------------------------------------------
  // splice
  // ------------------------------------------------------------------------

  /**
   * Even though the doc states "arbitrary expression", it's very rare for any others than names and parenthesized
   * expressions to occur, and it's very expensive to allow them.
   * Even only allowing list and quotes adds a full megabyte to the parser size.
   */
  _splice_exp: $ => choice(
    $._exp_name,
    alias($._exp_parens, $.parens),
    $.literal,
  ),

  _splice_dollars: $ => seq(
    $._cond_splice,
    choice('$', '$$'),
  ),

  splice: $ => seq($._splice_dollars, field('expression', $._splice_exp)),

  /**
   * Since `expression` includes `splice`, this allows for a top level dollar splice as well.
   */
  top_splice: $ => $.expression,

  quoter: $ => $._varids,

  /**
   * `_cond_quote_start` (in `quote_bracket`) is a zero-width token emitted by the scanner.
   * While the quoter and the bar may not be preceded by whitespace, this is not necessary to ensure here with
   * `token.immediate` since the scanner already verifies it.
   */
  quasiquote: $ => seq(
    quote_bracket($, $.quoter),
    optional(field('body', $.quasiquote_body)),
    choice(token('|]'), '⟧'),
  ),

  quoted_decls: $ => layout_sort($, $._cmd_layout_start_quote, field('declaration', $.declaration)),

  /**
  * An "expression quotation" is valid in an expression, and its body may contain an expression, type, pattern or
  * declaration layout.
  *
  * Which of these are valid is decided by the quoter: `e`, `t`, `p` or `d`.
  * If the quoter is empty, or the special oxford bracket character is used, the body is parsed as an expression.
  */
  _exp_quote: $ => seq(
    choice(
      seq(choice('⟦', quote_bracket($, optional('e'))), optional(alias($._exp, $.quoted_expression))),
      seq(quote_bracket($, 't'), optional(alias($._ktype, $.quoted_type))),
      seq(quote_bracket($, 'p'), optional(alias($._pat, $.quoted_pattern))),
      seq(quote_bracket($, 'd'), optional($.quoted_decls)),
    ),
    choice(token('|]'), '⟧'),
  ),

  _exp_typed_quote: $ => seq(
    $._cond_quote_start,
    '[',
    optional('e'),
    '||',
    optional(alias($._exp, $.quoted_expression)),
    token('||]'),
  ),

}
