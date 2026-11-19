const {
  parens,
  brackets,
  unboxed,
} = require('./util.js')

const decimal = /[0-9][0-9_]*/
const exponent = /[eE][+-]?[0-9_]+/
const hex_exponent = /[pP][+-]?[0-9a-fA-F_]+/
const magic_hash = rule => token(seq(rule, optional(token.immediate(/##?/))))

module.exports = {

  // ------------------------------------------------------------------------
  // literals
  // ------------------------------------------------------------------------

  // the `choice` here is necessary to avoid integers being parsed as floats
  float: _ => magic_hash(
    seq(
      decimal,
      choice(
        seq(/\.[0-9_]+/, optional(exponent)),
        exponent,
      ),
    ),
  ),

  char: _ => magic_hash(
    choice(
      /'[^']'/,
      // cbm fork (#2439): upstream's /'\\[^ ]*'/ also matched newlines and quotes,
      // so an escaped char literal ran on to the last `'` before the next space and
      // swallowed the following definition. Stop at the closing quote instead.
      /'\\('|[^'\s]+)'/,
    ),
  ),

  string: _ => magic_hash(
    seq(
      '"',
      repeat(choice(
        /[^\\"\n]/,
        /\\(\^)?./,
        /\\\n\s*\\/,
      )),
      '"',
    ),
  ),

  _integer_literal: _ => magic_hash(decimal),
  _binary_literal: _ => magic_hash(/0[bB][01_]+/),
  _octal_literal: _ => magic_hash(/0[oO][0-7]+/),

  _hex_literal: _ => magic_hash(
    seq(
      /0[xX][0-9a-fA-F_]+/,
      optional(/\.[0-9a-fA-F_]+/),
      optional(hex_exponent),
    )
  ),

  integer: $ => choice(
    $._binary_literal,
    $._integer_literal,
    $._octal_literal,
    $._hex_literal,
  ),

  _stringly: $ => choice(
    $.string,
    $.char,
  ),

  _number: $ => choice(
    $.integer,
    $.float,
  ),

  _plist: $ => brackets($),

  unit: $ => parens($),
  unboxed_unit: $ => unboxed($),

  prefix_tuple: $ => parens($, repeat1(',')),
  prefix_unboxed_tuple: $ => unboxed($, repeat1(',')),
  prefix_unboxed_sum: $ => unboxed($, repeat1($._unboxed_bar)),

  literal: $ => choice(
    $._stringly,
    $._number,
  ),

  _unit_cons: $ => choice(
    $.unit,
    $.unboxed_unit,
  ),

  _tuple_cons: $ => choice(
    $.prefix_tuple,
    $.prefix_unboxed_tuple,
    $.prefix_unboxed_sum,
  ),

}
