// ------------------------------------------------------------------------
// structure
// ------------------------------------------------------------------------

const sep1 = (s, rule) => seq(rule, repeat(seq(s, rule)))

const sep2 = (s, rule) => seq(rule, repeat1(seq(s, rule)))

const sep = (s, rule) => optional(sep1(s, rule))

// ------------------------------------------------------------------------
// syntax
// ------------------------------------------------------------------------

const parens = ($, ...rule) => seq($._paren_open, ...rule, $._paren_close)

const braces = ($, ...rule) => seq('{', $._cmd_brace_open, ...rule, '}', $._cmd_brace_close)

const brackets = ($, ...rule) => seq($._bracket_open, ...rule, $._bracket_close)

const ticked = (...rule) => seq('`', ...rule, '`')

const promoted = (...rule) => seq('\'', ...rule)

const prefix_at = ($, ...rule) => prec('prefix', seq($._prefix_at, ...rule))

const semi = $ => choice(repeat1(';'), $._cond_layout_semicolon)

const semi_opt = $ => optional(semi($))

const semis = ($, rule) => sep1(semi($), rule)

// ------------------------------------------------------------------------
// layout
// ------------------------------------------------------------------------

/**
 * More general variant of `layout_sort`.
 */
const layout_sort_single = ($, start, rule) => seq(
  choice(start, alias($._cmd_layout_start_explicit, '{')),
  rule,
  $._layout_end,
)

/**
 * Wrap a repeated rule in a layout.
 * This is used for `where`, `let`, `of`, `if` and `do`, and the toplevel module.
 * The `start` rule must be one of the externals starting with `_cmd_layout_<type>`, which instruct the scanner to push
 * a layout context with the current column as its indentation.
 * When a `_cond_layout_end` or `_cond_layout_semicolon` is encountered by the scanner, the recorded indent is compared
 * to the current one to make a decision.
 */
const layout_sort = ($, start, rule) => seq(
  choice(start, alias($._cmd_layout_start_explicit, '{')),
  optional(seq(
    semi_opt($),
    semis($, rule),
    semi_opt($),
  )),
  $._layout_end,
)

/**
 * Same as `layout`, but using `layout_sort_single`.
 * This is necessary for braces without repeating layout elements.
 * Usually it is enough to just use `braces` for this (e.g. records), but if the rule is in a choice with a full
 * layout, we need to allow the layout start token since the scanner emits that unconditionally based on preceding
 * tokens.
 */
const layout_single = ($, rule) => layout_sort_single($, $._cmd_layout_start, rule)

/**
 * Alias for `layout_sort` using the common layout type for the start token, which corresponds to declarations and GADT
 * constructors.
 */
const layout = ($, rule) => layout_sort($, $._cmd_layout_start, rule)

// ------------------------------------------------------------------------
// unboxed
// ------------------------------------------------------------------------

const unboxed = ($, ...rules) => seq($._unboxed_open, ...rules, $._unboxed_close)

/**
 * At least one element is filled, for expressions.
 */
const unboxed_tuple_nonempty = ($, rule) => unboxed(
  $,
  repeat(','),
  field('element', rule),
  repeat(seq(',', optional(field('element', rule))))
)

/**
 * All elements are filled in, for types and patterns.
 */
const unboxed_tuple_full = ($, rule) => unboxed($, sep1(',', field('element', rule)))

/**
 * Exactly one element is filled in, used by expressions, patterns and the special data constructors.
 */
const unboxed_sum_single = ($, rule) => unboxed(
  $,
  choice(
    seq(repeat1($._unboxed_bar), field('element', rule)),
    seq(field('element', rule), $._unboxed_bar)
  ),
  repeat($._unboxed_bar),
)

/**
 * All elements are filled in, for types.
 */
const unboxed_sum_full = ($, rule) => unboxed($, sep2($._unboxed_bar, field('element', rule)))

// ------------------------------------------------------------------------
// where
// ------------------------------------------------------------------------

const optional_where = ($, rule) => optional(seq($._where, optional(rule)))

// ------------------------------------------------------------------------
// misc
// ------------------------------------------------------------------------

const qualified = ($, id) => prec('qualified-id', seq(
  field('module', alias($._qualifying_module, $.module)),
  field('id', id),
))

const context = $ => optional(field('context', $.context))

const forall = $ => optional(field('forall', $._forall))

module.exports = {
  sep1,
  sep2,
  sep,
  parens,
  braces,
  brackets,
  ticked,
  promoted,
  prefix_at,
  semi,
  semi_opt,
  semis,
  layout_sort_single,
  layout_sort,
  layout_single,
  layout,
  unboxed,
  unboxed_tuple_nonempty,
  unboxed_tuple_full,
  unboxed_sum_single,
  unboxed_sum_full,
  optional_where,
  qualified,
  context,
  forall,
}
