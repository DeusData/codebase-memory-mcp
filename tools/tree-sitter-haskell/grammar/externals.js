module.exports = {

  /**
   * These rules are handled manually by the custom lexer in `src/scanner.c`.
   * Whenever the symbols are used in the rule tree, the parser executes the scanner.
   * Since several symbols are present both here and in `extras`, the scanner will be invoked before every single
   * natively lexed symbol, repeatedly, until it fails.
   * This makes indentation/layout tracking simpler.
   *
   * There are three special behavioral classes of symbols in this grammar, indicated by their name prefix:
   *
   * `cmd`: Commands are sequenced after key tokens like opening braces that unconditionally trigger a state change in
   *        the scanner.
   *        They don't require the scanner to _decide_ that a symbol is valid – the native parser has already committed
   *        to a symbol, and the command relays that information to the scanner so it can update its state and return.
   *        For example, after having parsed an opening brace, the scanner adds a brace context to the stack in reaction
   *        to `_cond_brace_open`, which will influence its future behavior.
   *        These symbols do not produce nodes in the parse tree.
   *
   * `cond`: Conditions are hard requirements in the grammar that are decided by the scanner based on its state and the
   *         lookahead.
   *         For example, whitespace-sensitive operators (tight infix /prefix ops) like the `@` in a type application or
   *         the `.` in a record projection are lexed when the following characters have certain properties.
   *         When the scanner decides that such a symbol is valid, the parser cannot attempt to use an alternative
   *         interpretation like it can with conflicts.
   *         This is often difficult, but also especially useful to force declarations and other layout items to be
   *         terminated when a new line has smaller or equal indent, preventing incorrect interpretations as top level
   *         expression splices spanning multiple lines.
   *
   * `phantom`: Phantom symbols are used to signal to the scanner that certain constructs are valid without requiring
   *            the scanner to actually parse them and produce text nodes for them.
   *            In the grammar, they are always optional, and the scanner never emits the phantom symbol itself, but
   *            decides whether to emit _other_ symbols when the phantom symbol is _not_ valid.
   *            This is used to implement GHC's tactic of allowing layouts to end _whenever a parse error occurs_, but
   *            using specialized heuristics.
   *
   * Most other symbols produce nodes, except for newline and the error sentinel.
   */
  externals: $ => [

    // This is an unused symbol that indicates to the scanner that a parse error has occurred.
    // Tree-sitter marks _all_ symbols as valid when calling the scanner after an error, so a symbol that's not used in
    // the grammar is only valid if that is the case.
    $.error_sentinel,

    // Emitted after every newline with equal or less indent to terminate layout-based rules, with a few exceptions.
    $._cond_layout_semicolon,

    // Instruct the scanner to push a layout context.
    // The first one is for declarations and miscellaneous other layouts.
    // The others are for the specific syntax construct.
    // The quote layout is for declaration quotes (`[d|data A ...|]`).
    $._cmd_layout_start,
    $._cmd_layout_start_do,
    $._cmd_layout_start_case,
    $._cmd_layout_start_if,
    $._cmd_layout_start_let,
    $._cmd_layout_start_quote,
    // This variant is used in a `choice` with the others, and serves only to create a terminal node for explicit
    // braces.
    // If the scanner emitted the same symbol for virtual and explicit braces, we would either get an anonymous node
    // ranging over the brace, or a terminal brace node even for virtual starts if we were to alias the symbol to '{'
    // unconditionally.
    // So we use separate symbols and alias only this one.
    // The same reasoning applies to `_cond_layout_end_explicit`.
    // The terminal could be ensured in different ways – adding an `optional('{')` after the start symbol, using
    // `seq($._cmd_layout_start_explicit, '{')` instead of including the brace in the scanner range, or branching the
    // entire layout on the start token to unconditionally use `_cmd_brace_close` instead of
    // `_cond_layout_end_explicit`.
    // However, these solutions are all very expensive, adding between 500 and 1000kB to the shared object size, and up
    // to a second in generation time.
    $._cmd_layout_start_explicit,

    // Emitted when a new line's indent mandates ending the current layout (depending on the layout sort), or when a
    // special inline layout-ending token is encountered, like an `in`.
    $._cond_layout_end,
    $._cond_layout_end_explicit,

    // Instruct the scanner to push or pop a brace context.
    $._cmd_brace_open,
    $._cmd_brace_close,

    // Instruct the scanner to push or pop a tuple expression context.
    // In parenthesized or bracketed expressions, certain tokens (like commas, bars, and closing brackets), can end
    // layouts, so the scanner must be aware of them.
    $._cmd_texp_start,
    $._cmd_texp_end,

    // Signal to the scanner that these symbols are valid.
    // See the explanation of phantom symbols above.
    $._phantom_where,
    $._phantom_in,
    $._phantom_arrow,
    $._phantom_bar,
    $._phantom_deriving,

    // Detect and emit text nodes for comments and CPP.
    // In particular, #else branches of CPP conditionals a fully contained in the resulting node, since their nonlinear
    // nature means they cannot be parsed.
    $.comment,
    $.haddock,
    $.cpp,
    $.pragma,

    // Starting quote brackets are a bit messy, so it's easier to let the scanner signal that it encountered one.
    // The body produces a text node until the ending bracket.
    $._cond_quote_start,
    $.quasiquote_body,

    // Whitespace-sensitive operators for splices (`$`, `$$`), projection and module dots (`a.b`, `A.b`), arithmetic
    // sequence dots `[1..10]`, as-pattern (`a@(Just b)`), type application (`@Int`), strictness and laziness
    // annotations (`!pat`, `~Int`), and modifiers (`Int %1 -> Int`).
    $._cond_splice,
    $._cond_qual_dot,
    $._cond_tight_dot,
    $._cond_prefix_dot,
    $._cond_dotdot,
    $._cond_tight_at,
    $._cond_prefix_at,
    $._cond_tight_bang,
    $._cond_prefix_bang,
    $._cond_tight_tilde,
    $._cond_prefix_tilde,
    $._cond_prefix_percent,

    // GHC lexes all qualified names as atomic tokens, but we can't do that because we need nodes for their
    // substructures.
    // However, infix expressions need single-token lookahead for the operator to resolve conflicts, which is
    // impossible without an external.
    // This symbol detects a qualified symbolic operator.
    $._cond_qualified_op,
    // This one performs an additional lookahead check for a following closing parenthesis, to disambiguate left
    // sections from infix ops in `(a - b +)`.
    $._cond_left_section_op,
    // This is an auxiliary for `_cond_left_section_op` that allows restarting the scanner without that symbol being
    // valid again when whitespace was skipped after a symop to discover that there's no parenthesis.
    // Without this, we would get wrong token ranges for the operator.
    $._cond_no_section_op,

    // This symbol always succeeds when a minus is ahead.
    // Infix expressions with minus as the operator conflict with function application.
    // `a - b` can be parsed as `a (- b)`, which would normally be solved by a simple precedence entry, but is
    // impossible to express because it contradicts another conflict: Negation in the left operand, `- a + b`, which
    // must be parsed as `(- a) + b`.
    // Simply sequencing this external before the operator solves this conflict, because a minus following an expression
    // can never be negation; it can only occur at the beginning of an expression.
    $._cond_minus,

    /**
     * The following symbols perform lookahead for various type constructs to resolve conflicts between infix types,
     * contexts, and type/datacon heads, because using GLR conflicts for these is very brittle and frequently leads to
     * misparsed trees.
     *
     * See the documentation under 'Constraints' in `src/scanner.c` for more.
     */
    $._cond_context,
    $._cond_infix,
    $._cond_data_infix,
    $._cond_assoc_tyinst,

    // Symbolic operators, producing text nodes.
    // These are very difficult to parse in the grammar, because unlike most languages, Haskell's operators are not a
    // finite set, and therefore cannot be lexically disambiguated from special operators like `->`.
    // In the presence of runtime conflicts, this can easily lead to the invalid interpretation of reserved operators as
    // identifiers.
    // Furthermore, the regexes for all the unicode categories produce very large ternary operator trees if specified
    // in the grammar.
    $._varsym,
    $._consym,

    // The newline is used as a dummy symbol that is emitted whenever the scanner has to update its state even though
    // the current position must be parsed by the grammar.
    /\n/,

  ],

}
