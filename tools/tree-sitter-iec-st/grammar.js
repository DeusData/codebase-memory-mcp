/**
 * @file IEC 61131-3 Structured Text grammar for tree-sitter
 * @author HeytalePazguato <Heytale.Pazguato@gmail.com>
 * @license MIT
 *
 * Implements the standard IEC 61131-3 (3rd edition, 2013) Structured Text
 * language. Vendor dialects (TwinCAT, Codesys, B&R, Siemens, Rockwell) are
 * deferred to separate dialect-grammar repos that extend this base.
 *
 * Section comments tag rules with the IEC 61131-3 §x.y reference where
 * applicable.
 *
 * Acknowledgments — patterns adapted (NOT copied wholesale) from:
 *   - tmatijevich/tree-sitter-structured-text   (MIT)
 *   - teunreyniers/tree-sitter-structured-text  (MIT)
 *   - klauer/blark (Lark grammar)               (MIT)
 */

/// <reference types="tree-sitter-cli/dsl" />
// @ts-check

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Build a case-insensitive keyword token. Tree-sitter has no per-pattern case
 * flag for `RegExp`; we emit a character-class for each letter.
 *
 * Tree-sitter's lexer uses maximal-munch matching. When the input is a longer
 * identifier such as `IF_THEN_ELSE`, the `identifier` rule (`[A-Za-z_]...`)
 * matches more characters than the keyword regex and wins. When the input is
 * exactly `IF` (or a case variant), both would match the same length and the
 * keyword's `prec(2, …)` token-level precedence breaks the tie in favor of
 * the keyword. This obviates the lookahead-based word-boundary check that
 * tree-sitter's regex engine does not support.
 *
 * The token is aliased to its canonical (upper-case) name so the syntax tree
 * always shows the same lexeme regardless of source casing.
 */
export function kw(name) {
  const pattern = name
    .split('')
    .map((c) =>
      /[A-Za-z]/.test(c) ? `[${c.toLowerCase()}${c.toUpperCase()}]` : c,
    )
    .join('');
  return alias(token(prec(2, new RegExp(pattern))), name);
}

// Block/POU terminator keywords. These are reserved words recognized by the
// external scanner (src/scanner.c) rather than by `kw()` so they are lexed in
// every state, including error recovery. That lets tree-sitter keep the
// enclosing block when an inner terminator is missing and localize the error,
// instead of collapsing the whole POU into a single ERROR node. The order here
// MUST match the `enum TokenType` in src/scanner.c.
export const TERMINATORS = [
  'END_IF',
  'END_CASE',
  'END_FOR',
  'END_WHILE',
  'END_REPEAT',
  'END_VAR',
  'END_STRUCT',
  'END_TYPE',
  'END_PROGRAM',
  'END_FUNCTION',
  'END_FUNCTION_BLOCK',
  'END_INTERFACE',
  'END_METHOD',
  'END_PROPERTY',
  'END_GET',
  'END_SET',
  'END_NAMESPACE',
  'END_CONFIGURATION',
  'END_RESOURCE',
];

/**
 * Reference a reserved terminator keyword. Aliased to its canonical (upper-
 * case) name so the syntax tree and editor queries see `END_IF` etc. exactly
 * as they did when these were `kw()` tokens.
 */
function endkw($, name) {
  return alias($[`_${name.toLowerCase()}`], name);
}

/** Comma-separated list of `rule`, optionally trailing-comma-tolerant. */
function commaSep(rule) {
  return optional(commaSep1(rule));
}

function commaSep1(rule) {
  return seq(rule, repeat(seq(',', rule)));
}

// ---------------------------------------------------------------------------
// Precedence ladder — IEC 61131-3 §6.6.5 (Table 55 of the 3rd edition).
// Lower number = lower precedence (looser binding). All operators are
// left-associative except `**` (power), which is right-associative.
// ---------------------------------------------------------------------------

const PREC = {
  OR: 4,
  XOR: 5,
  AND: 6,
  EQUALITY: 7,
  COMPARE: 8,
  ADD: 9,
  MULTIPLY: 10,
  POWER: 11,
  UNARY: 12,
  POSTFIX: 13,
  PRIMARY: 14,
  // Statement-level disambiguation
  CALL_STATEMENT: 1,
  PARENTHESIZED: 1,
};

// ---------------------------------------------------------------------------
// Grammar
// ---------------------------------------------------------------------------

export default grammar({
  name: 'iec61131_3_st',

  word: ($) => $.identifier,

  extras: ($) => [/\s/, $.comment],

  // Reserved block/POU terminator keywords, lexed by src/scanner.c. Order must
  // match both the TERMINATORS list above and the enum in the scanner.
  externals: ($) => TERMINATORS.map((name) => $[`_${name.toLowerCase()}`]),

  // Supertype rules let editor queries match the family without listing each
  // variant. Hidden rules (leading `_`) are only exposed via supertypes for
  // tree-sitter ≥ 0.22 — earlier versions may need this list adjusted.
  supertypes: ($) => [
    $._declaration,
    $._statement,
    $._expression,
    $._type_specifier,
    $._var_block,
    $._literal,
    $._access_modifier,
  ],

  conflicts: ($) => [
    // After `case_value :` an identifier is ambiguous between the start of
    // the case body (a statement) and the start of the next case_clause.
    // tree-sitter's GLR-style parser disambiguates by looking ahead for the
    // trailing `:` of the next case_value.
    [$.case_clause],

    // Inside an array initializer, `2(3)` denotes array repetition. The same
    // surface shape inside a normal expression denotes a call. The shared
    // comma-separated tail (`a, b`) inside the parens forces a conflict on
    // the repeat-list rule.
    [$.array_repetition, $.argument_list],

    // A TwinCAT attribute pragma can sit above a POU member
    // (`{attribute 'monitoring' := 'variable'}` then PROPERTY) and can also be
    // a statement inside a body. Seeing `{...}` the parser cannot yet tell
    // which, so let GLR carry both until the following keyword decides.
    [$._statement, $.method_declaration, $.property_declaration],

    // `__CATCH(e)` looks like a call expression until the parser has consumed
    // the keyword, so both readings must stay alive one token longer.
    [$._expression, $.try_statement],
  ],

  rules: {
    // -----------------------------------------------------------------------
    // 1. Source file
    // -----------------------------------------------------------------------
    source_file: ($) => repeat($._top_level_item),

    _top_level_item: ($) =>
      choice(
        $._declaration,
        $.using_directive,
        $.pragma,
        ';', // tolerate stray top-level semicolons
      ),

    // -----------------------------------------------------------------------
    // 2. Comments and pragmas
    //    Standard ST: line `//`, block `(* … *)`. Block comments do NOT nest
    //    in the standard. `{ … }` is reserved for pragmas (compiler/tool
    //    directives) and is parsed as an opaque node.
    // -----------------------------------------------------------------------
    comment: ($) =>
      token(
        choice(
          seq('//', /[^\r\n]*/),
          seq('(*', /[^*]*\*+([^*)][^*]*\*+)*/, ')'),
        ),
      ),

    pragma: ($) => token(seq('{', /[^}]*/, '}')),

    // -----------------------------------------------------------------------
    // 3. Identifiers — IEC 61131-3 §6.1.2
    // -----------------------------------------------------------------------
    identifier: ($) => /[A-Za-z_][A-Za-z0-9_]*/,

    // Compile-time path used for namespace-qualified type, namespace, and
    // using references. Distinct from `member_access_expression`, which is a
    // runtime access; the two have identical surface syntax but appear in
    // disjoint grammatical positions.
    qualified_identifier: ($) =>
      seq($.identifier, repeat1(seq('.', $.identifier))),

    // Helper: where any path-shaped token is acceptable.
    _name: ($) => choice($.identifier, $.qualified_identifier),

    // -----------------------------------------------------------------------
    // 4. Literals — IEC 61131-3 §6.6.2
    // -----------------------------------------------------------------------
    _literal: ($) =>
      choice(
        $.boolean_literal,
        $.integer_literal,
        $.real_literal,
        $.string_literal,
        $.time_literal,
        $.date_literal,
        $.time_of_day_literal,
        $.date_and_time_literal,
        $.typed_literal,
      ),

    // 4.1 Booleans
    boolean_literal: ($) => choice(kw('TRUE'), kw('FALSE')),

    // 4.2 Integers — plain, base-prefixed, with underscore separators.
    integer_literal: ($) =>
      token(
        choice(
          // base-prefixed: 2#1010, 8#777, 16#FFFF, with optional `_` separators
          /[+-]?(2|8|16)#[0-9A-Fa-f]+(_[0-9A-Fa-f]+)*/,
          // plain decimal with optional underscores
          /[+-]?[0-9]+(_[0-9]+)*/,
        ),
      ),

    // 4.3 Reals — must contain `.` or exponent to disambiguate from integer.
    real_literal: ($) =>
      token(
        choice(
          /[+-]?[0-9]+(_[0-9]+)*\.[0-9]+(_[0-9]+)*([eE][+-]?[0-9]+)?/,
          /[+-]?[0-9]+(_[0-9]+)*[eE][+-]?[0-9]+/,
        ),
      ),

    // 4.4 Strings — IEC 61131-3 §6.6.2.5
    //     '…' single-byte STRING, "…" double-byte WSTRING.
    //     Escape sequences begin with `$`: $$, $', $", $L, $N, $P, $R, $T,
    //     $<2-hex> (1-byte) or $<4-hex> (WSTRING).
    string_literal: ($) =>
      token(
        choice(
          seq(
            "'",
            repeat(
              choice(
                /[^'$\r\n]/,
                /\$[$'"LlNnPpRrTt]/,
                /\$[0-9A-Fa-f]{2}/,
              ),
            ),
            "'",
          ),
          seq(
            '"',
            repeat(
              choice(
                /[^"$\r\n]/,
                /\$[$'"LlNnPpRrTt]/,
                /\$[0-9A-Fa-f]{4}/,
              ),
            ),
            '"',
          ),
        ),
      ),

    // 4.5 Time, date, time-of-day, date-and-time — IEC 61131-3 §6.6.2.6
    // Prefixes T#, TIME#, LTIME#, D#, DATE#, LDATE#, TOD#, TIME_OF_DAY#,
    // LTOD#, DT#, DATE_AND_TIME#, LDT#.
    time_literal: ($) =>
      token(
        seq(
          choice('T#', 't#', 'TIME#', 'time#', 'LTIME#', 'ltime#'),
          /[+-]?/,
          repeat1(/[0-9]+(\.[0-9]+)?(d|h|m|s|ms|us|ns)/i),
        ),
      ),

    date_literal: ($) =>
      token(
        seq(
          choice('D#', 'd#', 'DATE#', 'date#', 'LDATE#', 'ldate#'),
          /[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}/,
        ),
      ),

    time_of_day_literal: ($) =>
      token(
        seq(
          choice(
            'TOD#',
            'tod#',
            'TIME_OF_DAY#',
            'time_of_day#',
            'LTOD#',
            'ltod#',
            'LTIME_OF_DAY#',
            'ltime_of_day#',
          ),
          /[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}(\.[0-9]+)?/,
        ),
      ),

    date_and_time_literal: ($) =>
      token(
        seq(
          choice(
            'DT#',
            'dt#',
            'DATE_AND_TIME#',
            'date_and_time#',
            'LDT#',
            'ldt#',
            'LDATE_AND_TIME#',
            'ldate_and_time#',
          ),
          /[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}-[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}(\.[0-9]+)?/,
        ),
      ),

    // 4.6 Typed literals — `TYPE#value`, e.g. INT#16, REAL#3.14, BOOL#TRUE.
    //     Time/date typed literals already match above via their prefix; this
    //     covers numeric, boolean, and string variants.
    typed_literal: ($) =>
      seq(
        field('type', $.identifier),
        token.immediate('#'),
        field(
          'value',
          choice(
            $.integer_literal,
            $.real_literal,
            $.boolean_literal,
            $.string_literal,
          ),
        ),
      ),

    // -----------------------------------------------------------------------
    // 5. Type specifiers — IEC 61131-3 §6.4
    // -----------------------------------------------------------------------
    _type_specifier: ($) =>
      choice(
        $.elementary_type,
        $.generic_type,
        $.string_type,
        $.array_type,
        $.pointer_type,
        $.reference_type,
        $.subrange_type,
        $.enumerated_type_inline,
        $.structure_type_inline,
        $._name, // user-defined type / qualified
      ),

    // 5.1 Elementary types — §6.4.1
    elementary_type: ($) =>
      choice(
        kw('BOOL'),
        kw('BYTE'),
        kw('WORD'),
        kw('DWORD'),
        kw('LWORD'),
        kw('SINT'),
        kw('USINT'),
        kw('INT'),
        kw('UINT'),
        kw('DINT'),
        kw('UDINT'),
        kw('LINT'),
        kw('ULINT'),
        kw('REAL'),
        kw('LREAL'),
        kw('TIME'),
        kw('LTIME'),
        kw('DATE'),
        kw('LDATE'),
        kw('TIME_OF_DAY'),
        kw('TOD'),
        kw('LTIME_OF_DAY'),
        kw('LTOD'),
        kw('DATE_AND_TIME'),
        kw('DT'),
        kw('LDATE_AND_TIME'),
        kw('LDT'),
        kw('CHAR'),
        kw('WCHAR'),
      ),

    // 5.2 Generic types — §6.4.4 (used in function/method signatures).
    generic_type: ($) =>
      choice(
        kw('ANY'),
        kw('ANY_DERIVED'),
        kw('ANY_ELEMENTARY'),
        kw('ANY_MAGNITUDE'),
        kw('ANY_NUM'),
        kw('ANY_REAL'),
        kw('ANY_INT'),
        kw('ANY_BIT'),
        kw('ANY_STRING'),
        kw('ANY_DATE'),
        kw('ANY_CHAR'),
        kw('ANY_CHARS'),
      ),

    // 5.3 String type with optional length — §6.4.1
    //     `prec.left` biases the parser toward consuming the optional `(N)`
    //     length specifier when present.
    string_type: ($) =>
      prec.left(
        1,
        seq(
          choice(kw('STRING'), kw('WSTRING')),
          optional(seq('(', field('length', $._expression), ')')),
        ),
      ),

    // 5.4 Array type — §6.4.3.2
    //     A trailing `:= initializer` is captured by surrounding context
    //     (type_definition / variable_declaration), not here, to avoid a
    //     parse conflict.
    array_type: ($) =>
      seq(
        kw('ARRAY'),
        '[',
        commaSep1(field('range', $.subrange)),
        ']',
        kw('OF'),
        field('element_type', $._type_specifier),
      ),

    subrange: ($) =>
      seq(
        field('lower', $._expression),
        '..',
        field('upper', $._expression),
      ),

    _array_initializer: ($) =>
      seq(
        '[',
        commaSep1(choice($._expression, $.array_repetition)),
        ']',
      ),

    // Inside an array initializer, `2(3)` denotes "three repeated twice", not
    // a call. The higher precedence biases the parser toward array_repetition
    // when it could also be parsed as a call expression.
    array_repetition: ($) =>
      prec(
        2,
        seq(field('count', $._expression), '(', commaSep1($._expression), ')'),
      ),

    // 5.5 Pointer / reference types — §6.4.5
    pointer_type: ($) =>
      seq(kw('POINTER'), kw('TO'), field('target', $._type_specifier)),

    reference_type: ($) =>
      seq(
        // `REF_TO` is the standard spelling; TwinCAT writes `REFERENCE TO`.
        choice(kw('REF_TO'), seq(kw('REFERENCE'), kw('TO'))),
        field('target', $._type_specifier),
      ),

    // 5.6 Subrange (named) type — §6.4.3.1
    //     `prec.left(1)` biases the parser toward consuming `(lower..upper)`
    //     after an elementary type when both alternatives could apply.
    subrange_type: ($) =>
      prec.left(
        1,
        seq(
          $.elementary_type,
          '(',
          field('lower', $._expression),
          '..',
          field('upper', $._expression),
          ')',
        ),
      ),

    // 5.7 Enumerated type — §6.4.3.3
    //     A trailing `:= default` is captured by the surrounding context
    //     (type_definition's default or variable_declaration's
    //     initial_value), not here, to avoid a parse conflict.
    // A base type may follow the enumerator list (`(Idle, Run) UINT;`) to fix the
    // representation — common in TwinCAT PackML state enums.
    enumerated_type_inline: ($) =>
      seq(
        '(',
        commaSep1($.enumerator),
        ')',
        optional(field('base_type', $.elementary_type)),
      ),

    enumerator: ($) =>
      seq(
        field('name', $.identifier),
        optional(seq(':=', field('value', $._expression))),
      ),

    // 5.8 Structure type — §6.4.3.4
    structure_type_inline: ($) =>
      seq(kw('STRUCT'), repeat1($.structure_field), endkw($, 'END_STRUCT')),

    structure_field: ($) =>
      seq(
        repeat($.pragma),
        field('name', $.identifier),
        // A DUT field can carry a located-variable address just like a VAR
        // entry (`bDone AT %I* : BOOL;`, spaced or glued).
        optional(
          choice(
            seq(kw('AT'), field('address', $.direct_address)),
            field('address', $.at_glued_address),
          ),
        ),
        ':',
        field('type', $._type_specifier),
        optional(seq(':=', field('default', $._expression))),
        ';',
      ),

    // -----------------------------------------------------------------------
    // 6. Variable declarations — IEC 61131-3 §6.5
    // -----------------------------------------------------------------------
    _var_block: ($) =>
      choice(
        $.var_block,
        $.var_input,
        $.var_output,
        $.var_in_out,
        $.var_temp,
        $.var_global,
        $.var_external,
        $.var_access,
        $.var_config,
      ),

    var_block: ($) =>
      seq(
        kw('VAR'),
        optional($.var_qualifier_list),
        repeat($._var_decl_line),
        endkw($, 'END_VAR'),
      ),
    var_input: ($) =>
      seq(
        kw('VAR_INPUT'),
        optional($.var_qualifier_list),
        repeat($._var_decl_line),
        endkw($, 'END_VAR'),
      ),
    var_output: ($) =>
      seq(
        kw('VAR_OUTPUT'),
        optional($.var_qualifier_list),
        repeat($._var_decl_line),
        endkw($, 'END_VAR'),
      ),
    var_in_out: ($) =>
      seq(
        kw('VAR_IN_OUT'),
        optional($.var_qualifier_list),
        repeat($._var_decl_line),
        endkw($, 'END_VAR'),
      ),
    var_temp: ($) =>
      seq(
        kw('VAR_TEMP'),
        optional($.var_qualifier_list),
        repeat($._var_decl_line),
        endkw($, 'END_VAR'),
      ),
    var_global: ($) =>
      seq(
        kw('VAR_GLOBAL'),
        optional($.var_qualifier_list),
        repeat($._var_decl_line),
        endkw($, 'END_VAR'),
      ),
    var_external: ($) =>
      seq(
        kw('VAR_EXTERNAL'),
        optional($.var_qualifier_list),
        repeat($._var_decl_line),
        endkw($, 'END_VAR'),
      ),
    var_access: ($) =>
      seq(
        kw('VAR_ACCESS'),
        repeat($.access_declaration),
        endkw($, 'END_VAR'),
      ),
    var_config: ($) =>
      seq(
        kw('VAR_CONFIG'),
        repeat($.instance_specific_init),
        endkw($, 'END_VAR'),
      ),

    var_qualifier_list: ($) => repeat1($.var_qualifier),

    var_qualifier: ($) =>
      choice(kw('CONSTANT'), kw('RETAIN'), kw('NON_RETAIN')),

    _var_decl_line: ($) =>
      // A stray `;` occurs in real projects (`x : REAL;;`) and TwinCAT accepts
      // it; tolerating it keeps one typo from costing a whole POU's symbols.
      choice($.variable_declaration, $.pragma, ';'),

    // §6.5.1 — name {, name} : type [AT %addr] [:= init] ;
    variable_declaration: ($) =>
      seq(
        field('names', commaSep1($.identifier)),
        // TwinCAT also writes the located-variable marker without a space
        // (`bDone AT%I* : BOOL;`), so the address token carries an optional
        // glued `AT` prefix of its own.
        optional(
          choice(
            seq(kw('AT'), field('address', $.direct_address)),
            field('address', $.at_glued_address),
          ),
        ),
        ':',
        // `fbInst : FB_Type(...)` — TwinCAT's shorthand for instantiating a
        // function block with an (optionally empty) argument list, where the
        // standard requires `:= (...)`. Restricted to a user-defined type name:
        // allowing it after any type specifier makes `s : STRING(255)` ambiguous
        // between a string length and an instance argument list.
        choice(
          seq(
            field('type', $._name),
            field('instance_init', $.instance_initializer),
          ),
          field('type', $._type_specifier),
        ),
        optional(seq(':=', field('initial_value', $._initializer))),
        ';',
      ),

    instance_initializer: ($) =>
      seq(
        '(',
        optional(
          commaSep1(choice($.structure_initializer_field, $._expression)),
        ),
        ')',
      ),

    _initializer: ($) =>
      choice(
        $._expression,
        $._array_initializer,
        $.structure_initializer,
      ),

    structure_initializer: ($) =>
      seq('(', commaSep1($.structure_initializer_field), ')'),

    structure_initializer_field: ($) =>
      seq(
        field('name', $.identifier),
        ':=',
        field('value', $._expression),
      ),

    // §6.5.5.2 — %{I,Q,M}{X,B,W,D,L}<address>
    at_glued_address: ($) =>
      token(/[aA][tT]%[IQMiqm][XBWDLxbwdl]?(\*|[0-9]+(\.[0-9]+)*)/),

    direct_address: ($) =>
      // TwinCAT leaves the offset to the compiler with `AT %I*` / `%Q*`; the
      // standard form carries explicit numbers.
      token(/%[IQM][XBWDL]?(\*|[0-9]+(\.[0-9]+)*)/),

    access_declaration: ($) =>
      seq(
        field('name', $.identifier),
        ':',
        field('path', $._name),
        ':',
        field('type', $._type_specifier),
        optional($.access_direction),
        ';',
      ),

    access_direction: ($) =>
      choice(kw('READ_WRITE'), kw('READ_ONLY')),

    instance_specific_init: ($) =>
      seq(
        field('path', $._name),
        ':',
        field('type', $._type_specifier),
        ':=',
        field('value', $._expression),
        ';',
      ),

    // -----------------------------------------------------------------------
    // 7. Expressions — IEC 61131-3 §6.6.5
    //    Precedence and associativity follow Table 55.
    // -----------------------------------------------------------------------
    _expression: ($) =>
      choice(
        $._literal,
        $.parenthesized_expression,
        $.call_expression,
        $.index_expression,
        $.member_access_expression,
        $.dereference_expression,
        $.address_of_expression,
        $.unary_expression,
        $.binary_expression,
        $.this_expression,
        $.super_expression,
        $.identifier, // chains form via member_access_expression
      ),

    parenthesized_expression: ($) =>
      prec(PREC.PARENTHESIZED, seq('(', $._expression, ')')),

    this_expression: ($) => kw('THIS'),
    super_expression: ($) => kw('SUPER'),

    call_expression: ($) =>
      prec(
        PREC.POSTFIX,
        seq(
          field('function', $._expression),
          field('arguments', $.argument_list),
        ),
      ),

    argument_list: ($) =>
      seq('(', commaSep(choice($.named_argument, $._expression)), ')'),

    named_argument: ($) =>
      seq(
        field('name', $.identifier),
        choice(':=', '=>'),
        field('value', $._expression),
      ),

    index_expression: ($) =>
      prec(
        PREC.POSTFIX,
        seq(
          field('object', $._expression),
          '[',
          field('index', commaSep1($._expression)),
          ']',
        ),
      ),

    member_access_expression: ($) =>
      prec.left(
        PREC.POSTFIX,
        seq(
          field('object', $._expression),
          '.',
          // `wError.0` — bit-in-word access, spelled with a numeric member.
          field('member', choice($.identifier, $.integer_literal)),
        ),
      ),

    dereference_expression: ($) =>
      prec(PREC.POSTFIX, seq(field('pointer', $._expression), '^')),

    address_of_expression: ($) =>
      seq(
        kw('ADR'),
        '(',
        field('operand', $._expression),
        ')',
      ),

    unary_expression: ($) =>
      prec.right(
        PREC.UNARY,
        seq(
          field('operator', choice('-', '+', kw('NOT'))),
          field('operand', $._expression),
        ),
      ),

    binary_expression: ($) => {
      const table = [
        [PREC.OR, choice(kw('OR'))],
        [PREC.XOR, choice(kw('XOR'))],
        [PREC.AND, choice(kw('AND'), '&')],
        [PREC.EQUALITY, choice('=', '<>')],
        [PREC.COMPARE, choice('<', '>', '<=', '>=')],
        [PREC.ADD, choice('+', '-')],
        [PREC.MULTIPLY, choice('*', '/', kw('MOD'))],
      ];
      const left = table.map(([precedence, operator]) =>
        prec.left(
          precedence,
          seq(
            field('left', $._expression),
            field('operator', operator),
            field('right', $._expression),
          ),
        ),
      );
      // Power is right-associative — IEC 61131-3 §6.6.5
      const power = prec.right(
        PREC.POWER,
        seq(
          field('left', $._expression),
          field('operator', '**'),
          field('right', $._expression),
        ),
      );
      return choice(...left, power);
    },

    // -----------------------------------------------------------------------
    // 8. Statements — IEC 61131-3 §6.6.4
    // -----------------------------------------------------------------------
    _statement: ($) =>
      choice(
        $.assignment_statement,
        $.reference_assignment_statement,
        $.invocation_statement,
        $.if_statement,
        $.case_statement,
        $.for_statement,
        $.while_statement,
        $.repeat_statement,
        $.exit_statement,
        $.continue_statement,
        $.return_statement,
        $.empty_statement,
        $.pragma, // pragmas may appear anywhere a statement may appear
        $.try_statement,
      ),

    // TwinCAT extension: structured exception handling. Not in IEC 61131-3, but
    // it appears in real projects and an unparsed __TRY costs the whole POU.
    try_statement: ($) =>
      seq(
        kw('__TRY'),
        optional($._statement_list),
        repeat(
          seq(
            kw('__CATCH'),
            optional(seq('(', field('exception', $.identifier), ')')),
            optional($._statement_list),
          ),
        ),
        optional(seq(kw('__FINALLY'), optional($._statement_list))),
        kw('__ENDTRY'),
      ),

    _statement_list: ($) => repeat1($._statement),

    // The left-hand side of an assignment is syntactically any expression;
    // semantic assignability (rejecting `1 + 2 := x`) is enforced downstream.
    assignment_statement: ($) =>
      seq(
        field('left', $._expression),
        ':=',
        field('right', $._expression),
        ';',
      ),

    reference_assignment_statement: ($) =>
      seq(
        field('left', $._expression),
        $.ref_assign,
        field('right', $._expression),
        ';',
      ),

    ref_assign: ($) => token(/[Rr][Ee][Ff]=/),

    // An invocation statement is just a call expression terminated by `;`.
    invocation_statement: ($) =>
      prec(PREC.CALL_STATEMENT, seq($.call_expression, ';')),

    empty_statement: ($) => ';',

    // 8.1 IF
    if_statement: ($) =>
      seq(
        kw('IF'),
        field('condition', $._expression),
        kw('THEN'),
        field('consequence', optional($._statement_list)),
        repeat($.elsif_clause),
        optional($.else_clause),
        endkw($, 'END_IF'),
      ),

    elsif_clause: ($) =>
      seq(
        kw('ELSIF'),
        field('condition', $._expression),
        kw('THEN'),
        field('body', optional($._statement_list)),
      ),

    else_clause: ($) =>
      seq(kw('ELSE'), field('body', optional($._statement_list))),

    // 8.2 CASE
    case_statement: ($) =>
      seq(
        kw('CASE'),
        field('subject', $._expression),
        kw('OF'),
        repeat1($.case_clause),
        optional($.else_clause),
        endkw($, 'END_CASE'),
      ),

    case_clause: ($) =>
      seq(
        commaSep1($.case_value),
        ':',
        field('body', optional($._statement_list)),
      ),

    case_value: ($) =>
      choice(
        $._expression,
        $.subrange,
      ),

    // 8.3 FOR
    for_statement: ($) =>
      seq(
        kw('FOR'),
        field('control', $.identifier),
        ':=',
        field('start', $._expression),
        kw('TO'),
        field('end', $._expression),
        optional(seq(kw('BY'), field('step', $._expression))),
        kw('DO'),
        field('body', optional($._statement_list)),
        endkw($, 'END_FOR'),
      ),

    // 8.4 WHILE
    while_statement: ($) =>
      seq(
        kw('WHILE'),
        field('condition', $._expression),
        kw('DO'),
        field('body', optional($._statement_list)),
        endkw($, 'END_WHILE'),
      ),

    // 8.5 REPEAT
    repeat_statement: ($) =>
      seq(
        kw('REPEAT'),
        field('body', optional($._statement_list)),
        kw('UNTIL'),
        field('condition', $._expression),
        endkw($, 'END_REPEAT'),
      ),

    exit_statement: ($) => seq(kw('EXIT'), ';'),
    continue_statement: ($) => seq(kw('CONTINUE'), ';'),
    return_statement: ($) =>
      seq(
        kw('RETURN'),
        optional(field('value', $._expression)),
        ';',
      ),

    // -----------------------------------------------------------------------
    // 9. Program Organization Units (POUs) — IEC 61131-3 §6.7
    // -----------------------------------------------------------------------
    _declaration: ($) =>
      choice(
        $.program_declaration,
        $.function_declaration,
        $.function_block_declaration,
        $.interface_declaration,
        $.type_declaration,
        $.namespace_declaration,
        $.configuration_declaration,
        $.global_var_declaration_block,
      ),

    // §6.7.2 — PROGRAM
    program_declaration: ($) =>
      seq(
        kw('PROGRAM'),
        repeat($._pou_modifier),
        field('name', $.identifier),
        repeat($._var_block),
        field('body', optional($._statement_list)),
        endkw($, 'END_PROGRAM'),
      ),

    // §6.7.1 — FUNCTION (with return type)
    function_declaration: ($) =>
      seq(
        kw('FUNCTION'),
        repeat($._pou_modifier),
        field('name', $.identifier),
        ':',
        field('return_type', $._type_specifier),
        repeat($._var_block),
        field('body', optional($._statement_list)),
        endkw($, 'END_FUNCTION'),
      ),

    // §6.7.3 — FUNCTION_BLOCK with optional EXTENDS / IMPLEMENTS / methods.
    function_block_declaration: ($) =>
      seq(
        optional($._fb_modifier),
        kw('FUNCTION_BLOCK'),
        repeat($._pou_modifier),
        field('name', $.identifier),
        optional(
          seq(kw('EXTENDS'), field('extends', $._name)),
        ),
        optional(
          seq(
            kw('IMPLEMENTS'),
            field('implements', commaSep1($._name)),
          ),
        ),
        repeat($._var_block),
        repeat($._fb_member),
        field('body', optional($._statement_list)),
        endkw($, 'END_FUNCTION_BLOCK'),
      ),

    _fb_modifier: ($) => choice(kw('ABSTRACT'), kw('FINAL')),

    // TwinCAT / CODESYS write the access and inheritance modifiers AFTER the POU
    // keyword (`FUNCTION_BLOCK PUBLIC FINAL FB_X`), where the standard puts them
    // before it. Accept a run of them in that position too; the modifiers stay
    // hidden so `name` keeps pointing at the identifier.
    _pou_modifier: ($) =>
      choice(
        kw('PUBLIC'),
        kw('PRIVATE'),
        kw('PROTECTED'),
        kw('INTERNAL'),
        kw('ABSTRACT'),
        kw('FINAL'),
      ),

    _fb_member: ($) => choice($.method_declaration, $.property_declaration),

    // §6.7.4 — INTERFACE
    interface_declaration: ($) =>
      seq(
        kw('INTERFACE'),
        repeat($._pou_modifier),
        field('name', $.identifier),
        optional(
          seq(kw('EXTENDS'), field('extends', commaSep1($._name))),
        ),
        repeat($.method_signature),
        repeat($.property_signature),
        endkw($, 'END_INTERFACE'),
      ),

    method_signature: ($) =>
      seq(
        // TwinCAT puts attribute pragmas directly above a member.
        repeat($.pragma),
        kw('METHOD'),
        optional($._access_modifier),
        field('name', $.identifier),
        optional(
          seq(':', field('return_type', $._type_specifier)),
        ),
        // TwinCAT terminates an interface prototype's signature line with `;`
        // and still lists its VAR blocks after it.
        optional(';'),
        repeat($._var_block),
        // TwinCAT writes an interface method as a one-line prototype terminated
        // by `;`; the standard closes it with END_METHOD.
        choice(endkw($, 'END_METHOD'), ';'),
      ),

    // Interface property — same shape as a class property accessor; the body
    // is allowed but typically empty in interface declarations.
    property_signature: ($) =>
      seq(
        // TwinCAT puts attribute pragmas directly above a member.
        repeat($.pragma),
        kw('PROPERTY'),
        optional($._access_modifier),
        field('name', $.identifier),
        ':',
        field('type', $._type_specifier),
        repeat($.property_accessor),
        endkw($, 'END_PROPERTY'),
      ),

    // §6.7.3 (3rd ed addenda) — METHOD inside FUNCTION_BLOCK / CLASS
    method_declaration: ($) =>
      seq(
        // TwinCAT puts attribute pragmas directly above a member.
        repeat($.pragma),
        kw('METHOD'),
        optional($._method_modifier),
        optional($._access_modifier),
        field('name', $.identifier),
        optional(
          seq(':', field('return_type', $._type_specifier)),
        ),
        repeat($._var_block),
        field('body', optional($._statement_list)),
        endkw($, 'END_METHOD'),
      ),

    _method_modifier: ($) => choice(kw('ABSTRACT'), kw('FINAL'), kw('OVERRIDE')),

    // PROPERTY with GET / SET accessor bodies
    property_declaration: ($) =>
      seq(
        // TwinCAT puts attribute pragmas directly above a member.
        repeat($.pragma),
        kw('PROPERTY'),
        optional($._access_modifier),
        field('name', $.identifier),
        ':',
        field('type', $._type_specifier),
        repeat1($.property_accessor),
        endkw($, 'END_PROPERTY'),
      ),

    property_accessor: ($) =>
      seq(
        choice(kw('GET'), kw('SET')),
        repeat($._var_block),
        field('body', optional($._statement_list)),
        choice(endkw($, 'END_GET'), endkw($, 'END_SET')),
      ),

    _access_modifier: ($) =>
      choice(
        kw('PUBLIC'),
        kw('PRIVATE'),
        kw('PROTECTED'),
        kw('INTERNAL'),
      ),

    // §6.7.5 — TYPE … END_TYPE
    type_declaration: ($) =>
      seq(kw('TYPE'), repeat1($.type_definition), endkw($, 'END_TYPE')),

    type_definition: ($) =>
      seq(
        field('name', $.identifier),
        ':',
        field('definition', $._type_specifier),
        optional(seq(':=', field('default', $._expression))),
        // TwinCAT omits the terminator after a block-shaped definition
        // (`END_STRUCT` / `END_UNION` then straight to `END_TYPE`), where the
        // standard always requires `;`.
        optional(';'),
      ),

    // §6.7.6 — Namespaces (3rd edition addition)
    namespace_declaration: ($) =>
      seq(
        optional(kw('INTERNAL')),
        kw('NAMESPACE'),
        field('name', $._name),
        repeat($._top_level_item),
        endkw($, 'END_NAMESPACE'),
      ),

    using_directive: ($) =>
      seq(kw('USING'), commaSep1(field('namespace', $._name)), ';'),

    // §6.8 — CONFIGURATION / RESOURCE
    configuration_declaration: ($) =>
      seq(
        kw('CONFIGURATION'),
        field('name', $.identifier),
        repeat($._var_block),
        repeat($.resource_declaration),
        endkw($, 'END_CONFIGURATION'),
      ),

    resource_declaration: ($) =>
      seq(
        kw('RESOURCE'),
        field('name', $.identifier),
        kw('ON'),
        field('cpu_type', $.identifier),
        repeat($._var_block),
        repeat($.task_declaration),
        repeat($.program_assignment),
        endkw($, 'END_RESOURCE'),
      ),

    task_declaration: ($) =>
      seq(
        kw('TASK'),
        field('name', $.identifier),
        '(',
        commaSep1($.task_parameter),
        ')',
        ';',
      ),

    task_parameter: ($) =>
      seq(field('name', $.identifier), ':=', field('value', $._expression)),

    program_assignment: ($) =>
      seq(
        kw('PROGRAM'),
        field('name', $.identifier),
        optional(seq(kw('WITH'), field('task', $.identifier))),
        ':',
        field('program_type', $._name),
        optional($.argument_list),
        ';',
      ),

    // Stand-alone VAR_GLOBAL block at top level (outside CONFIGURATION).
    global_var_declaration_block: ($) => $.var_global,
  },
});
