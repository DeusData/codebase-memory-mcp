/**
 * @file Shared grammar core for the Visual Basic dialect family (VBA, VB6)
 * @author harumiWeb (VBA grammar), VB6 dialect layer added on top
 * @license MIT
 *
 * One grammar factory, two entry points: `grammar.js` (vba) and
 * `vb6/grammar.js` (vb6) each call `defineGrammar(dialect)`. Everything at
 * statement level lives here once. Dialect deltas are the `isVBA` / `isVB6`
 * branches below and nothing else; a reviewer can grep for them.
 */

/// <reference types="tree-sitter-cli/dsl" />
// @ts-check

const DIALECTS = ["vba", "vb6"];

/**
 * @param {"vba" | "vb6"} dialect
 */
module.exports = function defineGrammar(dialect) {
  if (!DIALECTS.includes(dialect)) {
    throw new Error(`Unknown dialect "${dialect}"; expected one of ${DIALECTS.join(", ")}`);
  }
  const isVBA = dialect === "vba";
  const isVB6 = dialect === "vb6";

  // VB6 is whitespace-sensitive around the member operator: `Foo .Bar, x` is a
  // call to Foo with two arguments, `Foo.Bar , x` a call to Foo.Bar with an
  // omitted first argument. A CFG cannot see the space, so vb6/src/scanner.c
  // emits distinct tokens for a dot or bang that touches the previous token
  // (immediate) and one that follows whitespace (spaced). Chains use only the
  // immediate kind; an implicit member (`.Caption = 1`, `Foo .Bar`) may start
  // with either. VBA keeps the plain tokens and the base's behaviour.
  const dot = ($) => (isVB6 ? $._dot_immediate : ".");
  const bang = ($) => (isVB6 ? $._bang_immediate : "!");
  const memberOperator = ($) =>
    isVB6 ? choice($._dot_immediate, $._bang_immediate) : choice(".", "!");
  const implicitOperator = ($) =>
    isVB6
      ? choice($._dot_immediate, $._dot_spaced, $._bang_immediate, $._bang_spaced)
      : choice(".", "!");
  // VB6 anchors `#If` / `#ElseIf` to a following blank; VBA keeps the base's token.
  const preprocessorKeyword = (word) =>
    isVB6 ? token(seq(caseInsensitive(word), /[ \t]/)) : caseInsensitive(word);

  // The vba grammar retains the bang_identifier node and unsplit omitted-argument
  // lists. VB6-specific syntax remains behind isVB6 where its parse-table cost
  // would otherwise push the browser Wasm beyond the size gate. VBA accepts
  // Global, Let, and On Local Error, so those forms are shared by both dialects.

  return grammar({
    name: dialect,

    extras: ($) => [/[ \t\f]/, $.line_continuation, $.comment],

    externals: ($) =>
      isVB6 ? [$._dot_immediate, $._dot_spaced, $._bang_immediate, $._bang_spaced] : [],

    word: ($) => $.identifier,

    // Keep the permissive identifier vocabulary globally, but reserve
    // declaration keywords where a variable declarator name is expected.
    reserved: {
      global: (_) => [],
      variable_declarator: ($) => [$._dim_keyword, $._redim_keyword],
    },

    supertypes: ($) => [$.member_expression],

    conflicts: ($) => [
      [$._primary_expression, $._callable_expression],
      [$._expression, $._comparison_operand],
      [$._comparison_operand, $.unary_expression],
      [$._argument, $.parenthesized_expression],
      [$._condition_expression, $.parenthesized_expression],
      [$._argument_sequence],
      [$.block],
      [$.case_clause],
      [$.property_get_declaration, $._property_header],
      [$.property_let_declaration, $._property_header],
      [$.property_set_declaration, $._property_header],
      [$._statement, $._multiline_for_tail],
      [$._inline_statement, $.shared_next_for_body],
      ...(isVB6
        ? [[$.single_line_block, $._colon_for_inline_statement_sequence]]
        : [[$._colon_prefixed_for_block, $._colon_for_inline_statement_sequence]]),
      [$.goto_statement],
      [$._statement_separator, $._conditional_sub_headers],
      [$._statement_separator, $._conditional_function_headers],
      [$._print_output_expression, $._primary_expression],
      [$._unparenthesized_print_output_expression, $._callable_expression],
      [$._unparenthesized_print_output_expression, $._primary_expression],
      [$._unparenthesized_print_output_expression, $._expression],
      [$._unparenthesized_print_output_expression, $._print_output_call_operand],
      // VB6 only. `a.b!` type suffix versus `a.b!c` bang member access; see
      // _member_bang_suffixed_identifier.
      ...(isVB6 ? [[$._member_property, $._member_bang_suffixed_identifier]] : []),
      // VB6 only. `Foo .Bar` with no further arguments is the same tree under the
      // spaced-first-argument alternative and the generic call; GLR keeps both and
      // the alternative's prec.dynamic 2 settles it.
      ...(isVB6 ? [[$._spaced_implicit_member_expression, $.implicit_member_expression]] : []),
      // VB6 only. `Line (x1, y1)-(x2, y2)` and the identifier `Line` called with
      // arguments look identical up to the `)`; the coordinate pair is not an
      // expression, so GLR drops the call reading at the `-`, and prec.dynamic on
      // line_statement breaks the tie for the one-pair prefix `Line (a, b)`.
      ...(isVB6 ? [[$.line_statement, $._primary_expression]] : []),
      // VB6 only. `Foo a.b, c` / `Foo f(x), c`: the first argument can be read as the
      // implicit-member-first list or as an ordinary argument; the trees are the same
      // shape and prec.dynamic picks the former, which is the one that also covers
      // `Foo .Bar, c` where the ordinary reading would chain `.Bar` onto `Foo`.
    ],

    rules: {
      source_file: ($) => repeat($._top_level_item),

      _top_level_item: ($) =>
        choice(
          $.newline,
          $.line_number_top_level_item,
          $.frm_version_statement,
          $.frm_begin_block,
          $.frm_begin_property_block,
          $.frm_property_statement,
          $.preprocessor_const,
          $.preprocessor_if,
          $.attribute_statement,
          $.option_statement,
          $.implements_statement,
          $.def_type_statement,
          $.type_declaration,
          $.enum_declaration,
          $.declare_sub_statement,
          $.declare_function_statement,
          $.conditional_sub_declaration,
          $.conditional_function_declaration,
          $.conditional_property_declaration,
          $.sub_declaration,
          $.function_declaration,
          $.property_get_declaration,
          $.property_let_declaration,
          $.property_set_declaration,
          $.event_declaration,
          $.const_declaration,
          $.variable_declaration,
        ),

      newline: (_) => /\r?\n/,

      _statement_separator: ($) => choice($.newline, ":"),

      line_continuation: (_) => token(seq("_", /[ \t]*\r?\n/)),

      // `Rem-...` is accepted by the IDE, so Rem may be followed by any non-word char.
      // VB6 only: a comment whose line ends in ` _` continues onto the next line, as
      // in `' Declare Function X Lib "y" ( _` / `ByRef p() As Any) As Long`, which
      // three unrelated corpus projects rely on. The VBA parser keeps the base rule.
      comment: (_) =>
        isVB6
          ? token(
              choice(
                seq("'", /(?:[^\r\n]*[ \t]_[ \t]*\r?\n)*[^\r\n]*/),
                seq(caseInsensitive("Rem"), /(?:[^A-Za-z0-9_\r\n](?:[^\r\n]*[ \t]_[ \t]*\r?\n)*[^\r\n]*)?/),
              ),
            )
          : token(choice(seq("'", /.*/), seq(caseInsensitive("Rem"), /([ \t].*)?/))),

      frm_version_statement: ($) =>
        prec.right(seq(caseInsensitive("VERSION"), $.number_literal, optional($.identifier))),

      // `Begin VB.Form Form1` (VB5/6), `Begin {GUID} UserForm1` (VBA designer),
      // `BEGIN` alone (VB6 .cls header). VB6 additionally accepts the VB3/VB4-era
      // `Begin Form Form1` where the type is a bare identifier.
      frm_begin_block: ($) =>
        seq(
          caseInsensitive("Begin"),
          // VB6 also allows a bare identifier as the type, so a type must be followed
          // by a name or `Begin Foo` is ambiguous. VBA keeps the base's independent
          // optionals.
          ...(isVB6
            ? [
                optional(
                  choice(
                    seq(
                      field("type", choice($.member_expression, $.guid_literal, $.identifier)),
                      field("name", $.identifier),
                    ),
                    field("name", $.identifier),
                  ),
                ),
              ]
            : [
                optional(field("type", choice($.member_expression, $.guid_literal))),
                optional(field("name", $.identifier)),
              ]),
          $._statement_separator,
          repeat(
            choice(
              $.newline,
              $.frm_property_statement,
              $.frm_begin_block,
              $.frm_begin_property_block,
            ),
          ),
          caseInsensitive("End"),
        ),

      frm_begin_property_block: ($) =>
        seq(
          caseInsensitive("BeginProperty"),
          field("name", $.identifier),
          ...(isVB6
            ? [
                optional(field("index", $.frm_property_index)),
                optional(field("guid", $.guid_literal)),
              ]
            : []),
          $._statement_separator,
          repeat(choice($.newline, $.frm_property_statement, $.frm_begin_property_block)),
          caseInsensitive("EndProperty"),
        ),

      frm_property_statement: ($) =>
        seq(
          field(
            "name",
            choice(
              $.identifier,
              alias(caseInsensitive("Name"), $.identifier),
              // VB6 property bags index by control: `TabCaption(0) = "x"`,
              // `Tab(0).Control(1) = "txt(1)"`. The dotted VBA form (`Object.Tag`)
              // is folded into the same node so `Tab(0).Control` is not ambiguous
              // with a call_expression receiver.
              isVB6 ? alias($._frm_property_name_path, $.frm_property_name) : $.member_expression,
            ),
          ),
          "=",
          field("value", $._frm_property_value),
        ),

      ...(isVB6
        ? {
            _frm_property_name_path: ($) =>
              choice(
                seq(
                  $.identifier,
                  $.frm_property_index,
                  repeat(seq(dot($), $.identifier, optional($.frm_property_index))),
                ),
                seq($.identifier, repeat1(seq(dot($), $.identifier, optional($.frm_property_index)))),
              ),

            // The IDE writes Single properties in the machine locale, so a German or
            // Spanish machine saves `FontSize = 8,25` and `Angle = 143,073`.
            frm_locale_number: (_) => token(prec(1, /-?\d+,\d+(?:[Ee][+-]?\d+)?/)),
          }
        : {}),

      _frm_property_value: ($) =>
        choice(
          $.frm_blob_reference,
          ...(isVB6 ? [$.frm_object_reference, $.frm_shortcut_value, $.frm_locale_number] : []),
          $.frm_quoted_property_text,
          $._literal,
          $.member_expression,
          $.identifier,
          $.frm_property_text,
        ),

      frm_property_text: (_) => token(/[A-Za-z_][^\r\n']*/),

      frm_quoted_property_text: (_) => token(seq('"', /[^"\r\n]+/)),

      // `Icon = "Form1.frx":0000`; VB6 prefixes string-typed resources with `$`
      // as in `Text = $"Form1.frx":0A1C`.
      // The offset after the colon is bare hexadecimal (`:0A1C`), which is not a
      // number_literal; VB6 gets a dedicated token so `0A1C` does not split into
      // `0` + identifier. VBA keeps number_literal to preserve its published CST.
      frm_blob_reference: ($) =>
        isVB6
          ? seq(
              optional("$"),
              $.string_literal,
              ":",
              field("offset", alias(token(/[0-9A-Fa-f]+/), $.frm_blob_offset)),
            )
          : seq($.string_literal, ":", $.number_literal),

      ...(isVB6
        ? {
            // `Object = "{GUID}#2.0#0"; "MSCOMCTL.OCX"` component references
            // that precede the form's Begin block.
            frm_object_reference: ($) =>
              seq(field("class", $.string_literal), ";", field("file", $.string_literal)),

            // Menu accelerators: `Shortcut = ^O`, `{F5}`, `+{F1}`, `^{INSERT}`, `%{BKSP}`.
            frm_shortcut_value: (_) =>
              token(prec(1, /[\^+%]+[A-Za-z0-9]|[\^+%]*\{[A-Za-z0-9]+\}/)),

            frm_property_index: ($) => seq("(", $.number_literal, ")"),
          }
        : {}),

      attribute_statement: ($) =>
        seq(
          caseInsensitive("Attribute"),
          field("name", $._attribute_name),
          "=",
          field("value", $._literal),
          // VB6 Class Builder writes two-valued keys: `VB_Ext_KEY = "Top_Level" ,"Yes"`.
          ...(isVB6 ? [repeat(seq(",", field("value", $._literal)))] : []),
        ),

      // VBE export attributes can target procedures named with a contextual
      // keyword. Keep their identifiers local to Attribute syntax so ordinary
      // statement parsing remains unchanged.
      _attribute_name: ($) =>
        choice(
          $._attribute_identifier,
          alias($._attribute_member_expression, $.qualified_member_expression),
        ),

      _attribute_member_expression: ($) =>
        prec.left(
          3,
          seq(
            field("receiver", $._attribute_identifier),
            repeat1(
              seq(field("operator", memberOperator($)), field("member", $._attribute_identifier)),
            ),
          ),
        ),

      _attribute_identifier: ($) =>
        choice(
          $.identifier,
          alias(caseInsensitive("Load"), $.identifier),
          alias(caseInsensitive("Name"), $.identifier),
        ),

      option_statement: ($) =>
        seq(
          caseInsensitive("Option"),
          choice(
            caseInsensitive("Explicit"),
            seq(caseInsensitive("Private"), caseInsensitive("Module")),
            seq(
              caseInsensitive("Compare"),
              choice(caseInsensitive("Binary"), caseInsensitive("Text"), caseInsensitive("Database")),
            ),
            seq(caseInsensitive("Base"), $.number_literal),
          ),
        ),

      implements_statement: ($) =>
        seq(caseInsensitive("Implements"), field("name", $.type_expression)),

      type_declaration: ($) =>
        seq(
          optional(field("visibility", $.visibility)),
          caseInsensitive("Type"),
          field("name", $.identifier),
          $._statement_separator,
          repeat(choice($._statement_separator, $.type_member, $.type_preprocessor_if)),
          caseInsensitive("End"),
          caseInsensitive("Type"),
        ),

      // A Type member may be named `End` (`End As KeyState`); `End Type` is told apart
      // by the following token.
      type_member: ($) =>
        seq(
          field(
            "name",
            choice(
              $.identifier,
              $.bang_identifier,
              ...(isVB6 ? [alias(caseInsensitive("End"), $.identifier)] : []),
            ),
          ),
          optional(field("bounds", $.array_bounds)),
          field("type", $.as_type_clause),
        ),

      type_preprocessor_if: ($) =>
        seq(
          preprocessorKeyword("#If"),
          field("condition", $._condition_expression),
          caseInsensitive("Then"),
          $.newline,
          field("body", optional($.type_preprocessor_block)),
          repeat($.type_preprocessor_elseif),
          optional($.type_preprocessor_else),
          caseInsensitive("#End"),
          caseInsensitive("If"),
        ),

      type_preprocessor_block: ($) =>
        repeat1(choice($.newline, $.type_member, $.type_preprocessor_if)),

      type_preprocessor_elseif: ($) =>
        seq(
          preprocessorKeyword("#ElseIf"),
          field("condition", $._condition_expression),
          caseInsensitive("Then"),
          $.newline,
          field("body", optional($.type_preprocessor_block)),
        ),

      type_preprocessor_else: ($) =>
        seq(caseInsensitive("#Else"), $.newline, field("body", optional($.type_preprocessor_block))),

      enum_declaration: ($) =>
        seq(
          optional(field("visibility", $.visibility)),
          caseInsensitive("Enum"),
          field("name", $.identifier),
          $._statement_separator,
          repeat(choice($._statement_separator, $.enum_member, $.preprocessor_if)),
          caseInsensitive("End"),
          caseInsensitive("Enum"),
        ),

      enum_member: ($) =>
        seq(field("name", $.identifier), optional(seq("=", field("value", $._expression)))),

      declare_sub_statement: ($) =>
        prec.right(
          seq(
            optional(field("visibility", $.visibility)),
            caseInsensitive("Declare"),
            ...(isVBA ? [optional(field("ptrsafe_modifier", $.ptrsafe_modifier))] : []),
            caseInsensitive("Sub"),
            field("name", $.identifier),
            caseInsensitive("Lib"),
            field("library", $.string_literal),
            optional(seq(caseInsensitive("Alias"), field("alias", $.string_literal))),
            optional(field("parameters", $.parameter_list)),
          ),
        ),

      declare_function_statement: ($) =>
        prec.right(
          seq(
            optional(field("visibility", $.visibility)),
            caseInsensitive("Declare"),
            ...(isVBA ? [optional(field("ptrsafe_modifier", $.ptrsafe_modifier))] : []),
            caseInsensitive("Function"),
            field("name", $.identifier),
            caseInsensitive("Lib"),
            field("library", $.string_literal),
            optional(seq(caseInsensitive("Alias"), field("alias", $.string_literal))),
            optional(field("parameters", $.parameter_list)),
            optional(field("type", $.as_type_clause)),
          ),
        ),

      preprocessor_const: ($) =>
        seq(
          caseInsensitive("#Const"),
          field("name", $.identifier),
          "=",
          field("value", choice($.comparison_expression, $._expression)),
        ),

      // `#If` / `#ElseIf` carry their trailing blank so that `Close #iFile` lexes as
      // `#` + identifier instead of `#If` + `ile`; the lexer has no lookahead.
      preprocessor_if: ($) =>
        seq(
          preprocessorKeyword("#If"),
          field("condition", $._condition_expression),
          caseInsensitive("Then"),
          $.newline,
          field("body", optional($.preprocessor_block)),
          repeat($.preprocessor_elseif),
          optional($.preprocessor_else),
          caseInsensitive("#End"),
          caseInsensitive("If"),
        ),

      preprocessor_block: ($) => repeat1(choice($._preprocessor_item)),

      _preprocessor_item: ($) =>
        choice(
          $.newline,
          $._statement,
          $.declare_sub_statement,
          $.declare_function_statement,
          $.type_declaration,
          $.enum_declaration,
          $.conditional_sub_declaration,
          $.conditional_function_declaration,
          $.conditional_property_declaration,
          $.sub_declaration,
          $.function_declaration,
          $.property_get_declaration,
          $.property_let_declaration,
          $.property_set_declaration,
          $.event_declaration,
          ":",
        ),

      preprocessor_elseif: ($) =>
        seq(
          preprocessorKeyword("#ElseIf"),
          field("condition", $._condition_expression),
          caseInsensitive("Then"),
          $.newline,
          field("body", optional($.preprocessor_block)),
        ),

      preprocessor_else: ($) =>
        seq(caseInsensitive("#Else"), $.newline, field("body", optional($.preprocessor_block))),

      sub_declaration: ($) =>
        seq(
          $._sub_header,
          $._statement_separator,
          optional($._procedure_attributes),
          field("body", optional($.block)),
          field("end", $.end_sub_statement),
        ),

      function_declaration: ($) =>
        seq(
          $._function_header,
          $._statement_separator,
          optional($._procedure_attributes),
          field("body", optional($.block)),
          field("end", $.end_function_statement),
        ),

      property_get_declaration: ($) =>
        seq(
          $._property_get_header,
          $._statement_separator,
          optional($._procedure_attributes),
          field("body", optional($.block)),
          field("end", $.end_property_statement),
        ),

      property_let_declaration: ($) =>
        seq(
          $._property_let_header,
          $._statement_separator,
          optional($._procedure_attributes),
          field("body", optional($.block)),
          field("end", $.end_property_statement),
        ),

      property_set_declaration: ($) =>
        seq(
          $._property_set_header,
          $._statement_separator,
          optional($._procedure_attributes),
          field("body", optional($.block)),
          field("end", $.end_property_statement),
        ),

      _sub_header: ($) =>
        seq(
          optional($._procedure_modifier),
          caseInsensitive("Sub"),
          field("name", $.identifier),
          optional(field("parameters", $.parameter_list)),
        ),

      _function_header: ($) =>
        seq(
          optional($._procedure_modifier),
          caseInsensitive("Function"),
          field("name", choice($.identifier, $.bang_identifier)),
          optional(field("parameters", $.parameter_list)),
          optional(field("type", $.as_type_clause)),
        ),

      _property_header: ($) =>
        choice($._property_get_header, $._property_let_header, $._property_set_header),

      _property_get_header: ($) =>
        seq(
          optional($._procedure_modifier),
          caseInsensitive("Property"),
          field("accessor", $.get_accessor),
          field("name", $.identifier),
          optional(field("parameters", $.parameter_list)),
          optional(field("type", $.as_type_clause)),
        ),

      _property_let_header: ($) =>
        seq(
          optional($._procedure_modifier),
          caseInsensitive("Property"),
          field("accessor", $.let_accessor),
          field("name", $.identifier),
          optional(field("parameters", $.parameter_list)),
          optional(field("type", $.as_type_clause)),
        ),

      _property_set_header: ($) =>
        seq(
          optional($._procedure_modifier),
          caseInsensitive("Property"),
          field("accessor", $.set_accessor),
          field("name", $.identifier),
          optional(field("parameters", $.parameter_list)),
          optional(field("type", $.as_type_clause)),
        ),

      conditional_sub_declaration: ($) =>
        seq(
          $._conditional_sub_headers,
          $.newline,
          field("body", optional($.block)),
          caseInsensitive("End"),
          caseInsensitive("Sub"),
        ),

      conditional_function_declaration: ($) =>
        seq(
          $._conditional_function_headers,
          $.newline,
          field("body", optional($.block)),
          caseInsensitive("End"),
          caseInsensitive("Function"),
        ),

      conditional_property_declaration: ($) =>
        seq(
          $._conditional_property_headers,
          $.newline,
          field("body", optional($.block)),
          caseInsensitive("End"),
          caseInsensitive("Property"),
        ),

      _conditional_sub_headers: ($) =>
        seq(
          preprocessorKeyword("#If"),
          field("condition", $._condition_expression),
          caseInsensitive("Then"),
          $.newline,
          field("consequence", $._sub_header),
          $.newline,
          field("consequence_body", optional($.conditional_branch_body)),
          repeat(
            seq(
              preprocessorKeyword("#ElseIf"),
              field("condition", $._condition_expression),
              caseInsensitive("Then"),
              $.newline,
              field("alternative", $._sub_header),
              $.newline,
              field("alternative_body", optional($.conditional_branch_body)),
            ),
          ),
          optional(
            seq(
              caseInsensitive("#Else"),
              $.newline,
              field("alternative", $._sub_header),
              $.newline,
              field("alternative_body", optional($.conditional_branch_body)),
            ),
          ),
          caseInsensitive("#End"),
          caseInsensitive("If"),
        ),

      _conditional_function_headers: ($) =>
        seq(
          preprocessorKeyword("#If"),
          field("condition", $._condition_expression),
          caseInsensitive("Then"),
          $.newline,
          field("consequence", $._function_header),
          $.newline,
          field("consequence_body", optional($.conditional_branch_body)),
          repeat(
            seq(
              preprocessorKeyword("#ElseIf"),
              field("condition", $._condition_expression),
              caseInsensitive("Then"),
              $.newline,
              field("alternative", $._function_header),
              $.newline,
              field("alternative_body", optional($.conditional_branch_body)),
            ),
          ),
          optional(
            seq(
              caseInsensitive("#Else"),
              $.newline,
              field("alternative", $._function_header),
              $.newline,
              field("alternative_body", optional($.conditional_branch_body)),
            ),
          ),
          caseInsensitive("#End"),
          caseInsensitive("If"),
        ),

      _conditional_property_headers: ($) =>
        seq(
          preprocessorKeyword("#If"),
          field("condition", $._condition_expression),
          caseInsensitive("Then"),
          $.newline,
          field("consequence", $._property_header),
          $.newline,
          field("consequence_body", optional($.conditional_branch_body)),
          repeat(
            seq(
              preprocessorKeyword("#ElseIf"),
              field("condition", $._condition_expression),
              caseInsensitive("Then"),
              $.newline,
              field("alternative", $._property_header),
              $.newline,
              field("alternative_body", optional($.conditional_branch_body)),
            ),
          ),
          optional(
            seq(
              caseInsensitive("#Else"),
              $.newline,
              field("alternative", $._property_header),
              $.newline,
              field("alternative_body", optional($.conditional_branch_body)),
            ),
          ),
          caseInsensitive("#End"),
          caseInsensitive("If"),
        ),

      event_declaration: ($) =>
        prec.right(
          seq(
            optional(field("visibility", $.visibility)),
            caseInsensitive("Event"),
            field("name", $.identifier),
            optional(field("parameters", $.parameter_list)),
          ),
        ),

      _procedure_modifier: ($) =>
        choice(
          seq(field("visibility", $.visibility), optional(field("modifiers", $.static_modifier))),
          field("modifiers", $.static_modifier),
        ),

      _procedure_attributes: ($) => repeat1(seq($.attribute_statement, $.newline)),

      end_sub_statement: ($) => seq(caseInsensitive("End"), caseInsensitive("Sub")),

      end_function_statement: ($) => seq(caseInsensitive("End"), caseInsensitive("Function")),

      end_property_statement: ($) => seq(caseInsensitive("End"), caseInsensitive("Property")),

      static_modifier: (_) => caseInsensitive("Static"),

      _dim_keyword: (_) => caseInsensitive("Dim"),

      _redim_keyword: (_) => caseInsensitive("ReDim"),

      with_events_modifier: (_) => caseInsensitive("WithEvents"),

      // VBA7 only. In VB6 `PtrSafe` is not a keyword; a Declare carrying it is an error.
      ...(isVBA ? { ptrsafe_modifier: (_) => caseInsensitive("PtrSafe") } : {}),

      byval_modifier: (_) => caseInsensitive("ByVal"),

      byref_modifier: (_) => caseInsensitive("ByRef"),

      optional_modifier: (_) => caseInsensitive("Optional"),

      paramarray_modifier: (_) => caseInsensitive("ParamArray"),

      get_accessor: (_) => caseInsensitive("Get"),

      let_accessor: (_) => caseInsensitive("Let"),

      set_accessor: (_) => caseInsensitive("Set"),

      block: ($) => repeat1(choice($._statement_separator, $._statement)),

      conditional_branch_body: ($) =>
        seq($._statement, repeat(choice($._statement_separator, $._statement))),

      _statement: ($) =>
        choice(
          $.single_line_if_statement,
          $.if_statement,
          $.elseif_fragment,
          $.else_fragment,
          $.end_if_fragment,
          $.select_statement,
          $.for_statement,
          alias($._inline_for_statement, $.for_statement),
          $.for_each_statement,
          alias($._inline_for_each_statement, $.for_each_statement),
          $.do_statement,
          alias($._inline_do_statement, $.do_statement),
          $.while_statement,
          $.with_statement,
          alias($._inline_with_statement, $.with_statement),
          $.on_goto_statement,
          $.on_error_statement,
          $.let_statement,
          $.resume_statement,
          $.goto_statement,
          // VB6-only forms expand every VBA statement-list parse table.
          ...(isVB6
            ? [
                $.gosub_statement,
                $.return_statement,
                $.lset_statement,
                $.rset_statement,
              ]
            : []),
          $.label_statement,
          $.line_number_statement,
          $.exit_statement,
          $.end_statement,
          $.redim_statement,
          $.erase_statement,
          $.open_statement,
          $.input_statement,
          $.line_input_statement,
          $.print_statement,
          $.write_statement,
          $.debug_print_statement,
          $.close_statement,
          $.get_statement,
          $.put_statement,
          $.lock_statement,
          $.unlock_statement,
          $.seek_statement,
          $.reset_statement,
          $.raise_event_statement,
          $.name_statement,
          $.stop_statement,
          $.beep_statement,
          $.load_statement,
          $.unload_statement,
          ...(isVB6
            ? [$.line_statement, $.pset_statement, $.circle_statement, $.scale_statement]
            : []),
          $.preprocessor_const,
          $.preprocessor_if,
          $.def_type_statement,
          $.const_declaration,
          $.variable_declaration,
          $.set_statement,
          $.assignment_statement,
          $.call_statement,
          $.expression_statement,
        ),

      variable_declaration: ($) =>
        choice(
          seq(
            isVB6
              ? choice(field("visibility", $.visibility), $._dim_keyword)
              : field("visibility", $.visibility),
            field("with_events_modifier", $.with_events_modifier),
            commaSep1($.variable_declarator),
          ),
          seq(
            choice(
              seq(
                optional(field("visibility", $.visibility)),
                choice($._dim_keyword, field("static_modifier", $.static_modifier)),
              ),
              field("visibility", $.visibility),
            ),
            commaSep1($.variable_declarator),
          ),
        ),

      const_declaration: ($) =>
        seq(
          optional(field("visibility", $.visibility)),
          caseInsensitive("Const"),
          commaSep1($.const_declarator),
        ),

      variable_declarator: ($) =>
        choice(
          prec.right(
            1,
            seq(
              field("name", $._declarator_name),
              field("bounds", $.array_bounds),
              optional(field("type", $.as_type_clause)),
              optional(field("initializer", $.initializer)),
            ),
          ),
          seq(
            field("name", $._declarator_name),
            optional(field("type", $.as_type_clause)),
            optional(field("initializer", $.initializer)),
          ),
        ),

      _declarator_name: ($) =>
        choice(
          reserved("variable_declarator", $.identifier),
          alias($._declarator_bang_identifier, $.bang_identifier),
        ),

      _declarator_bang_identifier: ($) =>
        prec(2, seq(reserved("variable_declarator", $.identifier), bang($))),

      const_declarator: ($) =>
        seq(
          field("name", choice($.identifier, $.bang_identifier)),
          optional(field("type", $.as_type_clause)),
          optional(field("initializer", $.initializer)),
        ),

      initializer: ($) => seq("=", field("value", choice($.comparison_expression, $._expression))),

      parameter_list: ($) => seq("(", optional(commaSep1($.parameter)), ")"),

      parameter: ($) =>
        seq(
          optional(field("optional_modifier", $.optional_modifier)),
          optional(field("passing_mode", choice($.byval_modifier, $.byref_modifier))),
          optional(field("paramarray_modifier", $.paramarray_modifier)),
          field("name", choice($.identifier, $.bang_identifier)),
          optional(field("bounds", $.array_bounds)),
          optional(field("type", $.as_type_clause)),
          optional(field("default_value", $.initializer)),
        ),

      as_type_clause: ($) =>
        prec.right(
          seq(
            caseInsensitive("As"),
            optional(caseInsensitive("New")),
            field("type", $.type_expression),
            optional($.fixed_string_length),
            optional($.array_bounds),
          ),
        ),

      fixed_string_length: ($) => seq("*", field("length", $._expression)),

      array_bounds: ($) => seq("(", optional(commaSep1($.array_bound)), ")"),

      array_bound: ($) =>
        choice(
          seq(field("lower", $._expression), caseInsensitive("To"), field("upper", $._expression)),
          $._expression,
        ),

      type_expression: ($) =>
        prec(
          4,
          choice(
            $.dotted_type_expression,
            $.identifier,
            caseInsensitive("String"),
            caseInsensitive("Boolean"),
            caseInsensitive("Byte"),
            caseInsensitive("Integer"),
            caseInsensitive("Long"),
            ...(isVBA ? [caseInsensitive("LongLong"), caseInsensitive("LongPtr")] : []),
            caseInsensitive("Single"),
            caseInsensitive("Double"),
            caseInsensitive("Currency"),
            caseInsensitive("Date"),
            caseInsensitive("Variant"),
            caseInsensitive("Object"),
          ),
        ),

      dotted_type_expression: ($) =>
        prec(
          5,
          seq(
            $.identifier,
            repeat1(seq(dot($), choice($.identifier, alias(caseInsensitive("Line"), $.identifier)))),
          ),
        ),

      // `Global` is the VB3-era spelling of module-level `Public`; VB6 and VBA both still accept it.
      visibility: (_) =>
        choice(
          caseInsensitive("Public"),
          caseInsensitive("Private"),
          caseInsensitive("Friend"),
          caseInsensitive("Global"),
        ),

      if_statement: ($) =>
        prec.right(
          seq(
            optional(field("start_line", $.line_number_prefix)),
            caseInsensitive("If"),
            field("condition", $._condition_expression),
            caseInsensitive("Then"),
          ),
        ),

      elseif_fragment: ($) =>
        prec.right(
          seq(
            optional(field("start_line", $.line_number_prefix)),
            caseInsensitive("ElseIf"),
            field("condition", $._condition_expression),
            caseInsensitive("Then"),
          ),
        ),

      else_fragment: ($) =>
        prec.right(seq(optional(field("start_line", $.line_number_prefix)), caseInsensitive("Else"))),

      end_if_fragment: ($) =>
        prec.right(
          seq(
            optional(field("start_line", $.line_number_prefix)),
            caseInsensitive("End"),
            caseInsensitive("If"),
          ),
        ),

      single_line_if_statement: ($) =>
        prec.right(
          seq(
            caseInsensitive("If"),
            field("condition", $._condition_expression),
            caseInsensitive("Then"),
            optional(":"),
            field("consequence", choice($.inline_statement_sequence, $._inline_statement)),
            optional(
              seq(
                caseInsensitive("Else"),
                isVB6
                  ? optional(
                      field(
                        "alternative",
                        choice($.inline_statement_sequence, $._inline_statement),
                      ),
                    )
                  : field("alternative", choice($.inline_statement_sequence, $._inline_statement)),
              ),
            ),
          ),
        ),

      inline_statement_sequence: ($) =>
        prec.left(
          seq(
            $._inline_statement,
            repeat1(seq(":", ...(isVB6 ? [repeat(":")] : []), $._inline_statement)),
          ),
        ),

      _inline_statement: ($) =>
        choice(
          $.single_line_if_statement,
          $.for_statement,
          alias($._inline_for_statement, $.for_statement),
          $.for_each_statement,
          alias($._inline_for_each_statement, $.for_each_statement),
          $.do_statement,
          alias($._inline_do_statement, $.do_statement),
          $.while_statement,
          $.with_statement,
          alias($._inline_with_statement, $.with_statement),
          $.exit_statement,
          $.end_statement,
          $.on_error_statement,
          $.let_statement,
          $.resume_statement,
          $.goto_statement,
          // VB6-only forms expand every VBA statement-list parse table.
          ...(isVB6
            ? [
                $.gosub_statement,
                $.return_statement,
                $.lset_statement,
                $.rset_statement,
              ]
            : []),
          $.redim_statement,
          $.erase_statement,
          $.open_statement,
          $.input_statement,
          $.line_input_statement,
          $.print_statement,
          $.write_statement,
          $.debug_print_statement,
          $.close_statement,
          $.get_statement,
          $.put_statement,
          $.lock_statement,
          $.unlock_statement,
          $.seek_statement,
          $.reset_statement,
          $.raise_event_statement,
          $.name_statement,
          $.stop_statement,
          $.beep_statement,
          $.load_statement,
          $.unload_statement,
          ...(isVB6
            ? [$.line_statement, $.pset_statement, $.circle_statement, $.scale_statement]
            : []),
          $.const_declaration,
          $.variable_declaration,
          $.set_statement,
          $.assignment_statement,
          $.call_statement,
          $.expression_statement,
        ),

      select_statement: ($) =>
        seq(
          optional(field("start_line", $.line_number_prefix)),
          caseInsensitive("Select"),
          caseInsensitive("Case"),
          // VB6 only: a comparison as the selector, a `:` after it, and `#If` blocks
          // between clauses. The `#If` form alone cost the vba table 3,247 states and
          // 11.9 MB of parser.c, half of the growth that broke the browser artifact.
          field("value", isVB6 ? choice($.comparison_expression, $._expression) : $._expression),
          ...(isVB6 ? [optional(":")] : []),
          $.newline,
          repeat(choice($.newline, $.case_clause, ...(isVB6 ? [$.case_preprocessor_if] : []))),
          optional(field("end_line", $.line_number_prefix)),
          caseInsensitive("End"),
          caseInsensitive("Select"),
        ),

      // `#If` wrapping whole Case clauses, as dual-platform code does around
      // `Select Case`. Distinct from a `#If` inside a case body.
      case_preprocessor_if: ($) =>
        seq(
          preprocessorKeyword("#If"),
          field("condition", $._condition_expression),
          caseInsensitive("Then"),
          $.newline,
          repeat(choice($.newline, $.case_clause)),
          repeat(
            seq(
              preprocessorKeyword("#ElseIf"),
              field("condition", $._condition_expression),
              caseInsensitive("Then"),
              $.newline,
              repeat(choice($.newline, $.case_clause)),
            ),
          ),
          optional(seq(caseInsensitive("#Else"), $.newline, repeat(choice($.newline, $.case_clause)))),
          caseInsensitive("#End"),
          caseInsensitive("If"),
        ),

      case_clause: ($) =>
        seq(
          optional(field("line", $.line_number_prefix)),
          caseInsensitive("Case"),
          choice(caseInsensitive("Else"), commaSep1($.case_expression)),
          $._statement_separator,
          field("body", optional($.block)),
        ),

      case_expression: ($) =>
        choice(
          seq(caseInsensitive("Is"), choice("<", "<=", ">", ">=", "=", "<>"), $._expression),
          seq($._expression, caseInsensitive("To"), $._expression),
          // `Case Is ...` above is the distinct leading-`Is` range/comparison
          // form. A comparison beginning with an expression is also a valid
          // case value, notably `obj Is Nothing` in `Select Case True/False`.
          $.comparison_expression,
          $._expression,
        ),

      // Separate inline and multiline control forms, then alias them at use sites.
      // This preserves the public CST while limiting recursive parse-state expansion.
      for_statement: ($) => prec.right(seq($._for_header, $._multiline_for_tail)),

      _inline_for_statement: ($) => prec.right(seq($._for_header, $._inline_for_tail)),

      for_each_statement: ($) => prec.right(seq($._for_each_header, $._multiline_for_tail)),

      _inline_for_each_statement: ($) => prec.right(seq($._for_each_header, $._inline_for_tail)),

      _multiline_for_tail: ($) =>
        prec.right(
          choice(
            seq(
              ...(isVB6 ? [optional(":")] : []),
              $.newline,
              field(
                "body",
                choice(
                  $.for_statement,
                  alias($._inline_for_statement, $.for_statement),
                  $.for_each_statement,
                  alias($._inline_for_each_statement, $.for_each_statement),
                ),
              ),
            ),
            // Prefer the explicit `Next` tail over the nested-for shorthand
            // when both parse: inside a flat if-branch GLR otherwise ends the
            // outer For at the inner `Next j` and reparses `Next i` as a
            // call_statement.
            prec.dynamic(
              1,
              seq(
                choice(
                  seq(
                    ...(isVB6 ? [optional(":")] : []),
                    $.newline,
                    field("body", optional($.block)),
                  ),
                  field("body", alias($._colon_prefixed_for_block, $.block)),
                ),
                optional(field("end_line", $.line_number_prefix)),
                caseInsensitive("Next"),
                optional(field("next_variables", $.next_variable_list)),
              ),
            ),
          ),
        ),

      _inline_for_tail: ($) =>
        prec.right(
          choice(
            field("body", $.shared_next_for_body),
            isVB6
              ? seq(
                  ":",
                  field("body", optional($.single_line_block)),
                  optional(field("end_line", $.line_number_prefix)),
                  caseInsensitive("Next"),
                  optional(field("next_variables", $.next_variable_list)),
                )
              : prec.dynamic(
                  2,
                  seq(
                    field("body", optional($.single_line_block)),
                    optional(field("end_line", $.line_number_prefix)),
                    ":",
                    caseInsensitive("Next"),
                    optional(field("next_variables", $.next_variable_list)),
                  ),
                ),
          ),
        ),

      _for_header: ($) =>
        seq(
          optional(field("start_line", $.line_number_prefix)),
          caseInsensitive("For"),
          field("variable", $._assignable_expression),
          "=",
          field("start", $._expression),
          caseInsensitive("To"),
          field("end", $._expression),
          optional(seq(caseInsensitive("Step"), field("step", $._expression))),
        ),

      _for_each_header: ($) =>
        seq(
          optional(field("start_line", $.line_number_prefix)),
          caseInsensitive("For"),
          caseInsensitive("Each"),
          field("variable", $._assignable_expression),
          caseInsensitive("In"),
          field("collection", $._expression),
        ),

      do_statement: ($) =>
        prec.right(
          seq(
            optional(field("start_line", $.line_number_prefix)),
            caseInsensitive("Do"),
            optional($.do_condition),
            $.newline,
            field("body", optional($.block)),
            optional(field("end_line", $.line_number_prefix)),
            caseInsensitive("Loop"),
            optional($.do_condition),
          ),
        ),

      _inline_do_statement: ($) =>
        prec.right(
          seq(
            optional(field("start_line", $.line_number_prefix)),
            caseInsensitive("Do"),
            optional($.do_condition),
            ...(isVB6 ? [":"] : []),
            field("body", optional($.single_line_block)),
            optional(field("end_line", $.line_number_prefix)),
            ...(isVB6 ? [] : [":"]),
            caseInsensitive("Loop"),
            optional($.do_condition),
          ),
        ),

      do_condition: ($) =>
        seq(
          choice(caseInsensitive("While"), caseInsensitive("Until")),
          field("condition", $._condition_expression),
        ),

      while_statement: ($) =>
        seq(
          optional(field("start_line", $.line_number_prefix)),
          caseInsensitive("While"),
          field("condition", $._condition_expression),
          $._statement_separator,
          field("body", optional($.block)),
          optional(field("end_line", $.line_number_prefix)),
          caseInsensitive("Wend"),
        ),

      with_statement: ($) =>
        seq(
          optional(field("start_line", $.line_number_prefix)),
          caseInsensitive("With"),
          field("value", $._expression),
          $.newline,
          field("body", optional($.block)),
          optional(field("end_line", $.line_number_prefix)),
          caseInsensitive("End"),
          caseInsensitive("With"),
        ),

      _inline_with_statement: ($) =>
        seq(
          optional(field("start_line", $.line_number_prefix)),
          caseInsensitive("With"),
          field("value", $._expression),
          ...(isVB6 ? [":"] : []),
          field("body", optional($.single_line_block)),
          optional(field("end_line", $.line_number_prefix)),
          ...(isVB6 ? [] : [":"]),
          caseInsensitive("End"),
          caseInsensitive("With"),
        ),

      // VB6: each statement carries its trailing colon, so `: a = 1: b = 2: Next` needs
      // one token of lookahead after a colon (statement or terminator) and multi-
      // statement inline loops parse. VBA keeps the base shape, whose CST for the
      // one-statement case is pinned by the upstream corpus ("preserve current CST").
      single_line_block: ($) =>
        isVB6
          ? prec.left(repeat1(seq($._inline_statement, ":", repeat(":"))))
          : prec.left(repeat1(seq(":", $._inline_statement))),

      // A For header may begin its body after a colon and still place Next
      // on a later physical line. Alias the combined body to block so the public
      // body field keeps the same node type as every other multiline loop.
      _colon_prefixed_for_block: ($) =>
        prec.right(
          seq(
            ":",
            choice(
              alias(
                $._colon_for_inline_statement_sequence,
                $.inline_statement_sequence,
              ),
              seq($._inline_statement, ":"),
              $._inline_statement,
            ),
            $.newline,
            repeat(choice($._statement_separator, $._statement)),
          ),
        ),

      _colon_for_inline_statement_sequence: ($) =>
        prec.left(
          seq(
            $._inline_statement,
            repeat1(
              seq(":", ...(isVB6 ? [repeat(":")] : []), $._inline_statement),
            ),
            optional(":"),
          ),
        ),

      shared_next_for_body: ($) =>
        seq(
          ":",
          choice(
            $.for_statement,
            prec.dynamic(3, alias($._inline_for_statement, $.for_statement)),
            $.for_each_statement,
            prec.dynamic(3, alias($._inline_for_each_statement, $.for_each_statement)),
          ),
        ),

      next_variable_list: ($) => commaSep1($._assignable_expression),

      exit_statement: ($) =>
        seq(
          caseInsensitive("Exit"),
          choice(
            caseInsensitive("Sub"),
            caseInsensitive("Function"),
            caseInsensitive("Property"),
            caseInsensitive("For"),
            caseInsensitive("Do"),
          ),
        ),

      end_statement: (_) => token(/[Ee][Nn][Dd][ \t]*(?:\r?\n|:)/),

      on_goto_statement: ($) =>
        seq(
          caseInsensitive("On"),
          field("selector", $._expression),
          choice(caseInsensitive("GoTo"), caseInsensitive("GoSub")),
          commaSep1(field("target", choice($.identifier, lineNumber($)))),
        ),

      on_error_statement: ($) =>
        seq(
          caseInsensitive("On"),
          optional(caseInsensitive("Local")),
          caseInsensitive("Error"),
          choice(
            seq(caseInsensitive("GoTo"), field("target", choice($.identifier, lineNumber($)))),
            seq(caseInsensitive("Resume"), caseInsensitive("Next")),
          ),
        ),

      resume_statement: ($) =>
        prec.right(
          seq(
            caseInsensitive("Resume"),
            optional(
              choice(caseInsensitive("Next"), field("target", choice($.identifier, lineNumber($)))),
            ),
          ),
        ),

      goto_statement: ($) =>
        seq(
          caseInsensitive("GoTo"),
          field("target", choice($.identifier, lineNumber($))),
          optional(seq(":", $.newline)),
        ),

      gosub_statement: ($) =>
        seq(caseInsensitive("GoSub"), field("target", choice($.identifier, lineNumber($)))),

      return_statement: (_) => caseInsensitive("Return"),

      // `Let x = 1`: the explicit assignment keyword, common in pre-VB5 code.
      let_statement: ($) =>
        seq(
          caseInsensitive("Let"),
          field("left", $._assignable_expression),
          "=",
          field("right", choice($.logical_value_expression, $.comparison_expression, $._expression)),
        ),

      // `LSet a = b` / `RSet a = b`: left- and right-justified string assignment,
      // also used for copying between user-defined types.
      lset_statement: ($) =>
        seq(
          caseInsensitive("LSet"),
          field("left", $._assignable_expression),
          "=",
          field("right", $._expression),
        ),

      rset_statement: ($) =>
        seq(
          caseInsensitive("RSet"),
          field("left", $._assignable_expression),
          "=",
          field("right", $._expression),
        ),

      label_statement: ($) => prec(5, seq(field("name", choice($.identifier, lineNumber($))), ":")),

      line_number_prefix: ($) => prec.dynamic(1, prec.right(6, seq(lineNumber($), optional(":")))),

      line_number_statement: ($) =>
        prec.right(
          5,
          choice(
            seq(field("number", lineNumber($)), field("statement", $._numbered_statement)),
            field("number", lineNumber($)),
          ),
        ),

      line_number_top_level_item: ($) =>
        prec.right(
          5,
          choice(
            seq(
              field("number", lineNumber($)),
              optional(":"),
              field(
                "item",
                choice(
                  $.sub_declaration,
                  $.function_declaration,
                  $.property_get_declaration,
                  $.property_let_declaration,
                  $.property_set_declaration,
                ),
              ),
            ),
            seq(field("number", lineNumber($)), optional(":")),
          ),
        ),

      _numbered_statement: ($) =>
        choice(
          $.single_line_if_statement,
          $.on_error_statement,
          $.let_statement,
          $.resume_statement,
          $.goto_statement,
          // VB6-only forms expand every VBA statement-list parse table.
          ...(isVB6
            ? [
                $.gosub_statement,
                $.return_statement,
                $.lset_statement,
                $.rset_statement,
              ]
            : []),
          $.exit_statement,
          $.redim_statement,
          $.const_declaration,
          $.variable_declaration,
          $.def_type_statement,
          $.get_statement,
          $.put_statement,
          $.lock_statement,
          $.unlock_statement,
          $.seek_statement,
          $.reset_statement,
          $.debug_print_statement,
          $.raise_event_statement,
          $.name_statement,
          $.stop_statement,
          $.beep_statement,
          $.load_statement,
          $.unload_statement,
          $.set_statement,
          $.assignment_statement,
          $.call_statement,
          $.expression_statement,
        ),

      redim_statement: ($) =>
        choice(
          $._ambiguous_redim_statement,
          seq($._redim_keyword, optional(caseInsensitive("Preserve")), commaSep1($.redim_declarator)),
        ),

      _ambiguous_redim_statement: (_) =>
        token(prec(2, /[Rr][Ee][Dd][Ii][Mm][^\r\n]*\)[.!][A-Za-z_][A-Za-z0-9_]*\([^\r\n]*\)/)),

      redim_declarator: ($) =>
        prec(
          4,
          seq(
            field("name", choice($.identifier, $.bang_identifier, $.member_expression)),
            $.array_bounds,
            ...(isVB6 ? [optional(field("type", $.as_type_clause))] : []),
          ),
        ),

      erase_statement: ($) =>
        prec.right(
          seq(caseInsensitive("Erase"), commaSep1(field("target", $._assignable_expression))),
        ),

      open_statement: ($) =>
        seq(
          caseInsensitive("Open"),
          field("path", $._expression),
          caseInsensitive("For"),
          field("mode", $.file_mode),
          optional(seq(caseInsensitive("Access"), field("access", $.file_access))),
          optional(field("lock", $.file_lock)),
          caseInsensitive("As"),
          field("number", $.file_number),
          optional(seq(caseInsensitive("Len"), "=", field("record_length", $._expression))),
        ),

      file_mode: (_) =>
        choice(
          caseInsensitive("Append"),
          caseInsensitive("Binary"),
          caseInsensitive("Input"),
          caseInsensitive("Output"),
          caseInsensitive("Random"),
        ),

      file_access: (_) =>
        choice(
          caseInsensitive("Read"),
          caseInsensitive("Write"),
          seq(caseInsensitive("Read"), caseInsensitive("Write")),
        ),

      file_lock: (_) =>
        choice(
          caseInsensitive("Shared"),
          seq(
            caseInsensitive("Lock"),
            choice(
              caseInsensitive("Read"),
              caseInsensitive("Write"),
              seq(caseInsensitive("Read"), caseInsensitive("Write")),
            ),
          ),
        ),

      file_number: ($) => prec(1, choice($.file_number_literal, $._expression)),

      input_statement: ($) =>
        seq(
          caseInsensitive("Input"),
          field("number", $.file_number),
          ",",
          commaSep1(field("target", $._assignable_expression)),
        ),

      line_input_statement: ($) =>
        prec(
          1,
          seq(
            caseInsensitive("Line"),
            caseInsensitive("Input"),
            field("number", $.file_number),
            ",",
            field("target", $._assignable_expression),
          ),
        ),

      print_statement: ($) =>
        prec.right(
          3,
          seq(
            caseInsensitive("Print"),
            field("number", alias($._required_file_number, $.file_number)),
            optional(seq(",", optional(field("output", $.output_list)))),
          ),
        ),

      write_statement: ($) =>
        prec.right(
          3,
          seq(
            caseInsensitive("Write"),
            field("number", alias($._required_file_number, $.file_number)),
            optional(seq(",", optional(field("output", $.output_list)))),
          ),
        ),

      _required_file_number: ($) => $.file_number_literal,

      output_list: ($) =>
        prec.right(
          seq(
            field("value", $._print_output_expression),
            repeat(
              seq(field("position", $.char_position), field("value", $._print_output_expression)),
            ),
            optional(field("position", $.char_position)),
          ),
        ),

      _print_method_output_list: ($) =>
        prec.right(
          seq(
            field("value", $._unparenthesized_print_output_expression),
            repeat(
              seq(field("position", $.char_position), field("value", $._print_output_expression)),
            ),
            optional(field("position", $.char_position)),
          ),
        ),

      _print_output_expression: ($) =>
        choice($._unparenthesized_print_output_expression, $.parenthesized_expression),

      _unparenthesized_print_output_expression: ($) =>
        choice(
          prec.dynamic(65, alias($._print_output_call_binary_expression, $.binary_expression)),
          prec.dynamic(
            60,
            alias($._print_output_member_comparison_expression, $.comparison_expression),
          ),
          prec.dynamic(55, $.binary_expression),
          prec.dynamic(50, $.logical_value_expression),
          prec.dynamic(40, $.comparison_expression),
          prec.dynamic(
            30,
            alias($._print_output_call_member_expression, $.qualified_member_expression),
          ),
          prec.dynamic(5, $.member_expression),
          prec.dynamic(10, alias($._print_output_call_expression, $.call_expression)),
          $._literal,
          $.file_number_literal,
          alias(caseInsensitive("Line"), $.identifier),
          alias(caseInsensitive("Name"), $.identifier),
          $.identifier,
          ...(isVB6 ? [$.bang_identifier] : []),
          $.new_expression,
          $.addressof_expression,
          $.type_of_expression,
          $.unary_expression,
        ),

      _print_output_call_binary_expression: ($) =>
        choice(
          prec.right(14, seq($._print_output_call_operand, "^", $._expression)),
          prec.left(12, seq($._print_output_call_operand, choice("*", "/"), $._expression)),
          prec.left(11, seq($._print_output_call_operand, "\\", $._expression)),
          prec.left(10, seq($._print_output_call_operand, caseInsensitive("Mod"), $._expression)),
          prec.left(9, seq($._print_output_call_operand, choice("+", "-"), $._expression)),
          prec.left(8, seq($._print_output_call_operand, "&", $._expression)),
          prec.left(5, seq($._print_output_call_operand, caseInsensitive("And"), $._expression)),
          prec.left(4, seq($._print_output_call_operand, caseInsensitive("Or"), $._expression)),
          prec.left(3, seq($._print_output_call_operand, caseInsensitive("Xor"), $._expression)),
          prec.left(2, seq($._print_output_call_operand, caseInsensitive("Eqv"), $._expression)),
          prec.left(1, seq($._print_output_call_operand, caseInsensitive("Imp"), $._expression)),
        ),

      _print_output_call_operand: ($) =>
        choice(
          alias($._print_output_call_member_expression, $.qualified_member_expression),
          alias($._print_output_call_expression, $.call_expression),
        ),

      _print_output_member_comparison_expression: ($) =>
        prec.left(
          7,
          seq(
            field(
              "left",
              alias($._print_output_call_member_expression, $.qualified_member_expression),
            ),
            field("operator", $.comparison_operator),
            field("right", $._expression),
          ),
        ),

      _print_output_call_member_expression: ($) =>
        prec.left(
          4,
          seq(
            field(
              "receiver",
              choice(
                alias($._print_output_call_expression, $.call_expression),
                alias($._print_output_call_member_expression, $.qualified_member_expression),
              ),
            ),
            field("operator", memberOperator($)),
            field("member", $._member_property),
          ),
        ),

      _print_output_call_expression: ($) =>
        choice(
          prec(
            25,
            seq(
              field(
                "function",
                alias($._print_output_call_member_expression, $.qualified_member_expression),
              ),
              field("arguments", $.argument_list),
            ),
          ),
          prec(
            20,
            seq(
              field(
                "function",
                alias($._print_output_qualified_callable, $.qualified_member_expression),
              ),
              field("arguments", $.argument_list),
            ),
          ),
          prec(
            3,
            seq(field("function", $._callable_expression), field("arguments", $.argument_list)),
          ),
        ),

      _print_output_qualified_callable: ($) =>
        prec.left(
          20,
          seq(
            field("receiver", choice($.identifier, $.call_expression, $.member_expression)),
            field("operator", memberOperator($)),
            field("member", $._member_property),
          ),
        ),

      char_position: (_) => choice(";", ","),

      print_argument_sequence: ($) =>
        seq($._expression, repeat(seq(choice(",", ";"), $._expression))),

      debug_print_statement: ($) => prec.right(seq("?", optional(field("output", $.output_list)))),

      close_statement: ($) =>
        prec.right(seq(caseInsensitive("Close"), optional(commaSep1($.file_number)))),

      get_statement: ($) =>
        seq(
          caseInsensitive("Get"),
          field("number", $.file_number),
          optional(
            seq(
              ",",
              optional(field("record", $._expression)),
              ",",
              field("target", $._assignable_expression),
            ),
          ),
        ),

      put_statement: ($) =>
        seq(
          caseInsensitive("Put"),
          field("number", $.file_number),
          optional(
            seq(",", optional(field("record", $._expression)), ",", field("source", $._expression)),
          ),
        ),

      lock_statement: ($) =>
        seq(
          caseInsensitive("Lock"),
          field("number", $.file_number),
          optional(seq(",", field("range", $.file_record_range))),
        ),

      unlock_statement: ($) =>
        seq(
          caseInsensitive("Unlock"),
          field("number", $.file_number),
          optional(seq(",", field("range", $.file_record_range))),
        ),

      file_record_range: ($) =>
        seq(
          field("start", $._expression),
          optional(seq(caseInsensitive("To"), field("end", $._expression))),
        ),

      seek_statement: ($) =>
        seq(
          caseInsensitive("Seek"),
          field("number", $.file_number),
          ",",
          field("position", $._expression),
        ),

      reset_statement: (_) => caseInsensitive("Reset"),

      raise_event_statement: ($) =>
        prec.right(
          seq(caseInsensitive("RaiseEvent"), field("event", $.identifier), optional($.argument_list)),
        ),

      name_statement: ($) =>
        prec(
          1,
          seq(
            caseInsensitive("Name"),
            field("old_path", $._expression),
            caseInsensitive("As"),
            field("new_path", $._expression),
          ),
        ),

      stop_statement: (_) => caseInsensitive("Stop"),

      beep_statement: (_) => caseInsensitive("Beep"),

      load_statement: ($) => seq(caseInsensitive("Load"), field("target", $._assignable_expression)),

      unload_statement: ($) =>
        seq(caseInsensitive("Unload"), field("target", $._assignable_expression)),

      ...(isVB6
        ? {
            // Form / PictureBox / UserControl graphics methods, statement form only:
            //   Line [Step] (x1, y1) - [Step] (x2, y2) [, [color] [, B[F]]]
            //   [obj.]PSet [Step] (x, y) [, color]
            //   [obj.]Circle [Step] (x, y), radius [, [color] [, [start] [, [end] [, aspect]]]]
            // The `(x, y)` pair is not an expression, so these cannot be calls; the
            // dynamic precedence beats the call_statement reading of `Line (a, b)`.
            // The qualified `obj.Line (...)-(...)` form already exists in the base.
            line_statement: ($) =>
              prec.dynamic(
                10,
                seq(
                  caseInsensitive("Line"),
                  optional(caseInsensitive("Step")),
                  field("start", $.coordinate_pair),
                  "-",
                  optional(caseInsensitive("Step")),
                  field("end", $.coordinate_pair),
                  optional($._graphics_argument_tail),
                ),
              ),

            pset_statement: ($) =>
              prec.dynamic(
                10,
                seq(
                  field(
                    "method",
                    choice(
                      alias(caseInsensitive("PSet"), $.identifier),
                      alias($._pset_method_expression, $.qualified_member_expression),
                    ),
                  ),
                  optional(caseInsensitive("Step")),
                  field("point", $.coordinate_pair),
                  optional($._graphics_argument_tail),
                ),
              ),

            circle_statement: ($) =>
              prec.dynamic(
                10,
                seq(
                  field(
                    "method",
                    choice(
                      alias(caseInsensitive("Circle"), $.identifier),
                      alias($._circle_method_expression, $.qualified_member_expression),
                    ),
                  ),
                  optional(caseInsensitive("Step")),
                  field("center", $.coordinate_pair),
                  ",",
                  field("radius", $._expression),
                  optional($._graphics_argument_tail),
                ),
              ),

            // Same shape and precedence as the base's _line_method_expression, which
            // is how `obj.Line (...)-(...)` already wins over member access.
            _pset_method_expression: ($) =>
              prec(
                7,
                seq(
                  field("receiver", choice($.identifier, $.call_expression, $.member_expression)),
                  field("operator", $._dot_immediate),
                  field("member", alias(caseInsensitive("PSet"), $.identifier)),
                ),
              ),

            _circle_method_expression: ($) =>
              prec(
                7,
                seq(
                  field("receiver", choice($.identifier, $.call_expression, $.member_expression)),
                  field("operator", $._dot_immediate),
                  field("member", alias(caseInsensitive("Circle"), $.identifier)),
                ),
              ),

            // Trailing `, color, BF` with any argument omitted (`, , BF`).
            // The last item must carry an expression (VB forbids a trailing empty
            // argument), so after a comma the statement cannot end: no reduce competes
            // with shifting the argument, and `, vbRed, BF` stays in the tail.
            _graphics_argument_tail: ($) =>
              seq(
                repeat(seq(",", optional(field("argument", $._expression)))),
                ",",
                field("argument", $._expression),
              ),

            // `[obj.]Scale (x1, y1)-(x2, y2)`: sets a custom coordinate system.
            scale_statement: ($) =>
              prec.dynamic(
                10,
                seq(
                  field(
                    "method",
                    choice(
                      alias(caseInsensitive("Scale"), $.identifier),
                      alias($._scale_method_expression, $.qualified_member_expression),
                    ),
                  ),
                  field("start", $.coordinate_pair),
                  "-",
                  field("end", $.coordinate_pair),
                ),
              ),

            _scale_method_expression: ($) =>
              prec(
                7,
                seq(
                  field("receiver", choice($.identifier, $.call_expression, $.member_expression)),
                  field("operator", $._dot_immediate),
                  field("member", alias(caseInsensitive("Scale"), $.identifier)),
                ),
              ),
          }
        : {}),

      def_type_statement: ($) =>
        seq(
          field(
            "kind",
            choice(
              caseInsensitive("DefBool"),
              caseInsensitive("DefByte"),
              caseInsensitive("DefCur"),
              caseInsensitive("DefDate"),
              caseInsensitive("DefDbl"),
              caseInsensitive("DefDec"),
              caseInsensitive("DefInt"),
              caseInsensitive("DefLng"),
              ...(isVBA ? [caseInsensitive("DefLngLng"), caseInsensitive("DefLngPtr")] : []),
              caseInsensitive("DefObj"),
              caseInsensitive("DefSng"),
              caseInsensitive("DefStr"),
              caseInsensitive("DefVar"),
            ),
          ),
          commaSep1($.letter_range),
        ),

      letter_range: ($) =>
        prec.right(seq(field("start", $.identifier), optional(seq("-", field("end", $.identifier))))),

      set_statement: ($) =>
        seq(
          caseInsensitive("Set"),
          field("left", $._assignable_expression),
          "=",
          field("right", choice($.comparison_expression, $._expression)),
        ),

      // `arr(i) = x` is also readable as a call to `arr` with the comparison `(i) = x`
      // as its argument, and GLR holds both to the end of the line. The base breaks
      // that tie by tree shape, so vba keeps whatever it produced before. VB6 has no
      // consumers to keep stable and takes the assignment, prec.dynamic 1, which the
      // spaced-first-argument call (prec.dynamic 2) still outranks for `Foo .Bar = 1`.
      // The bonus is confined to a call-shaped target: on a plain name it would also
      // steer error recovery, turning `x = Foo(` from a MISSING `)` into an ERROR.
      assignment_statement: ($) => {
        const right = field(
          "right",
          choice($.logical_value_expression, $.comparison_expression, $._expression),
        );
        if (isVB6) {
          return choice(
            prec.dynamic(1, seq(field("left", $.call_expression), "=", right)),
            seq(
              // `Load = True` inside `Property Get Load()`: Load is a statement keyword
              // only when followed by a target, so a bare `Load =` is an assignment.
              field(
                "left",
                choice(
                  $._callable_expression,
                  alias(caseInsensitive("Line"), $.identifier),
                  alias(caseInsensitive("Load"), $.identifier),
                ),
              ),
              "=",
              right,
            ),
          );
        }
        return seq(field("left", $._assignable_expression), "=", right);
      },

      coordinate_pair: ($) =>
        prec(
          2,
          seq(
            "(",
            field("x", choice($.comparison_expression, $._expression)),
            ",",
            field("y", choice($.comparison_expression, $._expression)),
            ")",
          ),
        ),

      call_statement: ($) =>
        choice(
          // The base folds `Foo .Bar, x` lines into one opaque token because it cannot
          // see the space; vb6 resolves it with the scanner and keeps the structure.
          ...(isVBA ? [$._ambiguous_call_statement] : []),
          // VB6: `Foo .Bar, x`. The spaced dot rules out a chain, but a block needs no
          // separators, so GLR also sees `Foo` as an expression statement followed by
          // a new statement `.Bar, x`. This alternative accepts only a first argument
          // that starts with a spaced dot and carries the bonus that settles the tie.
          ...(isVB6
            ? [
                prec.dynamic(
                  2,
                  seq(
                    field("callee", $._callable_expression),
                    field(
                      "arguments",
                      alias($._spaced_first_argument_sequence, $.unparenthesized_argument_list),
                    ),
                  ),
                ),
              ]
            : []),
          prec.dynamic(4, field("callee", alias($._print_method_call_expression, $.call_expression))),
          prec.dynamic(3, prec.right(field("callee", $._print_method_callee))),
          prec.dynamic(
            2,
            prec.right(
              1,
              seq(
                field("callee", $._print_method_callee),
                field("arguments", alias($._print_method_output_list, $.output_list)),
              ),
            ),
          ),
          prec.right(
            1,
            choice(
              seq(
                caseInsensitive("Call"),
                field("callee", choice($.identifier, $.member_expression)),
                optional(field("arguments", $.argument_list)),
              ),
              seq(
                field("callee", alias($._line_method_expression, $.qualified_member_expression)),
                field("arguments", $.line_range_argument_list),
              ),
              // VB6: below the argument shifts (prec 3 on implicit_member_expression), so
              // that on a spaced dot after the callee the parser shifts into the argument
              // list instead of closing a callee-only call and starting a new statement.
              isVB6
                ? prec(-1, field("callee", $._callable_expression))
                : field("callee", $._callable_expression),
              seq(
                field("callee", $._callable_expression),
                field("arguments", choice($.argument_list, $.unparenthesized_argument_list)),
              ),
              field("callee", $.call_expression),
            ),
          ),
        ),

      _ambiguous_call_statement: (_) =>
        token(
          prec(
            2,
            choice(
              /[A-Za-z_][A-Za-z0-9_]*[ \t]+\.[A-Za-z_][A-Za-z0-9_]*[ \t]*,[^\r\n]*_[ \t]*\r?\n[ \t]*,[^\r\n]*/,
              /[A-Za-z_][A-Za-z0-9_]*[ \t]+\.[A-Za-z_][A-Za-z0-9_]*[ \t]*,[^\r\n]*/,
              /[A-Za-z_][A-Za-z0-9_]*[ \t]+[A-Za-z_][A-Za-z0-9_]*[ \t]*,[ \t]*,[ \t]*[A-Za-z_][A-Za-z0-9_]*[ \t]*,[ \t]*[A-Za-z_][A-Za-z0-9_]*[ \t]*:=[ \t]*[A-Za-z0-9_]+[ \t]*,[ \t]*[A-Za-z_][A-Za-z0-9_]*[ \t]*:=[ \t]*[A-Za-z0-9_]+/,
            ),
          ),
        ),

      // prec.dynamic -1 so that when GLR ties a longer statement against the same
      // text split into a statement plus a trailing expression statement (`GetIndex` +
      // `Key, , x`, or `pointers` + `(0)` in VB6; `Foo` + `GetName("x"), True`
      // in VBA, where `GetName ("x"), True` is a conceivable but invalid call), the
      // split loses. Unambiguous expression statements are unaffected.
      expression_statement: ($) => prec.dynamic(-1, $._expression),

      ...(isVB6
        ? {
            _spaced_first_argument_sequence: ($) =>
              seq(
                choice(
                  $._spaced_rooted_expression,
                  alias($._spaced_rooted_binary_expression, $.binary_expression),
                ),
                repeat($._omitted_argument_tail_item),
              ),

            // An expression whose first token is a spaced `.`/`!`: `.x`, `.x.y`, `.f(1)`.
            _spaced_rooted_expression: ($) =>
              choice(
                alias($._spaced_implicit_member_expression, $.implicit_member_expression),
                alias($._spaced_rooted_member, $.qualified_member_expression),
                alias($._spaced_rooted_call, $.call_expression),
              ),

            _spaced_implicit_member_expression: ($) =>
              prec.left(
                3,
                seq(
                  field("operator", choice($._dot_spaced, $._bang_spaced)),
                  field("member", $._member_property),
                ),
              ),

            _spaced_rooted_member: ($) =>
              prec.left(
                3,
                seq(
                  field("receiver", $._spaced_rooted_expression),
                  field("operator", memberOperator($)),
                  field("member", $._member_property),
                ),
              ),

            _spaced_rooted_call: ($) =>
              prec(
                2,
                seq(field("function", $._spaced_rooted_expression), field("arguments", $.argument_list)),
              ),

            // Same operator table as binary_expression, left operand rooted on the spaced member.
            _spaced_rooted_binary_expression: ($) =>
              choice(
                prec.right(14, seq($._spaced_rooted_operand, "^", $._expression)),
                prec.left(12, seq($._spaced_rooted_operand, choice("*", "/"), $._expression)),
                prec.left(11, seq($._spaced_rooted_operand, "\\", $._expression)),
                prec.left(10, seq($._spaced_rooted_operand, caseInsensitive("Mod"), $._expression)),
                prec.left(9, seq($._spaced_rooted_operand, choice("+", "-"), $._expression)),
                prec.left(8, seq($._spaced_rooted_operand, "&", $._expression)),
                prec.left(5, seq($._spaced_rooted_operand, caseInsensitive("And"), $._expression)),
                prec.left(4, seq($._spaced_rooted_operand, caseInsensitive("Or"), $._expression)),
                prec.left(3, seq($._spaced_rooted_operand, caseInsensitive("Xor"), $._expression)),
                prec.left(2, seq($._spaced_rooted_operand, caseInsensitive("Eqv"), $._expression)),
                prec.left(1, seq($._spaced_rooted_operand, caseInsensitive("Imp"), $._expression)),
              ),

            // prec 1: after `.A` an operator continues the argument rather than ending
            // the statement and starting a unary expression statement.
            _spaced_rooted_operand: ($) =>
              prec(
                1,
                choice(
                  $._spaced_rooted_expression,
                  alias($._spaced_rooted_binary_expression, $.binary_expression),
                ),
              ),
          }
        : {}),


      argument_list: ($) => seq("(", optional($._argument_sequence), ")"),

      unparenthesized_argument_list: ($) => $._unparenthesized_argument_sequence,

      line_range_argument_list: ($) =>
        seq(
          optional(caseInsensitive("Step")),
          field("start", $.coordinate_pair),
          optional(caseInsensitive("Step")),
          "-",
          ...(isVB6 ? [optional(caseInsensitive("Step"))] : []),
          field("end", $.coordinate_pair),
          isVB6 ? optional($._graphics_argument_tail) : optional(seq(",", $.print_argument_sequence)),
        ),

      _argument_sequence: ($) =>
        choice(
          prec(1, commaSep1($._argument)),
          prec.right(seq(optional($._argument), repeat1(seq(",", optional($._argument))))),
        ),

      _unparenthesized_argument_sequence: ($) =>
        choice(
          prec(2, $._omitted_argument_sequence),
          commaSep1($._argument),
          seq($._argument, repeat1(seq(";", $._argument))),
        ),

      // prec.right 2 sits on the repeated `, [arg]` unit, which is where the
      // shift/reduce happens: a block does not require separators between
      // statements, so after `Add , ,` the `"Port"` could start a new statement and
      // call_statement's own prec 1 would otherwise win and split the argument list.
      _omitted_argument_sequence: ($) =>
        choice(
          prec.right(2, seq(",", optional($._argument), repeat($._omitted_argument_tail_item))),
          prec.right(
            2,
            seq(
              $._argument,
              repeat(seq(",", $._argument)),
              ",",
              ",",
              optional($._argument),
              repeat($._omitted_argument_tail_item),
            ),
          ),
        ),

      // No dynamic precedence here. A per-item bonus rewards the reading that splits a
      // bare call in two: `CallByName a, b, c, d` becomes the expression statement
      // `CallByName` followed by a call to `a` whose omitted-first-argument list
      // collects one bonus per comma, and it beats the correct reading, which has no
      // omitted argument and scores nothing. Every bare call with three or more
      // arguments split that way while still parsing without an ERROR node.
      _omitted_argument_tail_item: ($) => prec.right(2, seq(",", optional($._argument))),

      _argument: ($) =>
        choice(
          $.byval_argument,
          $.named_argument,
          $.logical_value_expression,
          $.comparison_expression,
          $._expression,
        ),

      named_argument: ($) =>
        prec(
          1,
          seq(
            field("name", choice($.identifier, alias(caseInsensitive("Name"), $.identifier))),
            ":=",
            field(
              "value",
              choice($.logical_value_expression, $.comparison_expression, $._expression),
            ),
          ),
        ),

      byval_argument: ($) =>
        seq(caseInsensitive("ByVal"), field("value", choice($.comparison_expression, $._expression))),

      _condition_expression: ($) =>
        choice(
          $.condition_binary_expression,
          $.comparison_expression,
          $.parenthesized_condition_expression,
          $._expression,
        ),

      _primary_expression: ($) =>
        choice(
          $._literal,
          $.file_number_literal,
          $.call_expression,
          $.member_expression,
          ...(isVB6 ? [alias($._name_member_expression, $.qualified_member_expression)] : []),
          alias(caseInsensitive("Line"), $.identifier),
          alias(caseInsensitive("Name"), $.identifier),
          $.identifier,
          ...(isVB6 ? [$.bang_identifier] : []),
          $.new_expression,
          $.addressof_expression,
          $.type_of_expression,
          $.parenthesized_expression,
        ),

      _expression: ($) => choice($._primary_expression, $.binary_expression, $.unary_expression),

      comparison_expression: ($) =>
        prec.left(
          7,
          seq(
            field("left", $._comparison_operand),
            field("operator", $.comparison_operator),
            field("right", $._expression),
          ),
        ),

      comparison_operator: (_) =>
        choice("=", "<>", "<", "<=", ">", ">=", caseInsensitive("Is"), caseInsensitive("Like")),

      // A comparison may itself be the left operand (`a = b <> 0`, `x Is Nothing = False`);
      // prec.left 7 on comparison_expression makes the chain left-associative.
      // VB6 only for the chained form `a = b > c`. Admitting it in vba makes the call
      // reading of `arr(i) = x > 0` viable, callee `arr` with the single argument
      // `(i) = x > 0`, and it then wins the tie against the assignment; the base
      // grammar cannot parse the chain, so the call reading dies at `>` and the
      // assignment is the only tree left. Keeping vba on the base rule keeps that.
      _comparison_operand: ($) =>
        choice(
          $._primary_expression,
          $._signed_unary_expression,
          $.binary_expression,
          ...(isVB6 ? [$.comparison_expression] : []),
        ),

      logical_value_expression: ($) =>
        prec.dynamic(
          1,
          choice(
            prec.left(
              5,
              seq(
                choice($.comparison_expression, $._expression),
                caseInsensitive("And"),
                $.comparison_expression,
              ),
            ),
            prec.left(5, seq($.comparison_expression, caseInsensitive("And"), $._expression)),
            prec.left(
              5,
              seq(
                $.logical_value_expression,
                caseInsensitive("And"),
                choice($.comparison_expression, $._expression),
              ),
            ),
            prec.left(
              4,
              seq(
                choice($.comparison_expression, $._expression),
                caseInsensitive("Or"),
                $.comparison_expression,
              ),
            ),
            prec.left(4, seq($.comparison_expression, caseInsensitive("Or"), $._expression)),
            prec.left(
              4,
              seq(
                $.logical_value_expression,
                caseInsensitive("Or"),
                choice($.comparison_expression, $._expression),
              ),
            ),
          ),
        ),

      condition_binary_expression: ($) =>
        choice(
          prec.left(5, seq($._condition_expression, caseInsensitive("And"), $._condition_expression)),
          prec.left(4, seq($._condition_expression, caseInsensitive("Or"), $._condition_expression)),
          prec.left(3, seq($._condition_expression, caseInsensitive("Xor"), $._condition_expression)),
          prec.left(2, seq($._condition_expression, caseInsensitive("Eqv"), $._condition_expression)),
          prec.left(1, seq($._condition_expression, caseInsensitive("Imp"), $._condition_expression)),
        ),

      parenthesized_condition_expression: ($) =>
        prec.dynamic(1, seq("(", $._condition_expression, ")")),

      _assignable_expression: ($) =>
        choice(
          $._callable_expression,
          $.call_expression,
          alias(caseInsensitive("Line"), $.identifier),
        ),

      _callable_expression: ($) =>
        choice(
          $.identifier,
          ...(isVB6 ? [$.bang_identifier] : []),
          alias(caseInsensitive("Name"), $.identifier),
          ...(isVB6 ? [alias($._name_member_expression, $.qualified_member_expression)] : []),
          $.member_expression,
        ),

      // `Dim X!, Y!` / `total! = 0#`: `!` as the Single type-declaration character.
      // The lexer cannot tell it from the bang member operator (`rs!Field`), so the
      // parser decides on the next token: an identifier after `!` is member access,
      // anything else makes `!` a suffix. The result is a bang_identifier wrapping the
      // identifier and the `!` (aliasing to `identifier` itself does not wrap, since
      // identifier is the word token).
      // prec 2: prefer shifting the `!` over reducing the bare name, since a following
      // statement could otherwise begin with a bang implicit member (`!Field = 1`).
      // In vba the node appears only where a declaration names something; in
      // expression positions (`total! = 0#`, `Print x!`) it is VB6 only. There it
      // competes with the bang member operator in every expression state and cost the
      // vba table about 1,200 states, which is what pushed the browser artifact over
      // the size gate.
      bang_identifier: ($) => prec(2, seq($.identifier, bang($))),

      _print_method_callee: ($) =>
        choice(
          alias(caseInsensitive("Print"), $.identifier),
          alias($._qualified_print_method_expression, $.qualified_member_expression),
          alias($._implicit_print_method_expression, $.implicit_member_expression),
        ),

      _print_method_call_expression: ($) =>
        prec(7, seq(field("function", $._print_method_callee), field("arguments", $.argument_list))),

      _qualified_print_method_expression: ($) =>
        prec(
          7,
          seq(
            field("receiver", choice($.identifier, $.call_expression, $.member_expression)),
            field("operator", dot($)),
            field("member", alias(caseInsensitive("Print"), $.identifier)),
          ),
        ),

      _implicit_print_method_expression: ($) =>
        prec(
          7,
          seq(
            field("operator", isVB6 ? choice($._dot_immediate, $._dot_spaced) : "."),
            field("member", alias(caseInsensitive("Print"), $.identifier)),
          ),
        ),

      _line_method_expression: ($) =>
        prec(
          7,
          seq(
            field("receiver", choice($.identifier, $.call_expression, $.member_expression)),
            field("operator", dot($)),
            field("member", alias(caseInsensitive("Line"), $.identifier)),
          ),
        ),

      call_expression: ($) =>
        prec(
          2,
          choice(
            seq(field("function", $._callable_expression), field("arguments", $.argument_list)),
            seq(field("function", $.call_expression), field("arguments", $.argument_list)),
          ),
        ),

      new_expression: ($) =>
        prec(
          4,
          seq(caseInsensitive("New"), field("type", choice($.member_expression, $.identifier))),
        ),

      // `AddressOf Module2.Proc`: a dotted name, never a call, so `AddressOf f (x)`
      // stays unambiguous.
      addressof_expression: ($) =>
        seq(
          caseInsensitive("AddressOf"),
          field(
            "target",
            isVB6
              ? choice(
                  $.identifier,
                  alias($._addressof_dotted_target, $.qualified_member_expression),
                )
              : $.identifier,
          ),
        ),

      _addressof_dotted_target: ($) =>
        prec.right(
          1,
          seq(
            field("receiver", $.identifier),
            repeat1(seq(field("operator", dot($)), field("member", $.identifier))),
          ),
        ),

      type_of_expression: ($) =>
        seq(
          caseInsensitive("TypeOf"),
          field("value", $._expression),
          caseInsensitive("Is"),
          field("type", $.type_expression),
        ),

      member_expression: ($) => choice($.qualified_member_expression, $.implicit_member_expression),

      qualified_member_expression: ($) =>
        prec.left(
          3,
          seq(
            field(
              "receiver",
              choice(
                $.identifier,
                ...(isVB6 ? [alias($._name_member_expression, $.qualified_member_expression)] : []),
                $.call_expression,
                $.member_expression,
              ),
            ),
            // Inline choice, never a hidden rule: a `_member_operator` rule would add a
            // reduction after the token that competes with _bang_suffixed_identifier
            // (prec 2) as reduce/reduce, and `rst!Field` would become `rst!` + `Field`.
            field("operator", memberOperator($)),
            field("member", $._member_property),
          ),
        ),

      // `Name.fmtid` where a variable is called Name. prec 4 beats the reading
      // `Name` statement + implicit member `.fmtid` (prec 3).
      _name_member_expression: ($) =>
        prec.left(
          4,
          seq(
            field("receiver", alias(caseInsensitive("Name"), $.identifier)),
            field("operator", memberOperator($)),
            field("member", $._member_property),
          ),
        ),

      implicit_member_expression: ($) =>
        prec.left(3, seq(field("operator", implicitOperator($)), field("member", $._member_property))),

      // The bang variant sits outside the prec 6 wrapper so that it can carry its own
      // associativity; see _member_bang_suffixed_identifier.
      _member_property: ($) =>
        choice(
          prec(6, choice($.identifier, alias(caseInsensitive("Line"), $.identifier))),
          ...(isVB6 ? [alias($._member_bang_suffixed_identifier, $.bang_identifier)] : []),
        ),

      // `rec.Total! = 0#` versus `Ctl.Properties!Text`: after `a.b` the `!` is either
      // a type suffix on b or the bang operator, and only the token after decides.
      // prec.right 6 ties with the prec 6 of the plain member alternative, so the
      // shift/reduce is not settled statically and the declared conflict lets GLR
      // carry both readings; prec.dynamic -1 makes the suffix reading lose whenever
      // both survive, which is exactly the case where an identifier follows the `!`.
      _member_bang_suffixed_identifier: ($) =>
        prec.dynamic(-1, prec.right(6, seq($.identifier, bang($)))),

      parenthesized_expression: ($) =>
        seq("(", choice($._expression, $.comparison_expression, $.condition_binary_expression), ")"),

      binary_expression: ($) =>
        choice(
          prec.right(14, seq($._expression, "^", $._expression)),
          prec.left(12, seq($._expression, choice("*", "/"), $._expression)),
          prec.left(11, seq($._expression, "\\", $._expression)),
          prec.left(10, seq($._expression, caseInsensitive("Mod"), $._expression)),
          prec.left(9, seq($._expression, choice("+", "-"), $._expression)),
          prec.left(8, seq($._expression, "&", $._expression)),
          prec.left(5, seq($._expression, caseInsensitive("And"), $._expression)),
          prec.left(4, seq($._expression, caseInsensitive("Or"), $._expression)),
          prec.left(3, seq($._expression, caseInsensitive("Xor"), $._expression)),
          prec.left(2, seq($._expression, caseInsensitive("Eqv"), $._expression)),
          prec.left(1, seq($._expression, caseInsensitive("Imp"), $._expression)),
        ),

      unary_expression: ($) =>
        choice(
          $._signed_unary_expression,
          prec(6, seq(caseInsensitive("Not"), choice($.comparison_expression, $._expression))),
        ),

      _signed_unary_expression: ($) => prec(13, seq(choice("+", "-"), $._expression)),

      _literal: ($) =>
        choice(
          $.string_literal,
          $.number_literal,
          $.boolean_literal,
          $.nothing_literal,
          $.null_literal,
          $.empty_literal,
          $.date_literal,
        ),

      string_literal: (_) => token(seq('"', repeat(choice('""', /[^"\r\n]/)), '"')),

      number_literal: (_) =>
        token(
          choice(
            /-?&[Hh][0-9A-Fa-f]+[$%&!#@^]?/,
            ...(isVB6 ? [/-?&[Oo][0-7]+[$%&!#@^]?/] : []),
            /-?(?:\d+\.\d*|\.\d+|\d+)(?:[Ee][+-]?\d+)?[$%&!#@^]?/,
          ),
        ),

      boolean_literal: (_) => choice(caseInsensitive("True"), caseInsensitive("False")),

      nothing_literal: (_) => caseInsensitive("Nothing"),

      null_literal: (_) => caseInsensitive("Null"),

      empty_literal: (_) => caseInsensitive("Empty"),

      // No `"` inside: otherwise `Print #1, "# ..."` lexes `#1, "#` as a date.
      date_literal: (_) => (isVB6 ? token(/#[^#"\r\n]+#/) : token(/#[^#\r\n]+#/)),

      guid_literal: (_) => token(/\{[0-9A-Fa-f-]+\}/),

      file_number_literal: ($) => seq("#", field("number", $._expression)),

      identifier: (_) =>
        token(
          choice(
            /[A-Za-z_\u00C0-\u{10FFFF}][A-Za-z0-9_\u00C0-\u{10FFFF}]*[$%&#@^]?/u,
            prec(-1, /[A-Za-z_\u00C0-\u{10FFFF}][A-Za-z0-9_\u00C0-\u{10FFFF}]*!/u),
            /\[[^\]\r\n]+\]/,
          ),
        ),
    },
  });
};

function commaSep1(rule) {
  return seq(rule, repeat(seq(",", rule)));
}

function lineNumber($) {
  return prec(1, alias($.number_literal, $.line_number_literal));
}

function caseInsensitive(keyword) {
  return new RegExp(
    keyword
      .split("")
      .map((char) => {
        if (/[a-zA-Z]/.test(char)) {
          return `[${char.toLowerCase()}${char.toUpperCase()}]`;
        }
        return char.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      })
      .join(""),
  );
}
