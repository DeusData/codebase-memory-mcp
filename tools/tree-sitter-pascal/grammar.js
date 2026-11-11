/*
 * cbm self-maintained fork of tree-sitter-pascal (Unicode identifiers, #2211)
 *
 * Copyright (c) 2018 Benjamin Gray             (tree-sitter-pascal, MIT; maintained by Isopod/Philip Zander)
 * Copyright (c) 2026 DeusData                  (the "cbm fork (#2211)" hunk below)
 *
 * The VENDORED parser ships upstream's MIT LICENSE byte-identical; our copyright
 * for the patch lives here, with the source, and in THIRD_PARTY.md.
 *
 * Fork provenance (pinned; the commit this repo vendored before the fork):
 *   Isopod/tree-sitter-pascal @ 042119eca2e18a60e56317fb06ee3ba5c32cb447 (v0.10.2)
 *     grammar.js -> this file = upstream + this header + patches/unicode-identifiers.patch
 *   (the grammar has no external scanner)
 *
 * The patch (and nothing else) differs from upstream: upstream's identifier token
 * is ASCII-only ([&]?[a-zA-Z_]+[0-9_a-zA-Z]*), so a Delphi identifier with
 * non-ASCII letters -- e.g. the Cyrillic member names of 1C's OLE API called
 * through late-bound Variants -- became an ERROR node and the unit was flagged
 * parse_partial. The patched token takes a Unicode letter or '_' first, then
 * letters, combining marks, decimal digits or '_'. Keywords are still extracted
 * from this `word` token, so they lex as before; node types are unchanged.
 * On an upstream bump: re-fetch the pinned grammar.js, re-apply the patch,
 * regenerate, re-run test/corpus -- and drop the fork if upstream fixes it.
 *
 * Regenerate (ABI 14, as the upstream pin; NEVER use a CLI >= 0.26, which emits
 * ABI 16):
 *   cd tools/tree-sitter-pascal
 *   npx tree-sitter-cli@0.25.10 generate --abi 14
 *   npx tree-sitter-cli@0.25.10 test
 *   cp src/parser.c ../../internal/cbm/vendored/grammars/pascal/parser.c
 *   cp src/tree_sitter/parser.h ../../internal/cbm/vendored/grammars/pascal/tree_sitter/parser.h
 */

// Feature flags

// Support fpc's "public name" declaration hint, e.g.
//     procedure foo; public name '_FOO';
const public_name = true;
// Support extended RTTI attributes, e.g.
//     [MyAttr(42)]
//     procedure Foo;
const rtti        = true;
// Support Delphi's anonymous procedures & functions.
const lambda      = true;
// Support fpc-specific features.
const fpc         = true;
// Support delphi-specific features.
const delphi      = true;
// Support FPC PasCocoa extensions (for objective c interopability)
const objc        = true;
// Support generic types.
const templates   = delphi || fpc;
// Try to support preprocessor better.
const use_pp      = true;

// Helpers

const op = {
	infix:   (prio, lhs, op, rhs)      => prec.left(prio, seq(
		field('lhs',      lhs),
		field('operator', op),
		field('rhs',      rhs)
	)),
	prefix:  (prio, operator, operand) => prec.left(prio, seq(
		field('operator', operator),
		field('operand',  operand)
	)),
	postfix: (prio, operand, operator) => prec.left(prio, seq(
		field('operand',  operand),
		field('operator', operator)
	)),

	args: (prio, entity, open, args, close) => prec.left(prio, seq(
		field('entity', entity), open, field('args', args), close
	))
}

function delimited1(rule, delimiter = ',', precedence=0) {
	return seq(
		optional(repeat1(prec(precedence,seq(rule, delimiter)))),
		rule
	);
}

function delimited(rule, delimiter = ',') {
	return optional(delimited1(rule, delimiter));
}

// Preprocessor wrapper.
// This just supports a single `if[def] ... [else[if] ...]* endif` right now.
// It is inteded for code like this:
//
//   procedure foo;
//   {$ifdef bla}
//   var i: integer;
//   begin
//     inc(i);
//   end;
//   {$else}
//   var j: integer;
//   begin
//     dec(j);
//   end;
//   {$endif}
//
// If we don't handle this case explicitly, tree-sitter produces a completely
// broken AST, which severely messes up the syntax highlighting.
//
// Ideally, we would want to support nested ifdefs as well, but that will be
// more complex.
//
// A word of caution: It is tempting to sprinkle this macro in many more
// places, but unfortunately tihs results in a significant performance penalty.
// Use it sparingly! A general rule of thumb is to use it only in situations
// where otherwise a severly broken parse tree would be generated. For small
// errors that TreeSitter can recover from automatically, it is better not to
// use it.
function pp($, ...rule) {
	if (!use_pp)
		return seq(...rule);
	return (
		choice(
			seq(...rule),
			seq(
				alias(/\{\$if[^}]*\}/i, $.pp),
				...rule,
				repeat(seq(
					alias(/\{\$else[^}]*\}/i, $.pp),
					...rule
				)),
				alias(/\{\$end[^}]*\}/i, $.pp)
			),
		)
	);
}

// tr = Trailing
// Return the trailing equivalent of a rule, aliased to the non-trailing version.
const tr = ($,rule) =>
	rule[0] == '_' ? $[rule+'Tr'] : alias($[rule+'Tr'], $[rule])


function enable_if(cond, ...args) {
	return cond ? args : [];
}

// Generate rules for trailing & non-trailing statements
function statements(trailing) {
	let rn            = x => trailing ? x + 'Tr' : x
	let lastStatement = $ => trailing ? optional(tr($,'_statement')) : $._statement;
	let lastStatement1= $ => trailing ? tr($,'_statement') : $._statement;
	let semicolon     = trailing ? [] : [';'];

	return Object.fromEntries([
		[rn('if'),          $ => seq(
			$.kIf, field('condition', $._expr), $.kThen,
			field('then', lastStatement($))
		)],

		[rn('nestedIf'),    $ => prec(1,$.if)],

		[rn('ifElse'),      $ => prec.right(1, seq(
			$.kIf, field('condition', $._expr), $.kThen,
			field('then', optional(choice(tr($,'_statement'), $.if))),
			$.kElse,
			field('else', lastStatement($))
		))],

		[rn('while'),       $ => seq(
			$.kWhile, field('condition', $._expr), $.kDo,
			field('body', lastStatement($))
		)],

		[rn('repeat'),      $ => prec(2,seq(
			$.kRepeat,
			field('body', optional(tr($,'statements'))),
			$.kUntil, field('condition', $._expr),
			...semicolon
		))],

		[rn('for'),         $ => seq(
			$.kFor,
			field('start', $.assignment),
			choice($.kTo, $.kDownto),
			field('end', $._expr), $.kDo,
			field('body', lastStatement($))
		)],

		[rn('foreach'),     $ => seq(
			$.kFor,
			field('iterator', $._expr), $.kIn,
			field('iterable', $._expr), $.kDo,
			field('body', lastStatement($))
		)],

		[rn('exceptionHandler'), $ => seq(
			$.kOn,
			field('variable', optional(seq($.identifier, ':'))),
			field('exception', $.typeref), $.kDo,
			field('body', lastStatement($))
		)],

		[rn('exceptionElse'), $ => seq(
			$.kElse, repeat($._statement), lastStatement($)
		)],

		[rn('_exceptionHandlers'), $ => seq(
			repeat($.exceptionHandler),
			choice($.exceptionHandler, tr($,'exceptionHandler')),
			optional($.exceptionElse)
		)],

		[rn('try'),         $ => prec(2,seq(
			$.kTry,
			field('try', optional(tr($,'statements'))),
			choice(
				field('except', seq(
					$.kExcept,
					optional(
						choice(tr($,'statements'),
						tr($,'_exceptionHandlers'))
					)
				)),
				field('finally', seq(
					$.kFinally,
					optional(tr($,'statements'))
				))
			),
			$.kEnd, ...semicolon
		))],

		[rn('caseCase'),    $ => seq(
			field('label', $.caseLabel),
			field('body', lastStatement($))
		)],

		[rn('case'),        $ => prec(2,seq(
			$.kCase, $._expr, $.kOf,
			repeat($.caseCase),
			optional(tr($,'caseCase')),
			optional(seq(
				$.kElse,
				optional(':'),
				optional(tr($,'_statements'))
			)),
			$.kEnd, ...semicolon
		))],

		[rn('block'),       $ => seq(
			$.kBegin,
			optional(tr($,'_statements')),
			$.kEnd, ...semicolon
		)],

		[rn('asm'),         $ => seq(
			$.kAsm,
			optional($.asmBody),
			$.kEnd, ...semicolon
		)],

		[rn('with'),        $ => seq(
			$.kWith, delimited1(field('entity', $._expr)), $.kDo,
			field('body', lastStatement($))
		)],

		[rn('raise'),       $ => seq(
			$.kRaise,
			field('exception', $._expr),
			...semicolon
		)],

		[rn('statement'),   $ => choice(
			seq($._expr, ...semicolon),
		)],

		[rn('goto'),        $ => seq($.kGoto, $.identifier, ...semicolon)],

		[rn('_statement'),   $ => choice(
			...semicolon,
			seq($.assignment, ...semicolon),
			seq($.varDef, ...semicolon),
			alias($[rn('statement')], $.statement),
			alias($[rn('if')],        $.if),
			alias($[rn('ifElse')],    $.ifElse),
			alias($[rn('while')],     $.while),
			alias($[rn('repeat')],    $.repeat),
			alias($[rn('for')],       $.for),
			alias($[rn('foreach')],   $.foreach),
			alias($[rn('try')],       $.try),
			alias($[rn('case')],      $.case),
			alias($[rn('block')],     $.block),
			alias($[rn('with')],      $.with),
			alias($[rn('raise')],     $.raise),
			alias($[rn('goto')],      $.goto),
			alias($[rn('asm')],       $.asm),
		)],

	]);
}

module.exports = grammar({
	name: "pascal",

	extras: $ => [$._space, $.comment, $.pp],

	word: $ => $.identifier,

	conflicts: $ => [
		// The following conflict rules are only needed because "public" can be
		// a visibility or an attribute. *sigh*
		// TODO: We would probably avoid this by having separate decl* clauses
		// for use inside classes and at unit scope, since the "public"
		// attribute seems to only be valid for standalone routines.
		...enable_if(public_name,
			[$._declProc ], [ $._declOperator], [$.declConst], [$.declVar],
			[$.declType], [$.declProp]
		),
		// RTTI attributes clash with fpc declaration hints syntax since both
		// are surrounded by brackets.
		...enable_if(rtti,
			[ $.declProcFwd ], [ $.declVars], [ $.declConsts ], [ $.declTypes]
		),
		// `procedure (` could be a declaration of an anonymous procedure or
		// the call of a function named "procedure" (which doesn't actually
		// make sense, but for Treesitter it does), so we need another conflict
		// here.
		//...enable_if(lambda, [ $.lambda ]),
	],

	rules: {
		root:               $ => optional(choice(
			$.program,
			$.library,
			$.unit,
			$._definitions // For include files
		)),

		// HIGH LEVEL ----------------------------------------------------------

		program:            $ => seq(
			$.kProgram, $.moduleName, ';',
			optional($._definitions),
			tr($,'block'),
			$.kEndDot
		),

		library:            $ => seq(
			$.kLibrary, $.moduleName, ';',
			optional($._definitions),
			choice(tr($,'block'), $.kEnd),
			$.kEndDot
		),

		unit:               $ => seq(
			$.kUnit, $.moduleName, ';',
			repeat(choice(
				$.interface,
				$.implementation,
				$.initialization,
				$.finalization,
			)),
			$.kEnd, $.kEndDot
		),

		interface:       $ => seq($.kInterface, optional($._declarations)),
		implementation:  $ => seq($.kImplementation, optional($._definitions)),
		initialization:  $ => seq($.kInitialization, optional(tr($,'_statements'))),
		finalization:    $ => seq($.kFinalization, optional(tr($,'_statements'))),

		moduleName:      $ => delimited1($.identifier, $.kDot),

		// STATEMENTS ---------------------------------------------------------

		...statements(false),
		...statements(true),

		assignment:      $ => op.infix(1,
			choice($._expr, $.varAssignDef),
			choice(
				$.kAssign,
				...enable_if(fpc,
					$.kAssignAdd, $.kAssignSub, $.kAssignMul, $.kAssignDiv
				)
			),
			$._expr
		),
		varAssignDef:          $ => seq($.kVar, $.identifier,
			optional(seq(
				':',
				field('type', $.typeref)
			))),
		varDef:          $ => seq($.kVar, $.identifier, ':', field('type', $.typeref)),
		label:           $ => seq($.identifier, ':'),
		caseLabel:       $ => seq(delimited1(choice($._expr, $.range)), ':'),

		_statements:     $ => repeat1(choice($.varDef, $._statement,  $.label)),
		_statementsTr:   $ => seq(
			repeat(choice($._statement, $.label)),
			choice(tr($,'_statement'), $._statement)
		),

		statements:      $ => $._statements,
		statementsTr:    $ => $._statementsTr,

		asmBody: $ => repeat1(choice(
			///([a-zA-Z0-9_]+([eE][nN][dD])|[eE][nN][dD][a-zA-Z0-9_]+|([^eE]|[eE][^nN]|[eE][nN][^dD]))+/,
			$.identifier,       // Identifiers
			/[0-9a-fA-F]/,      // Numbers
			/[.,:;+\-*\[\]<>&%$]/, // Punctuation
			/\([^*]|\)/         // Parentheses that are not comments
		)),

		// EXPRESSIONS ---------------------------------------------------------

		_expr:           $ => choice(
			$._ref, $.exprBinary, $.exprUnary
		),

		_ref:            $ => choice(
			...enable_if(templates && fpc,
				// TODO: Ideally, the kSpecialize should be part of exprTpl,
				// but for some reason this leads to a rule conflict, so for
				// now we just put it here.
				//
				// Also, we have to write the rule in this weird weird way,
				// because if we just do
				//
				//   seq(optional($.kSpecialize), $.identifier)
				//
				// then we can't have a standalone identifier named
				// "specialize". (Bug in tree-sitter?)
				prec.left(choice(
					seq($.kSpecialize, $.identifier),
					seq(alias($.kSpecialize, $.identifier)),
				))
			),
			$.identifier,
			$._literal,  $.inherited, $.exprDot,
			$.exprBrackets, $.exprParens, $.exprSubscript, $.exprCall,
			alias($.exprDeref, $.exprUnary),
			alias($.exprAs, $.exprBinary),
			...enable_if(templates, $.exprTpl),
			...enable_if(lambda, $.lambda)
		),

		lambda:          $ => seq(
			choice($.kProcedure, $.kFunction),
			field('args', optional($.declArgs)),
			optional(seq(
				':',
				field('type', $.typeref),
			)),
			field('local', optional($._definitions)),
			field('body', choice(tr($, 'block'), tr($, 'asm'))),
		),

		inherited:       $ => prec.right(seq($.kInherited, optional($.identifier))),

		exprDot:         $ => op.infix(5, $._ref, $.kDot, $._ref),
		exprDeref:       $ => op.postfix(4, $._expr, $.kHat),

		exprAs:          $ => op.infix(3, $._expr, $.kAs,  $._expr),

		// Unfortunately, we can't use $.exprArgs for $.exprTpl because the
		// parser cannot handle it.
		//
		// There are two conflicting rules:
		//
		//   0. Binary comparison: a < b
		//   1. Template use:      a < b >
		//                         ^^^^^
		//                         prefix
		//
		// In order for this to work, the prefix must produce the same nodes in
		// both cases. This is not the case when we introduce a wrapper node.
		//
		// Example:
		//
		//   exprBinary
		//     identifier
		//     <
		//     identifier
		//
		//   vs.
		//
		//   exprTpl
		//     exprArgs <-- extra node
		//       identifier
		//       <
		//       identifier
		//       >
		//
		// Basically the way this works is that there is a tentative node like
		// "exprTplOrBinary", which looks like this:
		//
		//   exprTplOrBinary
		//     identifier
		//     <
		//     identifier
		//
		// At this point we don't yet know what we are dealing with.  The next
		// token will determine whether we are dealing with a comparison or a
		// template. Then the existing node is simply "renamed". Because of
		// this, we can't have an extra node in only one of the branches.
		//
		exprTpl:         $ => op.args(5, $._ref, $.kLt, delimited1($._expr, ',', 5),  $.kGt),
		exprSubscript:   $ => op.args(5, $._ref, '[',   $.exprArgs,  ']'  ),
		exprCall:        $ => op.args(5, $._ref, '(',   optional($.exprArgs), ')'  ),

		// Pascal legacy string formatting for WriteLn(foo:4:3) etc.
		legacyFormat:    $ => repeat1(seq(':', $._expr)),

		exprArgs:        $ => delimited1(seq($._expr, optional($.legacyFormat))),

		exprBinary:      $ => choice(
			op.infix(1, $._expr, $.kLt,  $._expr),
			op.infix(1, $._ref,  $.kLt,  $._expr),
			op.infix(1, $._expr, $.kEq,  $._expr),
			op.infix(1, $._expr, $.kNeq, $._expr),
			op.infix(1, $._expr, $.kGt,  $._expr),
			op.infix(1, $._expr, $.kLte, $._expr),
			op.infix(1, $._expr, $.kGte, $._expr),
			op.infix(1, $._expr, $.kIn,  $._expr),
			op.infix(1, $._expr, $.kIs,  $._expr),

			op.infix(2, $._expr, $.kAdd, $._expr),
			op.infix(2, $._expr, $.kSub, $._expr),
			op.infix(2, $._expr, $.kOr,  $._expr),
			op.infix(2, $._expr, $.kXor, $._expr),

			op.infix(3, $._expr, $.kMul, $._expr),
			op.infix(3, $._expr, $.kFdiv,$._expr),
			op.infix(3, $._expr, $.kDiv, $._expr),
			op.infix(3, $._expr, $.kMod, $._expr),
			op.infix(3, $._expr, $.kAnd, $._expr),
			op.infix(3, $._expr, $.kShl, $._expr),
			op.infix(3, $._expr, $.kShr, $._expr),
		),

		exprUnary:       $ => choice(
			op.prefix(4,  $.kNot,  $._expr),
			op.prefix(4,  $.kAdd,  $._expr),
			op.prefix(4,  $.kSub,  $._expr),
			op.prefix(4,  $.kAt,   $._expr),
		),

		exprParens:      $ => prec.left(5,seq('(', $._expr, ')')),

		// Set or array literal
		exprBrackets:       $ => seq(
			'[', delimited(choice($._expr, $.range)), ']'
		),

		// TYPES ---------------------------------------------------------------

		type:            $ => pp($,choice(
			$.typeref,
			$.declMetaClass,
			$.declEnum,
			$.declSet,
			$.declArray,
			$.declFile,
			$.declString,
			$.declProcRef,
		)),

		typeref:         $ => seq(
			...enable_if(fpc, field('_dummy', optional($.kSpecialize))),
			$._typeref,
			...enable_if(delphi, optional(seq($.kDeprecated, $._expr))),
		),

		_typeref:        $ => choice(
			$.identifier, $.typerefDot,
			...enable_if(templates, $.typerefTpl),
			$.typerefPtr,
		),

		typerefDot:      $ => op.infix(1,$._typeref, $.kDot, $._typeref),
		typerefTpl:      $ => op.args(1, $._typeref, $.kLt, $.typerefArgs, $.kGt),
		typerefPtr:      $ => op.prefix(1,$.kHat, $._typeref),
		typerefArgs:     $ => delimited1($._typeref),

		// GENERIC TYPE DECLARATION --------------------------------------------
		//
		// E.g. Foo<A: B, C: D<E>>.XYZ<T>
		//           ^     ^
		//     Note the optional constraints, which makes this different from a
		//     specialization
		//
		// We treat regular names as a special case of generic names. I.e. if
		// you see $._genericName somewhere, it doesn't mean that the name HAS
		// to be generic, it could just be a regular name like "TFoobar" or
		// "MyUnit.Foo".

		genericDot:      $ => op.infix(1,$._genericName, $.kDot, $._genericName),
		genericTpl:      $ => op.args(2,$._genericName, $.kLt, $.genericArgs, $.kGt),

		_genericName:    $ => choice(
			$.identifier, $.genericDot, ...enable_if(templates, $.genericTpl)
		),
		genericArgs:     $ => delimited1($.genericArg, ';'),
		genericArg:      $ => seq(
			field('name', delimited1($.identifier)),
			field('type', optional(seq(':', $.typeref))),
			field('defaultValue', optional($.defaultValue))
		),

		// LITERALS -----------------------------------------------------------

		_literal:        $ => choice(
			$.literalString,
			$.literalNumber,
			$.kNil, $.kTrue, $.kFalse
		),
		literalString:   $ => repeat1($._literalString),
		_literalString:  $ => choice(/'[^']*'/, $.literalChar),
		literalChar:     $ => seq('#', $._literalInt),
		literalNumber:   $ => choice($._literalInt, $._literalFloat),
		_literalInt:     $ => choice(
			token.immediate(/[-+]?[0-9]+/),
			token.immediate(/\$[a-fA-F0-9]+/)
		),
		_literalFloat:   $ => prec(10, /[-+]?[0-9]*\.?[0-9]+(e[+-]?[0-9]+)?/),

		range:           $ => seq(
			$._expr, '..', $._expr
		),

		// DEFINITIONS --------------------------------------------------------

		_definitions:    $ => repeat1($._definition),
		_definition:     $ => choice(
			$.declTypes, $.declVars, $.declConsts, $.defProc,
			alias($.declProcFwd, $.declProc),
			$.declLabels, $.declUses, $.declExports,

			// Not actually valid syntax, but helps the parser recover:
			prec(-1,$.blockTr)
		),

		defProc:         $ => seq(
			/*pp($,*/ field('header', $.declProc)/*)*/,
			pp(
			 	$,
				field('local', optional($._definitions)),
				field('body', choice(tr($, 'block'), tr($, 'asm'))),
				';'
			)
		),

		declProcFwd:     $ => seq(
			$._declProc,
			choice(seq($.kForward, ';'), $.procExternal),
			repeat($._procAttribute)
		),

		// DECLARATIONS -------------------------------------------------------

		_visibility:     $ => choice(
			$.kPublished, $.kPublic, $.kProtected, $.kPrivate
		),

		_declarations:   $ => repeat1(choice(
			$.declTypes, $.declVars, $.declConsts, $.declProc, $.declProp,
			alias($.declProcFwd, $.declProc),
			$.declUses, $.declLabels, $.declExports
		)),
		_classDeclarations: $ => repeat1(choice(
			$.declTypes, $.declVars, $.declConsts, $.declProc, $.declProp
		)),

		defaultValue:    $ => seq($.kEq, $._initializer),

		// Declaration sections

		declUses:        $ => seq($.kUses, delimited($.moduleName), ';'),
		declExports:     $ => seq($.kExports, delimited($.declExport), ';'),

		declTypes:       $ => seq(
			$.kType,
			repeat($.declType)
		),

		declVars:        $ => seq(
			optional($.kClass),
			choice($.kVar, $.kThreadvar),
			repeat($.declVar)
		),

		declConsts:      $ => seq(
			optional($.kClass),
			choice($.kConst, $.kResourcestring),
			repeat($.declConst),
		),

		// Declarations

		declType:        $ => seq(
			...enable_if(rtti, optional($.rttiAttributes)),
			...enable_if(fpc, optional($.kGeneric)),
			field('name', $._genericName), $.kEq,
			field('type',
				choice(
					seq(optional($.kType), $.type),
					choice($.type),
					$.declClass,
					$.declIntf,
					$.declHelper,
				)
			),
			';',
			repeat($._procAttribute)
		),

		declProc:        $ => seq(
			...enable_if(rtti, optional($.rttiAttributes)),
			choice($._declProc, $._declOperator),
		),

		declVar:         $ => seq(
			...enable_if(rtti, optional($.rttiAttributes)),
			field('name', delimited1($.identifier)),
			':',
			field('type', $.type),
			optional(choice(
				seq($.kAbsolute, $._ref),
				field('defaultValue', $.defaultValue)
			)),
			';',
			repeat(choice($._procAttribute, $.procExternal))
		),

		declConst:       $ => seq(
			...enable_if(rtti, optional($.rttiAttributes)),
			field('name', $.identifier),
			optional(seq(':', field('type', $.type))),
			field('defaultValue', $.defaultValue),
			';',
			repeat($._procAttribute)
		),

		declLabels:      $ => seq($.kLabel, delimited1($.declLabel), ';'),
		declLabel:       $ => field('name', $.identifier),

		declExport:      $ => seq($._genericName, repeat(seq(choice($.kName, $.kIndex), $._expr))),

		// Type declarations

		declEnum:        $ => seq('(', delimited1($.declEnumValue), ')'),
		declEnumValue:   $ => seq(field('name', $.identifier), field('value', optional($.defaultValue))),
		declSet:         $ => seq($.kSet, $.kOf, $.type),
		declArray:       $ => seq(
			optional($.kPacked),
			$.kArray,
			optional(seq('[', delimited(choice($.range, $._expr)), ']')),
			$.kOf, $.type
		),
		declFile:        $ => seq($.kFile, optional(seq($.kOf, $.type))),
		declString:      $ => prec.left(seq(
			$.kString,
			optional(seq('[', choice($._expr), ']'))
		)),

		declProcRef:     $ => prec.right(1,seq(
			optional(seq($.kReference, $.kTo)),
			choice($.kProcedure, $.kFunction),
			field('args', optional($.declArgs)),
			optional(seq(
				':',
				field('type', $.typeref),
			)),
			optional(seq($.kOf, $.kObject))
		)),

		declMetaClass:   $ => seq($.kClass, $.kOf, $.typeref),

		declClass:       $ => seq(
			optional($.kPacked),
			choice(
				$.kClass, $.kRecord, $.kObject,
				...enable_if(objc,
					$.kObjcclass, $.kObjccategory, $.kObjcprotocol
				)
			),
			optional(choice(
				$.kAbstract, $.kSealed,
				...enable_if(objc,
					seq($.kExternal, optional(seq($.kName, $._expr)))
				)
			)),
			field('parent', optional(seq('(',delimited($.typeref),')'))),
			optional($._declClass)
		),

		declIntf:        $ => seq(
			optional($.kPacked),
			choice(
				$.kInterface,
				...enable_if(delphi, $.kDispInterface)
			),
			field('parent', optional(seq('(',delimited($.typeref),')'))),
			field('guid', optional($.guid)),
			optional($._declClass)
		),

		declHelper:      $ => seq(
			choice($.kClass, $.kRecord, $.kType), $.kHelper,
			field('parent', optional(seq('(',delimited($.typeref),')'))),
			$.kFor, $.typeref,
			$._declClass
		),

		// Stuff for class/record/interface declarations

		guid:            $ => prec(1,seq('[', $._ref, ']')),

		_declClass:      $ => seq(
			optional($._declFields),
			optional($._classDeclarations),
			repeat($.declSection),
			optional($.declVariant),
			$.kEnd
		),

		declSection:     $ => seq(
			optional($.kStrict),
			choice($._visibility, ...enable_if(objc, $.kRequired, $.kOptional)),
			optional($._declFields),
			optional($._classDeclarations)
		),

		_declFields:     $ => repeat1($.declField),

		declField:       $ =>  seq(
			...enable_if(rtti, optional($.rttiAttributes)),
			field('name', delimited1($.identifier)),
			':',
			field('type', $.type),
			field('defaultValue', optional($.defaultValue)),
			';'
		),

		declProp:        $ => seq(
			...enable_if(rtti, optional($.rttiAttributes)),
			optional($.kClass),
			$.kProperty,
			field('name', $.identifier),
			field('args', optional($.declPropArgs)),
			':',
			field('type', $.type),
			repeat(choice(
				seq($.kIndex, field('index', $._expr)),
				...enable_if(delphi, seq($.kDispId, field('dispid', $._expr))),
				seq($.kRead, field('getter', $.identifier)),
				seq($.kWrite, field('setter', $.identifier)),
				seq($.kImplements, field('implements', delimited($._expr))),
				seq($.kDefault, field('defaultValue', $._expr)),
				seq($.kStored, field('stored', $._expr)),
				$.kNodefault,
			)),
			';',
			repeat($._procAttribute)
		),

		declPropArgs:    $ => seq('[', delimited($.declArg, ';'), ']'),

		// Variant records

		declVariant:     $ => prec.right(seq(
			$.kCase,
			field('name', optional(seq($.identifier, ':'))),
			field('type', $.typeref), $.kOf,
			delimited1($.declVariantClause, ';'),
			optional(';'),
		)),

		declVariantClause: $ => seq(
			$.caseLabel,
			'(',
			choice(
				seq(delimited(alias($.declVariantField, $.declField), ';'), optional(seq(';', $.declVariant))),
				seq($.declVariant),
			),
			optional(';'),
			')',
		),

		declVariantField: $ => seq(
			field('name', delimited1($.identifier)),
			':',
			field('type', $.type),
			field('defaultValue', optional($.defaultValue))
		),

		// Stuff for procedure / function / operator declarations

		_declProc:       $ => seq(
			...enable_if(fpc, optional($.kGeneric)),
			optional($.kClass),
			choice($.kProcedure, $.kFunction, $.kConstructor, $.kDestructor),
			field('name', $._genericName),
			field('args', optional($.declArgs)),
			optional(seq(
				':',
				field('type', $.typeref),
			)),
			field('assign', optional($.defaultValue)),
			';',
			repeat($._procAttributeNoExt)
		),

		_declOperator:   $ => seq(
			optional($.kClass),
			$.kOperator,
			field('name', $._operatorName),
			field('args', optional($.declArgs)),
			...enable_if(fpc, field('resultName', optional($.identifier))),
			':',
			field('type', $.type),
			field('assign', optional($.defaultValue)),
			';',
			repeat($._procAttributeNoExt)
		),

		operatorDot:     $ => op.infix(0, $._genericName, $.kDot, $.operatorName),
		_operatorName:   $ => seq(
			choice(
				$._genericName,
				...enable_if(fpc,
					$.operatorName,
					alias($.operatorDot, $.genericDot)
				)
			)
		),
		operatorName:    $ => choice(
			$.kDot, $.kLt, $.kEq, $.kNeq, $.kGt, $.kLte, $.kGte,
			$.kAdd, $.kSub, $.kMul, $.kFdiv, $.kDiv, $.kMod,
			$.kAssign,
			$.kOr, $.kXor, $.kAnd, $.kShl, $.kShr, $.kNot,
			$.kIn,
		),

		declArgs:        $ => seq('(', delimited($.declArg, ';'), ')'),

		declArg:         $ => choice(
			seq(
				choice($.kVar, $.kConst, $.kOut, $.kConstref),
				field('name', delimited1($.identifier)),
				optional(seq(
					':', field('type', $.type),
					field('defaultValue', optional($.defaultValue))
				))
			),
			seq(
				field('name', delimited1($.identifier)), ':',
				field('type', $.type),
				field('defaultValue', optional($.defaultValue))
			)
		),

		// Attributes & declaration hints

		_procAttribute:  $ => /*pp($,*/choice(
			seq(field('attribute', $.procAttribute), ';'),
			// FPC-specific syntax, e.g. procedure myproc; [public; alias:'bla'; cdecl];
			...enable_if(fpc, seq(
				'[',
				delimited(field('attribute', choice($.procAttribute, $.procExternal))),
				']', ';'
			))
		)/*)*/,
		_procAttributeNoExt: $ => /*pp($,*/ choice(
			seq(field('attribute', $.procAttribute), ';'),
			// FPC-specific syntax, e.g. procedure myproc; [public; alias:'bla'; cdecl];
			...enable_if(fpc, seq('[', delimited(field('attribute', choice($.procAttribute)), ';'), ']', ';'))
		)/*)*/,

		procAttribute:   $ => choice(
			$.kStatic, $.kVirtual, $.kDynamic, $.kAbstract, $.kOverride,
			$.kOverload, $.kReintroduce, $.kInline, $.kStdcall,
			$.kCdecl, $.kPascal, $.kRegister, $.kSafecall, $.kAssembler,
			$.kNoreturn, $.kLocal,  $.kFar, $.kNear,
			$.kDefault, $.kNodefault, $.kDeprecated, $.kExperimental,

			seq(
				choice(
					seq($.kMessage, optional($.kName)),
					$.kDeprecated
				),
				$._expr
			),

			...enable_if(fpc,
				$.kPlatform, $.kUnimplemented,
				$.kCppdecl, $.kCvar, $.kMwpascal, $.kNostackframe,
				$.kInterrupt, $.kIocheck, $.kHardfloat,
				$.kSoftfloat, $.kMs_abi_default, $.kMs_abi_cdecl,
				$.kSaveregisters, $.kSysv_abi_default, $.kSysv_abi_cdecl,
				$.kVectorcall, $.kVarargs, $.kWinapi,
				...enable_if(public_name, $.kPublic),
				seq(
					choice(
						$.kExport,
						seq($.kAlias, ':'),
						...enable_if(public_name, seq($.kPublic, $.kName)),
					),
					$._expr
				)
			),

			...enable_if(delphi, field('dispid', seq($.kDispId, $._expr))),
		),

		rttiAttributes:  $ => repeat1(seq(
			// Note: "Identifier:" is for tagging parameters of procedures (Delphi)
			'[', optional(seq($.identifier, ':')), delimited($._ref), ']'
		)),

		procExternal:    $ => seq(
			$.kExternal,
			optional($._expr),
			optional(seq(choice($.kName, $.kIndex), $._expr)),
			...enable_if(delphi, optional($.kDelayed)),
			';'
		),

		// INITIALIZERS --------------------------------------------------------

		_initializer:    $ => prec(2,seq(
			choice($._expr, $.recInitializer, $.arrInitializer)
		)),

		// record initializer
		recInitializer:  $ => seq(
			'(',
			delimited1( $.recInitializerField, ';'),
			')'
		),

		recInitializerField: $ => choice(
			seq(field('name',$.identifier), ':', field('value', $._initializer)),
			field('value', $._initializer)
		),

		// array initializer
		arrInitializer:  $ => prec(1,seq('(', delimited1($._initializer), ')')),

		// TERMINAL SYMBOLS ----------------------------------------------------

		kProgram:          $ => /program/i,
		kLibrary:          $ => /library/i,
		kUnit:             $ => /unit/i,
		kUses:             $ => /uses/i,
		kInterface:        $ => /interface/i,
		kDispInterface:    $ => /dispinterface/i,
		kImplementation:   $ => /implementation/i,
		kInitialization:   $ => /initialization/i,
		kFinalization:     $ => /finalization/i,
		kEndDot:           $ => '.',

		kBegin:            $ => /begin/i,
		kEnd:              $ => /end/i,
		kAsm:              $ => /asm/i,

		kVar:              $ => /var/i,
		kThreadvar:        $ => /threadvar/i,
		kConst:            $ => /const/i,
		kConstref:         $ => /constref/i,
		kResourcestring:   $ => /resourcestring/i,
		kOut:              $ => /out/i,
		kType:             $ => /type/i,
		kLabel:            $ => /label/i,
		kExports:          $ => /exports/i,

		kAbsolute:         $ => /absolute/i,

		kProperty:         $ => /property/i,
		kRead:             $ => /read/i,
		kWrite:            $ => /write/i,
		kImplements:       $ => /implements/i,
		kDefault:          $ => /default/i,
		kNodefault:        $ => /nodefault/i,
		kStored:           $ => /stored/i,
		kIndex:            $ => /index/i,
		kDispId:           $ => /dispid/i,

		kClass:            $ => /class/i,
		kInterface:        $ => /interface/i,
		kObject:           $ => /object/i,
		kRecord:           $ => /record/i,
		kObjcclass:        $ => /objcclass/i,
		kObjccategory:     $ => /objccategory/i,
		kObjcprotocol:     $ => /objcprotocol/i,
		kArray:            $ => /array/i,
		kFile:             $ => /file/i,
		kString:           $ => /string/i,
		kSet:              $ => /set/i,
		kOf:               $ => /of/i,
		kHelper:           $ => /helper/i,
		kPacked:           $ => /packed/i,

		kGeneric:          $ => /generic/i,
		kSpecialize:       $ => /specialize/i,

		kDot:              $ => '.',
		kLt:               $ => '<',
		kEq:               $ => '=',
		kNeq:              $ => '<>',
		kGt:               $ => '>',
		kLte:              $ => '<=',
		kGte:              $ => '>=',
		kAdd:              $ => '+',
		kSub:              $ => '-',
		kMul:              $ => '*',
		kFdiv:             $ => '/',
		kAt:               $ => '@',
		kHat:              $ => '^',
		kAssign:           $ => ':=',
		kAssignAdd:        $ => '+=', // Freepascal
		kAssignSub:        $ => '-=', // Freepascal
		kAssignMul:        $ => '*=', // Freepascal
		kAssignDiv:        $ => '/=', // Freepascal
		kOr:               $ => /or/i,
		kXor:              $ => /xor/i,
		kDiv:              $ => /div/i,
		kMod:              $ => /mod/i,
		kAnd:              $ => /and/i,
		kShl:              $ => /shl/i,
		kShr:              $ => /shr/i,
		kNot:              $ => /not/i,
		kIs:               $ => /is/i,
		kAs:               $ => /as/i,
		kIn:               $ => /in/i,

		kFor:              $ => /for/i,
		kTo:               $ => /to/i,
		kDownto:           $ => /downto/i,
		kIf:               $ => /if/i,
		kThen:             $ => /then/i,
		kElse:             $ => /else/i,
		kDo:               $ => /do/i,
		kWhile:            $ => /while/i,
		kRepeat:           $ => /repeat/i,
		kUntil:            $ => /until/i,
		kTry:              $ => /try/i,
		kExcept:           $ => /except/i,
		kFinally:          $ => /finally/i,
		kRaise:            $ => /raise/i,
		kOn:               $ => /on/i,
		kCase:             $ => /case/i,
		kWith:             $ => /with/i,
		kGoto:             $ => /goto/i,

		kFunction:         $ => /function/i,
		kProcedure:        $ => /procedure/i,
		kConstructor:      $ => /constructor/i,
		kDestructor:       $ => /destructor/i,
		kOperator:         $ => /operator/i,
		kReference:        $ => /reference/i,

		kPublished:        $ => /published/i,
		kPublic:           $ => /public/i,
		kProtected:        $ => /protected/i,
		kPrivate:          $ => /private/i,
		kStrict:           $ => /strict/i,
		kRequired:         $ => /required/i,
		kOptional:         $ => /optional/i,

		kForward:          $ => /forward/i,

		kStatic:           $ => /static/i,
		kVirtual:          $ => /virtual/i,
		kAbstract:         $ => /abstract/i,
		kSealed:           $ => /seled/i,
		kDynamic:          $ => /dynamic/i,
		kOverride:         $ => /override/i,
		kOverload:         $ => /overload/i,
		kReintroduce:      $ => /reintroduce/i,
		kInherited:        $ => /inherited/i,
		kInline:           $ => /inline/i,

		kStdcall:          $ => /stdcall/i,
		kCdecl:            $ => /cdecl/i,
		kCppdecl:          $ => /cppdecl/i,
		kPascal:           $ => /pascal/i,
		kRegister:         $ => /register/i,
		kMwpascal:         $ => /mwpascal/i,
		kExternal:         $ => /external/i,
		kName:             $ => /name/i,
		kMessage:          $ => /message/i,
		kDeprecated:       $ => /deprecated/i,
		kExperimental:     $ => /experimental/i,
		kPlatform:         $ => /platform/i,
		kUnimplemented:    $ => /unimplemented/i,
		kCvar:             $ => /cvar/i,
		kExport:           $ => /export/i,
		kFar:              $ => /far/i,
		kNear:             $ => /near/i,
		kSafecall:         $ => /safecal/i,
		kAssembler:        $ => /assembler/i,
		kNostackframe:     $ => /nostackframe/i,
		kInterrupt:        $ => /interrupt/i,
		kNoreturn:         $ => /noreturn/i,
		kIocheck:          $ => /iocheck/i,
		kLocal:            $ => /local/i,
		kHardfloat:        $ => /hardfloat/i,
		kSoftfloat:        $ => /softfloat/i,
		kMs_abi_default:   $ => /ms_abi_default/i,
		kMs_abi_cdecl:     $ => /ms_abi_cdecl/i,
		kSaveregisters:    $ => /saveregisters/i,
		kSysv_abi_default: $ => /sysv_abi_default/i,
		kSysv_abi_cdecl:   $ => /sysv_abi_cdecl/i,
		kVectorcall:       $ => /vectorcall/i,
		kVarargs:          $ => /varargs/i,
		kWinapi:           $ => /winapi/i,
		kAlias:            $ => /alias/i,
		// Delphi
		kDelayed:          $ => /delayed/i,

		kNil:              $ => /nil/i,
		kTrue:             $ => /true/i,
		kFalse:            $ => /false/i,

		kIfdef:            $ => /ifdef/i,
		kIfndef:           $ => /ifndef/i,
		kEndif:            $ => /endif/i,

		// cbm fork (#2211): Delphi accepts non-ASCII letters in identifiers (e.g. the
		// Cyrillic member names of 1C's OLE API); upstream is ASCII-only, so such a
		// name became an ERROR node. Keywords are still extracted from this `word`
		// token, so `begin`/`end`/`procedure` lex exactly as before.
		identifier:        $ => /[&]?[\p{L}_][\p{L}\p{M}\p{Nd}_]*/,

	  	_space:            $ => /[\s\r\n\t]+/,
		pp:                $ => /\{\$[^}]*\}/,
		comment:           $ => token(choice(
			seq('//', /.*/),
			seq('{', /([^$}][^}]*)?/, '}'),
			/[(][*]([^*]*[*]+[^)*])*[^*]*[*]+[)]/
		)),
	}
});
