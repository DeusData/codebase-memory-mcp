/**
 * The scanner is an extension to the built-in lexer that handles cases that are hard or impossible to express with the
 * high-level grammar rules.
 * Since Haskell is indentation sensitive and uses parse errors to end layouts, this component has many
 * responsibilities.
 *
 * tree-sitter runs the scanner at every position repeatedly until it fails, after which the built-in lexer consumes one
 * token.
 * When the scanner succeeds, it returns the index of a symbol in the `externals` array in `grammar/externals.js`, which
 * is then processed like other grammar symbols, except that it terminates any conflict branches in which the symbol
 * isn't valid.
 * The scanner's state is persisted and passed into the next run, but it is discarded when the scanner fails, i.e. when
 * it yields control back to the built-in lexer.
 *
 * The high-level workflow of the scanner consists of three distinct modes.
 * When the first character after whitespace is a newline, the scanner starts newline lookahead, otherwise it processes
 * an interior position.
 * If the state indicates that the previous run performed newline lookahead, it enters newline processing mode.
 *
 * In interior mode, a single lexing pass is performed.
 *
 * Such a pass consists of two steps:
 *
 * In the first step, the scanner identifies the immediate token by branching on the first character after whitespace
 * and examining different conditions to select one of the variants of the enum `Lexed`, which enumerates all known,
 * interesting, situations.
 * The position of the lexer may be advanced in the process to look at subsequent characters.
 * To avoid having to arrange different parts of the logic according to how many characters have been consumed,
 * lookahead is written to an array in the transient state on demand, so that each component can specify the index
 * relative to the position at the beginning of the run (modulo whitespace).
 * The entry point for this step is the function `lex`.
 *
 * The second step is different for each mode.
 * In interior mode, the `Lexed` token determines which symbol to return to the grammar based on the current state, like
 * layout contexts and valid symbols.
 * Most symbols do not contain any text, but only act as conditions in the grammar, but for symbolic operators, CPP,
 * comments, pragmas, and quasiquotes, the lexer is advanced to the end of the token and `mark_end` is called to
 * communicate the range to tree-sitter.
 *
 * In newline lookahead mode, the scanner performs repeated lexing passes until it encounters a `Lexed` token that is
 * not CPP or a comment.
 * In the second step of each pass, the token determines whether to terminate and/or which flags to set in the state to
 * guide processing in the next run.
 * If the lookahead loop has only made a single lexing pass that did not consume any characters of the following token
 * (because the first character did not match any of the conditions for lexing that require more lookahead), the scanner
 * switches to newline processing mode directly; otherwise it terminates the run after storing the newline information
 * in the persistent state.
 * This is possible by succeeding with the symbol `UPDATE`, which is mapped to newline in `externals`.
 * tree-sitter does not create a node in the parse tree for this symbol if `mark_end` wasn't called after consuming
 * lookahead, and immediately calls the scanner again at the same position.
 *
 * In either case, the scanner ends up in newline processing mode, in which it performs a series of highly
 * order-sensitive steps based on the data collected in lookahead mode, potentially returning multiple symbols in
 * successive runs until none of the newline-related conditions match.
 * This procedure ensures that nested layouts are terminated at the earliest position instead of extending over all
 * subsequent (top-level) whitespace, comments and CPP up to the next layout element.
 * Only when all layouts are terminated will the scanner process the final `Lexed` token that it stored in the state in
 * lookahead mode, using the same logic as in interior mode, and update the state to disable newline processing for the
 * next run.
 */

#include "tree_sitter/alloc.h"
#include "tree_sitter/array.h"
#include "tree_sitter/parser.h"

#include "unicode.h"
#include <assert.h>
#include <stdbool.h>
#include <string.h>

#define PEEK env->lexer->lookahead

#ifdef TREE_SITTER_DEBUG

#include <locale.h>

#define S_ADVANCE advance_debug(env)
#define S_SKIP skip_debug(env)
#define MARK(s) mark_debug(env, s)
#define dbg(...) do { fprintf(stderr, __VA_ARGS__); } while (0)

#else

// Move the parser position one character to the right.
#define S_ADVANCE advance(env)

// Move the parser position one character to the right, treating the consumed character as whitespace.
#define S_SKIP env->lexer->advance(env->lexer, true)

/**
 * Instruct the lexer that the current position is the end of the potentially detected symbol, causing the next run to
 * be started after this character in the success case.
 *
 * This is useful if the validity of the detected symbol depends on what follows.
 */
#define MARK(s) env->lexer->mark_end(env->lexer)

#define dbg(...) do {} while (0)

#endif

// Short circuit a parse step: If the argument expression returns 0, continue; otherwise return its result.
// This is used with enums, so casting to unsigned should not cause problems.
#define SEQ(expr) do { unsigned res = (unsigned) expr; if (res) return res; } while (0)

// --------------------------------------------------------------------------------------------------------
// Symbols
// --------------------------------------------------------------------------------------------------------

/**
 * This enum mirrors the symbols in `externals` in `grammar/externals.js`.
 * tree-sitter passes an array of booleans to the scanner whose entries are `true` if the symbol at the corresponding
 * index is valid at the current parser position.
 */
typedef enum {
  FAIL,
  SEMICOLON,
  START,
  START_DO,
  START_CASE,
  START_IF,
  START_LET,
  START_QUOTE,
  START_EXPLICIT,
  END,
  END_EXPLICIT,
  START_BRACE,
  END_BRACE,
  START_TEXP,
  END_TEXP,
  WHERE,
  IN,
  ARROW,
  BAR,
  DERIVING,
  COMMENT,
  HADDOCK,
  CPP,
  PRAGMA,
  QQ_START,
  QQ_BODY,
  SPLICE,
  QUAL_DOT,
  TIGHT_DOT,
  PREFIX_DOT,
  DOTDOT,
  TIGHT_AT,
  PREFIX_AT,
  TIGHT_BANG,
  PREFIX_BANG,
  TIGHT_TILDE,
  PREFIX_TILDE,
  PREFIX_PERCENT,
  QUALIFIED_OP,
  LEFT_SECTION_OP,
  NO_SECTION_OP,
  MINUS,
  CONTEXT,
  INFIX,
  DATA_INFIX,
  TYPE_INSTANCE,
  VARSYM,
  CONSYM,
  UPDATE,
} Symbol;

#ifdef TREE_SITTER_DEBUG

static const char *sym_names[] = {
  "fail",
  "semicolon",
  "start",
  "start_do",
  "start_case",
  "start_if",
  "start_let",
  "start_quote",
  "start_explicit",
  "end",
  "end_explicit",
  "start_brace",
  "end_brace",
  "start_texp",
  "end_texp",
  "where",
  "in",
  "arrow",
  "bar",
  "deriving",
  "comment",
  "haddock",
  "cpp",
  "pragma",
  "qq_start",
  "qq_body",
  "splice",
  "tight_dot",
  "proj_dot",
  "prefix_dot",
  "dotdot",
  "tight_at",
  "prefix_at",
  "tight_bang",
  "prefix_bang",
  "tight_tilde",
  "prefix_tilde",
  "prefix_percent",
  "qualified_op",
  "left_section_op",
  "no_section_op",
  "minus",
  "context",
  "infix",
  "data_infix",
  "type_instance",
  "varsym",
  "consym",
  "update",
};

#endif

// --------------------------------------------------------------------------------------------------------
// Data
// --------------------------------------------------------------------------------------------------------

#ifdef TREE_SITTER_DEBUG

typedef Array(int32_t) ParseLine;

/**
 * A vector of lines, persisted across runs, for visualizing the current lexer position and scanner lookahead.
 */
typedef Array(ParseLine) ParseLines;

/**
 * Info about calls to `mark_end` and how far the lexer has progressed in a run.
 * Discarded after each run.
 */
typedef struct {
  int marked;
  unsigned marked_line;
  unsigned start_col;
  unsigned start_line;
  unsigned end_col;
  const char *marked_by;
} Debug;

Debug debug_new(TSLexer *l) {
  return (Debug) {
    .marked = -1,
    .marked_line = 0,
    .start_col = l->get_column(l),
    .start_line = 0,
    .end_col = 0,
    .marked_by = "",
  };
}

#endif

/**
 * Different sorts of layout contexts that require special treatment.
 */
typedef enum {
  DeclLayout,
  DoLayout,
  CaseLayout,
  LetLayout,
  QuoteLayout,
  MultiWayIfLayout,
  Braces,
  TExp,
  ModuleHeader,
  NoContext,
} ContextSort;

#ifdef TREE_SITTER_DEBUG

static char const *context_names[] = {
  "decls",
  "do",
  "case",
  "let",
  "multi_way_if",
  "quote",
  "braces",
  "texp",
  "module_header",
  "none",
};

#endif

/**
 * The persistent state maintains a stack of layout contexts.
 * New entries are created when a layout symbol is valid at the current position, and they are removed when the indent
 * of a line satisfies conditions that depend on the current context sort, or when certain tokens (like `else`) occur.
 */
typedef struct {
  ContextSort sort;
  uint32_t indent;
} Context;

/**
 * This enumerates the lookahead tokens that have special meaning in the scanner.
 */
typedef enum {
  LNothing,
  LEof,
  LWhere,
  LIn,
  LThen,
  LElse,
  LDeriving,
  LModule,
  LUpper,
  LTick,
  LSymop,
  LSymopSpecial,
  LDotDot,
  LDotId,
  LDotSymop,
  LDotOpen,
  LDollar,
  LBang,
  LTilde,
  LAt,
  LPercent,
  LHash,
  LBar,
  LArrow,
  LCArrow,
  LTexpCloser,
  LQuoteClose,
  LPragma,
  LBlockComment,
  LLineComment,
  LBraceClose,
  LBraceOpen,
  LBracketOpen,
  LUnboxedClose,
  LSemi,
  LCppElse,
  LCpp,
} Lexed;

#ifdef TREE_SITTER_DEBUG

static const char *token_names[] = {
  "nothing",
  "eof",
  "where",
  "in",
  "then",
  "else",
  "deriving",
  "module",
  "upper",
  "tick",
  "symop",
  "symop-special",
  "dot-dot",
  "dot-id",
  "dot-symop",
  "dot-open",
  "dollar",
  "bang",
  "tilde",
  "at",
  "percent",
  "hash",
  "bar",
  "arrow",
  "ctr",
  "texp-closer",
  "quote-close",
  "pragma",
  "block-comment",
  "line-comment",
  "brace-close",
  "brace-open",
  "bracket-open",
  "unboxed-close",
  "semi",
  "cpp-else",
  "cpp",
};

#endif

/**
 * The current newline mode.
 * `NInit` is set during newline lookahead, and `NProcess` when lookahead has finished.
 * After processing is complete, the state is reset to `NInactive`.
 * `NResume` is a special variant that forces newline lookahead mode when a run starts without requiring a newline.
 * This is used for the beginning of the file and after pragmas (see `pragma`).
 */
typedef enum {
  NInactive,
  NInit,
  NProcess,
  NResume,
} NewlineState;

/**
 * The two newline modes need to operate across multiple scanner runs and adapt their behavior to the context
 * established by previous runs, encoded by this persistent state.
 */
typedef struct {
  NewlineState state;
  // The final token encountered after skipping comments and CPP.
  Lexed end;
  // The indent of `end`, used to decide layout actions before parsing intermediate extras.
  uint32_t indent;
  // When there is no token after extras, we shouldn't start layouts.
  bool eof;
  // Prohibit layout semicolons in future runs.
  bool no_semi;
  // Prohibit layout semicolons in future runs, but can be relaxed by some actions.
  // See `explicit_semicolon`.
  bool skip_semi;
  // Lookahead has advanced into `end`, so the scanner has to be restarted before processing the newline result.
  bool unsafe;
} Newline;

/**
 * The vector for the layout context stack.
 */
typedef Array(Context) Contexts;

/**
 * Whenever the lexer is advanced over non-(leading-)whitespace, the consumed character is appended to this vector.
 * This avoids having to ensure that different components that need to examine multiple lookahead characters have to be
 * run in the correct order.
 * Instead, we refer to lookahead by the character's index using the interface described in the section 'Lookahead'.
 *
 * For example, the functions `peek0`, `char0`, `char1` operate on the first/second character relative to the start of
 * the scanner run, and the implementation advances the lexer position when it is necessary.
 *
 * The field `offset` can be used to reset relative indexing to the current lexer position.
 * This is used, for example, in `newline_lookahead`, to perform repeated lexing passes, since `lex` uses the lookahead
 * interface.
 * After processing a `Lexed` token, `newline_lookahead` continues seeking ahead after comments and CPP, and when it
 * encounters the next token, it calls `reset_lookahead` to set `offset` to the current position, ensuring that `lex`
 * can use `char0` to test the following character.
 *
 * The terminology for advancing is:
 * - "Advance before character C at index N" means "`lexer->lookahead` returns C, but 'Lookahead' does not contain C and
 *   has size N"
 * - "Advance over character C at index N" means "`lexer->lookahead` returns the character following C, 'Lookahead'
 *   contains C and has size N+1" (or "advance before N+1")
 * - If the size of 'Lookahead' is already larger than N, and therefore C can be read from the vector, the
 *   postconditions may not hold (when independent steps access lookahead at different indexes)
 *
 * Example:
 *
 * Assume we are parsing the following line, and the scanner is called right after the `a` in the right-hand side:
 *
 * > calc a b = a Library.Math.** b
 *               ^ (lexer position: before the character above the ^, `lexer->lookahead` returns the space)
 * || 0/0 (content of `data` between bars, empty; `len` after bars, `offset` after slash)
 *
 * 'Lookahead' is initialized with `len = 0` and `offset = 0`.
 *
 * The full lookahead string (stored in tree-sitter's internals) at this position is ` Library.Math.** b`, and all
 * _absolute_ indexes point into that string.
 * Since tree-sitter only exposes the "next" character at a time, indexing requires advancing the lexer and copying
 * characters to 'Lookahead' on demand.
 *
 * An initial `skip_space` advances over the space between `a` and `Lib`, which does not update 'Lookahead'.
 *
 * > calc a b = a Library.Math.** b
 *                ^
 * || 0/0
 *
 * The uppercase character in `Lib` triggers the detection of qualified operators in `qualified_op`, which repeatedly
 * lexes module segments and dots.
 *
 * The module segment step starts (in `conid`) by checking that the next character is upper case using `peek0` (short
 * for `peek(0)`), which accesses the _first_ lookahead character – but _first_ is always relative to the current
 * `offset`.
 * We call the relative index `rel` and the absolute one `abs = offset + rel`.
 * Before `Lib`, this translates to `abs = rel = 0`.
 *
 * `peek` checks if 'Lookahead' already contains the character for this index (`abs < len`), so it can directly return
 * the value at `data[abs]`, which fails, since the vector is empty.
 * Instead, it will fetch the character directly from the tree-sitter lexer.
 * The lexer provides one character of lookahead outside of 'Lookahead', which is enough for this case.
 * `peek` is a conservative action, so it will not copy the character to 'Lookahead', and leave the lexer position
 * unchanged.
 *
 * `L` is upper case, so `qualified_op` switches to the next phase: Advancing to the end of the module segment, which
 * amounts to advancing before the first character that is not an identifier character:
 *
 * > advance_while(1, is_inner_id_char)
 *
 * This function applies the specified predicate to the character at the specified index.
 * If that returns `true`, it advances over the character and increments the index.
 * These steps are repeated until the predicate is `false`.
 * The index is returned, pointing to the character after the module segment.
 *
 * `peek0` doesn't modify lookahead, so the next character is still `L`.
 * We don't need to validate it again, so the starting index specified to `advance_while` is `1`.
 *
 * Let's look at the steps performed by this function in detail.
 * It starts by accessing the character at the initial index, calling `peek(1)`.
 * As for the `L` check, this calculates `abs = offset + rel = 0 + 1` and determines that it is smaller than `len`,
 * again.
 * However, this time the requested character is the _second_ lookahead character, so `peek` calls `advance_before(1)`,
 * which calls `advance` as many times as needed to access the character via `lexer->lookahead`, which is
 * `offset + n - len` times, so _once_ in this case.
 * The result is that `L` is copied to 'Lookahead' and `lexer->advance` is invoked one time, resulting in this new
 * state:
 *
 * > calc a b = a Library.Math.** b
 *                 ^
 * || 1/0
 *
 * Now `lexer->lookahead` returns `i`, which `conid` successfully validates as an "inner ID character", so it increments
 * the index to 2.
 * `peek(2)` performs the exact same steps as `peek(1)`, as do all subsequent steps until `peek(7)` returns `.`, which
 * fails the predicate, terminating the loop without advancing and returning 7 from `conid`, with the final state:
 *
 * > calc a b = a Library.Math.** b
 *                       ^
 * || 7/0
 *
 * `qualified_op` now examines the returned index:
 * If it is 0, the first character was not upper case and there is no module segment at this position, so lexing fails
 * and the scanner returns control to tree-sitter.
 * Otherwise, it calls `char_at(7, '.')` to require that the character after the module segment is a dot, with the same
 * consequences.
 *
 * Since our test code meets these conditions, `qualified_op` continues with `reset_lookahead_to(8)`.
 * This sets `offset` to 8, causing all future lookahead actions that use relative indexes to operate on characters
 * _after_ this new offset.
 * Here this is the first character after the dot, `M`.
 * Note that modifying the offset does not advance the lexer right away, so the lexer position will remain at 7:
 *
 * > calc a b = a Library.Math.** b
 *                       ^ (zero-based index 7)
 * || 7/8
 *
 * After a dot, `qualified_op` decides what to do next by determining whether what follows is a symbolic operator by
 * calling `symop_lookahead`, which uses the same predicate-based function as before, `advance_while(0, symop_char)`.
 * When that function calls `peek(0)`, the absolute index `offset + 0 = 8` is requested, which is not available, so the
 * lexer is advanced once:
 *
 * > calc a b = a Library.Math.** b
 *                        ^
 * || 8/8
 *
 * Note that `len == 8` means there are eight characters in 'Lookahead', up to and including the dot, while the index
 * `offset == 8` refers to the _ninth_ character, `M`.
 *
 * `M` is not a symop character, so `qualified_op` restarts the loop and parses the next module segment.
 * The process is identical to the previous iteration except for the value of `offset`, which causes all steps that
 * examine relative lookahead with `peek0` and `peek_at` add 8 to each index.
 *
 * Once the second dot is parsed, the symop test will succeed after advancing over both asterisks, which satisfies the
 * termination condition in `qualified_op`, and the scanner run finishes with the final state:
 *
 * > calc a b = a Library.Math.** b
 *                               ^
 * || 15/13
 */
typedef struct {
  int32_t *contents;
  uint32_t size;
  uint32_t capacity;
  uint32_t offset;
} Lookahead;

/**
 * The state that is persisted across scanner runs.
 *
 * Although 'Lookahead' is always reset when starting a new run, storing it in the state avoids having to allocate and
 * free the array repeatedly.
 * Instead we just reset the `len` attribute to 0 and reuse the previous memory.
 *
 * REVIEW: Can tree-sitter run the scanner concurrently on multiple nodes in the same file in some situations?
 */
typedef struct {
  Contexts contexts;
  Newline newline;
  Lookahead lookahead;
#ifdef TREE_SITTER_DEBUG
  ParseLines parse;
#endif
} State;

/**
 * Transient state and stuff provided by tree-sitter.
 */
typedef struct {
  TSLexer *lexer;
  const bool *symbols;
  uint32_t symop;
  State *state;
#ifdef TREE_SITTER_DEBUG
  Debug debug;
#endif
} Env;

static Env env_new(TSLexer *l, const bool * symbols, State *state) {
  return (Env) {
    .lexer = l,
    .symbols = symbols,
    .symop = 0,
    .state = state,
#ifdef TREE_SITTER_DEBUG
    .debug = debug_new(l),
#endif
  };
}

static void reset_newline(Env *env) { memset(&env->state->newline, 0, sizeof(Newline)); }

static bool newline_active(Env *env) { return env->state->newline.state == NInit || env->state->newline.state == NProcess; }

static bool newline_init(Env *env) { return env->state->newline.state == NInit; }

// --------------------------------------------------------------------------------------------------------
// Lexer interaction
// --------------------------------------------------------------------------------------------------------

static bool is_eof(Env *env) { return env->lexer->eof(env->lexer); }

static bool not_eof(Env *env) { return !(is_eof(env)); }

/**
 * The parser's position in the current line.
 * Note: This is expensive to use.
 */
static uint32_t column(Env *env) {
  return is_eof(env) ? 0 : env->lexer->get_column(env->lexer);
}

/**
 * tree-sitter's lexer interface maintains a current position that determines the lookahead character and the range of
 * text that is associated with the symbol selected by the scanner, if `mark_end` is called.
 *
 * It's not possible to read earlier characters once the lexer has advanced over them, so this function appends the
 * lookahead character to the array `lookahead` in the `State`.
 *
 * Don't add zeroes to the lookahead buffer when hitting EOF – it causes `no_lookahead` to report false negatives.
 */
static void advance(Env *env) {
  if (not_eof(env)) {
    int32_t c = PEEK;
    array_push(&env->state->lookahead, c);
    env->lexer->advance(env->lexer, false);
  }
}

static bool set_result_symbol(Env *env, Symbol result) {
  if (result != FAIL) {
    env->lexer->result_symbol = (TSSymbol) result;
    return true;
  }
  return false;
}

#ifdef TREE_SITTER_DEBUG

static void mark_debug(Env *env, const char *restrict marked_by) {
  dbg("mark: %s\n", marked_by);
  env->debug.marked = (int) column(env);
  env->debug.marked_line = 0;
  env->debug.marked_by = marked_by;
  env->lexer->mark_end(env->lexer);
}

static void append_parse_buffer(Env *env);

static void advance_debug(Env *env) {
  append_parse_buffer(env);
  advance(env);
}

static void skip_debug(Env *env) {
  append_parse_buffer(env);
  env->lexer->advance(env->lexer, true);
}

#endif

/**
 * `inline` has a noticeable impact, reaching parity with a macro.
 */
static inline bool valid(Env *env, Symbol s) { return env->symbols[s]; }

// --------------------------------------------------------------------------------------------------------
// Symbol constructors
// --------------------------------------------------------------------------------------------------------

static Symbol finish(Symbol s, const char *restrict desc) {
  // Suppress unused param warning
  (void) desc;
  dbg("finish: %s\n", desc);
  return s;
}

static Symbol finish_if_valid(Env *env, Symbol s, const char *restrict desc) {
  if (valid(env, s)) return finish(s, desc);
  return FAIL;
}

static Symbol finish_marked(Env *env, Symbol s, const char *restrict desc) {
  (void) desc;
  MARK(desc);
  return s;
}

static Symbol update_state(const char *restrict desc) {
  return finish(UPDATE, desc);
}

// --------------------------------------------------------------------------------------------------------
// Lookahead
// --------------------------------------------------------------------------------------------------------

/**
 * Ensure that at least `abs + 1` characters are present in the lookahead buffer by calling `advance` `len - abs + 1`
 * times.
 */
static void advance_over_abs(Env *env, uint32_t abs) {
  for (uint32_t i = env->state->lookahead.size; i <= abs; i++) S_ADVANCE;
}

/**
 * Ensure that at least `rel` characters after and including the current `offset` are present in the lookahead buffer by
 * calling `advance` as often as the difference between the desired index (`offset + rel`) and one less than the current
 * buffer size.
 *
 * Note: The character at the offset is included in the range, so that when `len == offset == rel == 0`, this function
 * advances once, over the character at index 0.
 */
static void advance_over(Env *env, uint32_t rel) {
  advance_over_abs(env, env->state->lookahead.offset + rel);
}

/**
 * Skip whitespace relative to `offset`, but keep characters that have already been copied to the buffer.
 *
 * Example:
 *
 * > a = b
 *    ^
 *
 * Assume step A sets `offset` to 1, pointing to the first space.
 * Step B calls `peek1`, to look at the `=`. This needs to advance over the space, which is copied to the lookahead
 * buffer, causing `lexer->lookahead` to return `=`.
 * Step C then calls `peek0`, sees that it is a space, and requests that it be skipped. Since it is already in the
 * buffer, calling `lexer-advance` would skip the wrong character.
 *
 * Hence, this function only skips indexes larger than the lookahead buffer's `len`.
 *
 * Additionally, if `offset` has been set to a position outside of the buffer, all characters up to that index are
 * copied to the buffer beforehand.
 */
static void skip_over(Env *env, uint32_t rel) {
  Lookahead *l = &env->state->lookahead;
  // Subtraction is safe because the condition establishes that `offset` is at least 1
  if (l->offset > l->size) advance_over_abs(env, l->offset - 1);
  uint32_t abs = l->offset + rel;
  for (uint32_t i = env->state->lookahead.size; i <= abs; i++) S_SKIP;
}

/**
 * Ensure that the lookahead buffer is large enough to allow reading the `n`th character.
 * Since `lexer->lookahead` points at the character after the buffer, it must have `offset + n - 1` elements.
 */
static void advance_before(Env *env, uint32_t rel) {
  uint32_t abs = env->state->lookahead.offset + rel;
  if (abs > 0) advance_over_abs(env, abs - 1);
}

/**
 * Return the lookahead character with index `n`.
 * If the index is larger than the lookahead buffer, return 0.
 *
 * Unsafe insofar as that it does not advance if the index points outside of the lookahead buffer.
 * This may happen in regular operation when a tool like `seq` attempts to look beyond EOF.
 */
static int32_t unsafe_peek_abs(Env *env, uint32_t abs) {
  return
    abs < env->state->lookahead.size ?
    env->state->lookahead.contents[abs] :
    0;
}

/**
 * Return the lookahead character with index `offset + n`.
 * See `unsafe_peek_abs`.
 */
static int32_t unsafe_peek(Env *env, uint32_t rel) {
  return unsafe_peek_abs(env, env->state->lookahead.offset + rel);
}

#ifdef TREE_SITTER_DEBUG

static void debug_peek(Env *env, uint32_t rel) {
  uint32_t abs = env->state->lookahead.offset + rel;
  dbg("peek ");
  if (env->state->lookahead.offset > 0) dbg("%u->", env->state->lookahead.offset);
  dbg("%u", rel);
  if (abs < env->state->lookahead.size)
    dbg(" cached | len: %u", env->state->lookahead.size);
  else if (abs > env->state->lookahead.size)
    dbg(" advance | len: %u", env->state->lookahead.size);
  dbg("\n");
}

#endif

/**
 * Return the lookahead character with index `offset + rel`.
 * If the character is not accessible, advance the position until it is.
 *
 * This "peeks" insofar as it doesn't advance over the requested character – `peek(0)` is equivalent to
 * `lexer->lookahead` if `offset == 0`.
 */
static int32_t peek(Env *env, uint32_t rel) {
#ifdef TREE_SITTER_DEBUG
  debug_peek(env, rel);
#endif
  if (env->state->lookahead.offset + rel < env->state->lookahead.size) return unsafe_peek(env, rel);
  else {
    advance_before(env, rel);
    return PEEK;
  }
}

/**
 * Return the first lookahead character after the `offset` without advancing the position.
 */
static int32_t peek0(Env *env) { return peek(env, 0); }

/**
 * Return the second lookahead character after the `offset` without advancing the position further than the first
 * character.
 */
static int32_t peek1(Env *env) { return peek(env, 1); }

/**
 * Return the third lookahead character after the `offset` without advancing the position further than the second
 * character.
 */
static int32_t peek2(Env *env) { return peek(env, 2); }

/**
 * Test the lookahead character at index `offset + n` for equality.
 */
static bool char_at(Env *env, uint32_t n, int32_t c) {
  return peek(env, n) == c;
}

/**
 * Test the lookahead character at index `offset` for equality.
 */
static bool char0(Env *env, int32_t c) {
  return char_at(env, 0, c);
}

/**
 * Test the lookahead character at index `offset + 1` for equality.
 */
static bool char1(Env *env, int32_t c) {
  return char_at(env, 1, c);
}

/**
 * Test the lookahead character at index `offset + 2` for equality.
 */
static bool char2(Env *env, int32_t c) {
  return char_at(env, 2, c);
}

/**
 * Set the offset to `index`, so that the indexes in future calls to lookahead functions like `char0` are interpreted
 * relative to this new value.
 *
 * Resets `symop` for soundness, even though no rule would continue after advancing over symbolic characters.
 *
 * See 'Lookahead' for an example.
 */
static void reset_lookahead_abs(Env *env, uint32_t abs) {
  dbg("reset: %u\n", abs);
  env->state->lookahead.offset = abs;
  env->symop = 0;
}

static void reset_lookahead_to(Env *env, uint32_t rel) {
  reset_lookahead_abs(env, env->state->lookahead.offset + rel);
}

/**
 * Move `offset` to the end of the consumed lookahead, causing `peek`, `char0` etc. to operate on characters following
 * the current position at the time this function is executed.
 */
static void reset_lookahead(Env *env) {
  reset_lookahead_abs(env, env->state->lookahead.size);
}

/**
 * Return whether the lookahead position has been advanced since starting the run, not considering skipped characters
 * (which are usually whitespace).
 * This is important to decide whether the scanner has to be restarted to emit certain symbols.
 *
 * For example, before starting layouts and generating layout semicolons after newlines, we skip whitespace and mark, so
 * that subsequent symbols start at their non-whitespace boundary instead of before the newline(s).
 * When newline lookahead mode finishes, it can continue directly with this step _only if_ no non-whitespace characters
 * were consumed, otherwise they would be included in the semicolon symbol.
 * We also cannot unconditionally mark after whitespace in newline lookahead mode since there are several potential
 * symbols that can be emitted before skipped whitespace is marked, like layout end, which should not extend beyond
 * newlines.
 */
static bool no_lookahead(Env *env) {
  return env->state->lookahead.size == 0;
}

/**
 * Return the column of the first lookahead character of the current run.
 * This is needed for starting layouts in interior mode, since we don't count positions across interior runs.
 */
static uint32_t start_column(Env *env) {
  return column(env) - env->state->lookahead.size;
}

/**
 * Increment `i` while the predicate is true for the lookahead character at that index (relative to `offset`), advancing
 * the position when `i` points beyond the end of the lookahead buffer.
 * Return the index after the last matching character.
 */
static uint32_t advance_while(Env *env, uint32_t i, bool (*pred)(int32_t)) {
  while (pred(peek(env, i))) { i++; }
  return i;
}

/**
 * Same as `advance_while`, using "not equal to `c`" for the predicate.
 * Stops at EOF.
 */
static uint32_t advance_until_char(Env *env, uint32_t i, int32_t c) {
  while (not_eof(env) && !char_at(env, i, c)) { i++; }
  return i;
}

// --------------------------------------------------------------------------------------------------------
// Context manipulation and conditions
// --------------------------------------------------------------------------------------------------------

static bool has_contexts(Env *env) { return env->state->contexts.size != 0; }

/**
 * Push a layout context onto the stack.
 */
static void push_context(Env *env, ContextSort sort, uint32_t indent) {
  dbg("push: %s %d\n", context_names[sort], indent);
  Context ctx = (Context) {.sort = sort, .indent = indent};
  array_push(&env->state->contexts, ctx);
}

/**
 * Remove a layout context from the stack.
 */
static void pop(Env *env) {
  if (has_contexts(env)) {
	dbg("pop: %s\n", context_names[array_back(&env->state->contexts)->sort]);
	array_pop(&env->state->contexts);
  }
}

static ContextSort current_context(Env *env) {
  return has_contexts(env) ? array_back(&env->state->contexts)->sort : NoContext;
}

static bool is_layout_context(Env *env) {
  return current_context(env) < Braces;
}

/**
 * Decide whether the current context requires generation of layout semicolons.
 * This is true for all layout contexts except for multi-way if, since that uses `|` to start layout elements.
 */
static bool is_semicolon_context(Env *env) {
  return current_context(env) < MultiWayIfLayout;
}

/**
 * Return the indent of the innermost layout context.
 * If there are non-layout contexts at the top of the stack, search downwards.
 */
static uint32_t current_indent(Env *env) {
  for (int32_t i = (int32_t) env->state->contexts.size - 1; i >= 0; i--) {
	Context *cur = array_get(&env->state->contexts, i);
    if (cur->sort < Braces) return cur->indent;
  }
  return 0;
}

static bool indent_less(Env *env, uint32_t indent) {
  return is_layout_context(env) && indent < current_indent(env);
}

static bool indent_lesseq(Env *env, uint32_t indent) {
  return is_layout_context(env) && indent <= current_indent(env);
}

static bool top_layout(Env *env) {
  return env->state->contexts.size == 1;
}

static bool in_module_header(Env *env) {
  return current_context(env) == ModuleHeader;
}

/**
 * Return the appropriate symbol to close the given context, or FAIL if it can't be closed.
 */
static Symbol context_end_sym(ContextSort s) {
  switch (s) {
    case TExp:
      return END_TEXP;
    case Braces:
      return END_BRACE;
    default:
      return s < Braces ? END : FAIL;
  }
}

// --------------------------------------------------------------------------------------------------------
// Character and lookahead conditions
// --------------------------------------------------------------------------------------------------------

#define NEWLINE_CASES \
  case '\n': \
  case '\r': \
  case '\f'


static bool is_newline(int32_t c) {
  switch (c) {
    NEWLINE_CASES:
      return true;
    default:
      return false;
  }
}

static bool varid_start_char(const int32_t c) { return c == '_' || is_varid_start_char(c); }

// TODO This should be combined with is_inner_id_char and made more explicit about when which char can occur.
// For example, lex_symop uses this to decide about prefix dot being a field selector, where single quotes aren't valid.
static bool is_id_char(const int32_t c) {
  return c == '_' || c == '\'' || is_identifier_char(c);
}

// TODO hashes only work at the end of identifiers
static bool is_inner_id_char(const int32_t c) {
  return is_id_char(c) || c == '#';
}

static bool quoter_char(const int32_t c) { return is_id_char(c) || c == '.'; }

static bool reserved_symbolic(const int32_t c) {
  switch (c) {
    case '(':
    case ')':
    case ',':
    case ';':
    case '[':
    case ']':
    case '`':
    case '{':
    case '}':
    case '"':
    case '\'':
    case '_':
      return true;
    default: return false;
  }
}

static bool symop_char(const int32_t c) {
  return is_symop_char(c) && !reserved_symbolic(c);
}

/**
 * Advance the position to the first character that's not valid for a symbolic operator, and return that position.
 * If the function has been called before, directly return the cached position.
 *
 * This consumes the entire symop, since the field denotes the length of the string and therefore the last (failing)
 * peek is _beyond_ the end, consuming the last valid char.
 */
static uint32_t symop_lookahead(Env *env) {
  if (env->symop == 0) {
    env->symop = advance_while(env, 0, symop_char);
    if (env->symop > 0)
      dbg("symop: %d, %.*ls\n", env->symop, env->symop, env->state->lookahead.contents + env->state->lookahead.offset);
  }
  return env->symop;
}

static bool is_symop(Env *env) {
  return symop_lookahead(env) > 0;
}

/**
 * The parser calls `scan` with all symbols declared as valid directly after it encountered an error.
 * The symbol `FAIL` is not used in the grammar, so it can only be valid in this error case.
 */

static bool after_error(Env *env) { return valid(env, FAIL); }

// --------------------------------------------------------------------------------------------------------
// Debug printing
// --------------------------------------------------------------------------------------------------------

#ifdef TREE_SITTER_DEBUG

static void push_parse_buffer_line(Env *env) {
  ParseLine new_line = array_new();
  array_reserve(&new_line, 1);
  array_push(&env->state->parse, new_line);
}

static ParseLine *ensure_parse_buffer(Env *env) {
  ParseLines *buffer = &env->state->parse;
  if (buffer->size == 0) push_parse_buffer_line(env);
  if (is_newline(PEEK)) push_parse_buffer_line(env);
  return array_back(buffer);
}

static void append_parse_buffer(Env *env) {
  ParseLine *current_line = ensure_parse_buffer(env);
  if (is_newline(PEEK)) {
    env->debug.marked_line++;
    env->debug.start_line++;
  }
  else if (column(env) >= current_line->size) array_push(current_line, PEEK);
}

static void fill_parse_buffer(Env *env) {
  env->debug.end_col = column(env);
  while (!(is_newline(PEEK) || is_eof(env))) S_ADVANCE;
}

static bool seq(Env *env, const char *restrict s);

static void print_lookahead(Env *env) {
  dbg("lookahead: %.*ls\n", env->state->lookahead.size, env->state->lookahead.contents);
}

static const char * space = "<space>";
static const char * newline_char = "\\n";

static const char * show_char(int32_t c) {
  switch (c) {
    NEWLINE_CASES:
      return newline_char;
    case ' ':
    case '\t':
    case '\v':
      return space;
    default:
      return NULL;
  }
}

static void print_lookahead_chars_from(Env *env, uint32_t start) {
  if (start < env->state->lookahead.size) {
    dbg("lookahead from %d: ", start);
    for (; start < env->state->lookahead.size; start++) {
      int32_t c = env->state->lookahead.contents[start];
      const char * s = show_char(c);
      if (s == NULL) dbg("%lc", c);
      else dbg("%s", s);
    }
    dbg("\n");
  }
  else
    dbg("print_lookahead_chars_from: Too large (%d / %d)", start, env->state->lookahead.size);
}

static void debug_contexts(Env *env) {
  if (env->state->contexts.size == 0) dbg("empty");
  bool empty = true;
  for (size_t i = 0; i < env->state->contexts.size; i++) {
    if (!empty) dbg("-");
	Context ctx = *array_get(&env->state->contexts, i);
    if (ctx.sort == ModuleHeader) dbg("pre");
    else if (ctx.sort == Braces) dbg("brace");
    else if (ctx.sort == TExp) dbg("texp");
    else {
      if (ctx.sort == DoLayout) dbg("do ");
      else if (ctx.sort == LetLayout) dbg("let ");
      else if (ctx.sort == CaseLayout) dbg("case ");
      else if (ctx.sort == MultiWayIfLayout) dbg("if ");
      else if (ctx.sort == QuoteLayout) dbg("quote ");
      dbg("%d", ctx.indent);
    }
    empty = false;
  }
}

void debug_newline(Env *env) {
  switch (env->state->newline.state) {
    case NInactive:
      dbg("no");
      break;
    case NInit:
      dbg("init");
      break;
    case NProcess:
      dbg("process");
      break;
    case NResume:
      dbg("resume");
      break;
  }
  if (env->state->newline.state != NInactive) dbg(" %d %s", env->state->newline.indent, token_names[env->state->newline.end]);
  if (env->state->newline.eof) dbg(" [eof]");
  if (env->state->newline.no_semi) dbg(" [no_semi]");
  if (env->state->newline.skip_semi) dbg(" [skip_semi]");
  if (env->state->newline.unsafe) dbg(" [unsafe]");
}

/**
 * Produce a comma-separated string of valid symbols.
 */
static void debug_valid(Env *env, const bool *syms) {
  if (after_error(env)) {
    dbg("all");
    return;
  }
  bool fst = true;
  for (Symbol i = FAIL; i <= UPDATE; i++) {
    if (syms[i]) {
      if (!fst) dbg(",");
      dbg("%s", sym_names[i]);
      fst = false;
    }
  }
}

static bool debug_init(Env *env) {
  setlocale(LC_ALL, "C.UTF-8");
  dbg("\n");
  dbg("state:\n  syms = ");
  debug_valid(env, env->symbols);
  dbg("\n  contexts = ");
  debug_contexts(env);
  dbg("\n  newline = ");
  debug_newline(env);
  dbg("\n");
  return false;
}

void sgr(const char *restrict code) {
  dbg("\x1b[%sm", code);
}

void color(unsigned c) {
  char code[3];
  sprintf(code, "3%d", c);
  sgr(code);
}

void palette() {
  color(4);
  dbg("before");
  color(2);
  dbg("  marked");
  color(3);
  dbg("  advanced");
  color(5);
  dbg("  lookahead");
  sgr("");
  dbg("\n");
}

static bool debug_parse_metadata = false;

static void dump_parse_metadata(Env *env) {
  Debug *debug = &env->debug;
  dbg(
      "lines: %d | start_line: %d | start_col: %d | marked_line: %d | marked: %d | end_col: %d | persist lines: %d\n",
      env->state->parse.size,
      debug->start_line,
      debug->start_col,
      debug->marked_line,
      debug->marked,
      debug->end_col,
      env->state->parse.size - debug->marked_line
  );
}

/**
 * Note: We're printing individual characters here instead of using a format with precision like `%.*ls` and slicing
 * the buffer, because:
 * - The buffer contains wide characters, but `fprintf` counts bytes
 * - `fwprintf` counts wide characters, but can't be interleaved with `fprintf`, so we'd have to use that function, and
 *   therefore wide literals, everywhere, which is tedious
 */
void debug_parse(Env *env) {
  Debug *debug = &env->debug;
  ParseLines *buffer = &env->state->parse;
  uint32_t lines = buffer->size;
  dbg("-----------------------\n");
  // For investigating mistakes in the debugging code.
  if (debug_parse_metadata) dump_parse_metadata(env);
  if (lines > 0) {
    color(4);
    for (uint32_t i = 0; i < lines; i++) {
	  ParseLine *line = array_get(buffer, i);
      int32_t *buf = line->contents;
      if (line->contents == NULL) break;
      uint32_t pos = 0;

      if (debug->start_line == lines - 1 - i) {
        while (pos < debug->start_col) { dbg("%lc", buf[pos]); pos++; }
        color(2);
      }

      if (debug->marked >= 0 && debug->marked_line == lines - 1 - i) {
        while ((int) pos < debug->marked) { dbg("%lc", buf[pos]); pos++; }
        color(3);
      }

      if (i == lines - 1) {
        while (pos < debug->end_col) { dbg("%lc", buf[pos]); pos++; }
        color(5);
      }

      while (pos < line->size) { dbg("%lc", buf[pos]); pos++; }

      dbg("\n");
    }
    sgr("");
  }
  dbg("-----------------------\n");
}

static unsigned serialize_parse_lines(char *cursor, ParseLines *parse, unsigned to_copy) {
  for (unsigned i = 0; i < parse->size; i++) {
	ParseLine *line = array_get(parse, i);
    unsigned line_size = line->size * sizeof(uint32_t);
    to_copy += line_size + sizeof(uint32_t);
    if (to_copy > TREE_SITTER_SERIALIZATION_BUFFER_SIZE) return 0;
    *((uint32_t *) cursor) = line->size;
    cursor += sizeof(line->size);
    memcpy(cursor, line->contents, line_size);
    cursor += line_size;
  }
  return to_copy;
}

static void deserialize_parse_lines(const char *cursor, ParseLines *parse, uint32_t size) {
  // Ensure ParseLines has room for at _least_ as many lines as the new state
  array_reserve(parse, size);
  for (unsigned i = 0; i < size; i++) {
	if (i >= parse->size) { array_push(parse, (ParseLine)array_new()); }
    ParseLine *line = &parse->contents[i];
    uint32_t line_len = *((uint32_t *) cursor);
    cursor += sizeof(uint32_t);
    array_reserve(line, line_len);
    line->size = line_len;
    unsigned line_size = line->size * sizeof(uint32_t);
    memcpy(line->contents, cursor, line_size);
    cursor += line_size;
  }
  // Free the excessive lines in the previous since we can't check in the next round whether there was a line in
  // a slot before and reuse the pointer.
  // This only happens when we didn't push any lines above, which would reset parse->len to len.
  for (unsigned i = parse->size; i > size; i--) { array_delete(array_get(parse, i - 1)); }
  // Truncate ParseLines in case the new state has fewer lines
  parse->size = size;
}

void debug_finish(Env *env, Symbol result) {
  dbg("result: ");
  if (result) dbg("%s, ", sym_names[result]);
  else dbg("<skipped>, ");
  if (env->debug.marked == -1) dbg("%d", column(env));
  else dbg("%s@%d", env->debug.marked_by, env->debug.marked);
  dbg("\n\n");
  fill_parse_buffer(env);
  debug_parse(env);
  env->state->parse.size -= env->debug.marked_line;
}

#endif

// --------------------------------------------------------------------------------------------------------
// Lookahead
// --------------------------------------------------------------------------------------------------------

/**
 * Check if lookahead contains the string `s` starting at position `offset + start`.
 * This advances only over matching characters.
 */
static bool seq_from(Env *env, const char *restrict s, uint32_t start) {
  uint32_t len = (uint32_t) strlen(s);
  for (uint32_t i = 0; i < len; i++) {
    int32_t c = s[i];
    int32_t c2 = peek(env, start + i);
    if (c != c2) return false;
  }
  peek(env, start + len);
  return true;
}

/**
 * Check if lookahead contains the string `s` starting at position `offset`.
 */
static bool seq(Env *env, const char *restrict s) {
  return seq_from(env, s, 0);
}

/**
 * Advance until the next newline or EOF, used to consume the body of a comment.
 */
static void take_line(Env *env) {
  while (not_eof(env) && !is_newline(PEEK)) S_ADVANCE;
}

static bool is_space_or_tab(int32_t c) {
  return c == ' ' || c == '\t';
}

/**
 * Advance until the next newline or EOF, used to consume the body of a cpp directive.
 * Escaped newlines are treated as line continuations, which allow spaces and tabs between backslash and newline.
 */
static void take_line_escaped_newline(Env *env) {
  for (;;) {
    while (not_eof(env) && !is_newline(PEEK) && PEEK != '\\') S_ADVANCE;
    if (PEEK == '\\') {
      S_ADVANCE;
      if (is_space_or_tab(PEEK)) {
        while (is_space_or_tab(PEEK)) S_ADVANCE;
        if (is_newline(PEEK)) S_ADVANCE;
      }
      else S_ADVANCE;
    }
    else return;
  }
}

/**
 * Skip the lexer until the following character is neither space nor tab.
 * Return whether any characters were skipped.
 */
static bool skip_space(Env *env) {
  if (!is_space_char(PEEK)) return false;
  S_SKIP;
  while(is_space_char(PEEK)) S_SKIP;
  return true;
}

/**
 * Skip the lexer until the following character is not a newline.
 * Return whether any characters were skipped.
 */
static bool skip_newlines(Env *env) {
  if (!is_newline(PEEK)) return false;
  S_SKIP;
  while(is_newline(PEEK)) S_SKIP;
  return true;
}

typedef enum {
  NoSpace,
  Indented,
  BOL,
} Space;

/**
 * Alternate between skipping space and newlines, and return which was seen last.
 * This does not use the lookahead buffer, but directly accesses the lexer.
 * Only to be used when it is certain that no whitespace has been copied to the buffer by previous steps, and that no
 * previous characters should be included in the range of non-zero-width symbol.
 */
static Space skip_whitespace(Env *env) {
  Space space = NoSpace;
  while (true) {
    if (skip_space(env)) space = Indented;
    else if (skip_newlines(env)) space = BOL;
    else return space;
  };
}

/**
 * Advance the lexer until the following character is neither space nor tab, starting at position `offset + start`, and
 * return the index of the next character.
 */
static uint32_t take_space_from(Env *env, uint32_t start) {
  return advance_while(env, start, is_space_char);
}

/**
 * Ensure that the character after a keyword like `module` is not a character that would change its meaning to be an
 * identifier.
 */
static bool token_end(int32_t c) { return !is_inner_id_char(c); }

/**
 * Check if lookahead contains the string `s` starting at position `offset + start`, followed by a non-id character.
 * See `seq`.
 */
static bool token_from(Env *env, const char *restrict s, uint32_t start) {
  return seq_from(env, s, start) && token_end(peek(env, start + (uint32_t) strlen(s)));
}

/**
 * `token_from` at the current offset.
 */
static bool token(Env *env, const char *restrict s) {
  return seq(env, s) && token_end(peek(env, (uint32_t) strlen(s)));
}

/**
 * Check if lookahead contains any of the strings in `tokens` starting at position `offset + start`, followed by a
 * non-id character.
 */
static bool any_token_from(Env *env, size_t n, const char * tokens[], uint32_t start) {
  for (size_t i = 0; i < n; i++) {
    if (token_from(env, tokens[i], start)) return true;
  }
  return false;
}

static bool match_symop(Env *env, const char *restrict target) {
  return symop_lookahead(env) == strlen(target) && seq(env, target);
}

static bool uninitialized(Env *env) { return !has_contexts(env); }

static uint32_t conid(Env *env) {
  if (!is_conid_start_char(peek0(env))) return 0;
  return advance_while(env, 1, is_inner_id_char);
}

typedef enum {
  NoQualifiedName,
  QualifiedTarget,
  QualifiedConid,
} QualifiedName;

static QualifiedName qualified_name(Env *env, bool (*name)(Env *)) {
  bool qualified = false;
  while (true) {
    uint32_t end = conid(env);
    if (end == 0) break;
    if (!char_at(env, end, '.')) {
      if (qualified) return QualifiedConid;
      else break;
    }
    qualified = true;
    reset_lookahead_to(env, end + 1);
    if (name(env)) return true;
  }
  return NoQualifiedName;
}

/**
 * Use the lookahead buffer to determine whether a character is escaped, by counting the number of backslashes.
 */
static bool odd_backslashes_before(Env *env, int32_t index) {
  bool odd = false;
  while (index >= 0 && peek(env, (uint32_t) index) == '\\') {
    odd = !odd;
    index--;
  }
  return odd;
}

/**
 * Advance before the next unescaped double quote.
 */
static uint32_t take_string_literal(Env *env) {
  uint32_t end = 1;
  while (true) {
    end = advance_until_char(env, end, '"') + 1;
    if (is_eof(env) || !odd_backslashes_before(env, (int) end - 2)) return end;
  }
}

/**
 * Advance before the single quote that validly ends a character literal.
 * If there is none, return 1.
 * Either the first character is a backslash, or the second character is a single quote.
 *
 * A single quote followed by backslash is a char unless it was part of a varid, in which case the backslash is a
 * lambda.
 * The caller must make sure to lex varids beforehand.
 */
static uint32_t take_char_literal(Env *env) {
  if (char1(env, '\\')) return advance_until_char(env, 2, '\'') + 2;
  else return char_at(env, 2, '\'') ? 3 : 1;
}

// --------------------------------------------------------------------------------------------------------
// Lookahead: CPP
// --------------------------------------------------------------------------------------------------------

typedef enum {
  CppNothing,
  CppStart,
  CppElse,
  CppEnd,
  CppOther,
} CppDirective;

static const char *cpp_tokens_start[3] = {
  "if",
  "ifdef",
  "ifndef",
};

static bool cpp_cond_start(Env *env, uint32_t start) {
  return any_token_from(env, 3, cpp_tokens_start, start);
}

static const char *cpp_tokens_else[4] = {
  "else",
  "elif",
  "elifdef",
  "elifndef",
};

static bool cpp_cond_else(Env *env, uint32_t start) {
  return any_token_from(env, 4, cpp_tokens_else, start);
}

static bool cpp_cond_end(Env *env, uint32_t start) { return token_from(env, "endif", start); }

static const char *cpp_tokens_other[7] = {
  "define",
  "undef",
  "include",
  "pragma",
  "error",
  "warning",
  "line",
};

static bool cpp_directive_other(Env *env, uint32_t start) {
  return
    any_token_from(env, 7, cpp_tokens_other, start)
    ||
    // A hash followed by nothing but whitespace is CPP.
    // If non-whitespace follows whitespace, it is a parse error, unless we're in a brace layout; then it is a varsym.
    // Complete overkill to parse this, but eh!
    is_newline(peek(env, start))
    ||
    // shebang for scripts
    (char1(env, '!') && uninitialized(env))
    ;
}

/**
 * If the first character at `offset` is a hash, skip space and try all tokens that start a CPP directive.
 * Return the matching variant of the enum `CppDirective`.
 */
static CppDirective cpp_directive(Env *env) {
  if (!char0(env, '#')) return CppNothing;
  uint32_t start = take_space_from(env, 1);
  if (cpp_cond_start(env, start)) return CppStart;
  else if (cpp_cond_else(env, start)) return CppElse;
  else if (cpp_cond_end(env, start)) return CppEnd;
  else if (cpp_directive_other(env, start)) return CppOther;
  else return CppNothing;
}

// --------------------------------------------------------------------------------------------------------
// Starting layouts
// --------------------------------------------------------------------------------------------------------

/**
 * Opening and closing braces are always followed by a command (`grammar/util.js`), so this can unconditionally push a
 * context.
 * See `grammar/externals.js` for more.
 *
 * Note: This is not related to regular brace layouts, which are handled by `start_layout`!
 * Aside from layouts, braces are also used for records and inferred type variables, where indentation is also ignored!
 * Therefore, we add a context to skip steps like semicolon generation.
 *
 * Check out some examples in the tests:
 * - data: record zero indent
 * - type decl: inferred quantifier at column 0
 */
static Symbol start_brace(Env *env) {
  if (valid(env, START_BRACE)) {
    push_context(env, Braces, 0);
    return finish(START_BRACE, "start_brace");
  }
  return FAIL;
}

/**
 * See `start_brace`.
 */
static Symbol end_brace(Env *env) {
  if (valid(env, END_BRACE) && current_context(env) == Braces) {
    pop(env);
    return finish(END_BRACE, "end_brace");
  }
  return FAIL;
}

/**
 * Return the first valid layout start symbol.
 */
static Symbol valid_layout_start_sym(Env *env) {
  for (Symbol i = START; i < END; i++) {
    if (valid(env, i)) return i;
  }
  return FAIL;
}

/**
 * Map `Symbol` to `ContextSort`.
 */
static ContextSort layout_sort(Symbol s) {
  switch (s) {
    case START_DO:
      return DoLayout;
    case START_CASE:
      return CaseLayout;
    case START_IF:
      return MultiWayIfLayout;
    case START_LET:
      return LetLayout;
    case START_QUOTE:
      return QuoteLayout;
    default:
      return DeclLayout;
  }
}

typedef struct {
  Symbol sym;
  ContextSort sort;
} StartLayout;

/**
 * Determine whether the layout sort corresponding to the potentially valid symbol can start at this position.
 * If the context stack is `uninitialized`, the first layout is added by `process_token_init`.
 * In newline processing mode, brace layouts cannot be started because there may be comments before the brace that need
 * to be emitted first.
 * Regular `if/then/else` conditionals are always valid at the same position as multi-way if layouts.
 * If we were to unconditionally start a layout when START_IF is valid, it would never be possible to parse the former,
 * so this skips that layout sort unless the `Lexed` token is `LBar`.
 */
static StartLayout valid_layout_start(Env *env, Lexed next) {
  StartLayout start = {.sym = valid_layout_start_sym(env), .sort = NoContext};
  if (uninitialized(env) || start.sym == FAIL) return start;
  ContextSort sort = layout_sort(start.sym);
  switch (next) {
    case LBar:
      break;
    case LBraceOpen:
      if (newline_active(env)) return start;
      sort = Braces;
      start.sym = START_EXPLICIT;
      break;
    default:
      if (sort == MultiWayIfLayout) return start;
      break;
  }
  start.sort = sort;
  return start;
}

/**
 * If the current context is a brace layout, any indent is legal for a new layout.
 * Otherwise, compare with the indent of the current context.
 * Since starting layouts is allowed in tuple expressions, we look at the last real indent, skipping over `TExp`s, using
 * 0 if none exists (which should never be the case).
 *
 * According to the docs for `NondecreasingIndentation`, the rule is that a nested context may start at the same column
 * _if the enclosing context is a do expression_.
 * From experimental evidence, it appears though that this is the other way round – a do expression within, say, a case
 * alt can start at the same level as the case layout.
 */
static bool indent_can_start_layout(Env *env, ContextSort sort, uint32_t indent) {
  if (current_context(env) == Braces) return true;
  uint32_t cur = current_indent(env);
  return (indent > cur || (indent == cur && sort == DoLayout));
}

/**
 * Start the given layout if the position allows it:
 *
 * - If the current context is `ModuleHeader`, the layout must be the `where` after `module`, so any indent is valid.

 * - If the new layout is a brace layout, legal indent is technically required, but we can be lenient since there's no
 *   other way to interpret an opening brace after a layout opener.
 *   However, we need to mark to include the brace in the range to create a terminal (see `grammar/externals.js` for
 *   why).
 *
 * - Otherwise, examine indent.
 */
static Symbol start_layout(Env *env, const StartLayout start, uint32_t indent, const char * restrict desc) {
  if (in_module_header(env)) pop(env);
  else if (start.sort == Braces) MARK("start_layout brace");
  else if (!indent_can_start_layout(env, start.sort, indent)) return FAIL;
  push_context(env, start.sort, indent);
  return finish(start.sym, desc);
}

/**
 * The indent of a layout started at an interior token can only be determined by calling `get_column`.
 * This is an expensive operation, but hopefully it is rare enough to not make a substantial dent.
 * Because we might have advanced beyond the layout's start position to check conditions, we need to subtract the length
 * of the lookahead buffer from the current column.
 * Whitespace is skipped, and not added to the buffer, so the resulting position is after whitespace.
 */
static Symbol start_layout_interior(Env *env, Lexed next) {
  StartLayout start = valid_layout_start(env, next);
  if (start.sort == NoContext) return FAIL;
  return start_layout(env, start, start_column(env), "interior");
}

/**
 * The indent of a layout started at the beginning of a line is determined by `newline_lookahead`, so this does not have
 * to compute it.
 */
static Symbol start_layout_newline(Env *env) {
  StartLayout start = valid_layout_start(env, env->state->newline.end);
  if (start.sort == NoContext) return FAIL;
  Symbol result = start_layout(env, start, env->state->newline.indent, "newline");
  if (result != FAIL) env->state->newline.no_semi = true;
  return result;
}

/**
 * See `token_end_layout_texp`.
 */
static Symbol texp_context(Env *env) {
  if (valid(env, START_TEXP)) {
    push_context(env, TExp, 0);
    return finish(START_TEXP, "texp_context");
  }
  else if (valid(env, END_TEXP) && current_context(env) == TExp) {
    pop(env);
    return finish(END_TEXP, "texp_context");
  }
  else return FAIL;
}

// --------------------------------------------------------------------------------------------------------
// Ending layouts
// --------------------------------------------------------------------------------------------------------

/**
 * Separated this from `end_layout` because it caused some weird performance glitches.
 */
static Symbol end_layout_unchecked(Env *env, const char *restrict desc) {
  pop(env);
  return finish(END, desc);
}

/**
 * If a layout end is valid at this position, pop a context and succeed with layout end.
 */
static Symbol end_layout(Env *env, const char *restrict desc) {
  if (valid(env, END)) return end_layout_unchecked(env, desc);
  else return FAIL;
}

/**
 * Explicit brace layouts need a dedicated symbol, see `_cmd_layout_start_explicit` for an explanation.
 * Includes the brace in the range.
 */
static Symbol end_layout_brace(Env *env) {
  if (valid(env, END_EXPLICIT) && current_context(env) == Braces) {
    advance_over(env, 0);
    MARK("end_layout_brace");
    pop(env);
    return finish(END_EXPLICIT, "brace");
  }
  else return FAIL;
}

/**
 * End a layout based on indent decrease.
 *
 * If the indent of the current line is smaller than the indent of the current layout, we end the layout in most cases.
 * Exceptions are:
 *
 * - Brace layouts
 * - The top-level layout, which should only be ended at the end of file.
 *   For leniency, we change the current indent to the smaller value.
 */
static Symbol end_layout_indent(Env *env) {
  if (valid(env, END) && indent_less(env, env->state->newline.indent)) {
    if (top_layout(env)) {
	  array_back(&env->state->contexts)->indent = env->state->newline.indent;
      return update_state("end top layout");
    }
    else {
      env->state->newline.skip_semi = false;
      return end_layout_unchecked(env, "indent");
    }
  }
  return FAIL;
}

/**
 * An expression layout may be closed by an infix operator when it is not valid at that position:
 *
 * a :: IO Int
 * a = do a <- pure 5
 *        pure a
 *        >>= pure
 *
 * In this situation, the indent of the operator causes a semicolon to be generated, which leads to varsym being invalid
 * lookahead.
 * The layout is closed and the entire `do` block becomes the left operand of the `>>=`.
 * The same applies for `infix` id operators.
 *
 * It doesn't apply to multi-way if layouts, because those don't use semicolons.
 */
static Symbol end_layout_infix(Env *env) {
  if (!valid(env, VARSYM) && !valid(env, CONSYM)) return end_layout(env, "symop invalid");
  return FAIL;
}

/**
 * A case alt can have a `where` clause attached to it, so a case layout is ended by a `where` only if its indent is
 * equal to or smaller than the layout indent.
 *
 * A `do` or `if` cannot have a `where`, so they are always terminated.
 *
 * It would be tempting to leave it at that, but there can be multiple successive `where` clauses.
 * If a `case` is followed by two of them (greater indent), the first one would attach to the last alt.
 * The second one would have to close the `case` layout and attach to the next higher allowed place (e.g. the enclosing
 * function decl), but if its indent is greater, this cannot be detected here – it would just seem like a `where`
 * attaching to an alt, since we don't keep track of the number of `where`s encountered (and we couldn't, since we're
 * dealing with layouts, not case alts).
 *
 * By tracking the validity of `where` symbols, we can simplify the condition for `do` and `if`: End any layout when
 * `where` is parsed, but invalid.
 */
static Symbol end_layout_where(Env *env) {
  if (valid(env, END) && !valid(env, WHERE) && is_layout_context(env)) return end_layout(env, "where");
  return FAIL;
}

/**
 * Ending layouts with `in` heavily relies on parse errors in GHC, so this is difficult.
 * The heuristic here is that if `in` is not valid (i.e. a parse error), we pop any layout.
 *
 * Take the example of some inline layouts in a let decl:
 * `let a = case a of a -> do a in a`
 * The `do` and `case` layouts have valid `END` symbols at the `in`, but `in` itself is not valid as long as the `case`
 * hasn't reduced, so we pop until we find `IN`.
 *
 * This isn't enough though, since `let` also opened a layout that ends here, so we have to test for that explicitly.
 *
 * Note that this doesn't allow the `in` of a nested `let` to close the outer `let`, since the `END` for that isn't
 * valid before the inner `let` has reduced.
 */
static Symbol end_layout_in(Env *env) {
  if (valid(env, END) && (!valid(env, IN) || current_context(env) == LetLayout)) return end_layout(env, "in");
  return FAIL;
}

/**
 * For GADT constructor layouts.
 */
static Symbol end_layout_deriving(Env *env) {
  if (valid(env, END) && !valid(env, DERIVING) && !top_layout(env) && current_context(env) == DeclLayout)
    return end_layout(env, "deriving");
  return FAIL;
}

/**
 * Return `true` if there is a `TExp` context on the stack and only layouts above it.
 */
static bool layouts_in_texp(Env *env) {
  if (is_layout_context(env) && (env->state->contexts.size > 1)) {
    for (int32_t i = (int32_t) env->state->contexts.size - 2; i >= 0; i--) {
	  Context *cur = array_get(&env->state->contexts, i);
      if (cur->sort == TExp || cur->sort == Braces) return true;
      else if (cur->sort > Braces) break;
    }
  }
  return false;
}

/**
 * Tuple expressions are constructs that syntactically delimit their contents in an unambiguous way that makes parsing
 * a lot easier.
 * In GHC, this concept is used to classify productions like view patterns and annotated expressions.
 * For us, unfortunately, it also means that there are significantly more circumstances in which layouts can be ended by
 * parse errors.
 *
 * In practice, it means that expression layouts can be closed by commas, vertical bars and closing brackets and parens
 * when they are elements in a list or tuple-like construct:
 *
 * (case a of a -> a, do a; a, if | a -> a | a -> a)
 * [case a of a -> a | a <- a]
 *
 * We encode this as a special context sort, `TExp`, that is pushed and popped at opening and closing brackets.
 *
 * Some other constructs, like guards, have similar characteristics, so we use the same mechanism for them:
 *
 * > a = case a of
 * >   a | let a = a -> a
 *
 * Here the let layout must be ended by parse error, so we start a tuple expression at the bar and end it at the arrow.
 */
static Symbol token_end_layout_texp(Env *env) {
  return (valid(env, END) && layouts_in_texp(env)) ? end_layout(env, "texp") : FAIL;
}

static Symbol force_end_context(Env *env) {
  for (int32_t i = (int32_t) env->state->contexts.size - 1; i >= 0; i--) {
	ContextSort ctx = array_get(&env->state->contexts, i)->sort;
    Symbol s = context_end_sym(ctx);
    pop(env);
    if (s != FAIL && valid(env, s)) return finish(s, "force_end_context");
  }
  return FAIL;
}

// --------------------------------------------------------------------------------------------------------
// Operators
// --------------------------------------------------------------------------------------------------------

/**
 * Opening tokens are a class of characters that may immediately follow prefix operators like bang pattern `!` or type
 * application `@`.
 */
static bool opening_token(Env *env, uint32_t i) {
  int32_t c = peek(env, i);
  switch (c) {
    case 0x27e6: // ⟦
    case 0x2987: // ⦇
    case '(':
    case '[':
    case '"':
      return true;
    case '{':
      return peek(env, i + 1) != '-';
    default:
      // Includes single quote
      return is_id_char(c);
  }
}

/**
 * Test for reserved operators of two characters.
 */
static bool valid_symop_two_chars(int32_t first_char, int32_t second_char) {
  switch (first_char) {
    case '=':
      return second_char != '>';
    case '<':
      return second_char != '-';
    case ':':
      return second_char != ':';
    default:
      return true;
  }
}

/**
 * If a prefix operator is not followed by an opening token, it may still be a valid varsym.
 */
static Lexed lex_prefix(Env *env, Lexed t) {
  return opening_token(env, 1) ? t : LSymop;
}

/**
 * If a splice operator is not followed by an opening token, it may still be a valid varsym.
 * We only allow variables and parenthesized expressions for performance reasons, though.
 */
static Lexed lex_splice(int32_t c) {
  return varid_start_char(c) || c == '(' ? LDollar : LSymop;
}

/**
 * Lex special occurrences of symbolic operator characters, or declare a valid operator.
 *
 * For the dot:
 *
 * - Two dots: `..`: Only used for arithmetic sequences (`[a..10]`).
 *   These conflict with record field projection (`[a.b, c]`) and infix operators (`[a..+b]`), and it's too hard to
 *   disambiguate them without this special rule.
 *
 * - Tight dot `a.b.c`: A regular tight op, but it has to get a separate symbol from qualified module dots since those
 *   can be followed by symops.
 *
 * - Prefix dot `(.a)`: A regular prefix op, for record dot field selectors.
 *
 * - Qualified dot `A.B.c`, `A.B.C`, `A.B.+`: Used primarily for qualified modules, but needs to be accepted for field
 *   selectors as well due to ambiguity.
 *   This is not a regular tight op since it needs to allow symops and conid.
 */
static Lexed lex_symop(Env *env) {
  uint32_t len = symop_lookahead(env);
  if (len == 0) return LNothing;
  int32_t c1 = unsafe_peek(env, 0);
  if (len == 1) {
    switch (c1) {
      case '?':
        // A `?` can be the head of an implicit parameter, if followed by a varid.
        return varid_start_char(peek1(env)) ? LNothing : LSymop;
      case '#':
        return char1(env, ')') ? LUnboxedClose : LHash;
      case '|':
        return char1(env, ']') ? LQuoteClose : LBar;
      case '!':
        return lex_prefix(env, LBang);
      case '~':
        return lex_prefix(env, LTilde);
      case '@':
        return lex_prefix(env, LAt);
      case '%':
        return lex_prefix(env, LPercent);
      case '$':
        return lex_splice(peek1(env));
      case '.':
        if (is_id_char(peek1(env))) return LDotId;
        else if (opening_token(env, 1)) return LDotOpen;
        else return LSymop;
      case 0x2192: // →
      case 0x22b8: // ⊸
        return LArrow;
      case 0x21d2: // ⇒
        return LCArrow;
      case '=':
      case 0x27e7: // ⟧
      case 0x2988: // ⦈
        return LTexpCloser;
      case '*':
      case '-':
        return LSymopSpecial;
      case '\\':
      case 0x2190: // ←
      case 0x2200: // ∀
      case 0x2237: // ∷
      case 0x2605: // ★
      case 0x27e6: // ⟦
      case 0x2919: // ⤙
      case 0x291a: // ⤚
      case 0x291b: // ⤛
      case 0x291c: // ⤜
      case 0x2987: // ⦇
        return LNothing;
    }
  }
  else if (len == 2) {
    if (seq(env, "->")) return LArrow;
    if (seq(env, "=>")) return LCArrow;
    int32_t c2 = unsafe_peek(env, 1);
    switch (c1) {
      case '$':
        if (c2 == '$') return lex_splice(peek2(env));
        break;
      case '|':
        if (c2 == '|' && char2(env, ']')) return LQuoteClose;
        break;
      case '.':
        if (c2 == '.') return LDotDot;
        else return LDotSymop;
        break;
      case '#':
        // Unboxed unit `(##)` and unboxed sum with missing space `(#| Int #)`
        if (c2 == '#' || c2 == '|') return LSymopSpecial;
        break;
      default:
        if (!valid_symop_two_chars(c1, c2)) return LNothing;
        break;
    }
  }
  else switch (c1) {
    case '-':
      if (seq(env, "->.")) return LArrow;
      break;
    case '.':
      return LDotSymop;
  }
  return LSymop;
}

/**
 * If the next character after whitespace starting from `start` is a closing parenthesis, finish with `LEFT_SECTION_OP`.
 * This is called after a previous step has already lexed a valid infix operator (symbolic or ticked varid).
 *
 * Left section operators must be detected here to disambiguate from infix expressions:
 *
 * > f = (1 - 2 +)
 *
 * When lookahead is `+`, the parser must decide whether to reduce `1 - 2` to `infix` because it is the operand of a
 * section, or to shift into another `infix`.
 * With a single lookahead token, this is not decidable.
 *
 * Note: The obvious solution would be to make `infix` left-associative, so it would always reduce.
 * Unfortunately, this doesn't work for minus, due to apparently unsurmountable problems caused by the
 * apply/infix/negation conflict.
 *
 * Note: This will fail if there are extras between the operator and the parenthesis (and the ticks and the varid).
 *
 * Note: If the operator isn't followed by a parenthesis, it will be parsed as an infix operator in the next step, since
 * those are always valid when left sections are (except for qualified symops).
 * However, this function advances over whitespace to find the paren, so if the next step marks and finishes, it will
 * either:
 * - Include the whitespace in its range, if this consumes it
 * - Have a zero-width range, if this skips whitespace
 * To mitigate this, we introduce the auxiliary symbol `NO_SECTION_OP`, which is (optionally) valid before infix
 * operators, but not before section operators.
 * When this function finds any whitespace before the parenthesis, it will finish with that symbol, so that
 * `LEFT_SECTION_OP` won't be valid in the next run, but all other infix operators are.
 */
static Symbol left_section_op(Env *env, uint32_t start) {
  if (valid(env, LEFT_SECTION_OP)) {
    advance_before(env, start);
    Space space = skip_whitespace(env);
    if (char_at(env, start, ')')) return finish(LEFT_SECTION_OP, "left section");
    if (space != NoSpace) return finish_if_valid(env, NO_SECTION_OP, "left section");
  }
  return FAIL;
}

/**
 * Specialization of `left_section_op` for ticked infix identifiers.
 */
static Symbol left_section_ticked(Env *env) {
  if (valid(env, LEFT_SECTION_OP)) {
    uint32_t end_tick = advance_until_char(env, 1, '`');
    // Could be EOF
    if (char_at(env, end_tick, '`')) {
      return left_section_op(env, end_tick + 1);
    }
  }
  return FAIL;
}

/**
 * This calls `symop_lookahead` to ensure that the position has advanced beyond the end of the symop, which is necessary
 * because newline lookahead may have validated the symop in a previous run.
 * This marks the range to emit a terminal.
 */
static Symbol finish_symop(Env *env, Symbol s) {
  if (valid(env, s) || valid(env, LEFT_SECTION_OP)) {
    uint32_t after_symop = symop_lookahead(env);
    SEQ(left_section_op(env, after_symop));
    MARK("symop");
    return s;
  }
  return FAIL;
}

/**
 * Tight ops like `dot.syntax` require that no initial whitespace was skipped.
 */
static Symbol tight_op(Env *env, bool whitespace, Symbol s) {
  if (!whitespace) return finish_if_valid(env, s, "tight_op");
  else return FAIL;
}

/**
 * Used for situations where the operator is followed by an opening token, and so can be a prefix op if it is preceded
 * by whitespace; but is no valid tight op and therefore becomes a regular operator if not preceded by whitespace or the
 * symbol is not valid.
 *
 * Only used for `%` (modifier).
 */
static Symbol prefix_or_varsym(Env *env, bool whitespace, Symbol s) {
  if (whitespace) SEQ(finish_if_valid(env, s, "prefix_or_varsym"));
  return finish_symop(env, VARSYM);
}

/**
 * Used for situations where the operator is followed by an opening token, and so can be a tight op if it is not
 * preceded by whitespace; but is no valid prefix op and therefore becomes a regular operator if preceded by whitespace
 * or the symbol is not valid.
 *
 * Only used for `.`, when a projection selector `(.fieldname)` is not valid at this position, so the dot becomes the
 * composition operator.
 */
static Symbol tight_or_varsym(Env *env, bool whitespace, Symbol s) {
  SEQ(tight_op(env, whitespace, s));
  return finish_symop(env, VARSYM);
}

/**
 * Used for situations where the operator is followed by an opening token, and so can be a tight op if it is not
 * preceded by whitespace or a prefix op if it is.
 *
 * If neither of those symbols is valid, fall back to a regular operator.
 *
 * Used for `!`, `~` and `@`.
 */
static Symbol infix_or_varsym(Env *env, bool whitespace, Symbol prefix, Symbol tight) {
  SEQ(finish_if_valid(env, whitespace ? prefix : tight, "infix_or_varsym"));
  return finish_symop(env, VARSYM);
}

static Symbol qualified_op(Env *env) {
  if (qualified_name(env, is_symop) == QualifiedTarget) {
    SEQ(left_section_op(env, symop_lookahead(env)));
    return QUALIFIED_OP;
  }
  return FAIL;
}

// --------------------------------------------------------------------------------------------------------
// Token lookahead
// --------------------------------------------------------------------------------------------------------

/**
 * Detect the start of a quasiquote: An opening bracket followed by an optional varid and a vertical bar, all without
 * whitespace in between.
 */
static bool is_qq_start(Env *env) {
  uint32_t end = advance_while(env, 1, quoter_char);
  return char_at(env, end, '|');
}

/**
 * An end token is a keyword like `else` or `deriving` that can end a layout without newline or indent.
 */
static Lexed try_end_token(Env *env, const char * restrict target, Lexed match) {
  if (token(env, target)) return match;
  else return LNothing;
}

/**
 * Check that a symop consists only of minuses after the second character.
 */
static bool only_minus(Env *env) {
  uint32_t i = 2;
  while (peek(env, i) == '-') i++;
  return !symop_char(peek(env, i));
}

/**
 * Check that a symop consists only of minuses, making it a comment herald.
 */
static bool line_comment_herald(Env *env) {
  return seq(env, "--") && only_minus(env);
}

static Lexed lex_cpp(Env *env) {
  switch(cpp_directive(env)) {
    case CppElse:
      return LCppElse;
    case CppNothing:
      return LNothing;
    default:
      return LCpp;
  }
}

/**
 * Lex pragmas, comments and CPP.
 */
static Lexed lex_extras(Env *env, bool bol) {
  switch (peek0(env)) {
    case '{':
      if (char1(env, '-')) return char2(env, '#') ? LPragma : LBlockComment;
      break;
    case '#':
      if (bol) return lex_cpp(env);
      break;
    case '-':
      if (line_comment_herald(env)) return LLineComment;
      break;
    default:
      break;
  }
  return LNothing;
}

/**
 * The main lexing entry point, branching on the first character, then advancing as far as necessary to identify all
 * interesting tokens.
 */
static Lexed lex(Env *env, bool bol) {
  SEQ(lex_extras(env, bol));
  if (symop_char(peek0(env))) SEQ(lex_symop(env));
  else switch (peek0(env)) {
    case 'w':
      return try_end_token(env, "where", LWhere);
    case 'i':
      return try_end_token(env, "in", LIn);
    case 't':
      return try_end_token(env, "then", LThen);
    case 'e':
      return try_end_token(env, "else", LElse);
    case 'd':
      return try_end_token(env, "deriving", LDeriving);
    case 'm':
      if ((uninitialized(env) || in_module_header(env)) && token(env, "module")) return LModule;
      break;
    case '{':
      return LBraceOpen;
    case '}':
      return LBraceClose;
    case ';':
      return LSemi;
    case '`':
      return LTick;
    case '[':
      if (valid(env, QQ_START) && is_qq_start(env)) return LBracketOpen;
      break;
    case ']':
    case ')':
    case ',':
      return LTexpCloser;
    default:
      if (is_conid_start_char(peek0(env))) return LUpper;
      break;
  }
  return LNothing;
}

// --------------------------------------------------------------------------------------------------------
// CPP
// --------------------------------------------------------------------------------------------------------

/**
 * This tests for `#endif` directly after taking a line, so it only matches it at the first column.
 * Int finishes right before the `#endif`, so that pragma is parsed by `cpp_consume` in the next round.
 */
static Symbol cpp_else(Env *env, bool emit) {
  uint32_t nesting = 1;
  do {
    take_line_escaped_newline(env);
    if (emit) MARK("cpp_else");
    S_ADVANCE;
    reset_lookahead(env);
    switch (cpp_directive(env)) {
      case CppStart:
        nesting++;
        break;
      case CppEnd:
        nesting--;
        break;
      default:
        break;
    }
  }
  while (not_eof(env) && nesting > 0);
  if (emit) return finish(CPP, "cpp-else");
  else return FAIL;
}

static Symbol cpp_line(Env *env) {
  take_line_escaped_newline(env);
  return finish_marked(env, CPP, "cpp");
}

// --------------------------------------------------------------------------------------------------------
// Comments
// --------------------------------------------------------------------------------------------------------

/**
 * Distinguish between haddocks and plain comments by matching on the first non-whitespace character.
 */
static Symbol comment_type(Env *env) {
  uint32_t i = 2;
  while (peek(env, i) == '-') i++;
  while (not_eof(env)) {
    int32_t c = peek(env, i++);
    if (c == '|' || c == '^') return HADDOCK;
    else if (!is_space_char(c)) break;
  }
  return COMMENT;
}

/**
 * Inline comments extend over all consecutive lines that start with comments.
 * Could be improved by requiring equal indent.
 */
static Symbol inline_comment(Env *env) {
  Symbol sym = comment_type(env);
  do {
    take_line(env);
    MARK("inline comment");
    S_ADVANCE;
    reset_lookahead(env);
  } while (line_comment_herald(env));
  return sym;
}

static uint32_t consume_block_comment(Env *env, uint32_t col) {
  uint32_t level = 0;
  for (;;) {
    if (is_eof(env)) return col;
    col++;
    switch (PEEK) {
      case '{':
        S_ADVANCE;
        if (PEEK == '-') {
          S_ADVANCE;
          col++;
          level++;
        }
        break;
      case '-':
        S_ADVANCE;
        if (PEEK == '}') {
          S_ADVANCE;
          col++;
          if (level == 0) return col;
          level--;
        }
        break;
      NEWLINE_CASES:
        S_ADVANCE;
        col = 0;
        break;
      case '\t':
        S_ADVANCE;
        col += 7;
        break;
      default:
        S_ADVANCE;
        break;
    }
  }
}

/**
 * Since {- -} comments can be nested arbitrarily, this has to keep track of how many have been opened, so that the
 * outermost comment isn't closed prematurely.
 */
static Symbol block_comment(Env *env) {
  Symbol sym = comment_type(env);
  consume_block_comment(env, env->state->lookahead.size);
  return finish_marked(env, sym, "block_comment");
}

// --------------------------------------------------------------------------------------------------------
// Pragma
// --------------------------------------------------------------------------------------------------------

static bool consume_pragma(Env *env) {
  if (seq(env, "{-#")) {
    while (!seq(env, "#-}") && not_eof(env)) {
      reset_lookahead(env);
      advance_over(env, 0);
    }
    return true;
  }
  return false;
}

/**
 * Since pragmas can occur anywhere, like comments, but contrarily determine indentation when occurring at the beginning
 * of a line in layouts, this sets `NResume` to continue newline processing with the indent of the pragma.
 *
 * If the pragma is followed by newline, this only ensures that no semicolon is emitted (since this rule is run before
 * `semicolon` and `NResume` restarts lookahead).
 *
 * Otherwise it ensures that the following token is treated as a layout element with the correct indent.
 */
static Symbol pragma(Env *env) {
  if (consume_pragma(env)) {
    MARK("pragma");
    if (env->state->newline.state != NInactive) env->state->newline.state = NResume;
    return finish(PRAGMA, "newline");
  }
  return FAIL;
}

// --------------------------------------------------------------------------------------------------------
// Quasiquote
// --------------------------------------------------------------------------------------------------------

static Symbol qq_body(Env *env) {
  for (;;) {
    if (is_eof(env)) return finish(QQ_BODY, "qq_body");
    else if (PEEK == 0x27e7) {
      return finish_marked(env, QQ_BODY, "qq_body");
    }
    else if (PEEK == '|') {
      MARK("qq_body");
      S_ADVANCE;
      if (PEEK == ']') {
        return finish(QQ_BODY, "qq_body");
      }
    } else S_ADVANCE;
  }
}

// --------------------------------------------------------------------------------------------------------
// Semicolon
// --------------------------------------------------------------------------------------------------------

/**
 * When encountering explicit semicolons, we want to ensure that a subsequent newline doesn't trigger a layout
 * semicolon, so we set `skip_semi`.
 * If the next symbol is not a newline (and not another semicolon), the scanner will immediate end up in
 * `resolve_semicolon`, matching the condition, where we unset the flag to avoid a mid-line semicolon from influencing
 * an unrelated newline.
 *
 * Take this example:
 *
 * > a = 1;;
 * > b = 2
 * > ;;c = 3
 *
 * At the first semicolon, `explicit_semicolon` is called (conditioned on `LSemi` in `process_token_interior`) and
 * SEMICOLON is valid, so the flag is set.
 * The scanner will be called again immediately without advancing, and first enter `resolve_semicolon`, which does
 * nothing because the next token is still `LSemi`.
 * Next it will enter `explicit_semicolon` again.
 * SEMICOLON is valid, but since the flag is set we fall through and defer to internal lexing.
 * The grammar advances into `semi` (in `util.js`), which causes SEMICOLON to become invalid.
 * The scanner is executed before the second semicolon, where both functions skip again, this time additionally because
 * SEMICOLON is now invalid.
 *
 * In the next scan, the newline branch is taken in `scan`, so this function is not executed again.
 * Newline lookahead finds the next line to begin at column 0, which would usually trigger a layout semicolon in
 * `semicolon`, but that is inhibited by `skip_semi`, so the scan only skips whitespace and resets the newline state,
 * which unsets `skip_semi` again.
 * In the following scan, the conditions for both functions are unfulfilled, so parsing continues regularly until the
 * next newline.
 *
 * Newline lookahead now encounters the third semicolon on the next line and sets `no_semi`, which supersedes
 * `skip_semi` and prohibits layout semicolon irreversibly, so the explicit semicolons are parsed by the grammar.
 *
 * Now consider an inline semicolon:
 *
 * > f = let
 * >   a = 1; b = 2
 * >   c = 3; {- x -}
 * >   d = 4
 * >   in c
 *
 * When the semicolon is lexed, `explicit_semicolon` sets `skip_semi`.
 * If we would not reset it until the newline, no layout semicolon would be generated before `c`, resulting in a parse
 * error at `=`.
 * Therefore, `resolve_semicolon` unsets `skip_semi` when lexing `b`, triggered by `skip_semi` being set and the next
 * token not being `LSemi`.
 *
 * The semicolon after `c = 3` is followed by a comment, so it is unclear if there is going to be another layout element
 * in the same line.
 * If there is none, the situation is the same as in the first example's first line; if another layout element were to
 * follow, `skip_semi` would need to be reset like in this example's first line.
 * Therefore, `resolve_semicolon` also keeps the flag as it is in this case.
 */
static Symbol explicit_semicolon(Env *env) {
  if (valid(env, SEMICOLON) && !env->state->newline.skip_semi) {
    env->state->newline.skip_semi = true;
    return update_state("explicit semicolon enable");
  }
  return FAIL;
}

static Symbol resolve_semicolon(Env *env, Lexed next) {
  if (env->state->newline.skip_semi) {
    switch(next) {
      case LLineComment:
      case LBlockComment:
      case LPragma:
      case LSemi:
        break;
      default:
        env->state->newline.skip_semi = false;
        return update_state("explicit semicolon disable");
    }
  }
  return FAIL;
}

/**
 * Generate a layout semicolon after a newline if the indent is less or equal to the current layout's indent, unless:
 *
 * - The current context doesn't use layout semicolons, which is the case for explicit brace layouts, tuple expressions,
 *   the module header and multi-way if layouts.
 *
 * - `no_semi` was set because newline lookahead found an explicit semicolon in the next line, or this function was
 *   executed before for the same newline.
 *
 * - `skip_semi` was set because the previous line ended with an explicit semicolon.
 */
static Symbol semicolon(Env *env) {
  if (
      is_semicolon_context(env)
      &&
      !(env->state->newline.no_semi || env->state->newline.skip_semi)
      &&
      indent_lesseq(env, env->state->newline.indent)
     ) {
    env->state->newline.no_semi = true;
    return finish(SEMICOLON, "newline");
  }
  else return FAIL;
}

// --------------------------------------------------------------------------------------------------------
// High-level `Lexed` dispatch
// --------------------------------------------------------------------------------------------------------

/**
 * Process a `Lexed` token if it results in a layout end or an extra.
 *
 * This is called by `newline_post` before marking, so the actions must not fail after advancing.
 */
static Symbol process_token_safe(Env *env, Lexed next) {
  switch (next) {
    case LWhere:
      return end_layout_where(env);
    case LIn:
      return end_layout_in(env);
    case LThen:
    case LElse:
      return end_layout(env, "then/else");
    case LDeriving:
      return end_layout_deriving(env);
    case LBar:
      if (!valid(env, BAR)) return end_layout(env, "bar");
      break;
    case LPragma:
      return pragma(env);
    case LBlockComment:
      return block_comment(env);
    case LLineComment:
      return inline_comment(env);
    case LCppElse:
      return cpp_else(env, true);
    case LCpp:
      return cpp_line(env);
    case LSymop:
    case LTick:
    case LHash:
      return end_layout_infix(env);
    case LUnboxedClose:
      SEQ(token_end_layout_texp(env));
      return end_layout_infix(env);
    case LArrow:
      if (!valid(env, ARROW)) return token_end_layout_texp(env);
      break;
    case LTexpCloser:
      return token_end_layout_texp(env);
    case LQuoteClose:
      return end_layout(env, "quote bracket");
      break;
    default:
      break;
  }
  return FAIL;
}

/**
 * Process a `Lexed` token if it results in a symbolic operator.
 */
static Symbol process_token_symop(Env *env, bool whitespace, Lexed next) {
  switch (next) {
    case LDotDot:
      SEQ(finish_if_valid(env, DOTDOT, "symop"));
      return tight_op(env, whitespace, QUAL_DOT);
    case LDotId:
      SEQ(finish_if_valid(env, whitespace ? PREFIX_DOT : TIGHT_DOT, "symop"));
      return tight_op(env, whitespace, QUAL_DOT);
    case LDotSymop:
      return tight_or_varsym(env, whitespace, QUAL_DOT);
    case LDotOpen:
      return prefix_or_varsym(env, whitespace, PREFIX_DOT);
    case LBang:
      return infix_or_varsym(env, whitespace, PREFIX_BANG, TIGHT_BANG);
    case LTilde:
      return infix_or_varsym(env, whitespace, PREFIX_TILDE, TIGHT_TILDE);
    case LAt:
      return infix_or_varsym(env, whitespace, PREFIX_AT, TIGHT_AT);
    case LPercent:
      return prefix_or_varsym(env, whitespace, PREFIX_PERCENT);
    case LSymop:
      if (char0(env, ':')) return finish_symop(env, CONSYM);
      else return finish_symop(env, VARSYM);
    // The following are handled here despite not being purely symop tokens because `process_token_symop` is executed
    // last, and these handlers all have potentially quite far lookahead and can fail.
    case LSymopSpecial:
      SEQ(left_section_op(env, symop_lookahead(env)));
      if (valid(env, MINUS) && match_symop(env, "-")) return finish(MINUS, "symop");
      break;
    case LUnboxedClose:
    case LHash:
      return left_section_op(env, symop_lookahead(env));
    case LTick:
      return left_section_ticked(env);
    case LUpper:
      if (valid(env, QUALIFIED_OP) || valid(env, LEFT_SECTION_OP)) SEQ(qualified_op(env));
      break;
    default:
      break;
  }
  return FAIL;
}

static Symbol process_token_splice(Env *env, Lexed next) {
  switch (next) {
    case LDollar:
      return finish_if_valid(env, SPLICE, "symop");
    default:
      break;
  }
  return FAIL;
}

/**
 * Process a `Lexed` token for an interior position.
 */
static Symbol process_token_interior(Env *env, Lexed next) {
  switch (next) {
    case LBraceClose:
      SEQ(end_layout_brace(env));
      return token_end_layout_texp(env);
    // Skip layout start
    case LModule:
      return FAIL;
    case LSemi:
      return explicit_semicolon(env);
    case LBracketOpen:
      return finish(QQ_START, "qq_start");
    default:
      break;
  }
  SEQ(process_token_safe(env, next));
  return start_layout_interior(env, next);
}

/**
 * Process a `Lexed` token to initialize the context stack.
 */
static Symbol process_token_init(Env *env, uint32_t indent, Lexed next) {
  switch (next) {
    case LModule:
      push_context(env, ModuleHeader, 0);
      return update_state("init");
    case LBraceOpen:
      advance_over(env, 0);
      MARK("init brace");
      push_context(env, Braces, indent);
      return finish(START_EXPLICIT, "init");
    default:
      push_context(env, DeclLayout, indent);
      return finish(START, "init");
  }
}

// --------------------------------------------------------------------------------------------------------
// Newline actions
// --------------------------------------------------------------------------------------------------------

/**
 * `NoSpace` + `newline_init()` means that we're at the very beginning of the file, where we start in `NResume` mode
 * without a newline character that can tell us where we are.
 */
static Symbol newline_extras(Env *env, Space space) {
  bool bol = space == BOL || (space == NoSpace && newline_init(env));
  Lexed next = lex_extras(env, bol);
  dbg("newline extras token: %s\n", token_names[next]);
  return process_token_safe(env, next);
}

// Don't finish newline processing before pragmas – they are indicators of layout indent, but since they are extras,
// they cannot consume a semicolon, so when there's a pragma on a line of its own, we would get two semicolons if we
// finished here.
// It's guaranteed that the newline state was committed at least once because `newline_lookahead` sets `unsafe` when
// finding a pragma.
static Symbol newline_process(Env *env) {
  dbg("newline post\n");
  uint32_t indent = env->state->newline.indent;
  Lexed end = env->state->newline.end;
  SEQ(end_layout_indent(env));
  SEQ(process_token_safe(env, end));
  Space space = skip_whitespace(env);
  MARK("newline_post");
  if (env->state->newline.unsafe) SEQ(newline_extras(env, space));
  if (!env->state->newline.eof) SEQ(start_layout_newline(env));
  // TODO it is only necessary to run this late because of very few situations, like nondecreasing indent.
  // But it has the consequence that whitespace is included in the parent in nested layouts.
  // Maybe there's a way to run it before and after `start_layout_newline` with conditions.
  SEQ(semicolon(env));
  reset_newline(env);
  if (uninitialized(env)) SEQ(process_token_init(env, indent, end));
  else {
    SEQ(process_token_symop(env, true, end));
    SEQ(process_token_splice(env, end));
  }
  return update_state("newline final");
}

static Symbol newline_post(Env *env) {
  Symbol res = newline_process(env);
  if (newline_init(env)) env->state->newline.state = NProcess;
  return res;
}

/**
 * Repeatedly lex lookahead until encountering something that is neither a comment nor CPP, skipping whitespace and
 * newlines in between.
 */
static void newline_lookahead(Env *env, Newline *newline) {
  for (;;) {
    // Using `peek0` to look for whitespace requires the lookahead buffer to have been reset immediately before this
    // statement – so before the call to this function or at the end of the for loop body.
    // The reason this isn't using `lexer->lookahead` is that the function may be called at an interior position, to
    // skip extras.
    switch (peek0(env)) {
      NEWLINE_CASES:
        skip_over(env, 0);
        newline->indent = 0;
        break;
      case '\t':
        skip_over(env, 0);
        newline->indent += 8;
        break;
      default:
        if (is_space_char(peek0(env))) {
          skip_over(env, 0);
          newline->indent++;
          break;
        }
        newline->end = lex(env, newline->indent == 0);
        dbg("newline token: %s, %lc\n", token_names[newline->end], peek0(env));
        // Newlines without extras are only safe if `lex` didn't advance the lexer over non-whitespace.
        newline->unsafe |= !no_lookahead(env);
        switch (newline->end) {
          case LEof:
            newline->indent = 0;
            newline->eof = true;
            return;
          // If/then blocks can have semicolons, but don't have a layout.
          // Allowing layout semicolons costs 100kB.
          case LThen:
          case LElse:
          case LSemi:
            newline->no_semi = true;
            return;
          case LBlockComment:
            newline->indent = consume_block_comment(env, newline->indent + 2);
            break;
          case LLineComment:
            newline->indent = 0;
            take_line(env);
            break;
          case LCppElse:
            cpp_else(env, false);
            take_line_escaped_newline(env);
            break;
          case LCpp:
            take_line_escaped_newline(env);
            break;
          default:
            return;
        }
    }
    reset_lookahead(env);
  }
}

/**
 * Perform newline lookahead, then either finish the run if the position was advanced into the next token, or directly
 * start newline processing if not.
 */
static Symbol newline_start(Env *env) {
  dbg("newline lookahead\n");
  env->state->newline.state = NInit;
  newline_lookahead(env, &env->state->newline);
  if (env->state->newline.unsafe) return update_state("newline lookahead");
  else return newline_post(env);
}

/**
 * Perform newline lookahead with preset indent, used at the beginning of a file and after pragmas.
 */
static Symbol newline_resume(Env *env) {
  dbg("newline resume\n");
  uint32_t indent = env->state->newline.indent;
  // Skip space between the pragma end and the next token, which might be the first real token (or another pragma or
  // comment, or newline).
  // We don't want to count the space as indent.
  skip_space(env);
  reset_newline(env);
  env->state->newline.indent = indent;
  return newline_start(env);
}

// --------------------------------------------------------------------------------------------------------
// Constraints
// --------------------------------------------------------------------------------------------------------

/**
 * The following mechanism avoids the conflict between types and classes.
 * Consider this situation:
 *
 * > data A = B b % C => D d :+ E
 * > data E = F f => G g
 *
 * After the `=`, a diverse set of constructs are valid.
 *
 * - Data constructor
 *   - Infix `D d :+ E` -> `(type/name) (type/variable) (constructor_operator) (type/name)`
 *   - Prefix `G g` -> `(name) (type/variable)`
 * - Context
 *   - Infix `B b % C` -> `(type/name) (type/variable) (operator) (type/name)`
 *   - Prefix `F f` -> `(constraint/name) (type/variable)`
 *
 * Each of these starts with a `(name)` with different reduction rules that can only be resolved when the arrow or a
 * data constructor-ending token is encountered.
 * The conflict between `D` and `G` is an additional hurdle that is not addressed here.
 *
 * Constraint lookahead scans ahead until it finds `=>` or a clear rejection criterion like `=` or (layout) semicolon,
 * emitting `_cond_context` to unlock the rules `_qtype_context`, `context` and `_ctr_context`.
 *
 * However, even the two context variants conflict, since infix classes have types in their operands, while a prefix
 * constraint starts with a class name.
 * To mitigate this, constraint lookahead additionally emits `_cond_infix` when it encounters an infix operator.
 * This symbol is only emitted when `_cond_context` is not valid (because it was parsed right before) or because no `=>`
 * is encountered afterwards (because the current position is in parentheses).
 * This only works because infix classes are localized within contexts – disambiguating all infix types like this is
 * impossible without completely restructuring the grammar.
 *
 * Note that this problem could easily be avoided by parsing all contexts as types, accepting that queries for class
 * names would be more verbose and couldn't match more complex constraints.
 * Furthermore, a much simpler fix would be a runtime conflict, which has the potential to result in randomly incorrect
 * parse trees.
 *
 * Similarly to contexts, data constructor heads have infix type-related conflicts that aren't as severe but can easily
 * piggyback on this mechanism, so they are included.
 *
 * Lastly, associated type families and instances conflict because they can both be heralded by `type` alone, so the
 * decision to reduce to type head or instance head nodes is informed by the presence of `::` or `=` without `|`.
 */

/**
 * Result of constraint lookahead.
 */
typedef enum {
  // Continue searching
  CtrUndecided,
  // Clear evidence found that no context or infix class is ahead.
  CtrImpossible,
  // The context arrow `=>` was found.
  CtrArrowFound,
  // An infix operator was found.
  CtrInfixFound,
  // An `=` was found.
  CtrEqualsFound,
  // A `|` was found.
  CtrBarFound,
} CtrResult;

#ifdef TREE_SITTER_DEBUG

static const char *ctr_result_names[] = {
  "undecided",
  "impossible",
  "arrow",
  "infix",
  "equals",
  "bar",
};

#endif

/**
 * Constraint lookahead state.
 */
typedef struct {
  // The amount of characters to skip after an iteration.
  // For example, after lexing a `conid` the next token can be lexed at the end of the identifier.
  uint32_t reset;
  // The number of nested brackets.
  // When this is nonzero, end tokens are not treated as pertaining to the current expression.
  uint32_t brackets;
  // A context arrow was found.
  bool context;
  // An infix operator was found.
  bool infix;
  bool data_infix;
  bool type_instance;
} CtrState;

/**
 * Increment the bracket count.
 */
static CtrResult ctr_bracket_open(CtrState *state) {
  state->brackets++;
  state->reset = 1;
  return CtrUndecided;
}

/**
 * Decrement the bracket count.
 * If the count was zero already, parsing started inside of brackets that are closed here, so lookahead is terminated.
 */
static CtrResult ctr_bracket_close(CtrState *state) {
  if (state->brackets == 0) return CtrImpossible;
  state->brackets--;
  state->reset = 1;
  return CtrUndecided;
}

/**
 * If the given token is ahead, terminate lookahead unsuccessfully.
 */
static CtrResult ctr_stop_on_token(Env *env, const char * restrict target) {
  return token(env, target) ? CtrImpossible : CtrUndecided;
}

/**
 * Check if the lexed token is `=>` or an infix operator.
 *
 * This is performed only when the current position is not in a bracketed expression, i.e. at top level relative to the
 * initial lexer position.
 * Otherwise the token belongs to a later, nested expression.
 *
 * Certain tokens are proof that no context can start at the current position, like `::` or `forall`, so lookahead is
 * terminated.
 * It is still possible that an infix class can be parsed, for example in this type when starting at the at `C` and
 * terminating at `::`:
 * > `a :: (C + D :: Constraint) => E`
 */
static CtrResult ctr_top(Env *env, Lexed next) {
  switch (next) {
    case LCArrow:
      return CtrArrowFound;
    case LSymop:
    case LSymopSpecial:
    case LTilde:
    case LTick:
      return CtrInfixFound;
    case LBar:
      return CtrBarFound;
    case LArrow:
    case LWhere:
    case LDotDot:
    case LSemi:
      break;
    case LTexpCloser:
      switch (peek0(env)) {
        case '=':
          return CtrEqualsFound;
        default:
          break;
      }
      break;
    default:
      switch (peek0(env)) {
        // Symop is processed in `ctr_lookahead_step`, so `=` and `::` can not be a prefix
        case '=':
          return CtrEqualsFound;
        case 0x2200: // ∀
          break;
        case ':':
          if (char1(env, ':')) break;
          return CtrUndecided;
        case 'f':
          SEQ(ctr_stop_on_token(env, "forall"));
          return ctr_stop_on_token(env, "family");
        case 'i':
          return ctr_stop_on_token(env, "instance");
        default:
          return CtrUndecided;
      }
  }
  return CtrImpossible;
}

/**
 * Process a lexed token for constraint lookahead:
 * - Update bracket nesting count
 * - Advance over pragmas, strings, chars and conids
 * - Set the reset index for certain tokens
 *
 * If the token wasn't identified to be irrelevant for the lookahead result, and the current bracket nesting level is
 * zero, call `ctr_top`.
 */
static CtrResult ctr_lookahead_step(Env *env, CtrState *state, Lexed next) {
  state->reset = 1;
  switch (next) {
    case LBraceClose:
      return ctr_bracket_close(state);
    case LUnboxedClose:
      SEQ(ctr_bracket_close(state));
      state->reset = 2;
      return CtrUndecided;
    case LBraceOpen:
      return ctr_bracket_open(state);
    case LSymopSpecial:
    case LSymop:
      state->reset = symop_lookahead(env);
      break;
    case LUpper:
      state->reset = conid(env);
      return CtrUndecided;
    case LDotId:
      return CtrUndecided;
    case LPragma:
      if (consume_pragma(env)) state->reset = 3;
      return CtrUndecided;
    case LTexpCloser:
    case LNothing:
      switch (peek0(env)) {
        case ')':
        case ']':
          return ctr_bracket_close(state);
        case '(':
        case '[':
          return ctr_bracket_open(state);
        case '"':
          state->reset = take_string_literal(env);
          return CtrUndecided;
        case '\'':
          state->reset = take_char_literal(env);
          return CtrUndecided;
        default:
          if (varid_start_char(peek0(env))) state->reset = advance_while(env, 1, is_id_char);
          break;
      }
    default:
      break;
  }
  if (state->brackets != 0) return CtrUndecided;
  return ctr_top(env, next);
}

/**
 * Main loop for context lookahead.
 *
 * Perform newline lookahead and terminate if the end of the current layout element is encountered.
 * Otherwise use the new end token to detect a context arrow or infix operator.
 * If no termination criterion is fulfilled, reset lookahead and repeat.
 *
 * Newline lookahead skips over extras.
 *
 * A context arrow is always a termination criterion; an infix operator only if CONTEXT isn't valid.
 */
static Symbol constraint_lookahead(Env *env) {
  dbg("type lookahead\n");
  CtrState state = {.reset = 0};
  bool done = false;
  while (!done && not_eof(env)) {
    // Setting indent to 99999 only to not trigger the following termination condition when no newline was advanced over
    Newline newline = {.state = 0, .indent = 99999};
    newline_lookahead(env, &newline);
    if (newline.indent <= current_indent(env) && current_context(env) != Braces) break;
    CtrResult result = ctr_lookahead_step(env, &state, newline.end);
    dbg("type: %lc, %s\n", peek0(env), ctr_result_names[result]);
    switch (result) {
      case CtrArrowFound:
        state.context = true;
        done = true;
        break;
      case CtrInfixFound:
        if (char0(env, ':') || char0(env, '`')) state.data_infix = true;
        state.infix = true;
        // Context has precedence, e.g. `instance a + a => A` finds `+` first and would treat that as the class name of
        // the head, then failing on the right operand.
        done = !valid(env, CONTEXT);
        break;
      case CtrEqualsFound:
        done = !valid(env, TYPE_INSTANCE);
        state.type_instance = true;
        break;
      case CtrBarFound:
        done = true;
        state.type_instance = false;
        break;
      case CtrImpossible:
        done = true;
      case CtrUndecided:
        break;
    }
    reset_lookahead_to(env, state.reset);
    state.reset = 0;
  }
  if (state.context) SEQ(finish_if_valid(env, CONTEXT, "ctr"));
  if (state.infix) SEQ(finish_if_valid(env, INFIX, "ctr"));
  if (state.data_infix) SEQ(finish_if_valid(env, DATA_INFIX, "ctr"));
  if (state.type_instance) SEQ(finish_if_valid(env, TYPE_INSTANCE, "ctr"));
  return FAIL;
}

// --------------------------------------------------------------------------------------------------------
// Actions that are executed for interior positions
// --------------------------------------------------------------------------------------------------------

static Symbol process_token_constraint(Env *env) {
  if (
      valid(env, CONTEXT)
      ||
      valid(env, INFIX)
      ||
      valid(env, DATA_INFIX)
      ||
      valid(env, TYPE_INSTANCE)
      )
    return constraint_lookahead(env);
  return FAIL;
}

static Symbol interior(Env *env, bool whitespace) {
  Lexed next = lex(env, false);
  dbg("interior, column %d, ws %d, token %s\n", column(env), whitespace, token_names[next]);
  SEQ(resolve_semicolon(env, next));
  SEQ(process_token_interior(env, next));
  SEQ(process_token_symop(env, whitespace, next));
  SEQ(process_token_constraint(env));
  SEQ(process_token_splice(env, next));
  return FAIL;
}

// --------------------------------------------------------------------------------------------------------
// Initial actions
// --------------------------------------------------------------------------------------------------------

/**
 * These are conditioned only on symbols and don't advance, except for `qq_body`, which cannot fail.
 */
static Symbol pre_ws_commands(Env *env) {
  SEQ(texp_context(env));
  SEQ(start_brace(env));
  SEQ(end_brace(env));
  // Leading whitespace must be included in the node.
  if (valid(env, QQ_BODY)) return qq_body(env);
  if (newline_active(env)) SEQ(newline_post(env));
  else if (env->state->newline.state == NResume) SEQ(newline_resume(env));
  return FAIL;
}

static Symbol scan_main(Env *env) {
  MARK("main");
  SEQ(pre_ws_commands(env));
  bool whitespace = skip_space(env);
  if (is_newline(PEEK)) return newline_start(env);
  else if (not_eof(env)) return interior(env, whitespace);
  return FAIL;
}

#ifdef TREE_SITTER_DEBUG

static Symbol scan_debug(Env *env) {
  if (debug_init(env)) return update_state("debug init parse buffer");
  Symbol result = scan_main(env);
  debug_finish(env, result);
  return result;
}

#endif

static bool process_result(Env *env, Symbol result) {
  if (result == FAIL && is_eof(env) && no_lookahead(env)) {
    MARK("eof whitespace");
    // Inlined `end_layout` because of perf glitch
    if (valid(env, END)) result = end_layout_unchecked(env, "eof");
    else if (valid(env, SEMICOLON)) result = finish(SEMICOLON, "eof");
    else {
      result = force_end_context(env);
      if (result == FAIL) {
        dbg("eof | context cap: %d | lookahead cap: %d | parse cap: %d\n",
          env->state->contexts.capacity, env->state->lookahead.capacity, env->state->parse.capacity);}
    }
  }
  return set_result_symbol(env, result);
}


static bool scan(Env *env) {
  if(after_error(env)) { dbg("error recovery\n"); return false; }
#ifdef TREE_SITTER_DEBUG
  Symbol result = scan_debug(env);
#else
  Symbol result = scan_main(env);
#endif
  return process_result(env, result);
}

// --------------------------------------------------------------------------------------------------------
// API
// --------------------------------------------------------------------------------------------------------

typedef struct {
  unsigned contexts;
  Newline newline;
#ifdef TREE_SITTER_DEBUG
  unsigned parse;
#endif
} Persist;

/**
 * This function allocates the persistent state of the parser that is passed into the other API functions.
 */
void *tree_sitter_haskell_external_scanner_create() {
  State *state = ts_calloc(1, sizeof(State));
  array_reserve(&state->contexts, 8);
  array_reserve(&state->lookahead, 8);
#ifdef TREE_SITTER_DEBUG
  array_reserve(&state->parse, 20);
#endif
  return state;
}

/**
 * Main logic entry point.
 * Since the state is a singular vector, it can just be cast and used directly.
 */
bool tree_sitter_haskell_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
  Env env = env_new(lexer, valid_symbols, (State*) payload);
  return scan(&env);
}

unsigned tree_sitter_haskell_external_scanner_serialize(void *payload, char *buffer) {
  State *state = (State *) payload;
  Persist persist = {.contexts = state->contexts.size, .newline = state->newline};
#ifdef TREE_SITTER_DEBUG
  persist.parse = state->parse.size;
#endif
  unsigned contexts_size = persist.contexts * sizeof(Context);
  memcpy(buffer, &persist, sizeof(Persist));
  unsigned to_copy = sizeof(Persist) + contexts_size;
  if (to_copy > TREE_SITTER_SERIALIZATION_BUFFER_SIZE) return 0;
  memcpy(buffer + sizeof(Persist), state->contexts.contents, contexts_size);
#ifdef TREE_SITTER_DEBUG
  to_copy = serialize_parse_lines(buffer + sizeof(Persist) + contexts_size, &state->parse, to_copy);
#endif
  return to_copy;
}

void tree_sitter_haskell_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {
  State *state = (State *) payload;
  Persist p;
  Persist *persist;
  if (length > 0)
    persist = (Persist *) buffer;
  else {
    p = (Persist) {.contexts = 0};
    persist = &p;
    persist->newline.state = NResume;
  }
  unsigned contexts_size = persist->contexts * sizeof(Context);
  state->newline = persist->newline;
  array_reserve(&state->contexts, persist->contexts);
  state->contexts.size = persist->contexts;
  if (length > 0)
    memcpy(state->contexts.contents, buffer + sizeof(Persist), contexts_size);
  state->lookahead.size = 0;
  state->lookahead.offset = 0;
  array_reserve(&state->lookahead, 8);
#ifdef TREE_SITTER_DEBUG
  if (length > 0)
    deserialize_parse_lines(buffer + sizeof(Persist) + contexts_size, &state->parse, persist->parse);
#endif
}

void tree_sitter_haskell_external_scanner_destroy(void *payload) {
  State *state = (State*) payload;
#ifdef TREE_SITTER_DEBUG
  palette();
  ParseLines *parse = &state->parse;
  for (unsigned i = 0; i < parse->size; i++) array_delete(array_get(parse, i));
  array_delete(parse);
#endif
  array_delete(&state->contexts);
  array_delete(&state->lookahead);
  ts_free(state);
}
