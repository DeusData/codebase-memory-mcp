#include "tree_sitter/array.h"
#include "tree_sitter/parser.h"
#include "tsp_unicode.h"
#include "tsp_keywords.h"
#include "tsp_intuit_more.h"
#include "tsp_intuit_readline.h"

// grumble grumble no stdlib
static char *tsp_strchr(register const char *s, int c) {
  do {
    if (*s == c) {
      return (char *)s;
    }
  } while (*s++);
  return (0);
}
/* Set this to #define instead to enable debug printing */
#undef DEBUGGING

/* for debug */
#ifdef DEBUGGING
#include <stdio.h>
#define DEBUG(fmt, ...) fprintf(stderr, "scanner.c:%d DEBUG: " fmt, __LINE__, __VA_ARGS__)
#else
#define DEBUG(fmt, ...)
#endif

#define streq(a, b) (strcmp(a, b) == 0)

#include <wctype.h>

enum TokenType {
  /* non-ident tokens */
  TOKEN_APOSTROPHE,
  TOKEN_DOUBLE_QUOTE,
  TOKEN_BACKTICK,
  TOKEN_SEARCH_SLASH,
  NO_TOKEN_SEARCH_SLASH_PLZ,
  TOKEN_OPEN_READLINE_BRACKET,
  TOKEN_OPEN_FILEGLOB_BRACKET,
  PERLY_SEMICOLON,
  PERLY_HEREDOC,
  TOKEN_CTRL_Z,
  /* immediates */
  TOKEN_QUOTELIKE_BEGIN,
  TOKEN_QUOTELIKE_MIDDLE_CLOSE,
  TOKEN_QUOTELIKE_MIDDLE_SKIP,
  TOKEN_QUOTELIKE_END_ZW,
  TOKEN_QUOTELIKE_END,
  TOKEN_Q_STRING_CONTENT,
  TOKEN_QQ_STRING_CONTENT,
  TOKEN_ESCAPE_SEQUENCE,
  TOKEN_ESCAPED_DELIMITER,
  TOKEN_DOLLAR_IN_REGEXP,
  TOKEN_REGEXP_OPEN_BRACKET,
  TOKEN_REGEXP_OPEN_BRACE,
  TOKEN_POD,
  TOKEN_GOBBLED_CONTENT,
  TOKEN_ATTRIBUTE_VALUE_BEGIN,
  TOKEN_ATTRIBUTE_VALUE,
  TOKEN_PROTOTYPE,
  TOKEN_SIGNATURE_START,
  TOKEN_HEREDOC_DELIM,
  TOKEN_COMMAND_HEREDOC_DELIM,
  TOKEN_HEREDOC_START,
  TOKEN_HEREDOC_MIDDLE,
  TOKEN_HEREDOC_END,
  TOKEN_FAT_COMMA_AUTOQUOTED,
  TOKEN_FAT_COMMA_AUTOQUOTED_AHEAD,
  TOKEN_FILETEST,
  TOKEN_BRACE_AUTOQUOTED,
  /* zero-width lookahead tokens */
  TOKEN_BRACE_END_ZW,
  TOKEN_DOLLAR_IDENT_ZW,
  TOKEN_NO_INTERP_WHITESPACE_ZW,
  /* zero-width high priority token */
  TOKEN_NONASSOC,
  /* synthetic tokens for error recovery */
  TOKEN_RECOVER_PAREN_CLOSE,
  TOKEN_RECOVER_BRACKET_CLOSE,
  TOKEN_RECOVER_BRACE_CLOSE,
  TOKEN_RECOVER_ARROW,
  TOKEN_RECOVER_BLOCK_CLOSE,
  /* opaque body of a `format NAME = ... .` declaration: everything from the
   * line after `=` up to and including the lone-`.` terminator line */
  TOKEN_FORMAT_CONTENT,
  /* `x` repetition operator glued to its count (`"ab"x3`) */
  TOKEN_X_OP,
  /* `class`/`role`/`method` emitted only in declaration position */
  TOKEN_KW_CLASS,
  TOKEN_KW_ROLE,
  TOKEN_KW_METHOD,
  /* `async`/`try` contextual keywords, emitted only in keyword position */
  TOKEN_KW_ASYNC,
  TOKEN_KW_TRY,
  /* error condition is always last */
  TOKEN_ERROR
};

#define MAX_TSPSTRING_LEN 8
/* this is a arbitrary string where we only care about the first
 * MAX_TSPSTRING_LEN chars */
typedef struct {
  int length;
  int32_t contents[MAX_TSPSTRING_LEN];
} TSPString;

/* we record the length, b/c that's still relevant for our cheapo comparison */
static void tspstring_push(TSPString *s, int32_t c) {
  if (s->length++ < MAX_TSPSTRING_LEN) s->contents[s->length - 1] = c;
}

static bool tspstring_eq(TSPString *s1, TSPString *s2) {
  if (s1->length != s2->length) return false;
  int max_len = s1->length < MAX_TSPSTRING_LEN ? s1->length : MAX_TSPSTRING_LEN;
  for (int i = 0; i < max_len; i++) {
    if (s1->contents[i] != s2->contents[i]) return false;
  }
  return true;
}

static void tspstring_reset(TSPString *s) { s->length = 0; }

static int32_t close_for_open(int32_t c) {
  switch (c) {
    case '(':
      return ')';
    case '[':
      return ']';
    case '{':
      return '}';
    case '<':
      return '>';
    /* TODO: Add aaaaalll the Unicode ones */
    default:
      return 0;
  }
}

typedef struct {
  int32_t open, close, count;
  /* Recognises a pattern-LEADING '['/'{' -- the quote's own opening delimiter
   * appearing (after optional whitespace) as the literal first element of the
   * body (m{{...}}, qr{ {...} }, s{{...}}, m[[...]]) -- where the S_intuit_more
   * subscript heuristic (tsp_intuit_more) has no preceding variable to weigh and
   * so must not be consulted.
   *
   * Decided ONCE by a bounded lookahead at the opening delimiter (see the open
   * handler), where the lexer sits exactly at the body start: if the first
   * non-space body char is the delimiter again, the body leads with a literal
   * group.  Looking at the actual first char sidesteps both problems of the
   * earlier column+flag scheme -- columns repeat across lines, and the scanner
   * can't see grammar-lexed interpolation -- because we never have to ask "has
   * content been seen?": we just check what the first char *is*.
   *
   * Set at the opener, consumed (cleared) by that first bracket. */
  bool body_leads_with_delim;
} TSPQuote;

static TSPQuote tspquote_new() { return (TSPQuote){0, 0, 0, false}; }

enum HeredocState { HEREDOC_NONE, HEREDOC_START, HEREDOC_UNKNOWN, HEREDOC_CONTINUE, HEREDOC_END };

/* Perl allows multiple heredocs queued on a single line, consumed FIFO after
 * the newline.  We hold up to HEREDOC_QUEUE_MAX pending heredocs; each one
 * carries its own delimiter and interpolate/indent flags.  The consume-FSM
 * state (heredoc_state) applies to the FRONT of the queue (the one currently
 * being consumed); when it finishes we dequeue and advance to the next. */
#define HEREDOC_QUEUE_MAX 8
typedef struct {
  TSPString delim;
  bool interpolates, indents;
} HeredocEntry;

typedef struct {
  Array(TSPQuote) quotes;
  /* FIFO queue of pending heredocs (front = index 0). heredoc_count is how
   * many entries are live; heredoc_state is the consume state of the front. */
  HeredocEntry heredoc_queue[HEREDOC_QUEUE_MAX];
  uint8_t heredoc_count;
  enum HeredocState heredoc_state;
  bool recovery_emitted;
} LexerState;

static void lexerstate_push_quote(LexerState *state, int32_t opener) {
  TSPQuote q = tspquote_new();
  // if it's not a paired delim, we get a 0 here
  int32_t closer = close_for_open(opener);
  q.close = closer ? closer : opener;
  q.open = closer ? opener : 0;
  q.count = 0;
  array_push(&state->quotes, q);
}

// consider `qq( hi ${\q(there)} $sner )`; we MUST start our check from the END of the
// quote stack
static int32_t lexerstate_is_quote_opener(LexerState *state, int32_t check) {
  // this will loop over all of the TSPQuotes in the state's quote and check if
  // it has an opener (like '{'). we return i + 1 b/c 0 is a valid index + we need that
  // for falsy
  for (int i = state->quotes.size - 1; i >= 0; i--) {
    TSPQuote *q = array_get(&state->quotes, i);
    if (q->open && check == q->open) return i + 1;
  }
  return 0;
}

static void lexerstate_saw_opener(LexerState *state, int32_t idx) {
  // we must loop over all the quotes and increment the count for the first one
  // that matches the codepoint passed in c
  TSPQuote *q = array_get(&state->quotes, idx - 1);
  q->count++;
  DEBUG("Got a opener for %c, we are at %d \n", q->open, q->count);
}

static int32_t lexerstate_is_quote_closer(LexerState *state, int32_t c) {
  // same as above, just for the closer
  for (int i = state->quotes.size - 1; i >= 0; i--) {
    TSPQuote *q = array_get(&state->quotes, i);
    if (q->close && c == q->close) return i + 1;
  }
  return 0;
}

static void lexerstate_saw_closer(LexerState *state, int32_t idx) {
  // same as above, just for the closer
  TSPQuote *q = array_get(&state->quotes, idx - 1);
  if (q->count) {
    q->count--;
    DEBUG("Got a closer, we are at %d \n", q->count);
  }
}

static bool lexerstate_is_quote_closed(LexerState *state, int32_t idx) {
  TSPQuote *q = array_get(&state->quotes, idx - 1);
  return !q->count;
}

static void lexerstate_pop_quote(LexerState *state, int32_t idx) {
  array_erase(&state->quotes, idx - 1);
}

static bool lexerstate_is_paired_delimiter(LexerState *state) {
  TSPQuote *q = array_back(&state->quotes);
  return !!q->open;
}

/* Is `c` the innermost quote's pattern-leading bracket -- the delimiter-matching
 * '['/'{' the open-handler lookahead flagged as the body's first literal element?
 * One-shot: clears the flag so a later same-delimiter bracket is treated as
 * ordinary (subscript/class) content. */
static bool lexerstate_take_body_lead(LexerState *state, int32_t c) {
  if (!state->quotes.size) return false;
  TSPQuote *q = array_back(&state->quotes);
  if (q->body_leads_with_delim && c == q->open) {
    q->body_leads_with_delim = false;
    return true;
  }
  return false;
}

//   in order to emulate a sublex, we basically need to have a new escape type character
//   that can pop up anywhere so long as there's an active string, to escape the string
//   char
//   with nested strings of the same type, it escapes them top down, observe:
//       s/things ${\q\/hello\/} / ${\q[hi again]} /;
//   the outer string gets a single escape, which is then invisible to the inner string
//
//   here's another relevant example:
//        qq( ${\\('hi')} );
//   the backslash needs to be escaped so it doesn't escape the quote_char from being counted for
//   pairs
//
//   ooh, a better solution to the backslash issue is that we should just add it to the
//   lexer, which will let us check if there's some quote_{opener,closer} in front of it
//   (which only happens inside of a string)

// we can match perl's behavior if we are intentionally destructive here and find our match

/* The front of the queue is the heredoc currently being consumed. */
static HeredocEntry *lexerstate_front_heredoc(LexerState *state) {
  return &state->heredoc_queue[0];
}

/* ENQUEUE a pending heredoc to the back of the FIFO.  When this is the first
 * pending heredoc, prime the consume FSM at HEREDOC_START.
 *
 * Overflow handling: if the queue is already full we OVERWRITE THE LAST SLOT
 * with the new entry rather than dropping it.  The last slot then always
 * tracks the FINAL heredoc's terminator, so the scanner stays in heredoc-mode
 * and greedily consumes all overflow content up to that final terminator —
 * absorbing the excess into the last body instead of leaking it into the code
 * stream.  For >MAX heredocs on a line this means the first MAX-1 bodies parse
 * correctly, the last body greedily swallows the remaining overflow (wrong but
 * BOUNDED), and code after the heredoc block is unaffected (no desync).  This
 * is graceful degradation for a pathological input. */
static void lexerstate_add_heredoc(LexerState *state, TSPString *delim, bool interp, bool indent) {
  HeredocEntry *e;
  if (state->heredoc_count >= HEREDOC_QUEUE_MAX)
    e = &state->heredoc_queue[HEREDOC_QUEUE_MAX - 1];
  else
    e = &state->heredoc_queue[state->heredoc_count++];
  e->delim = *delim;
  e->interpolates = interp;
  e->indents = indent;
  if (state->heredoc_count == 1) state->heredoc_state = HEREDOC_START;
}

/* DEQUEUE the finished front heredoc and advance to the next pending one.
 * Each heredoc body is wrapped in its own `heredoc_content` grammar node which
 * begins with a fresh `_heredoc_start` zero-width token; that token only fires
 * in HEREDOC_START.  So when another heredoc remains queued we re-arm the FSM
 * to HEREDOC_START, exactly as if its delimiter had just been declared. */
static void lexerstate_finish_heredoc(LexerState *state) {
  if (state->heredoc_count > 0) {
    state->heredoc_count--;
    for (int i = 0; i < state->heredoc_count; i++)
      state->heredoc_queue[i] = state->heredoc_queue[i + 1];
  }
  state->heredoc_queue[state->heredoc_count] = (HeredocEntry){0};
  state->heredoc_state = state->heredoc_count > 0 ? HEREDOC_START : HEREDOC_NONE;
}

#define ADVANCE_C                                                             \
  do {                                                                        \
    if (lexer->lookahead == '\r')                                             \
      DEBUG("> advance U+%04X = \\r\n", lexer->lookahead);                    \
    else if (lexer->lookahead == '\n')                                        \
      DEBUG("> advance U+%04X = \\n\n", lexer->lookahead);                    \
    else                                                                      \
      DEBUG("> advance U+%04X = '%c'\n", lexer->lookahead, lexer->lookahead); \
    lexer->advance(lexer, false);                                             \
    c = lexer->lookahead;                                                     \
  } while (0)

#define TOKEN(type)              \
  do {                           \
    DEBUG("token(%s)\n", #type); \
    lexer->result_symbol = type; \
    return true;                 \
  } while (0)

#define MARK_END                        \
  do {                                  \
    lexer->mark_end(lexer);             \
    DEBUG("marking end of token\n", 0); \
  } while (0)

// Try each recovery token in priority order.  Uses a macro because
// TOKEN() contains `return true`.  brace_ok: skip brace recovery at '}'
// (that's the real closer); ';' is safe (only valid in subscript context).
#define EMIT_RECOVERY_TOKENS(brace_ok) do { \
    if (valid_symbols[TOKEN_RECOVER_ARROW])        { state->recovery_emitted = true; TOKEN(TOKEN_RECOVER_ARROW); } \
    if (valid_symbols[TOKEN_RECOVER_PAREN_CLOSE])  { state->recovery_emitted = true; TOKEN(TOKEN_RECOVER_PAREN_CLOSE); } \
    if (valid_symbols[TOKEN_RECOVER_BRACKET_CLOSE]) { state->recovery_emitted = true; TOKEN(TOKEN_RECOVER_BRACKET_CLOSE); } \
    if ((brace_ok) && valid_symbols[TOKEN_RECOVER_BRACE_CLOSE]) { state->recovery_emitted = true; TOKEN(TOKEN_RECOVER_BRACE_CLOSE); } \
  } while(0)

static void skip_whitespace(TSLexer *lexer) {
  while (1) {
    int32_t c = lexer->lookahead;
    if (!c) return;
    if (is_tsp_whitespace(c)) lexer->advance(lexer, true);
    /* continue */
    else
      return;
  }
}

static bool skip_ws_to_eol(TSLexer *lexer) {
  while (1) {
    int32_t c = lexer->lookahead;
    if (!c) return false;
    if (is_tsp_whitespace(c)) {
      lexer->advance(lexer, true);
      if (c == '\n') return true;
    } else
      return false;
  }
}

// Forward declarations (defined after _skip_chars)
static bool isidfirst(int32_t c);
static bool isidcont(int32_t c);

enum PeekResult {
  PEEK_NO_MATCH,      // first char not a keyword starter — lexer untouched
  PEEK_KEYWORD,       // statement keyword (not followed by =>)
  PEEK_FAT_COMMA,     // keyword followed by => — caller MARK_ENDs then goto fat_comma_check
  PEEK_AUTOQUOTE_KEY, // non-keyword word followed by => — peek already marked the
                      // word boundary; caller goes straight to fat_comma_check
  PEEK_NOT_KEYWORD,   // word read but not a keyword — caller MUST return false
};

// How a word read by the keyword peek classifies (set by KEYWORD_MATCH).
enum KwKind {
  KW_NONE,       // not a statement keyword
  KW_ALWAYS,     // always a statement keyword (package/use/no/class/role)
  KW_NEEDS_NAME, // declaration only if followed by a name (sub/method)
};

static enum PeekResult peek_is_statement_keyword(TSLexer *lexer, bool *is_structural) {
  int32_t la = lexer->lookahead;
  *is_structural = false;

  if (KEYWORD_FIRST_CHAR_FILTER(la))
    return PEEK_NO_MATCH;

  // Read the word (character set derived from keyword list)
  char word[16];
  int len = 0;
  while (KEYWORD_WORD_CHAR(la)) {
    if (len < 15) word[len++] = (char)la;
    lexer->advance(lexer, false);
    la = lexer->lookahead;
  }
  word[len] = '\0';

  // Classify *before* consuming any identifier tail: a statement keyword must
  // end exactly at the keyword charset boundary.  A longer identifier (e.g.
  // "service", "naming") read only partway by the loop above is never a
  // keyword — but it can still be a fat-comma autoquote key, so we don't bail.
  enum KwKind kind = KW_NONE;
  if (!isidcont(la))
    KEYWORD_MATCH(word, kind);

  if (kind == KW_NONE) {
    // Not a statement keyword.  It may still be a fat-comma autoquote key
    // (`name => 1` on a continuation line inside an open list, where the
    // recovery gate fires and would otherwise lex it as a plain bareword).
    // Consume the rest of the identifier, mark the token end at the word
    // boundary, then look past whitespace for '='.  The whole word must be
    // covered, so skip with advance(false): advance(true) would reset the
    // token start past the word and emit a zero-width key.  mark_end fixes the
    // end at the word, so trailing whitespace stays out of the token anyway.
    while (isidcont(la)) {
      lexer->advance(lexer, false);
      la = lexer->lookahead;
    }
    lexer->mark_end(lexer);
    while (is_tsp_whitespace(la)) {
      lexer->advance(lexer, false);
      la = lexer->lookahead;
    }
    if (la == '=') return PEEK_AUTOQUOTE_KEY;
    return PEEK_NOT_KEYWORD;
  }

  // A statement keyword.  Structural OO keywords (method/class/role) never
  // legitimately nest inside a sub/method body, so they alone trigger body
  // block-close recovery.  use/no/sub/package commonly DO appear in a body
  // (e.g. `no strict 'refs';`) and must not trigger it.
  //
  // `sub` is the tempting one: a bare nested `sub foo {...}` is almost always a
  // "won't stay shared" bug, not intent — so closing the enclosing body at it
  // would often be the *helpful* read.  We deliberately don't.  It is valid Perl
  // and it is genuinely out there (~140 nested bare subs across a 92k-file
  // corpus: perl's own regen/Porting codegen scripts, some CPAN, real app code),
  // so the parser tree-s it faithfully; diagnosing the footgun belongs to the
  // layer above (linter/LSP), not the grammar.  Lexical `my sub` (which *does*
  // truly nest, and is legit) is a separate story and is excluded for free
  // anyway: it starts with `my`, takes the KW_NONE path above, and never reaches
  // here.  (`kind` was already classified above; don't re-run KEYWORD_MATCH.)
  *is_structural = (strcmp(word, "method") == 0 ||
                    strcmp(word, "class") == 0 ||
                    strcmp(word, "role") == 0);

  // The caller already marked a zero-width position at the keyword start for
  // recovery-token emission; leave that mark alone (don't mark_end here) and
  // skip trailing whitespace with advance(true) — the recovery-safe behavior.
  // A word-end mark here would make the recovery token swallow the keyword
  // (and breaks the keyword-boundary tests).
  while (is_tsp_whitespace(la)) {
    lexer->advance(lexer, true);
    la = lexer->lookahead;
  }

  // Fat comma => means even a keyword is a hash key, not a statement
  // (`package => 1`).  We can't mark the word boundary here without clobbering
  // the recovery mark, so the caller emits a zero-width _fat_comma_autoquoted
  // _ahead marker and tree-sitter re-lexes the word through the normal
  // autoquote path with a correct span.  (Plain words and non-keyword builtins
  // — sort/map/ref/state/… — take the KW_NONE path above and autoquote in a
  // single pass.)
  if (la == '=') return PEEK_FAT_COMMA;

  // For sub/method: only a declaration if followed by an identifier (name)
  if (kind == KW_NEEDS_NAME && !isidfirst(la))
    return PEEK_NOT_KEYWORD;  // anonymous sub/method

  return PEEK_KEYWORD;
}

static void _skip_chars(TSLexer *lexer, int maxlen, const char *allow) {
  int32_t c = lexer->lookahead;

  while (maxlen)
    if (!c)
      return;
    else if (tsp_strchr(allow, c)) {
      ADVANCE_C;
      if (maxlen > 0) maxlen--;
    } else
      break;
}
#define skip_hexdigits(lexer, maxlen) _skip_chars(lexer, maxlen, "0123456789ABCDEFabcdef")
#define skip_digits(lexer, maxlen) _skip_chars(lexer, maxlen, "0123456789")
#define skip_octdigits(lexer, maxlen) _skip_chars(lexer, maxlen, "01234567")

static void skip_braced(TSLexer *lexer) {
  int32_t c = lexer->lookahead;

  if (c != '{') return;

  ADVANCE_C;
  while (c && c != '}') ADVANCE_C;

  ADVANCE_C;
}

static bool isidfirst(int32_t c) { return c == '_' || is_tsp_id_start(c); }

static bool isidcont(int32_t c) { return c == '_' || is_tsp_id_continue(c); }

// in any interpolatable case, we wanna stop parsing on these chars
// there's a matching rule in the grammar to catch when it doesn't match a rule
static bool is_interpolation_escape(int32_t c) { return c < 256 && tsp_strchr("$@-[{\\", c); }

/* The runtime hands serialize() a FIXED buffer of TREE_SITTER_SERIALIZATION_BUFFER_SIZE
 * (1024) bytes — `self->lexer.debug_buffer`, a member embedded in the Lexer/TSParser
 * struct.  Writing past it is intra-object memory corruption (ASAN doesn't flag it),
 * and returning a length > the buffer trips the runtime's `assert(length <= 1024)`
 * (or, in NDEBUG builds, silently clobbers adjacent parser fields).  So we must bound
 * the serialized size to the buffer, NOT to UINT8_MAX.
 *
 * Budget: everything except the quotes is fixed-size — the two count bytes, the
 * recovery flag, and the whole heredoc FIFO at its worst case (HEREDOC_QUEUE_MAX
 * full entries).  Whatever room is left holds quotes.  A deeply nested (or, in a
 * mid-edit buffer, deeply *unclosed*) interpolation like `qq{ ${\ qq{ ... }} }`
 * stacks one quote per level and can blow past this; we cap it.  Truncating the
 * stack degrades parsing of pathologically-nested input (>~59 levels) but keeps it
 * BOUNDED — the same graceful-degradation contract the heredoc overflow already uses.
 */
#define SER_FIXED_OVERHEAD \
  (1 /* quote_count   */ + \
   1 /* heredoc_state */ + 1 /* heredoc_count */ + \
   HEREDOC_QUEUE_MAX * (1 /* interpolates */ + 1 /* indents */ + (int)sizeof(TSPString)) + \
   1 /* recovery_emitted */)
#define MAX_SERIALIZED_QUOTES \
  ((TREE_SITTER_SERIALIZATION_BUFFER_SIZE - SER_FIXED_OVERHEAD) / (int)sizeof(TSPQuote))

unsigned int tree_sitter_perl_external_scanner_serialize(void *payload, char *buffer) {
  LexerState *state = payload;
  size_t size = 0;

  // Serialize the quotes array, capped to what fits in the fixed buffer (see above).
  size_t quote_count = state->quotes.size;
  if (quote_count > MAX_SERIALIZED_QUOTES) {
    quote_count = MAX_SERIALIZED_QUOTES;
  }
  buffer[size++] = (char)quote_count;

  if (quote_count > 0) {
    memcpy(&buffer[size], state->quotes.contents, quote_count * sizeof(TSPQuote));
  }
  size += quote_count * sizeof(TSPQuote);

  // Serialize the heredoc consume state, then the whole pending FIFO queue:
  // a count byte followed by each entry's flags + delimiter.
  buffer[size++] = (char)state->heredoc_state;
  buffer[size++] = (char)state->heredoc_count;
  for (uint8_t i = 0; i < state->heredoc_count; i++) {
    HeredocEntry *e = &state->heredoc_queue[i];
    buffer[size++] = (char)e->interpolates;
    buffer[size++] = (char)e->indents;
    memcpy(&buffer[size], &e->delim, sizeof(TSPString));
    size += sizeof(TSPString);
  }
  buffer[size++] = (char)state->recovery_emitted;

  return size;
}

void tree_sitter_perl_external_scanner_deserialize(void *payload, const char *buffer,
                                                   unsigned int length) {
  LexerState *state = payload;
  size_t size = 0;
  array_delete(&state->quotes);
  if (length > 0) {
    // Deserialize the quotes array
    size_t quote_count = (uint8_t)buffer[size++];
    // Defensive: a well-formed buffer never exceeds this (serialize caps it), but
    // clamp anyway so a malformed/old buffer can't drive an oversized memcpy.
    if (quote_count > MAX_SERIALIZED_QUOTES) quote_count = MAX_SERIALIZED_QUOTES;
    if (quote_count > 0) {
      array_reserve(&state->quotes, quote_count);
      state->quotes.size = quote_count;
      memcpy(state->quotes.contents, &buffer[size], quote_count * sizeof(TSPQuote));
      size += quote_count * sizeof(TSPQuote);
    }

    // Deserialize the heredoc consume state and the pending FIFO queue.
    state->heredoc_state = (enum HeredocState)buffer[size++];
    state->heredoc_count = (uint8_t)buffer[size++];
    if (state->heredoc_count > HEREDOC_QUEUE_MAX) state->heredoc_count = HEREDOC_QUEUE_MAX;
    for (uint8_t i = 0; i < state->heredoc_count; i++) {
      HeredocEntry *e = &state->heredoc_queue[i];
      e->interpolates = (bool)buffer[size++];
      e->indents = (bool)buffer[size++];
      memcpy(&e->delim, &buffer[size], sizeof(TSPString));
      size += sizeof(TSPString);
    }
    state->recovery_emitted = (bool)buffer[size++];
  } else {
    state->heredoc_count = 0;
    state->heredoc_state = HEREDOC_NONE;
  }
}

bool tree_sitter_perl_external_scanner_scan(void *payload, TSLexer *lexer,
                                            const bool *valid_symbols) {
  LexerState *state = payload;

  bool is_ERROR = valid_symbols[TOKEN_ERROR];
  bool skipped_whitespace = false;
  bool crossed_newline = false;
  bool recovery_emitted = state->recovery_emitted;
  state->recovery_emitted = false;

  int32_t c = lexer->lookahead;

  if (!is_ERROR && valid_symbols[TOKEN_GOBBLED_CONTENT]) {
    while (!lexer->eof(lexer)) ADVANCE_C;

    TOKEN(TOKEN_GOBBLED_CONTENT);
  }

  /* `format NAME = <newline> BODY .` — the body (line after `=` up to and
   * including a lone `.` line) is opaque content the LR grammar can't parse, read
   * as one TOKEN_FORMAT_CONTENT.  Lives before whitespace handling so the body's
   * leading newline isn't skipped separately. */
  if (!is_ERROR && valid_symbols[TOKEN_FORMAT_CONTENT]) {
    /* swallow the remainder of the `format ... =` line, then its newline */
    while (c != '\n' && !lexer->eof(lexer)) ADVANCE_C;
    if (c == '\n') ADVANCE_C;
    /* now read body lines; stop after a line consisting solely of `.`
     * (perl: a lone `.`, trailing whitespace tolerated) */
    while (!lexer->eof(lexer)) {
      /* peek whether this whole line is just `.` */
      bool only_dot = false;
      if (c == '.') {
        ADVANCE_C;
        only_dot = true;
        while (c == ' ' || c == '\t' || c == '\r') ADVANCE_C;
        if (c != '\n' && !lexer->eof(lexer)) only_dot = false;
      }
      if (only_dot) {
        if (c == '\n') ADVANCE_C; /* include the terminator's newline */
        break;
      }
      /* not the terminator: consume to end of this line */
      while (c != '\n' && !lexer->eof(lexer)) ADVANCE_C;
      if (c == '\n') ADVANCE_C;
    }
    MARK_END;
    TOKEN(TOKEN_FORMAT_CONTENT);
  }

  /* we use this to force tree-sitter to stay on the error branch of a nonassoc
   * operator */
  if (!is_ERROR && valid_symbols[TOKEN_NONASSOC]) TOKEN(TOKEN_NONASSOC);

  /* The `x` repetition operator glued to its count (`"ab"x3`, `("")x4`). The
   * internal lexer would greedily take `x3` as one identifier, so when the
   * grammar offers the operator here (an operator is expected — perl's
   * XOPERATOR state, which `valid_symbols` encodes) and `x` is glued to a digit,
   * we emit it explicitly, consuming just the `x`. Anything else (`xor`, an
   * identifier, a space-delimited `x`) falls through to the normal lexer. */
  if (!is_ERROR && valid_symbols[TOKEN_X_OP] && c == 'x') {
    ADVANCE_C;
    if (c >= '0' && c <= '9') { MARK_END; TOKEN(TOKEN_X_OP); }
    return false;
  }

  // this is whitespace sensitive, so it must go before any whitespace is
  // skipped
  if (valid_symbols[TOKEN_HEREDOC_MIDDLE] && !is_ERROR && state->heredoc_count > 0) {
    HeredocEntry *front = lexerstate_front_heredoc(state);
    DEBUG("Beginning heredoc contents\n", 0);
    if (state->heredoc_state != HEREDOC_CONTINUE) {
      TSPString line = {0};
      // read as many lines as we can
      while (!lexer->eof(lexer)) {
        tspstring_reset(&line);
        // interpolating heredocs may need to stop in the middle of the line;
        // indented heredocs may START in the beggining of a known line
        bool is_valid_start_pos =
            state->heredoc_state == HEREDOC_END || lexer->get_column(lexer) == 0;
        bool saw_escape = false;
        DEBUG("Starting loop at col %d\n", lexer->get_column(lexer));
        if (is_valid_start_pos && front->indents) {
          DEBUG("Skipping initial whitespace in heredoc\n", 0);
          skip_whitespace(lexer);
          c = lexer->lookahead;
        }
        // we may be doing lookahead now
        MARK_END;
        // read the whole line, b/c we want it
        while (c != '\n' && !lexer->eof(lexer)) {
          // we need special handling for windows line ending, b/c we can't
          // count it in our lookahead
          if (c == '\r') {
            ADVANCE_C;
            if (c == '\n') break;
            tspstring_push(&line, '\r');
          }
          tspstring_push(&line, c);
          if (c == '$' || c == '@' || c == '\\') saw_escape = true;
          ADVANCE_C;
        }
        DEBUG("got length %d, want length %d\n", line.length, front->delim.length);
        if (is_valid_start_pos && tspstring_eq(&line, &front->delim)) {
          // if we've read already, we return everything up until now
          if (state->heredoc_state != HEREDOC_END) {
            state->heredoc_state = HEREDOC_END;
            TOKEN(TOKEN_HEREDOC_MIDDLE);
          }
          MARK_END;
          lexerstate_finish_heredoc(state);
          TOKEN(TOKEN_HEREDOC_END);
        }
        if (saw_escape && front->interpolates) {
          // we'll repeat this line in continue mode where we'll pause midline
          state->heredoc_state = HEREDOC_CONTINUE;
          TOKEN(TOKEN_HEREDOC_MIDDLE);
        }
        // eat the \n and loop again; can't skip whitespace b/c the next line
        // may care
        ADVANCE_C;
      }
    } else {
      DEBUG("Entering heredoc continue mode\n", 0);
      // handle the continue case; read ahead until we get a \n or an escape
      bool saw_chars = false;
      while (1) {
        if (is_interpolation_escape(c)) {
          MARK_END;
          break;
        }
        if (c == '\n') {
          MARK_END;
          state->heredoc_state = HEREDOC_UNKNOWN;
          TOKEN(TOKEN_HEREDOC_MIDDLE);
        }
        saw_chars = true;
        ADVANCE_C;
      }
      if (saw_chars) TOKEN(TOKEN_HEREDOC_MIDDLE);
    }
  }

  if (!is_ERROR && iswspace(c) && valid_symbols[TOKEN_NO_INTERP_WHITESPACE_ZW]) {
    TOKEN(TOKEN_NO_INTERP_WHITESPACE_ZW);
  }
  crossed_newline = skip_ws_to_eol(lexer);
  /* heredocs override everything, so they must be here before */
  if (valid_symbols[TOKEN_HEREDOC_START]) {
    if (state->heredoc_state == HEREDOC_START && lexer->get_column(lexer) == 0) {
      state->heredoc_state = HEREDOC_UNKNOWN;
      TOKEN(TOKEN_HEREDOC_START);
    }
  }

  if (!is_ERROR && valid_symbols[TOKEN_ATTRIBUTE_VALUE_BEGIN] && c == '(') {
    /* This has to be an external scanner symbol so it takes precedence over
     * signature_or_prototype */
    TOKEN(TOKEN_ATTRIBUTE_VALUE_BEGIN);
  }

  if (!is_ERROR && valid_symbols[TOKEN_ATTRIBUTE_VALUE]) {
    DEBUG("Attribute value started...\n", 0);

    int delimcount = 0;
    while (!lexer->eof(lexer)) {
      if (c == '\\') {
        ADVANCE_C;
        /* ignore the next char */
      } else if (c == '(')
        delimcount++;
      else if (c == ')') {
        if (delimcount)
          delimcount--;
        else {
          /* Do not consume the ')' */
          break;
        }
      }

      ADVANCE_C;
    }

    TOKEN(TOKEN_ATTRIBUTE_VALUE);
  }

  if (is_tsp_whitespace(c)) {
    // NOTE - the first whitespace skipping is skip_ws_to_eol over in heredoc
    // handling
    skipped_whitespace = true;
    skip_whitespace(lexer);
    c = lexer->lookahead;
  }

  // CTRL-Z must be here, b/c it cares about whitespace
  if (c == 26 && valid_symbols[TOKEN_CTRL_Z]) TOKEN(TOKEN_CTRL_Z);

  bool any_recovery_valid =
    valid_symbols[TOKEN_RECOVER_ARROW] ||
    valid_symbols[TOKEN_RECOVER_PAREN_CLOSE] ||
    valid_symbols[TOKEN_RECOVER_BRACKET_CLOSE] ||
    valid_symbols[TOKEN_RECOVER_BRACE_CLOSE] ||
    valid_symbols[TOKEN_RECOVER_BLOCK_CLOSE] ||
    valid_symbols[PERLY_SEMICOLON];

  // Contextual keywords: class/role/method are declarations only when a name (or
  // a block, for method) follows; otherwise they're ordinary barewords
  // (`class($cv)`, `role { … }`, `-role`).  Decide here, before emitting, so
  // there's no keyword-vs-call ambiguity.  The OO keywords are deferred while
  // recovery is pending because they trigger the structural body block-close,
  // which must fire first; async/try don't, so they're classified unconditionally.
  bool oo_kw_valid = valid_symbols[TOKEN_KW_CLASS] || valid_symbols[TOKEN_KW_ROLE] || valid_symbols[TOKEN_KW_METHOD];
  bool asynctry_valid = valid_symbols[TOKEN_KW_ASYNC] || valid_symbols[TOKEN_KW_TRY];
  bool recovery_pending = any_recovery_valid && (crossed_newline || recovery_emitted);
  bool enter_oo = oo_kw_valid && !recovery_pending && (c == 'c' || c == 'r' || c == 'm');
  bool enter_asynctry = asynctry_valid && (c == 'a' || c == 't');
  if (!is_ERROR && (enter_oo || enter_asynctry)) {
    char word[8];
    int wlen = 0;
    while (isidcont(c)) {  // consume the whole identifier (store first 7 chars)
      if (wlen < 7) word[wlen++] = (char)c;
      lexer->advance(lexer, false);
      c = lexer->lookahead;
    }
    word[wlen] = '\0';
    MARK_END;  // the token covers exactly the word (lookahead past here is free)
    while (is_tsp_whitespace(c)) {  // peek past whitespace at the next token
      lexer->advance(lexer, false);
      c = lexer->lookahead;
    }
    bool is_class  = !recovery_pending && valid_symbols[TOKEN_KW_CLASS]  && strcmp(word, "class")  == 0;
    bool is_role   = !recovery_pending && valid_symbols[TOKEN_KW_ROLE]   && strcmp(word, "role")   == 0;
    bool is_method = !recovery_pending && valid_symbols[TOKEN_KW_METHOD] && strcmp(word, "method") == 0;
    bool is_async  = valid_symbols[TOKEN_KW_ASYNC]  && strcmp(word, "async")  == 0;
    bool is_try    = valid_symbols[TOKEN_KW_TRY]    && strcmp(word, "try")    == 0;

    // `try` is the try/catch keyword only before a block `{`; `try(...)`/`try =>`
    // /`try;` are bareword calls.  (`catch` is never over-reserved.)
    if (is_try) {
      if (c == '{') TOKEN(TOKEN_KW_TRY);
    }

    // `async` is a keyword before a block (`async { … }` — threads::async, a bare
    // block run as an anon sub; F::AA itself has only `async sub`) or before a
    // sub/method/extended declaration.  Else it's a bareword/call.
    if (is_async) {
      if (c == '{') TOKEN(TOKEN_KW_ASYNC);
      if (c == 's' || c == 'm' || c == 'e') {  // peek for sub/method/extended
        char nword[9];
        int nlen = 0;
        while (isidcont(c)) {
          if (nlen < 8) nword[nlen++] = (char)c;
          lexer->advance(lexer, false);
          c = lexer->lookahead;
        }
        nword[nlen] = '\0';
        if (strcmp(nword, "sub") == 0 || strcmp(nword, "method") == 0 ||
            strcmp(nword, "extended") == 0)
          TOKEN(TOKEN_KW_ASYNC);
        // Not a decl: `async` is a bareword/call.  Must `return false` (re-lex),
        // NOT `goto kw_autoquote` like the path below — we consumed past `async`
        // into the next word, so the handler would read *that* word's `=>` as
        // async's own fat comma and wrongly autoquote `async` (`async send=>1`).
        return false;
      }
    }

    // `class NAME` / `role NAME` — a name following => declaration; else bareword.
    if ((is_class || is_role) && isidfirst(c))
      TOKEN(is_class ? TOKEN_KW_CLASS : TOKEN_KW_ROLE);

    // `method NAME { … }` / `method { … }` are declarations (named/anonymous);
    // `method NAME => sub { … }` (MooseX) is a call — peek past the name for `=>`.
    if (is_method) {
      if (c == '{' || c == '(') TOKEN(TOKEN_KW_METHOD);  // anonymous method
      if (isidfirst(c)) {
        while (isidcont(c)) { lexer->advance(lexer, false); c = lexer->lookahead; }  // skip the name
        while (is_tsp_whitespace(c)) { lexer->advance(lexer, false); c = lexer->lookahead; }
        if (c != '=') TOKEN(TOKEN_KW_METHOD);  // `{`/`(`/`:`/`;` => decl; `=>` => call
        return false;  // method NAME => … : re-lex as a call (the name autoquotes)
      }
    }

    // Bareword/call, or an autoquoted key (`class => 1`, `$h{m}`).  Can't just
    // `return false` — a quote-op word (`m`/`s`/`y`) would re-lex as a match — so
    // fall through to the canonical autoquote handler; MARK_END already fixed the
    // token at the word, and it emits the autoquote or returns false on no match.
    goto kw_autoquote;
  }

  // === Error recovery: close unclosed delimiters and insert semicolons ===
  //
  // When the scanner sees '}', ';', EOF, or a statement keyword on a new
  // line, it emits recovery tokens to unwind open delimiters.  Each call
  // peels one layer — the parser keeps calling until everything is closed.
  //
  // Syntactic boundary: '}', ';', or EOF
  if (!is_ERROR) {
    if (c == '}' || c == ';' || lexer->eof(lexer)) {
      EMIT_RECOVERY_TOKENS(c != '}');
    }
  }

  if (valid_symbols[PERLY_SEMICOLON]) {
    if (c == '}' || lexer->eof(lexer)) {
      if (is_ERROR || !valid_symbols[TOKEN_BRACE_END_ZW]) {
        DEBUG("Fake PERLY_SEMICOLON at end-of-scope\n", 0);
        TOKEN(PERLY_SEMICOLON);
      }
    }
  }
  if (lexer->eof(lexer)) return false;

  // Statement keyword boundary: a statement keyword on a new line means
  // the previous expression is done.  Also fires when a recovery token
  // was just emitted (recovery_emitted flag) since the newline was already
  // consumed by the previous call.
  // Gate on the first-char filter so we don't call MARK_END (and freeze
  // the token end at the keyword-start position) when the lookahead can't
  // possibly start a keyword.  Without this, a later TOKEN() emission
  // (e.g. TOKEN_APOSTROPHE/TOKEN_DOUBLE_QUOTE which do ADVANCE_C+TOKEN
  // and never re-mark) inherits the stale mark and reports a zero-width
  // open-quote, which derails the string body.
  if ((crossed_newline || recovery_emitted) && !is_ERROR && any_recovery_valid &&
      !KEYWORD_FIRST_CHAR_FILTER(c)) {
    MARK_END;  // zero-width position for recovery tokens
    bool is_structural = false;
    enum PeekResult peek = peek_is_statement_keyword(lexer, &is_structural);
    if (peek == PEEK_KEYWORD) {
      DEBUG("keyword boundary\n", 0);
      EMIT_RECOVERY_TOKENS(true);
      // Body block-close: only a structural OO keyword (method/class/role)
      // closes a half-typed sub/method body.  Fires after any inner paren/
      // bracket closers above; the enclosing class block can't accept this
      // token, so the cascade stops at one body (no over-closing).
      if (is_structural && valid_symbols[TOKEN_RECOVER_BLOCK_CLOSE]) {
        DEBUG("body block recovery at structural keyword\n", 0);
        // Re-arm the cascade so the next scan re-enters and can close the NEXT
        // enclosing recover-aware block (e.g. an unclosed `if`/loop body inside
        // the method: close the if-block, then the method body).
        state->recovery_emitted = true;
        TOKEN(TOKEN_RECOVER_BLOCK_CLOSE);
      }
      // A statement terminator may need to fire BEFORE the body block-close:
      // after an inner closer (e.g. `my @x = [` -> `]`) completes an expression,
      // the statement still needs `;` before the enclosing body can reduce and
      // close.  Re-arm the recovery cascade (recovery_emitted) so the next scan
      // re-enters here and can emit the block-close.  Harmless when nothing
      // follows: the re-entry finds no further valid recovery token.
      if (valid_symbols[PERLY_SEMICOLON]) {
        state->recovery_emitted = true;
        TOKEN(PERLY_SEMICOLON);
      }
    }
    if (peek == PEEK_FAT_COMMA) {
      // A statement keyword used as a fat-comma key (`package => 1`).  We can't
      // mark the word boundary here (peek preserved the caller's zero-width
      // recovery mark for the keyword path, and re-marking would consume the
      // keyword).  Instead emit a zero-width "autoquote ahead" marker at the
      // word start: tree-sitter re-lexes the word via the normal autoquote
      // path, which marks the word boundary correctly.  The recovery gate
      // won't re-fire on re-entry (no newline precedes the word that scan), so
      // there's no loop.  Falls back to the old (zero-width-span) emission if
      // the marker isn't accepted in this state.
      if (valid_symbols[TOKEN_FAT_COMMA_AUTOQUOTED_AHEAD])
        TOKEN(TOKEN_FAT_COMMA_AUTOQUOTED_AHEAD);
      MARK_END;
      c = lexer->lookahead;
      goto fat_comma_check;
    }
    if (peek == PEEK_AUTOQUOTE_KEY) {
      // A non-keyword bareword used as a fat-comma key (`name => 1`) on a
      // continuation line where the recovery gate fired.  Peek already marked
      // the token end at the word boundary (covering exactly the word), so do
      // NOT MARK_END again — that would re-mark at '=' and emit a zero-width
      // key.  Resume at the autoquote handler's => check.
      c = lexer->lookahead;
      goto fat_comma_check;
    }
    if (peek == PEEK_NOT_KEYWORD)
      return false;
  }

  if (valid_symbols[TOKEN_OPEN_FILEGLOB_BRACKET] || valid_symbols[TOKEN_OPEN_READLINE_BRACKET] || valid_symbols[PERLY_HEREDOC]) {
      if (c == '<') {
          ADVANCE_C;
          MARK_END;
          // ah, we have a heredoc; let's just go down to that section then
          if (c == '<') goto heredoc_token_handling;
          // Gather the `<...>` body as we go, starting right after `<`, so the
          // fileglob content heuristic below sees the WHOLE body -- including
          // the `$ident` prefix the readline probe consumes here (otherwise a
          // `<$sner >` glob would reach the heuristic with content " ").
          char content[256];
          size_t clen = 0;
          if (c == '$') { content[clen++] = '$'; ADVANCE_C; }
          // we now zoooom as many ident chars as we can
          while (isidcont(c)) {
            if (clen < sizeof(content)) content[clen++] = (c < 0x80) ? (char)c : (char)0x7f;
            ADVANCE_C;
          }
          // if ident chars took us until the closing `>` then we're readline FILEHANDLE
          if (c == '>') TOKEN(TOKEN_OPEN_READLINE_BRACKET);
          // Otherwise `<...>` is either a fileglob (`<*.c>`) or the relational
          // `<` operator (`CONST < 0`); both are live after an ambiguous
          // bareword.  Gather the body and the bytes after the `>` (pure
          // lookahead past MARK_END, so the token stays the one-char `<`;
          // non-ASCII -> 0x7f), then let tsp_is_fileglob() decide -- see
          // tsp_intuit_readline.h.
          if (valid_symbols[TOKEN_OPEN_FILEGLOB_BRACKET]) {
            // `content`/`clen` already hold the `$ident` prefix from above.
            // Stop at a statement boundary so we don't chase a `>` onto the
            // next line.
            while (c != '>' && c != '<' && c != ';' && c != '\n' &&
                   !lexer->eof(lexer)) {
              if (clen < sizeof(content))
                content[clen++] = (c < 0x80) ? (char)c : (char)0x7f;
              ADVANCE_C;
            }

            if (c == '>') {
              ADVANCE_C;  // step past the closing `>` (still pure lookahead)
              char after[256];
              size_t alen = 0;
              while (alen < sizeof(after) && c != '\n' && !lexer->eof(lexer)) {
                after[alen++] = (c < 0x80) ? (char)c : (char)0x7f;
                ADVANCE_C;
              }
              if (tsp_is_fileglob(content, clen, after, alen)) {
                lexerstate_push_quote(state, '<');
                TOKEN(TOKEN_OPEN_FILEGLOB_BRACKET);
              }
            }
          }
          // not a readline, not a fileglob — let `<` fall through to the
          // grammar as the relational operator.
          return false;
      }

  }

  if (!is_ERROR && valid_symbols[TOKEN_DOLLAR_IDENT_ZW]) {
    // false on word chars, another dollar or {  -- but if whitespace intervened
    // the `$` can't begin a glued deref ($$foo needs the name glued on), so a
    // following word char is a separate token: `$$ eq …`, `$$ and …` are the
    // PID var `$$`, not `${$eq}`. Treat skipped whitespace as a hard boundary.
    if (!tsp_strchr("${", c) && (skipped_whitespace || !isidcont(c))) {
      if (c == ':') {
        // NOTE - it's a syntax error to do $$:, so that's why we return
        // dollar_ident_zw in that case
        MARK_END;
        ADVANCE_C;
        if (c == ':') {
          // we can safely bail out here b/c we know that $:: is handled in the
          // grammar, and that's the only place we can ever get to this rule
          // here
          return false;
        }
      }
      TOKEN(TOKEN_DOLLAR_IDENT_ZW);
    }
  }

  if ((valid_symbols[TOKEN_SEARCH_SLASH] && c == '/') &&
      !valid_symbols[NO_TOKEN_SEARCH_SLASH_PLZ]) {
    ADVANCE_C;
    MARK_END;

    if (c != '/') {
      lexerstate_push_quote(state, '/');
      TOKEN(TOKEN_SEARCH_SLASH);
    }
    /* if we didn't get a search-slash, we fall back to the main parser */
    return false;
  }
  if (valid_symbols[TOKEN_APOSTROPHE] && c == '\'') {
    ADVANCE_C;
    lexerstate_push_quote(state, '\'');
    TOKEN(TOKEN_APOSTROPHE);
  }
  if (valid_symbols[TOKEN_DOUBLE_QUOTE] && c == '"') {
    ADVANCE_C;
    lexerstate_push_quote(state, '"');
    TOKEN(TOKEN_DOUBLE_QUOTE);
  }
  if (valid_symbols[TOKEN_BACKTICK] && c == '`') {
    ADVANCE_C;
    lexerstate_push_quote(state, '`');
    TOKEN(TOKEN_BACKTICK);
  }

  if (valid_symbols[TOKEN_DOLLAR_IN_REGEXP] && c == '$') {
    DEBUG("Dollar in regexp\n", 0);
    ADVANCE_C;

    /* Accept this literal dollar if it's followed by closing delimiter */
    if (lexerstate_is_quote_closer(state, c)) TOKEN(TOKEN_DOLLAR_IN_REGEXP);

    /* Several other situations are interpreted literally */
    switch (c) {
      case '(':
      case ')':
      case '|':
        TOKEN(TOKEN_DOLLAR_IN_REGEXP);
    }

    return false;
  }

  /* A '[' or '{' in a pattern is ambiguous: it can begin a subscript on the
   * preceding variable (/$foo[0]/, /$foo{k}/) or a character class /
   * quantifier (/[abc]/, /a{2,3}/).  We replicate perl's S_intuit_more
   * heuristic (tsp_intuit_more) to decide.  When it's a class/quantifier we
   * emit a literal opener (aliased back to '[' / '{' in the grammar); when
   * it's a subscript we decline so the grammar's token.immediate('[' / '{')
   * takes over. */
  if ((valid_symbols[TOKEN_REGEXP_OPEN_BRACKET] && c == '[') ||
      (valid_symbols[TOKEN_REGEXP_OPEN_BRACE] && c == '{')) {
    /* A pattern-LEADING '['/'{' that is the quote's own opening delimiter is a
     * balanced nested delimiter (m{{...}}, qr{ {...} }, s{{...}}, m[[...]]),
     * NOT a subscript: there is no preceding variable for intuit_more to weigh,
     * and its subscript verdict here would be meaningless.  Fall through to the
     * Q/QQ content scanner, which consumes the opener while tracking the nesting
     * count so the matching inner close doesn't terminate the quote early.
     * Anything beyond the leading position is genuinely ambiguous again, so the
     * heuristic below applies as before. */
    bool leading = lexerstate_take_body_lead(state, c);
    if (leading) {
      /* leave the opener for the content scanner below */
    } else {
    int32_t open = c;
    int32_t close = (open == '[') ? ']' : '}';

    char buf[256];
    int n = 0;
    buf[n++] = (char)open;

    ADVANCE_C;   /* consume the opener... */
    MARK_END;    /* ...the emitted token is exactly that one char */

    /* Read ahead to gather the construct for the heuristic, stopping once we
     * have buffered the matching close (so intuit_more can find it) or we run
     * out of room / input.  These advances past MARK_END are pure lookahead;
     * the token stays one char wide.  Non-ASCII collapses to a placeholder
     * since the heuristic is ASCII-only by design (a best-effort gap). */
    while (n < (int)sizeof(buf) && c != 0 && !lexer->eof(lexer)) {
      buf[n++] = (c < 0x80) ? (char)c : (char)0x7f;
      if (c == close) break;
      ADVANCE_C;
    }

    if (!tsp_intuit_more(buf, n)) {
      /* If this class/quantifier opener is also the quote's own delimiter
       * (`m{ \d{2} }`, `m[ [abc] ]`), it opens a nested level — bump the count
       * so the matching inner close doesn't terminate the quote early. The
       * content scanner does this when it consumes `{`/`[` inline; we must do
       * the same here, since we only reach this branch when the opener leads a
       * scan (e.g. right after an escape) and the content scanner is bypassed. */
      int32_t qi = lexerstate_is_quote_opener(state, open);
      if (qi) lexerstate_saw_opener(state, qi);
      TOKEN(open == '[' ? TOKEN_REGEXP_OPEN_BRACKET : TOKEN_REGEXP_OPEN_BRACE);
    }
    /* subscript: let the grammar's immediate '[' / '{' win */
    return false;
    }
  }

  if (valid_symbols[TOKEN_POD]) {
    int column = lexer->get_column(lexer);
    if (column == 0 && c == '=') {
      DEBUG("POD started...\n", 0);

      /* Keep going until the linefeed after a line beginning `=cut` */
      static const char *cut_marker = "=cut";
      int stage = -1;

      while (!lexer->eof(lexer)) {
        if (c == '\r')
          ; /* ignore */
        else if (stage < 1 && c == '\n')
          stage = 0;
        else if (stage >= 0 && stage < 4 && c == cut_marker[stage])
          stage++;
        else if (stage == 4 && (c == ' ' || c == '\t'))
          stage = 5;
        else if (stage == 4 && c == '\n')
          stage = 6;
        else
          stage = -1;

        if (stage > 4) break;

        ADVANCE_C;
      }
      if (stage < 6)
        while (!lexer->eof(lexer)) {
          if (c == '\n') break;

          ADVANCE_C;
        }
      /* If we got this far then either we reached stage 6, or we're at EOF */
      TOKEN(TOKEN_POD);
    }
  }

  /* By now if we haven't recognised the token we shouldn't attempt to look
   * for the remaining ones when in an error condition */
  if (is_ERROR) return false;

  if (valid_symbols[TOKEN_HEREDOC_DELIM] || valid_symbols[TOKEN_COMMAND_HEREDOC_DELIM]) {
    // by default, indentation is false and interpolation is true
    bool should_indent = false;
    bool should_interpolate = true;

    TSPString delim = {0};
    tspstring_reset(&delim);
    if (!skipped_whitespace) {
      if (c == '~') {
        ADVANCE_C;
        should_indent = true;
      }
      if (c == '\\') {
        ADVANCE_C;
        should_interpolate = false;
      }
      if (isidfirst(c)) {
        while (isidcont(c)) {
          tspstring_push(&delim, c);
          ADVANCE_C;
        }
        lexerstate_add_heredoc(state, &delim, should_interpolate, should_indent);
        TOKEN(TOKEN_HEREDOC_DELIM);
      }
    }
    // if we picked up a ~ before, we may have to skip to hit the quote
    if (should_indent) {
      skip_whitespace(lexer);
      c = lexer->lookahead;
    }
    // if we picked up a \ before, we cannot allow even an immediate quote
    if (should_interpolate && (c == '\'' || c == '"' || c == '`')) {
      int delim_open = c;
      should_interpolate = c != '\'';
      ADVANCE_C;
      while (c != delim_open && !lexer->eof(lexer)) {
        // backslashes escape the quote char and nothing else, not even a
        // backslash
        if (c == '\\') {
          int to_add = c;
          ADVANCE_C;
          if (c == delim_open) {
            to_add = delim_open;
            ADVANCE_C;
          }
          tspstring_push(&delim, to_add);
        } else {
          tspstring_push(&delim, c);
          ADVANCE_C;
        }
      }
      // We stop the loop above either at the closing quote or at EOF.  Only
      // commit a heredoc if we actually found the closer; an EMPTY delimiter
      // (`<<''` / `<<""`) is legal — perl terminates its body at the next blank
      // line, which the body matcher already handles (an empty delim compares
      // equal to an empty line).  Keying off the closer rather than a non-empty
      // delim is also more correct: it won't mis-fire on EOF-without-closer.
      if (c == delim_open) {
        // gotta eat that delimiter
        ADVANCE_C;
        // gotta null terminate up in here
        lexerstate_add_heredoc(state, &delim, should_interpolate, should_indent);
        if (delim_open == '`') TOKEN(TOKEN_COMMAND_HEREDOC_DELIM);
        TOKEN(TOKEN_HEREDOC_DELIM);
      }
    }
  }

  // the idea here is in a 3 part quotelike, we return a skip instead of a begin
  if (valid_symbols[TOKEN_QUOTELIKE_MIDDLE_SKIP]) {
    if (!lexerstate_is_paired_delimiter(state)) TOKEN(TOKEN_QUOTELIKE_MIDDLE_SKIP);
  }

  if (valid_symbols[TOKEN_QUOTELIKE_BEGIN]) {
    int delim = c;
    if (skipped_whitespace && c == '#') return false;
    // we must do a two char lookahead to avoid turning the "=" in => into a
    // quote char
    MARK_END;
    ADVANCE_C;

    // gotta safely handle $hash{q}
    if (valid_symbols[TOKEN_BRACE_END_ZW] && delim == '}') {
      TOKEN(TOKEN_BRACE_END_ZW);
    }
    MARK_END;

    // In a paired multi-part quote (s{}{}, tr[][], ...), the replacement pair
    // opens a fresh quote whose delimiter may differ from the pattern's. The
    // pattern's quote is still on top of the stack (middle_close consumes its
    // closer but doesn't pop it), so its closer would wrongly stay active while
    // we scan the replacement. Now that the second pair is opening, retire it.
    // MIDDLE_SKIP being a valid symbol here means we're at the middle choice
    // point (between pattern and replacement) rather than an initial begin.
    if (valid_symbols[TOKEN_QUOTELIKE_MIDDLE_SKIP] && state->quotes.size) {
      lexerstate_pop_quote(state, state->quotes.size);
    }

    lexerstate_push_quote(state, delim);
    /* Pattern-leading bracket lookahead (see TSPQuote.body_leads_with_delim):
     * for a paired delimiter, peek past any leading whitespace -- if the body's
     * first real char is the delimiter again (m{{...}}, qr{ {...} }, m[[...]]),
     * it's a literal leading group, not a subscript.  This is pure lookahead:
     * MARK_END already covers just the opener, so these advances don't extend
     * the emitted token and the body is re-scanned from the opener's end. */
    if (close_for_open(delim)) {
      while (is_tsp_whitespace(c)) ADVANCE_C;
      if (c == delim) array_back(&state->quotes)->body_leads_with_delim = true;
    }
    TOKEN(TOKEN_QUOTELIKE_BEGIN);
  }

  if (c == '\\' &&
      // If we're inside a quotelike that is using the `\` as a delimiter then
      // this doesn't count
      !(valid_symbols[TOKEN_QUOTELIKE_END] && lexerstate_is_quote_closer(state, '\\'))) {
    // eat the reverse-solidus
    ADVANCE_C;
    // let's see what that reverse-solidus was hiding!
    // note that we may have fallen through here from a HEREDOC_MIDDLE, so we
    // need to accept the token explicitly after we've read our heart's content
    int esc_c = c;
    // if we escaped a whitespace, the space comes through, it just hides the
    // \ char
    if (!is_tsp_whitespace(c)) ADVANCE_C;

    if (valid_symbols[TOKEN_ESCAPED_DELIMITER]) {
      if (lexerstate_is_quote_opener(state, esc_c) || lexerstate_is_quote_closer(state, esc_c)) {
        MARK_END;
        TOKEN(TOKEN_ESCAPED_DELIMITER);
      }
    }

    if (valid_symbols[TOKEN_ESCAPE_SEQUENCE]) {
      // Inside any kind of string, \\ is always an escape sequence
      MARK_END;
      if (esc_c == '\\') TOKEN(TOKEN_ESCAPE_SEQUENCE);

      if (valid_symbols[TOKEN_Q_STRING_CONTENT]) {
        // Inside a q() string, only \\ is a valid escape sequence; all else is
        // literal
        TOKEN(TOKEN_Q_STRING_CONTENT);
      }

      switch (esc_c) {
        case 'x':
          if (c == '{')
            skip_braced(lexer);
          else
            skip_hexdigits(lexer, 2);
          break;

        case 'N':
          skip_braced(lexer);
          break;

        case 'o':
          /* TODO: contents should just be octal */
          skip_braced(lexer);
          break;

        case '0':
          skip_octdigits(lexer, 3);
          break;

        default:
          break;
      }

      TOKEN(TOKEN_ESCAPE_SEQUENCE);
    }
  }

  if (valid_symbols[TOKEN_Q_STRING_CONTENT] || valid_symbols[TOKEN_QQ_STRING_CONTENT]) {
    bool is_qq = valid_symbols[TOKEN_QQ_STRING_CONTENT];
    bool valid = false;

    while (c) {
      if (c == '\\') break;
      int32_t quote_index = lexerstate_is_quote_opener(state, c);
      if (quote_index)
        lexerstate_saw_opener(state, quote_index);
      else {
        quote_index = lexerstate_is_quote_closer(state, c);
        if (quote_index) {
          if (lexerstate_is_quote_closed(state, quote_index)) {
            break;
          }
          lexerstate_saw_closer(state, quote_index);
        } else if (is_qq && is_interpolation_escape(c))
          break;
      }

      valid = true;
      ADVANCE_C;
    }

    if (valid) {
      if (is_qq)
        TOKEN(TOKEN_QQ_STRING_CONTENT);
      else
        TOKEN(TOKEN_Q_STRING_CONTENT);
    }
  }

  if (valid_symbols[TOKEN_QUOTELIKE_MIDDLE_CLOSE]) {
    int32_t quote_index = lexerstate_is_quote_closer(state, c);
    if (quote_index && lexerstate_is_quote_closed(state, quote_index)) {
      ADVANCE_C;
      TOKEN(TOKEN_QUOTELIKE_MIDDLE_CLOSE);
    }
  }

  if (valid_symbols[TOKEN_QUOTELIKE_END]) {
    int32_t quote_index = lexerstate_is_quote_closer(state, c);
    if (quote_index) {
      if (valid_symbols[TOKEN_QUOTELIKE_END_ZW]) TOKEN(TOKEN_QUOTELIKE_END_ZW);

      ADVANCE_C;
      lexerstate_pop_quote(state, quote_index);
      TOKEN(TOKEN_QUOTELIKE_END);
    }
  }

  // we only check for the prototype token b/c it 100% overlaps w/ signatures
  if (c == '(' && (valid_symbols[TOKEN_PROTOTYPE] || valid_symbols[TOKEN_SIGNATURE_START])) {
    // you can't reallllllly know if you're getting a prototype or a signature, but we can
    // be optimistic as follows - all alnums are invalid in a prototype, so if there's ANY
    // valid identifier char, then we assume we got a sig; otherwise we make it to the end
    // of the nested parens + assume it was all a prototype
    ADVANCE_C;
    // Now, we begin lookahead
    lexer->mark_end(lexer);

    int count = 0;

    while (!lexer->eof(lexer)) {
      if (c == ')' && !count) {
        ADVANCE_C;
        break;
      } else if (c == ')')
        count--;
      else if (c == '(')
        count++;
      else if (is_tsp_id_continue(c))
        TOKEN(TOKEN_SIGNATURE_START);

      ADVANCE_C;
    }

    // we gotta accept all the stuff that was in the prototype now
    lexer->mark_end(lexer);
    TOKEN(TOKEN_PROTOTYPE);
  }

  // we hold on to the current char in case we need to do some fancy stuff w/ it in 2 char
  // lookaheads below
  int32_t c1 = c;
  if (c == '-' && valid_symbols[TOKEN_FILETEST]) {
    ADVANCE_C;
    if (tsp_strchr("rwxoRWXOezsfdlpSbctugkTBMAC", c)) {
      ADVANCE_C;
      if (!isidcont(c)) TOKEN(TOKEN_FILETEST);
    }
    return false;
  }
  // A lone non-identifier punctuation variable inside ${...} — ${@}, ${!},
  // ${%}, ${$}, ${"} … — names the punctuation variable $@/$!/…, NOT a
  // sigil/operator. The main lexer would otherwise pick the sigil/operator token
  // (and for @ / % that token swallows the closing brace, decapitating the rest
  // of the file). Recognize it here with the same '}'-lookahead the bareword
  // path uses: one var char immediately followed (modulo whitespace) by '}' is
  // the autoquoted name. Identifiers, digits and ^carets are handled by the
  // grammar's other varname alternatives; '{' would begin a block, '#' a comment.
  if (valid_symbols[TOKEN_BRACE_AUTOQUOTED] && !isidfirst(c) &&
      c > ' ' && c != '}' && c != '{' && c != '^' && c != '#' &&
      !(c >= '0' && c <= '9')) {
    ADVANCE_C;
    MARK_END;
    while (is_tsp_whitespace(c)) ADVANCE_C;
    if (c == '}') TOKEN(TOKEN_BRACE_AUTOQUOTED);
    return false;
  }
  if (isidfirst(c) &&
      (valid_symbols[TOKEN_FAT_COMMA_AUTOQUOTED] || valid_symbols[TOKEN_BRACE_AUTOQUOTED])) {
    // we zip until the end of the identifier; then we do a lookeahed to see if it's autoquoted
    do {
      ADVANCE_C;
    } while (c && isidcont(c));
    MARK_END;
    // TODO - carefully check if we got a 2 char quote such that we need to guard against
    // the comment char here - the quoting is space-sensitive so we'll have to track that
    // also

    // NOTE - TS is annoying about skipping chars after you've hit done
    // mark_end, so we have to do the regular advance so our token actually shows up
    // The contextual-keyword block jumps here (after reading its word + MARK_END)
    // to reuse this comment-skip + fat_comma_check instead of duplicating them.
    kw_autoquote:
    while (is_tsp_whitespace(c) || c == '#') {
      while (is_tsp_whitespace(c)) ADVANCE_C;
      // now we need to skip comments - we get in a funny way if we have a quotelike
      // operator followed by a comment as the quote char
      if (c == '#') {
        ADVANCE_C;
        while (lexer->get_column(lexer) && !lexer->eof(lexer)) ADVANCE_C;
      }
      if (lexer->eof(lexer)) return false;
      // TODO - in theory there could be POD here that we needa skip over (EYES ROLL)
    }
    // The keyword peek's fat comma detection gotos here when it finds a
    // statement keyword followed by '=' on a new line.  The peek has already
    // consumed the word + whitespace; lexer is at '='.  MARK_END was set by
    // the peek's caller (covers the word).  Same goto pattern as heredoc vs
    // diamond operator disambiguation.
    fat_comma_check:
    c1 = lexer->lookahead;
    ADVANCE_C;
    if (valid_symbols[TOKEN_FAT_COMMA_AUTOQUOTED]) {
      if (c1 == '=' && c == '>') TOKEN(TOKEN_FAT_COMMA_AUTOQUOTED);
    }
    if (valid_symbols[TOKEN_BRACE_AUTOQUOTED]) {
      if (c1 == '}') TOKEN(TOKEN_BRACE_AUTOQUOTED);
    }
  } else {
    /* it's ZW time! */
    MARK_END;
    /* let's get the next lookahead */
    ADVANCE_C;
    int32_t c2 = c;
    if (lexer->eof(lexer)) return false;
#define EQ2(s) (c1 == s[0] && c2 == s[1])

    /* NOTE - we need this to NOT be valid_symbol guarded, b/c we need this to
     * crash errant GLR branches, see gh#92 */
    if (EQ2("<<")) {
        // this branch is mainly for the GLR crashing; we enter here from the FILEGLOB vs
        // READLINE handling further up. that's why there's this ugly label here
heredoc_token_handling:
      DEBUG("checking if << is indeed a heredoc\n", 0);
      ADVANCE_C;
      MARK_END;
      if (c == '\\' || c == '~' || isidfirst(c)) {
        TOKEN(PERLY_HEREDOC);
      }
      skip_whitespace(lexer);
      c = lexer->lookahead;
      if (c == '\'' || c == '"' || c == '`') {
        TOKEN(PERLY_HEREDOC);
      }
      return false;
    }

    if (valid_symbols[TOKEN_BRACE_END_ZW]) {
      DEBUG("ZW-lookahead for brace-end in autoquote\n", 0);
      if (c1 == '}') TOKEN(TOKEN_BRACE_END_ZW);
    }
  }

  return false;
}

void *tree_sitter_perl_external_scanner_create() {
  LexerState *state = calloc(1, sizeof(LexerState));
  array_init(&state->quotes);
  tree_sitter_perl_external_scanner_deserialize(state, NULL, 0);
  return state;
}

void tree_sitter_perl_external_scanner_destroy(void *payload) {
  LexerState *state = payload;
  array_delete(&state->quotes);
  free(state);
}
