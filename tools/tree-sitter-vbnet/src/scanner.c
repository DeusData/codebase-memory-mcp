#include "tree_sitter/parser.h"
#include <string.h>
#include <wctype.h>

// External tokens for the two constructs that are not LR(1)-parseable with
// tree-sitter's newline-as-extra treatment:
//
// 1. QUERY_CLAUSE_CONTINUATION — a newline run that CONTINUES a LINQ query
//    (`From x In xs` ↵ `Where …`). At a clause boundary the parser cannot
//    know from the newline alone whether the query ends (newline = statement
//    terminator) or continues (newline = insignificant, next line starts with
//    a query keyword). The scanner looks past the newline at the next word
//    and only emits the token when it is a query-clause keyword — so the
//    terminator/continuation choice is made by the lexer, not the LR table.
//
// 2. XML_LITERAL — a whole VB XML literal (`<Tags><Tag/></Tags>`, including
//    `<%= expr %>` embedded expressions, comments, CDATA) consumed as one
//    opaque token. Only valid where a literal can begin an expression, so a
//    relational `<` (which always FOLLOWS an expression) can never collide.

enum TokenType {
  QUERY_CLAUSE_CONTINUATION,
  XML_LITERAL,
};

void *tree_sitter_vbnet_external_scanner_create(void) { return NULL; }
void tree_sitter_vbnet_external_scanner_destroy(void *payload) { (void)payload; }
unsigned tree_sitter_vbnet_external_scanner_serialize(void *payload, char *buffer) {
  (void)payload; (void)buffer;
  return 0; // stateless
}
void tree_sitter_vbnet_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {
  (void)payload; (void)buffer; (void)length;
}

static inline void advance(TSLexer *lexer) { lexer->advance(lexer, false); }
static inline void skip_ws(TSLexer *lexer) { lexer->advance(lexer, true); }

// Read one alphabetic word (advancing past it), lowercased into buf.
// Returns its length (0 if the next char is not a letter).
static int read_word(TSLexer *lexer, char *buf, int cap) {
  int n = 0;
  while (iswalpha((wint_t)lexer->lookahead)) {
    if (n < cap - 1) buf[n] = (char)towlower((wint_t)lexer->lookahead);
    n++;
    advance(lexer);
  }
  buf[n < cap - 1 ? n : cap - 1] = 0;
  return n;
}

static bool is_query_keyword(const char *w) {
  static const char *kws[] = {
    "where", "select", "order",    "group", "join", "let",  "skip",
    "take",  "distinct", "aggregate", "from",  "into", "on",   NULL,
  };
  for (int i = 0; kws[i]; i++)
    if (strcmp(w, kws[i]) == 0) return true;
  return false;
}

// [ \t\r]* '\n' [ \t\r\n]*  followed by a query-clause keyword.
// The token covers only the whitespace/newline run (mark_end before the
// keyword lookahead), so the clause keyword itself lexes normally after it.
static bool scan_query_continuation(TSLexer *lexer) {
  bool saw_newline = false;
  for (;;) {
    int32_t c = lexer->lookahead;
    if (c == ' ' || c == '\t' || c == '\r') {
      advance(lexer);
    } else if (c == '\n') {
      saw_newline = true;
      advance(lexer);
    } else {
      break;
    }
  }
  if (!saw_newline) return false;
  lexer->mark_end(lexer);

  char w[12];
  int n = read_word(lexer, w, sizeof w);
  if (n == 0 || n >= (int)(sizeof w)) return false;
  if (!is_query_keyword(w)) return false;

  // `Select Case` opens a statement, never a select clause.
  if (strcmp(w, "select") == 0) {
    while (lexer->lookahead == ' ' || lexer->lookahead == '\t') advance(lexer);
    char w2[8];
    int m = read_word(lexer, w2, sizeof w2);
    if (m > 0 && m < (int)(sizeof w2) && strcmp(w2, "case") == 0) return false;
  }

  lexer->result_symbol = QUERY_CLAUSE_CONTINUATION;
  return true;
}

// Consume a VB string inside an embedded `<%= … %>` region ("" escapes).
static void consume_vb_string(TSLexer *lexer) {
  advance(lexer); // opening quote
  for (;;) {
    if (lexer->lookahead == 0) return;
    if (lexer->lookahead == '"') {
      advance(lexer);
      if (lexer->lookahead != '"') return; // closing (not an escaped "")
      // escaped quote: fall through, keep consuming
    }
    advance(lexer);
  }
}

// Consume `<%= … %>` starting at the '%' (the '<' is already consumed).
// Embedded expressions NEST (`<%= From t In ts Select <Tag><%= t.Name %></Tag> %>`,
// the staxrip WriteTagfile shape), so track <% / %> depth.
static bool consume_embedded_expression(TSLexer *lexer) {
  advance(lexer); // '%'
  int depth = 1;
  for (;;) {
    int32_t c = lexer->lookahead;
    if (c == 0) return false;
    if (c == '"') { consume_vb_string(lexer); continue; }
    if (c == '<') {
      advance(lexer);
      if (lexer->lookahead == '%') { depth++; advance(lexer); }
      continue;
    }
    if (c == '%') {
      advance(lexer);
      if (lexer->lookahead == '>') {
        advance(lexer);
        if (--depth == 0) return true;
      }
      continue;
    }
    advance(lexer);
  }
}

// Consume `<!-- … -->` / `<![CDATA[ … ]]>` / `<!DOCTYPE …>` starting at '!'.
static bool consume_bang_construct(TSLexer *lexer) {
  advance(lexer); // '!'
  if (lexer->lookahead == '-') {
    // comment: to -->
    int dashes = 0;
    for (;;) {
      int32_t c = lexer->lookahead;
      if (c == 0) return false;
      if (c == '-') { dashes++; advance(lexer); continue; }
      if (c == '>' && dashes >= 2) { advance(lexer); return true; }
      dashes = 0;
      advance(lexer);
    }
  }
  if (lexer->lookahead == '[') {
    // CDATA: to ]]>
    int brackets = 0;
    for (;;) {
      int32_t c = lexer->lookahead;
      if (c == 0) return false;
      if (c == ']') { brackets++; advance(lexer); continue; }
      if (c == '>' && brackets >= 2) { advance(lexer); return true; }
      brackets = 0;
      advance(lexer);
    }
  }
  // DOCTYPE-ish: to bare >
  for (;;) {
    int32_t c = lexer->lookahead;
    if (c == 0) return false;
    if (c == '>') { advance(lexer); return true; }
    advance(lexer);
  }
}

// Consume `<? … ?>` starting at the '?'.
static bool consume_processing_instruction(TSLexer *lexer) {
  advance(lexer); // '?'
  for (;;) {
    int32_t c = lexer->lookahead;
    if (c == 0) return false;
    if (c == '?') {
      advance(lexer);
      if (lexer->lookahead == '>') { advance(lexer); return true; }
      continue;
    }
    advance(lexer);
  }
}

static bool is_name_start(int32_t c) {
  return iswalpha((wint_t)c) || c == '_';
}

static bool scan_xml_literal(TSLexer *lexer) {
  // Only same-line blanks are skipped: a leading newline must stay unconsumed
  // so it can serve as a statement/block terminator (a successful scan would
  // otherwise swallow it as trivia). XML on a continuation line still works:
  // the newline is consumed as an extra first, then the scanner re-runs.
  while (lexer->lookahead == ' ' || lexer->lookahead == '\t') skip_ws(lexer);
  if (lexer->lookahead != '<') return false;
  advance(lexer);
  // Only element-start markup opens an XML literal here; comments/PIs as the
  // outermost construct (`Dim d = <?xml …`) are rare enough to leave alone,
  // and a bare `<` (comparison) never begins an expression.
  if (!is_name_start(lexer->lookahead)) return false;

  int depth = 0;      // open (unclosed) elements
  bool in_tag = true; // inside <...> of an element tag
  bool closing = false;
  int32_t quote = 0;

  for (;;) {
    int32_t c = lexer->lookahead;
    if (c == 0) return false; // unterminated: let the normal parser error

    if (in_tag) {
      if (quote) {
        if (c == quote) quote = 0;
        advance(lexer);
        continue;
      }
      if (c == '"' || c == '\'') { quote = c; advance(lexer); continue; }
      if (c == '<') {
        advance(lexer);
        if (lexer->lookahead == '%') {
          // attribute value embedded expression: attr=<%= x %>
          if (!consume_embedded_expression(lexer)) return false;
          continue;
        }
        continue; // stray < in a tag: malformed, keep consuming
      }
      if (c == '/') {
        advance(lexer);
        if (lexer->lookahead == '>') {
          // self-closing tag
          advance(lexer);
          in_tag = false;
          if (depth == 0) break; // single self-closed root: done
          continue;
        }
        continue;
      }
      if (c == '>') {
        advance(lexer);
        if (closing) {
          depth--;
          if (depth <= 0) break; // root element closed: done
        } else {
          depth++;
        }
        in_tag = false;
        continue;
      }
      advance(lexer);
      continue;
    }

    // text content between tags
    if (c == '<') {
      advance(lexer);
      int32_t d = lexer->lookahead;
      if (d == '/') { advance(lexer); closing = true; in_tag = true; continue; }
      if (d == '%') {
        if (!consume_embedded_expression(lexer)) return false;
        continue;
      }
      if (d == '!') {
        if (!consume_bang_construct(lexer)) return false;
        continue;
      }
      if (d == '?') {
        if (!consume_processing_instruction(lexer)) return false;
        continue;
      }
      closing = false;
      in_tag = true; // opening tag
      continue;
    }
    advance(lexer);
  }

  lexer->mark_end(lexer);
  lexer->result_symbol = XML_LITERAL;
  return true;
}

bool tree_sitter_vbnet_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
  (void)payload;
  int32_t c = lexer->lookahead;
  bool ws = (c == ' ' || c == '\t' || c == '\n' || c == '\r');

  // A scan attempt advances the lexer, so exactly ONE attempt runs per
  // invocation — never chain a second attempt after a failed first (its
  // reads would start mid-stream). The leading character picks the branch;
  // a false return discards the attempt entirely.
  if (valid_symbols[QUERY_CLAUSE_CONTINUATION] && ws) {
    return scan_query_continuation(lexer);
  }
  if (valid_symbols[XML_LITERAL] && (c == '<' || ws)) {
    return scan_xml_literal(lexer);
  }
  return false;
}
