#include "tree_sitter/parser.h"
#include <string.h>
#include <wctype.h>
#include <assert.h>

#define IS_SPACE_TABS(char) ((char) == ' ' || (char) == '\t')
#define HEREDOC_MARKER_LEN 32

typedef struct {
  // The EOF markers (but they can be whatever so lex that correctly)
  char separator;
  bool ignore_comments;
  uint8_t marker_len;
  char heredoc_marker[HEREDOC_MARKER_LEN];
} Scanner;

typedef struct {
  char * mandat;               /// Mandatory part of the command
  char * opt;                  /// Optional part of the command
  bool ignore_comments_after;  /// Ignore comments until EOL
} keyword;

#include "keywords.h"

enum TokenType {
  NO,
  INV,
  CMD_SEPARATOR,
  LINE_CONTINUATION,
  SCRIPT_HEREDOC_MARKER,
  LET_HEREDOC_MARKER,
  HEREDOC_END,
  SEP_FIRST,
  SEP,
  SCOPE_DICT,
  SCOPE,
  STRING,
  COMMENT,
  LINE_CONTINUATION_COMMENT,
  BANG_FILTER,
  // Many many many many keywords that are impossible to lex otherwise, see
  // src/keywords.h
  KEYWORDS_BASE
};

// TODO(clason): remove when added to tree-sitter's WASM stdlib
static inline bool _iswpunct(wint_t wch) {
  return (wch >= 33 && wch <= 47) ||
         (wch >= 58 && wch <= 64) ||
         (wch >= 91 && wch <= 96) ||
         (wch >= 123 && wch <= 126);
}

void *tree_sitter_vim_external_scanner_create() {
  Scanner *s = (Scanner *)malloc(sizeof(Scanner));
  s->separator = '\0';
  s->marker_len = 0;
  s->ignore_comments = false;
  memset(s->heredoc_marker, '\0', HEREDOC_MARKER_LEN);

  return (void *)s;
}

void tree_sitter_vim_external_scanner_destroy(void *payload) {
  Scanner *s = (Scanner *)payload;

  free(s);
}

/// Serialize / deserialize
//
// Memory layout is :
//
// [ marker_len, marker ... (marker_len size) ]

#define SC_IGNORE_COMMENTS 0
#define SC_PAIRED_SEP 1
#define SC_MARK_LEN 2
#define SC_MARK 3


unsigned int tree_sitter_vim_external_scanner_serialize(void *payload,
                                                        char *buffer) {
  Scanner *s = (Scanner *)payload;
  buffer[SC_PAIRED_SEP] = s->separator;
  buffer[SC_IGNORE_COMMENTS] = s->ignore_comments;
  buffer[SC_MARK_LEN] = s->marker_len;

  strncpy(buffer + SC_MARK, s->heredoc_marker, s->marker_len);

  return s->marker_len + SC_MARK;
}

void tree_sitter_vim_external_scanner_deserialize(void *payload,
                                                  const char *buffer,
                                                  unsigned length) {
  if (length == 0) {
    return;
  }

  Scanner *s = (Scanner *)payload;
  s->ignore_comments = buffer[SC_IGNORE_COMMENTS];
  s->separator = buffer[SC_PAIRED_SEP];
  s->marker_len = buffer[SC_MARK_LEN];

  // Sanity check, just to be sure
  assert(s->marker_len + SC_MARK == length);
  assert(s->marker_len < HEREDOC_MARKER_LEN);

  if (s->marker_len > 0) {
    strncpy(s->heredoc_marker, buffer + SC_MARK, s->marker_len);
  }
}

static void advance(TSLexer *lexer, bool skip) { lexer->advance(lexer, skip); }

static void skip_space_tabs(TSLexer *lexer) {
  while (IS_SPACE_TABS(lexer->lookahead)) {
    advance(lexer, true);
  }
}

static bool check_prefix(TSLexer *lexer, char *prefix, unsigned int prefix_len,
                  enum TokenType token) {
  for (unsigned int i = 0; i < prefix_len; i++) {
    if (lexer->lookahead == prefix[i]) {
      advance(lexer, false);
    } else {
      return false;
    }
  }

  lexer->result_symbol = token;
  return true;
}

static bool try_lex_heredoc_marker(Scanner *scanner, TSLexer *lexer)
{
  char marker[UINT8_MAX] = { '\0' };
  uint16_t marker_len = 0;

  if (iswlower(lexer->lookahead)) {
    return false;
  }

  // We should be at the start of the script marker
  // Note that :let-heredocs do not allow for spaces in the endmarker
  while ((!IS_SPACE_TABS(lexer->lookahead)) && lexer->lookahead && lexer->lookahead != '\n' && lexer->lookahead != '\r' && marker_len < HEREDOC_MARKER_LEN) {
    marker[marker_len] = lexer->lookahead;
    marker_len++;
    advance(lexer, false);
  }

  if (marker_len == HEREDOC_MARKER_LEN || marker_len == 0) {
    return false;
  }

  strncpy(scanner->heredoc_marker, marker, marker_len);
  scanner->marker_len = marker_len;
  memset(scanner->heredoc_marker + marker_len, '\0', HEREDOC_MARKER_LEN - marker_len);

  return true;
}

static bool is_valid_string_delim(char c) {
  return c == '\'' || c == '"';
}

static bool lex_literal_string(TSLexer *lexer) {
  while (true) {
    if(lexer->lookahead == '\'') {
      // Maybe end of string, but not sure, it could be double quote
      advance(lexer, false);
      if (lexer->lookahead == '\'') {
        advance(lexer, false);
      } else {
        lexer->result_symbol = STRING;
        lexer->mark_end(lexer);
        return true;
      }
    } else if (lexer->lookahead == '\n' || lexer->lookahead == '\r') {
      // Not sure at this point, look after that if there's not a \\ character
      lexer->mark_end(lexer);
      if (lexer->lookahead == '\r') {
          advance(lexer, true);
          if (lexer->lookahead == '\n') {
              advance(lexer, true);
          }
      } else {
          advance(lexer, true);
      }
      skip_space_tabs(lexer);
      if (lexer->lookahead != '\\') {
        // Was an invalid end...
        return false;
      }
    } else if (lexer->lookahead == '\0') {
      return false;
    } else {
      advance(lexer, false);
    }
  }
}

// FIXME: this does not support comments like `" Hello "mister" how are you ?`
static bool lex_escapable_string(TSLexer *lexer) {
  while (true) {
    if (lexer->lookahead == '\\') {
      advance(lexer, false);
      advance(lexer, false);
    } else if (lexer->lookahead == '"') {
      advance(lexer, false);
      lexer->mark_end(lexer);
      lexer->result_symbol = STRING;
      return true;
    } else if (lexer->lookahead == '\n' || lexer->lookahead == '\r') {
      // Not sure at this point, look after that if there's not a \\ character
      lexer->mark_end(lexer);
      if (lexer->lookahead == '\r') {
          advance(lexer, false);
          if (lexer->lookahead == '\n') {
              advance(lexer, false);
          }
      } else {
          advance(lexer, false);
      }
      skip_space_tabs(lexer);
      if (lexer->lookahead != '\\') {
        // Was a comment...
        lexer->mark_end(lexer);
        lexer->result_symbol = COMMENT;
        return true;
      }
    } else if (lexer->lookahead == '\0') {
      return false;
    } else {
      advance(lexer, false);
    }
  }
}

static bool lex_string(TSLexer *lexer) {
  char string_delim;

  if (!is_valid_string_delim(lexer->lookahead)) {
    return false;
  }

  string_delim = lexer->lookahead;
  advance(lexer, false);

  switch (string_delim) {
    case '"':
      return lex_escapable_string(lexer);
    case '\'':
      return lex_literal_string(lexer);
    default:
      assert(0);
  }
}

static bool try_lex_keyword(char *possible, keyword keyword) {
  if (strlen(possible) > strlen(keyword.mandat) + strlen(keyword.opt)) {
    return false;
  }

  // Try lexing mandatory part
  size_t i;
  for (i = 0; keyword.mandat[i] && possible[i]; i++) {
    if (possible[i] != keyword.mandat[i]) {
      return false;
    }
  }

  // The mandatory part is not complete
  if (keyword.mandat[i] && !possible[i])
    return false;

  size_t mandat_len = i;
  // Now try lexing optional part
  for (size_t i = 0; keyword.opt[i] && possible[mandat_len + i]; i++) {
    if (possible[mandat_len + i] != keyword.opt[i]) {
      return false;
    }
  }

  return true;
}

static bool scope_correct(TSLexer *lexer) {
  const char *SCOPES = "lbstvwg<";
  for (size_t i = 0; SCOPES[i]; i++) {
    if (lexer->lookahead == SCOPES[i]) {
      return true;
    }
  }

  return false;
}

static bool lex_scope(TSLexer *lexer) {
  if (!scope_correct(lexer)) {
    return false;
  }

  if (lexer->lookahead == '<') {
    advance(lexer, false);
    const char sid[4 + 1] = "SID>";
    for (size_t i = 0; sid[i] && lexer->lookahead; i++) {
      if (lexer->lookahead != sid[i]) {
        return false;
      }
      advance(lexer, false);
    }
    lexer->result_symbol = SCOPE;
    return true;
  } else {
    advance(lexer, false);

    if (lexer->lookahead != ':') {
      return false;
    }
    advance(lexer, false);

    if (iswalnum(lexer->lookahead) || lexer->lookahead == '{' || lexer->lookahead == '_') {
      lexer->result_symbol = SCOPE;
    } else {
      lexer->result_symbol = SCOPE_DICT;
    }

    return true;
  }
}

bool tree_sitter_vim_external_scanner_scan(void *payload, TSLexer *lexer,
                                           const bool *valid_symbols) {
  Scanner *s = (Scanner *)payload;
  assert(valid_symbols[LINE_CONTINUATION]);

  skip_space_tabs(lexer);
  if (!lexer->lookahead) {
    return false;
  }

  // Not sure about the punctuation here...
  if (valid_symbols[SEP_FIRST] && _iswpunct(lexer->lookahead)) {
    s->separator = lexer->lookahead;
    advance(lexer, false);
    s->ignore_comments = true;
    lexer->result_symbol = SEP_FIRST;
    return true;
  } else if (valid_symbols[SEP] && s->separator == lexer->lookahead) {
    // No need to check for s->separator == 0 above because we know
    // lexer->lookahead is != 0
    advance(lexer, false);
    s->ignore_comments = false;
    lexer->result_symbol = SEP;
    return true;
  }

  if (valid_symbols[BANG_FILTER] && lexer->lookahead == '!') {
    advance(lexer, false);
    s->ignore_comments = true;
    lexer->result_symbol = BANG_FILTER;
    return true;
  }

  // options can be inverted by prepending a 'no' or 'inv'
  if (valid_symbols[NO] && lexer->lookahead == 'n') {
    return check_prefix(lexer, "no", 2, NO);
  } else if (valid_symbols[INV] && lexer->lookahead == 'i') {
    return check_prefix(lexer, "inv", 3, INV);
  }

  // cmd separator and | this is not trivial at all because of how line
  // continuations are handled after encoutering an EOL :
  //  - Next line starts by a `\\` ?
  //    - Yes : is next character `/` or `?` ?
  //      - Yes : Next line is another command (preceded by a range)
  //      - No : This is a line continuation
  //    - No : Next line is another command.
  //
  // This ambiguity forces us to use the mark_end function and lookahead more
  // than just past the final newline and indentationg character.
  if (lexer->lookahead == '\n' || lexer->lookahead == '\r') {
      if (lexer->lookahead == '\r') {
          advance(lexer, false);
          if (lexer->lookahead == '\n') {
              advance(lexer, false);
          }
      } else {
          advance(lexer, false);
      }
    lexer->mark_end(lexer);
    skip_space_tabs(lexer);

    if (lexer->lookahead == '\\') {
      // You think this is a line continuation ? It might not
      advance(lexer, false);

      if (lexer->lookahead == '/'
          || lexer->lookahead == '?'
          || lexer->lookahead == '&') {
        // Actually this might be a range before a command
        if (valid_symbols[CMD_SEPARATOR]) {
          lexer->result_symbol = CMD_SEPARATOR;
          s->ignore_comments = false;
          return true;
        } else {
          return false;
        }
      }

      lexer->mark_end(lexer);
      lexer->result_symbol = LINE_CONTINUATION;
      return true;
    } else if (s->marker_len == 0 && check_prefix(lexer, "\"\\ ", 3, LINE_CONTINUATION_COMMENT)) {
      while (lexer->lookahead != '\0' && lexer->lookahead != '\n' && lexer->lookahead != '\r') {
        advance(lexer, false);
      }
      lexer->mark_end(lexer);
      return true;
    } else if (valid_symbols[CMD_SEPARATOR]) {
      lexer->result_symbol = CMD_SEPARATOR;
      s->ignore_comments = false;
      return true;
    } else {
      return false;
    }
  }

  if (valid_symbols[CMD_SEPARATOR] && lexer->lookahead == '|') {
    advance(lexer, false);
    if (lexer->lookahead == '|') {
      // This is an or expression
      return false;
    }
    lexer->result_symbol = CMD_SEPARATOR;
    return true;
  }

  if (scope_correct(lexer) && (valid_symbols[SCOPE_DICT] || valid_symbols[SCOPE])) {
    if (lex_scope(lexer)) {
      return true;
    } else {
      return false;
    }
  }

  if (valid_symbols[SCRIPT_HEREDOC_MARKER]) {
    lexer->result_symbol = SCRIPT_HEREDOC_MARKER;
    return try_lex_heredoc_marker(s, lexer);
  }
  if (valid_symbols[LET_HEREDOC_MARKER]) {
    lexer->result_symbol = LET_HEREDOC_MARKER;
    return try_lex_heredoc_marker(s, lexer);
  }
  if (valid_symbols[HEREDOC_END]) {
    uint8_t marker_len = s->marker_len != 0 ? s->marker_len : 1;
    char *marker = s->marker_len != 0 ? s->heredoc_marker : ".";

    if (!check_prefix(lexer, marker, marker_len, HEREDOC_END)) {
      return false;
    }

    // Ensure there aren't any remaining characters in the line
    if (lexer->lookahead != '\0' && lexer->lookahead != '\n' && lexer->lookahead != '\r') {
      return false;
    }

    // Found the end marker
    s->marker_len = 0;
    memset(s->heredoc_marker, '\0', HEREDOC_MARKER_LEN);

    return true;
  }

  if (valid_symbols[COMMENT] && !valid_symbols[STRING]
      && lexer->lookahead == '"' && !s->ignore_comments) {
    // This con only be a comment
    do {
      advance(lexer, false);
    } while (lexer->lookahead != '\n' && lexer->lookahead != '\r' && lexer->lookahead != '\0');

    lexer->result_symbol = COMMENT;
    return true;
  } else if (valid_symbols[STRING]) {
    return lex_string(lexer);
  }

  // Other keywords
  if (iswlower(lexer->lookahead)) {
#define KEYWORD_SIZE 30
    char keyword[KEYWORD_SIZE + 1] = { lexer->lookahead, 0 };

    advance(lexer, false);

    size_t i = 1;
    for (; i < KEYWORD_SIZE && iswalpha(lexer->lookahead); i++) {
      keyword[i] = lexer->lookahead;
      advance(lexer, false);
    }

    if (i == KEYWORD_SIZE) {
      return false;
    }

    keyword[i] = '\0';

    // Now really try to find the keyword
    for (kwid t = FUNCTION; t < UNKNOWN_COMMAND; t++) {
      if (valid_symbols[t + KEYWORDS_BASE] && try_lex_keyword(keyword, keywords[t])) {
        lexer->result_symbol = t + KEYWORDS_BASE;
        s->ignore_comments = keywords[t].ignore_comments_after;
        return true;
      }
    }

    // This is quite possibly a keyword, we just don't know which one yet
    if (valid_symbols[UNKNOWN_COMMAND + KEYWORDS_BASE]) {
      lexer->result_symbol = UNKNOWN_COMMAND + KEYWORDS_BASE;
      return true;
    }
#undef KEYWORD_SIZE
  }

  return false;
}

// vim: tabstop=2