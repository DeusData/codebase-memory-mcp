#include "tree_sitter/array.h"
#include "tree_sitter/parser.h"

#include <string.h>
#include <wctype.h>

// Mostly a copy paste of tree-sitter-javascript/src/scanner.c

enum TokenType {
  AUTOMATIC_SEMICOLON,
  IMPORT_LIST_DELIMITER,
  SAFE_NAV,
  MULTILINE_COMMENT,
  STRING_START,
  STRING_END,
  STRING_CONTENT,
  PRIMARY_CONSTRUCTOR_KEYWORD,
  IMPORT_DOT,
};

/* Pretty much all of this code is taken from the Julia tree-sitter
   parser.

   Julia has similar problems with multiline comments that can be nested,
   line comments, as well as line and multiline strings.

   The most heavily edited section is `scan_string_content`,
   particularly with respect to interpolation.
 */

// Block comments are easy to parse, but strings require extra-attention.

// The main problems that arise when parsing strings are:
// 1. Triple quoted strings allow single quotes inside. e.g. """ "foo" """.
// 2. Non-standard string literals don't allow interpolations or escape
//    sequences, but you can always write \" and \`.

// To efficiently store a delimiter, we take advantage of the fact that:
// (int)'"' == 34 && (34 & 1) == 0
// i.e. " has an even numeric representation, so we can store a triple
// quoted delimiter as (delimiter + 1).

#define DELIMITER_LENGTH 3

typedef char Delimiter;

// We use a stack to keep track of the string delimiters.
typedef Array(Delimiter) Stack;

static inline void stack_push(Stack *stack, char chr, bool triple) {
  if (stack->size >= TREE_SITTER_SERIALIZATION_BUFFER_SIZE) abort();
  array_push(stack, (Delimiter)(triple ? (chr + 1) : chr));
}

static inline Delimiter stack_pop(Stack *stack) {
  if (stack->size == 0) abort();
  return array_pop(stack);
}

static inline void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

static inline void advance(TSLexer *lexer) { lexer->advance(lexer, false); }

// Scanner functions

static bool scan_string_start(TSLexer *lexer, Stack *stack) {
  if (lexer->lookahead != '"') return false;
  advance(lexer);
  lexer->mark_end(lexer);
  for (unsigned count = 1; count < DELIMITER_LENGTH; ++count) {
    if (lexer->lookahead != '"') {
      // It's not a triple quoted delimiter.
      stack_push(stack, '"', false);
      return true;
    }
    advance(lexer);
  }
  lexer->mark_end(lexer);
  stack_push(stack, '"', true);
  return true;
}

static bool scan_string_content(TSLexer *lexer, Stack *stack) {
  if (stack->size == 0) return false;  // Stack is empty. We're not in a string.
  Delimiter end_char = stack->contents[stack->size - 1];  // peek
  bool is_triple = false;
  bool has_content = false;
  if (end_char & 1) {
    is_triple = true;
    end_char -= 1;
  }
  while (lexer->lookahead) {
    if (lexer->lookahead == '$') {
      // if we did not just start reading stuff, then we should stop
      // lexing right here, so we can offer the opportunity to lex a
      // interpolated identifier
      if (has_content) {
        lexer->result_symbol = STRING_CONTENT;
        return has_content;
      }
      // otherwise, if this is the start, determine if it is an
      // interpolated identifier.
      // otherwise, it's just string content, so continue
      advance(lexer);
      if (iswalpha(lexer->lookahead) || lexer->lookahead == '{') {
        // this must be a string interpolation, let's
        // fail so we parse it as such
        return false;
      }
      lexer->result_symbol = STRING_CONTENT;
      lexer->mark_end(lexer);
      return true;
    }
    if (lexer->lookahead == '\\') {
      // if we see a \, then this might possibly escape a dollar sign
      // in which case, we should not defer to the interpolation
      advance(lexer);
      // this dollar sign is escaped, so it must be content.
      // we consume it here so we don't enter the dollar sign case above,
      // which leaves the possibility that it is an interpolation 
      if (lexer->lookahead == '$') {
        advance(lexer);
        // however this leaves an edgecase where an escaped dollar sign could
        // appear at the end of a string (e.g "aa\$") which isn't handled
        // correctly; if we were at the end of the string, terminate properly
        if (lexer->lookahead == end_char) {
          stack_pop(stack);
          advance(lexer);
          lexer->mark_end(lexer);
          lexer->result_symbol = STRING_END;
          return true;
        }
      } else if (is_triple && lexer->lookahead == end_char) {
        // In triple-quoted strings, `\` is NOT an escape character. So `\"` is
        // also literal backslash + quote, and the `"` might be the start of
        // the closing `"""`. Don't advance past it (at the end of the while
        // loop). Let the next iteration handle it.
        has_content = true;
        continue;
      }
    } else if (lexer->lookahead == end_char) {
      if (is_triple) {
        lexer->mark_end(lexer);
        for (unsigned count = 1; count < DELIMITER_LENGTH; ++count) {
          advance(lexer);
          if (lexer->lookahead != end_char) {
            lexer->mark_end(lexer);
            lexer->result_symbol = STRING_CONTENT;
            return true;
          }
        }

        /* This is so if we lex something like
           """foo"""
              ^
           where we are at the `f`, we should quit after
           reading `foo`, and ascribe it to STRING_CONTENT.

           Then, we restart and try to read the end.
           This is to prevent `foo` from being absorbed into
           the STRING_END token.
         */
        if (has_content && lexer->lookahead == end_char) {
          lexer->result_symbol = STRING_CONTENT;
          return true;
        }

        /* Since the string internals are all hidden in the syntax
           tree anyways, there's no point in going to the effort of
           specifically separating the string end from string contents.
           If we see a bunch of quotes in a row, then we just go until
           they stop appearing, then stop lexing and call it the
           string's end.
         */
        lexer->result_symbol = STRING_END;
        lexer->mark_end(lexer);
        while (lexer->lookahead == end_char) {
          advance(lexer);
          lexer->mark_end(lexer);
        }
        stack_pop(stack);
        return true;
      }
      if (has_content) {
        lexer->mark_end(lexer);
        lexer->result_symbol = STRING_CONTENT;
        return true;
      }
      stack_pop(stack);
      advance(lexer);
      lexer->mark_end(lexer);
      lexer->result_symbol = STRING_END;
      return true;
    }
    advance(lexer);
    has_content = true;
  }
  return false;
}

static bool scan_multiline_comment(TSLexer *lexer) {
  if (lexer->lookahead != '/') return false;
  advance(lexer);
  if (lexer->lookahead != '*') return false;
  advance(lexer);

  bool after_star = false;
  unsigned nesting_depth = 1;
  for (;;) {
    switch (lexer->lookahead) {
      case '*':
        advance(lexer);
        after_star = true;
        break;
      case '/':
        advance(lexer);
        if (after_star) {
          after_star = false;
          nesting_depth -= 1;
          if (nesting_depth == 0) {
            lexer->result_symbol = MULTILINE_COMMENT;
            lexer->mark_end(lexer);
            return true;
          }
        } else {
          after_star = false;
          if (lexer->lookahead == '*') {
            nesting_depth += 1;
            advance(lexer);
          }
        }
        break;
      case '\0':
        // Accept unterminated block comments at EOF rather than rejecting them.
        // This matches JetBrains PSI behavior which recognizes unclosed /* as a
        // BLOCK_COMMENT token (plus an error element). Without this, the scanner
        // returns false and tree-sitter tries to parse the comment delimiters
        // as operators/expressions.
        if (lexer->eof(lexer)) {
          lexer->result_symbol = MULTILINE_COMMENT;
          lexer->mark_end(lexer);
          return true;
        }
        return false;
      default:
        advance(lexer);
        after_star = false;
        break;
    }
  }
}

static bool scan_whitespace_and_comments(TSLexer *lexer) {
  while (iswspace(lexer->lookahead)) skip(lexer);
  return true;
}

// Test for any identifier character other than the first character.
// This is meant to match the regexp [\p{L}_\p{Nd}]
// as found in '_alpha_identifier' (see grammar.js).
static bool is_word_char(int32_t c) {
  return (iswalnum(c) || c == '_');
}

// Scan for [the end of] a nonempty alphanumeric identifier or
// alphanumeric keyword (including '_').
static bool scan_for_word(TSLexer *lexer, const char* word, unsigned len) {
    skip(lexer);
    for (unsigned i = 0; i < len; ++i) {
      if (lexer->lookahead != word[i]) return false;
      skip(lexer);
    }
    // check that the identifier stops here
    if (is_word_char(lexer->lookahead)) return false;
    return true;
}

// Check if a sequence of characters matches the given word and is followed
// by a non-word character. Uses skip() so characters are not included in
// the current token.
static bool check_word(TSLexer *lexer, const char *word, unsigned len) {
  for (unsigned i = 0; i < len; i++) {
    if (lexer->lookahead != word[i]) return false;
    skip(lexer);
  }
  return !is_word_char(lexer->lookahead);
}

// Check if the current position has a visibility modifier (public, private,
// protected, internal) followed by horizontal whitespace and "constructor".
// Uses skip() — safe to call speculatively since no token boundary is changed.
static bool check_modifier_then_constructor(TSLexer *lexer) {
  // Buffer the first word to identify the modifier
  char word[20];
  unsigned len = 0;
  while (is_word_char(lexer->lookahead) && len < 19) {
    word[len++] = (char)lexer->lookahead;
    skip(lexer);
  }
  word[len] = '\0';

  if (strcmp(word, "public") != 0 && strcmp(word, "private") != 0 &&
      strcmp(word, "protected") != 0 && strcmp(word, "internal") != 0) {
    return false;
  }

  // Skip horizontal whitespace (not newlines)
  while (lexer->lookahead == ' ' || lexer->lookahead == '\t') skip(lexer);

  return check_word(lexer, "constructor", 11);
}

// Look ahead past one or more annotations (e.g. @Bar, @com.example.Bar,
// @Bar(x=1)) and optional visibility modifier, then check for 'constructor'.
// All characters are consumed with skip() so nothing affects token boundaries.
static bool check_annotation_then_constructor(TSLexer *lexer) {
  // Skip one or more '@annotation' sequences
  while (lexer->lookahead == '@') {
    skip(lexer); // skip '@'
    if (!is_word_char(lexer->lookahead)) return false;
    // Read annotation name, including dot-separated qualifiers
    // (e.g. com.example.Inject)
    while (is_word_char(lexer->lookahead)) skip(lexer);
    while (lexer->lookahead == '.') {
      skip(lexer); // skip '.'
      if (!is_word_char(lexer->lookahead)) break;
      while (is_word_char(lexer->lookahead)) skip(lexer);
    }
    // Skip optional '(...)' argument list (handle nested parens and strings)
    if (lexer->lookahead == '(') {
      unsigned depth = 1;
      skip(lexer);
      while (depth > 0 && lexer->lookahead != '\0' && !lexer->eof(lexer)) {
        if (lexer->lookahead == '"') {
          // Skip over string literal to avoid miscounting parens inside strings
          skip(lexer);
          while (lexer->lookahead != '"' && lexer->lookahead != '\0' && !lexer->eof(lexer)) {
            if (lexer->lookahead == '\\') skip(lexer); // skip escaped char
            skip(lexer);
          }
          if (lexer->lookahead == '"') skip(lexer); // skip closing quote
        } else {
          if (lexer->lookahead == '(') depth++;
          else if (lexer->lookahead == ')') depth--;
          skip(lexer);
        }
      }
    }
    // Skip whitespace and newlines between annotations or before constructor
    while (iswspace(lexer->lookahead)) skip(lexer);
  }
  // Allow an optional visibility modifier before 'constructor'
  if (is_word_char(lexer->lookahead) && lexer->lookahead != 'c') {
    return check_modifier_then_constructor(lexer);
  }
  // Check directly for 'constructor'
  return check_word(lexer, "constructor", 11);
}

static bool scan_automatic_semicolon(TSLexer *lexer, const bool *valid_symbols) {
  lexer->result_symbol = AUTOMATIC_SEMICOLON;
  lexer->mark_end(lexer);

  bool sameline = true;
  for (;;) {
    if (lexer->eof(lexer)) return true;

    if (lexer->lookahead == ';') {
      advance(lexer);
      lexer->mark_end(lexer);
      return true;
    }

    if (!iswspace(lexer->lookahead)) break;

    if (lexer->lookahead == '\n') {
      skip(lexer);
      sameline = false;
      break;
    }

    if (lexer->lookahead == '\r') {
      skip(lexer);

      if (lexer->lookahead == '\n') skip(lexer);

      sameline = false;
      break;
    }

    skip(lexer);
  }

  // Skip whitespace and comments
  if (!scan_whitespace_and_comments(lexer))
    return false;

  if (sameline) {
    switch (lexer->lookahead) {
      // Insert imaginary semicolon before an 'import' but not in front
      // of other words or keywords starting with 'i'
      case 'i':
        return scan_for_word(lexer, "mport", 5);

      case ';':
        advance(lexer);
        lexer->mark_end(lexer);
        return true;

      // Don't insert a semicolon in other cases
      default:
        return false;
    }
  }

  switch (lexer->lookahead) {
      case ',':
      case '.':
      case ':':
      case '*':
      case '%':
      case '>':
      case '<':
      case '=':
      case '{':
      case '[':
      case '(':
      case '?':
      case '|':
      case '&':
        return false;

      // Handle `/` — could be division, line comment, or block comment.
      // For division: no ASI (continuation operator).
      // For line comments (`//`): skip the comment(s) and check the next
      // real token. If continuation, suppress ASI (return false — tree-sitter
      // resets, parses line_comment internally, then re-checks ASI).
      // If non-continuation, insert ASI (return true at original mark_end).
      // For block comments (`/*`): advance through the comment and produce
      // MULTILINE_COMMENT. The parser then re-calls the scanner for the ASI
      // decision on whatever token follows the comment.
      case '/': {
        advance(lexer);
        if (lexer->lookahead == '/') {
          // Line comment — skip to end of line using skip() since
          // line_comment is an internal token (the grammar handles it).
          skip(lexer);
          while (lexer->lookahead != '\n' && lexer->lookahead != '\r' &&
                 lexer->lookahead != 0 && !lexer->eof(lexer)) {
            skip(lexer);
          }
          // Skip any whitespace and further comments after this line comment.
          while (iswspace(lexer->lookahead)) skip(lexer);
          while (lexer->lookahead == '/') {
            skip(lexer);
            if (lexer->lookahead == '/') {
              skip(lexer);
              while (lexer->lookahead != '\n' && lexer->lookahead != '\r' &&
                     lexer->lookahead != 0 && !lexer->eof(lexer)) {
                skip(lexer);
              }
              while (iswspace(lexer->lookahead)) skip(lexer);
            } else if (lexer->lookahead == '*') {
              skip(lexer);
              unsigned depth = 1;
              bool star = false;
              while (depth > 0 && !lexer->eof(lexer)) {
                if (lexer->lookahead == '*') { skip(lexer); star = true; }
                else if (lexer->lookahead == '/') {
                  skip(lexer);
                  if (star) { star = false; depth--; }
                  else { if (lexer->lookahead == '*') { depth++; skip(lexer); } star = false; }
                } else { star = false; skip(lexer); }
              }
              while (iswspace(lexer->lookahead)) skip(lexer);
            } else {
              // A lone '/' (division) after a comment — division is a
              // continuation operator, suppress ASI. Tree-sitter resets
              // to P0, internal lexer matches the line comment, then the
              // scanner is called again and sees the bare '/' directly.
              return false;
            }
          }
          // Now check the next real token.
          switch (lexer->lookahead) {
            case '.': case ',': case ':': case '*': case '%':
            case '>': case '<': case '=': case '{': case '[':
            case '(': case '?': case '|': case '&': case '/':
              return false;
            case '!':
              skip(lexer);
              if (lexer->lookahead == '=') return false;
              return true;
            case 'e':
              if (scan_for_word(lexer, "lse", 3)) return false;
              return true;
            case 'a':
              if (scan_for_word(lexer, "s", 1)) return false;
              return true;
            case 'w':
              if (scan_for_word(lexer, "here", 4)) return false;
              return true;
            case 'c':
              if (scan_for_word(lexer, "atch", 4)) return false;
              return true;
            case 'f':
              if (scan_for_word(lexer, "inally", 6)) return false;
              return true;
            default:
              return true;
          }
        } else if (lexer->lookahead == '*') {
          // Block comment after a newline. Use advance() to read through the
          // comment so the content is available for MULTILINE_COMMENT if we
          // decide to produce it. DON'T call mark_end yet — we defer that
          // decision until we know what follows the comment.
          advance(lexer);
          unsigned nesting_depth = 1;
          bool after_star = false;
          while (nesting_depth > 0 && !lexer->eof(lexer)) {
            switch (lexer->lookahead) {
              case '*':
                advance(lexer);
                after_star = true;
                break;
              case '/':
                advance(lexer);
                if (after_star) {
                  after_star = false;
                  nesting_depth--;
                } else {
                  if (lexer->lookahead == '*') {
                    nesting_depth++;
                    advance(lexer);
                  }
                  after_star = false;
                }
                break;
              case '\0':
                if (lexer->eof(lexer)) {
                  // Unterminated block comment at EOF — produce it.
                  lexer->result_symbol = MULTILINE_COMMENT;
                  lexer->mark_end(lexer);
                  return true;
                }
                // fallthrough
              default:
                advance(lexer);
                after_star = false;
                break;
            }
          }
          // Skip whitespace after the block comment. Don't skip further
          // comments — the continuation switch handles '/' and '*', so
          // subsequent comments will be correctly treated as continuation.
          // Skipping them here would swallow them (they'd never appear
          // as separate tokens in the parse tree).
          while (iswspace(lexer->lookahead)) skip(lexer);
          // Check the next real token to decide: MULTILINE_COMMENT or ASI?
          //
          // IMPORTANT: For keyword checks (else, as, where, !=), we must
          // call mark_end BEFORE scan_for_word/skip, because those functions
          // advance the cursor past the keyword. If mark_end were called
          // after, the MULTILINE_COMMENT span would swallow the keyword
          // and the parser would never see it.
          switch (lexer->lookahead) {
            case '.': case ',': case ':': case '%':
            case '>': case '<': case '=': case '{': case '[':
            case '(': case '?': case '|': case '&': case '/':
            case '*':
              // Continuation operator — produce MULTILINE_COMMENT.
              lexer->mark_end(lexer);
              lexer->result_symbol = MULTILINE_COMMENT;
              return true;
            case '!':
              // mark_end before consuming '!' so it's not swallowed.
              lexer->mark_end(lexer);
              skip(lexer);
              if (lexer->lookahead == '=') {
                // != is continuation — produce MULTILINE_COMMENT.
                lexer->result_symbol = MULTILINE_COMMENT;
                return true;
              }
              // Unary ! — not continuation. Produce ASI at original
              // position (mark_end was at P0 before, now at '!' position,
              // but the token has no advance()d content past the comment,
              // so tree-sitter will re-scan from here).
              return true;
            case 'e':
              lexer->mark_end(lexer);
              if (scan_for_word(lexer, "lse", 3)) {
                lexer->result_symbol = MULTILINE_COMMENT;
                return true;
              }
              return true;
            case 'a':
              lexer->mark_end(lexer);
              if (scan_for_word(lexer, "s", 1)) {
                lexer->result_symbol = MULTILINE_COMMENT;
                return true;
              }
              return true;
            case 'w':
              lexer->mark_end(lexer);
              if (scan_for_word(lexer, "here", 4)) {
                lexer->result_symbol = MULTILINE_COMMENT;
                return true;
              }
              return true;
            default:
              // the original position (P0, before the comment), so the
              // ASI token is zero-width. The block comment will be
              // re-scanned as MULTILINE_COMMENT on the next parse step.
              return true;
          }
        }
        // Bare `/` (not `//` or `/*`) — division. No ASI.
        return false;
      }

      // In Kotlin, `+` and `-` after a newline are always prefix operators,
      // not binary continuation. If a binary operation is intended, the
      // operator must be placed at the end of the previous line:
      //   a +       // binary: a + b
      //     b
      //   a         // prefix: a; +b
      //   + b
      // The grammar ensures AUTOMATIC_SEMICOLON is only valid where a
      // statement could end, so this won't fire inside () or [] where
      // newlines don't terminate statements.
      case '+':
      case '-':
        return true;

      // Don't insert a semicolon before `!=`, but do insert one before a unary `!`.
      case '!':
        skip(lexer);
        return lexer->lookahead != '=';

      // Don't insert a semicolon before an else
      case 'e':
        return !scan_for_word(lexer, "lse", 3);

      // Don't insert a semicolon before an as
      case 'a':
        return !scan_for_word(lexer, "s", 1);

      // Don't insert a semicolon before a where
      case 'w':
        return !scan_for_word(lexer, "here", 4);

      // Don't insert a semicolon before `instanceof`, or before `internal`
      // when followed by `constructor` in a class declaration context.
      case 'i':
        if (valid_symbols[PRIMARY_CONSTRUCTOR_KEYWORD] &&
            !valid_symbols[STRING_CONTENT] &&
            check_modifier_then_constructor(lexer)) {
          return false;
        }
        // Note: lexer has advanced past the word. For "instanceof", scan_for_word
        // can no longer match. But since "instanceof" is not a Kotlin keyword
        // (Kotlin uses "is"), this is acceptable — ASI is inserted, which is
        // the correct behavior for any non-constructor identifier.
        return true;

      // Don't insert a semicolon before `public/private/protected constructor`
      // in class declaration context.
      case 'p':
        if (valid_symbols[PRIMARY_CONSTRUCTOR_KEYWORD] &&
            !valid_symbols[STRING_CONTENT] &&
            check_modifier_then_constructor(lexer)) {
          return false;
        }
        return true;

      // Don't insert a semicolon before `constructor` if the parser expects
      // a primary constructor (class declaration context). In class body
      // context, PRIMARY_CONSTRUCTOR_KEYWORD won't be valid, so ASI is
      // inserted normally before secondary constructors.
      // Guard against error recovery mode where all symbols are valid.
      // Instead of suppressing ASI, we emit the constructor keyword directly
      // since it's an external token and the internal lexer won't match it.
      case 'c':
        if (valid_symbols[PRIMARY_CONSTRUCTOR_KEYWORD] &&
            !valid_symbols[STRING_CONTENT]) {
          const char *kw = "constructor";
          bool matched = true;
          for (unsigned i = 0; i < 11; i++) {
            if (lexer->lookahead != kw[i]) { matched = false; break; }
            advance(lexer);
          }
          if (matched && !is_word_char(lexer->lookahead)) {
            lexer->result_symbol = PRIMARY_CONSTRUCTOR_KEYWORD;
            lexer->mark_end(lexer);
            return true;
          }
          // If constructor didn't match, we've advanced past some chars.
          // Can't reliably check 'catch' now. Just insert ASI.
          return true;
        }
        // Not in constructor context — check for 'catch'
        return !scan_for_word(lexer, "atch", 4);

      // Don't insert a semicolon before finally (continues try_expression)
      case 'f':
        return !scan_for_word(lexer, "inally", 6);

      // Don't insert a semicolon before an annotation that precedes 'constructor'
      // e.g. `class Foo\n@Bar\nconstructor(...)` — the @Bar is a constructor modifier
      case '@':
        if (valid_symbols[PRIMARY_CONSTRUCTOR_KEYWORD] &&
            !valid_symbols[STRING_CONTENT] &&
            check_annotation_then_constructor(lexer)) {
          return false;
        }
        return true;

      case ';':
        advance(lexer);
        lexer->mark_end(lexer);
        return true;

      default:
        return true;
  }
}

static bool scan_safe_nav(TSLexer *lexer) {
  lexer->result_symbol = SAFE_NAV;
  lexer->mark_end(lexer);

  // skip white space
  if (!scan_whitespace_and_comments(lexer))
    return false;

  if (lexer->lookahead != '?')
    return false;

  advance(lexer);

  if (!scan_whitespace_and_comments(lexer))
    return false;

  if (lexer->lookahead != '.')
    return false;

  advance(lexer);
  lexer->mark_end(lexer);
  return true;
}

static bool scan_line_sep(TSLexer *lexer) {
  // Line Seps: [ CR, LF, CRLF ]
  int state = 0;
  while (true) {
    switch(lexer->lookahead) {
      case  ' ':
      case '\t':
      case '\v':
        // Skip whitespace
        advance(lexer);
        break;

      case '\n':
        advance(lexer);
        return true;

      case '\r':
        if (state == 1)
          return true;

        state = 1;
        advance(lexer);
        break;

      default:
        // We read a CR
        if (state == 1)
          return true;

        return false;
    }
  }
}

static bool scan_import_list_delimiter(TSLexer *lexer) {
  // Import lists are terminated either by an empty line or a non import statement
  lexer->result_symbol = IMPORT_LIST_DELIMITER;
  lexer->mark_end(lexer);

  // if eof; return true
  if (lexer->eof(lexer))
    return true;

  // Scan for the first line seperator
  if (!scan_line_sep(lexer))
    return false;

  // if line.sep line.sep; return true
  if (scan_line_sep(lexer)) {
    lexer->mark_end(lexer);
    return true;
  }

  // if line.sep [^import]; return true
  while (true) {
    switch (lexer->lookahead) {
      case  ' ':
      case '\t':
      case '\v':
        // Skip whitespace
        advance(lexer);
        break;

      case 'i':
        return !scan_for_word(lexer, "mport", 5);

      default:
        return true;
    }
  }
}

// Scan a dot in import identifiers. Matches '.' normally, but when the dot
// is followed by a newline and then the 'import' keyword, produces an
// AUTOMATIC_SEMICOLON (zero-width, before the dot) instead. This cleanly
// terminates the current import_header, preventing malformed imports
// (e.g. trailing dots) from bleeding into subsequent valid imports.
static bool scan_import_dot(TSLexer *lexer) {
  if (lexer->lookahead != '.') return false;

  // Mark end BEFORE consuming the dot — this is where ASI would go
  lexer->mark_end(lexer);

  advance(lexer);

  // Peek ahead: skip horizontal whitespace, check for newline
  bool found_newline = false;
  while (iswspace(lexer->lookahead)) {
    if (lexer->lookahead == '\n' || lexer->lookahead == '\r') {
      found_newline = true;
    }
    skip(lexer);
  }

  if (found_newline && lexer->lookahead == 'i' &&
      scan_for_word(lexer, "mport", 5)) {
    // Trailing dot followed by 'import' on next line — produce ASI
    // instead of the dot. mark_end was set before the dot, so the
    // semicolon is zero-width at that position.
    lexer->result_symbol = AUTOMATIC_SEMICOLON;
    return true;
  }

  // Normal dot — include it in the token
  lexer->result_symbol = IMPORT_DOT;
  lexer->mark_end(lexer);
  return true;
}

bool tree_sitter_kotlin_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
  if (valid_symbols[AUTOMATIC_SEMICOLON]) {
    bool ret = scan_automatic_semicolon(lexer, valid_symbols);
    if (!ret && valid_symbols[SAFE_NAV] && lexer->lookahead == '?') {
      return scan_safe_nav(lexer);
    }

    // if we fail to find an automatic semicolon, it's still possible that we may
    // want to lex a string or comment later
    if (ret) return ret;
  }

  // Match dots in import identifiers, refusing dots that would cause
  // malformed imports to bleed into subsequent import statements.
  if (valid_symbols[IMPORT_DOT]) {
    if (scan_import_dot(lexer)) return true;
  }

  // Match 'constructor' keyword for primary constructors when on the same line
  // (the cross-newline case is handled inside scan_automatic_semicolon)
  if (valid_symbols[PRIMARY_CONSTRUCTOR_KEYWORD] && !valid_symbols[STRING_CONTENT]) {
    while (iswspace(lexer->lookahead)) skip(lexer);
    if (lexer->lookahead == 'c') {
      const char *kw = "constructor";
      bool matched = true;
      for (unsigned i = 0; i < 11; i++) {
        if (lexer->lookahead != kw[i]) { matched = false; break; }
        advance(lexer);
      }
      if (matched && !is_word_char(lexer->lookahead)) {
        lexer->result_symbol = PRIMARY_CONSTRUCTOR_KEYWORD;
        lexer->mark_end(lexer);
        return true;
      }
    }
  }

  if (valid_symbols[IMPORT_LIST_DELIMITER]) {
    return scan_import_list_delimiter(lexer);
  }

  // content or end
  if (valid_symbols[STRING_CONTENT] && scan_string_content(lexer, payload)) {
    return true;
  }

  // a string might follow after some whitespace, so we can't lookahead
  // until we get rid of it
  while (iswspace(lexer->lookahead)) skip(lexer);

  if (valid_symbols[STRING_START] && scan_string_start(lexer, payload)) {
    lexer->result_symbol = STRING_START;
    return true;
  }

  if (valid_symbols[MULTILINE_COMMENT] && scan_multiline_comment(lexer)) {
    return true;
  }

  if (valid_symbols[SAFE_NAV]) {
    return scan_safe_nav(lexer);
  }

  return false;
}

void *tree_sitter_kotlin_external_scanner_create() {
  Stack *stack = ts_calloc(1, sizeof(Stack));
  if (stack == NULL) abort();
  array_init(stack);
  return stack;
}

void tree_sitter_kotlin_external_scanner_destroy(void *payload) {
  Stack *stack = (Stack *)payload;
  array_delete(stack);
  ts_free(stack);
}

unsigned tree_sitter_kotlin_external_scanner_serialize(void *payload, char *buffer) {
  Stack *stack = (Stack *)payload;
  if (stack->size > 0) {
    // it's an undefined behavior to memcpy 0 bytes
    memcpy(buffer, stack->contents, stack->size);
  }
  return stack->size;
}

void tree_sitter_kotlin_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {
  Stack *stack = (Stack *)payload;
  if (length > 0) {
    array_reserve(stack, length);
    memcpy(stack->contents, buffer, length);
    stack->size = length;
  } else {
    array_clear(stack);
  }
}