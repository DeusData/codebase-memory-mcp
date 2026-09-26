#include "objectscript_common.h"
#include "tree_sitter/parser.h"
#include <string.h>
#include <wctype.h>

// There is no way to extend enums, so keep this in sync with base.h
// All new entries should be appended at the bottom of the list
enum TokenType {
   IRIS_USERNAME= OBJECTSCRIPT_CORE_TOKEN_TYPE_MAX,
   PYTHON_MIME_TYPE,
   _IS_PYTHON,
   TEXT_MIME_TYPE,
};

struct ObjectScript_Udl_Scanner {
  char in_body;
  struct ObjectScript_Core_Scanner core_scanner;
};

/// This is the interesting function. The rest is infrastructure
static bool scan(struct ObjectScript_Udl_Scanner *scanner, TSLexer *lexer, const bool *valid_symbols) {
  if (valid_symbols[SENTINEL]) {
    return false;
  }
  if (valid_symbols[IRIS_USERNAME] && !(lexer->lookahead=='}' || lexer->lookahead=='@' || lexer->lookahead=='*')) {
    int count=0;
    lexer->mark_end(lexer);
    while(!lexer->eof(lexer) && !(lexer->lookahead=='}' || lexer->lookahead=='@' || lexer->lookahead=='*') && count<160 ) {
      count++;
      advance(lexer);
    }


    if(lexer->lookahead=='}') {
      lexer->mark_end(lexer);
      lexer->result_symbol = IRIS_USERNAME;
      return true;
    }
    else {
      return false;
    }

  }

  if (valid_symbols[_IS_PYTHON]) {
      scanner->core_scanner.is_python = true;
      lexer->result_symbol = _IS_PYTHON;
      return true;
  }

  if (valid_symbols[PYTHON_MIME_TYPE]) {
        lexer->mark_end(lexer);
        int count = 0;
        while(iswspace(lexer->lookahead)) {
            count ++;
            advance(lexer);
        }
        if (count > 0) {
            lexer -> mark_end(lexer);
            lexer->result_symbol = _WHITESPACE;
            return true;
        }
        bool has_string = false;
        if (lexer->lookahead == '"') {
            has_string = true;
            advance(lexer);
        }
    const char* text_mime = "text/";
    const char *text_python = "x-python";
    const char *application_python = "application/python";
    const char *markdown = "markdown";
    const char *plain = "plain";
    bool is_python = true;
    bool is_text = true;
    bool is_markdown = false;
    for (unsigned i = 0; i < 5; i++) {
        if (lexer->lookahead != text_mime[i]) {
            is_text = false;
            break;
        }
        if (lexer->eof(lexer)) break;
        advance(lexer);
    }
    if (is_text) {
        for (unsigned i = 0; i < 8; i++) {
            if (lexer->lookahead != text_python[i]) {
                is_python = false;
                break;
            }
            if (lexer->eof(lexer)) break;
            advance(lexer);
        } if (!is_python) {
            is_markdown = true;
            for (unsigned i = 0; i < 8; i++) {
                if (lexer->lookahead != markdown[i]) {
                    is_markdown = false;
                    break;
                }
                if (lexer->eof(lexer)) break;
                advance(lexer);
            }
        } if (!is_markdown && !is_python) {
            is_markdown = true;
            for (unsigned i = 0; i < 5; i++) {
                if (lexer->lookahead != plain[i]) {
                    is_markdown = false;
                    break;
                }
                if (lexer->eof(lexer)) break;
                advance(lexer);
            }
        }
    }
    else {
        for (unsigned i = 0; i < 18; i++) {
            if (lexer->lookahead != application_python[i]) {
                is_python = false;
                break;
            }
            if (lexer->eof(lexer)) break;
            advance(lexer);
        }
    }
    if (has_string) {
        advance(lexer);
    }

    if (is_python) {
        scanner->core_scanner.is_python = true;
        lexer->mark_end(lexer);
        lexer->result_symbol = PYTHON_MIME_TYPE;
        return true;
    }

    if (is_markdown) {
        scanner->core_scanner.is_text = true;
        lexer->mark_end(lexer);
        lexer->result_symbol = TEXT_MIME_TYPE;
        return true;
    }

    return false;

  }

  return ObjectScript_Core_Scanner_scan(&scanner->core_scanner, lexer,
                                        valid_symbols);
}

void *tree_sitter_objectscript_udl_external_scanner_create() {
  struct ObjectScript_Udl_Scanner *scanner =
      (struct ObjectScript_Udl_Scanner *)calloc(
          1, sizeof(struct ObjectScript_Udl_Scanner));
  scanner->in_body = 0;
  ObjectScript_Core_Scanner_init(&scanner->core_scanner);
  scanner->core_scanner.column1_statement_mode = false;
  return scanner;
}

bool tree_sitter_objectscript_udl_external_scanner_scan(
    void *payload, TSLexer *lexer, const bool *valid_symbols) {
  return scan(payload, lexer, valid_symbols);
}

unsigned tree_sitter_objectscript_udl_external_scanner_serialize(void *payload,
                                                                 char *buffer) {
  struct ObjectScript_Udl_Scanner *scanner =
      (struct ObjectScript_Udl_Scanner *)payload;
  memcpy(buffer, scanner, sizeof(struct ObjectScript_Udl_Scanner));
  return sizeof(struct ObjectScript_Udl_Scanner);
}

void tree_sitter_objectscript_udl_external_scanner_deserialize(
    void *payload, const char *buffer, unsigned length) {
  // This one is a bit funky.
  // length includes the sizeof(struct Scanner) and the structs it points to
  memcpy(payload, buffer, length);
}

void tree_sitter_objectscript_udl_external_scanner_destroy(void *payload) {
  struct ObjectScript_Udl_Scanner *scanner =
      (struct ObjectScript_Udl_Scanner *)payload;
  free(scanner);
}
