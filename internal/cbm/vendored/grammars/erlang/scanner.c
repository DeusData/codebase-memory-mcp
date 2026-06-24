/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* This file is initially inspired by the one from
   https://github.com/tree-sitter/tree-sitter-python/blob/1030ed7f0df3c7c0c80b23242f12d2f60e11c79a/src/scanner.c
 */

/* See
   https://tree-sitter.github.io/tree-sitter/creating-parsers#external-scanners
   for how the file works. */

#include "tree_sitter/array.h"
#include "tree_sitter/parser.h"

#include <stdint.h>

enum TokenType {
  /* Triple Quoted String, as per https://www.erlang.org/eeps/eep-0064
   *
   * Start is three or more `"`, may only be followed by whitespace to end of
   * line End is three or more `"`, may only be preceded by whitespace from
   * start of line The starting and ending `"` count must be the same
   */
  TQ_STRING,
  /* A TQ_STRING preceded by /~[sSbB]?/ */
  TQ_SIGIL_STRING,
  /* Will only be in valid_symbols when in error recovery mode */
  ERROR_SENTINEL
};

static inline void advance(TSLexer* lexer) {
  lexer->advance(lexer, false);
  /* fprintf(stderr, "Scanner advance:lookahead: '%c'.\n", lexer->lookahead); */
}

static inline void skip(TSLexer* lexer) {
  lexer->advance(lexer, true);
  /* fprintf(stderr, "Scanner skip:lookahead: '%c'.\n", lexer->lookahead); */
}

/* Note: writing to stderr will show up in tests, e.g. */
/* make gen && npm run test -- --filter "quoted binary explicit sigil in tq
 * string, 5 quotes, with 3 inside" */

/* static inline void dump_la(TSLexer* lexer) { */
/*   fprintf(stderr, "Scanner lookahead: '%c'.\n", lexer->lookahead); */
/* } */

static inline bool is_whitespace(TSLexer* lexer) {
  return (
      /* The test is based on the ?WHITE_SPACE/1 macro in
         elp_scan.erl, and matches the one in grammar.js */
      (lexer->lookahead >= 0x01 && lexer->lookahead <= 0x20) ||
      (lexer->lookahead >= 0x80 && lexer->lookahead <= 0xA0));
}

bool tree_sitter_erlang_external_scanner_scan(
    void* unused_payload,
    TSLexer* lexer,
    const bool* valid_symbols) {
  (void)unused_payload;

  if (valid_symbols[TQ_STRING] || valid_symbols[TQ_SIGIL_STRING]) {
    /* Skip any leading whitespace */
    while (is_whitespace(lexer)) {
      skip(lexer);
    }
    bool is_sigil_string = false;
    if (valid_symbols[TQ_SIGIL_STRING]) {
      /* Look for /~[sSbB]?/ */
      if (lexer->lookahead == '~') {
        is_sigil_string = true;
        advance(lexer);
        switch (lexer->lookahead) {
          case 's':
          case 'S':
          case 'b':
          case 'B':
            advance(lexer);
            break;
          case '"':
            break;
          default:
            return false;
        }
      }
    }

    /* TODO: break into functions */
    uint16_t delimiter_count = 0;
    if (lexer->lookahead == '"') {
      advance(lexer);
      if (lexer->lookahead == '"') {
        advance(lexer);
        if (lexer->lookahead == '"') {
          advance(lexer);

          /* We may have seen the start of a tq string, look for the rest */
          delimiter_count = 3;
          while (lexer->lookahead == '"') {
            delimiter_count++;
            advance(lexer);
          }
          /* skip whitespace to end of line */
          while (lexer->lookahead != '\n' && is_whitespace(lexer)) {
            advance(lexer);
          }

          if (lexer->lookahead == '\n') {
            advance(lexer);
          } else {
            return false;
          }

          for (;;) {
            /* Look for end marker, skipping chars in the meantime */
            /* newline, whitespace, same number of '"' at end */
            if (lexer->lookahead == '\n') {
              advance(lexer);
              /* skip whitespace to first '"' */
              while (lexer->lookahead != '\n' && is_whitespace(lexer)) {
                advance(lexer);
              }

              /* match same number of final '"' */
              uint16_t check = delimiter_count;
              while (check > 0) {
                if (lexer->lookahead != '"') {
                  break;
                } else {
                  advance(lexer);
                }
                check--;
              }
              if (check <= 0) {
                lexer->mark_end(lexer);
                if (is_sigil_string) {
                  lexer->result_symbol = TQ_SIGIL_STRING;
                } else {
                  lexer->result_symbol = TQ_STRING;
                }
                return true;
              }
            } else {
              if (lexer->eof(lexer)) {
                return false;
              } else {
                advance(lexer);
              }
            }
          }
        }
      }
    }
  }

  return false;
}

unsigned tree_sitter_erlang_external_scanner_serialize(
    void* unused_payload,
    char* unused_buffer) {
  (void)unused_payload;
  (void)unused_buffer;
  return 0;
}

void tree_sitter_erlang_external_scanner_deserialize(
    void* unused_payload,
    const char* unused_buffer,
    unsigned unused_length) {
  (void)unused_payload;
  (void)unused_buffer;
  (void)unused_length;
}

void* tree_sitter_erlang_external_scanner_create() {
  return 0;
}

void tree_sitter_erlang_external_scanner_destroy(void* unused_payload) {
  (void)unused_payload;
}