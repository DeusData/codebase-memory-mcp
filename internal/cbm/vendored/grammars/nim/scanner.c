/* Copyright (c) 2023 Leorize <leorize+oss@disroot.org>
 *
 * SPDX-License-Identifier: MPL-2.0
 */

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>

#include "tree_sitter/alloc.h"
#include "tree_sitter/parser.h"

#ifdef __GNUC__
#  define _nonnull_(...) __attribute__((nonnull(__VA_ARGS__)))
#  define _returns_nonnull_ __attribute__((returns_nonnull))
#  define _const_ __attribute__((const))
#  define _pure_ __attribute__((pure))
#else
#  define _nonnull_(...)
#  define _returns_nonnull_
#  define _const_
#  define _pure_
#endif

#ifdef TREE_SITTER_INTERNAL_BUILD
#  define dprintf(...) fprintf(stderr, __VA_ARGS__)
#  define dputs(msg) fputs(msg, stderr)
#  define DBG(msg)  \
    if (debug_mode) \
    (void)fprintf(stderr, "lex_nim: %s():%d: %s\n", __func__, __LINE__, msg)
#  define DBG_F(fmt, ...) \
    if (debug_mode)       \
    (void)fprintf(        \
        stderr, "lex_nim: %s():%d: " fmt, __func__, __LINE__, ##__VA_ARGS__)

static bool debug_mode = false; /* NOLINT(*-global-variables) */
#else
#  define dprintf(...) ((void)0)
#  define dputs(msg) ((void)0)
#  define DBG(msg) ((void)0)
#  define DBG_F(fmt, ...) ((void)0)
static const bool debug_mode = false;
#endif

#ifndef __wasm__
#  define RUNTIME_ASSERT(cond)                                            \
    if (!(cond)) {                                                        \
      (void)fprintf(                                                      \
          stderr, "lex_nim: %s():%d: Assertion `%s' failed.\n", __func__, \
          __LINE__, #cond);                                               \
      abort();                                                            \
    }
#else
// WASM doesn't have printing enabled
#  define RUNTIME_ASSERT(cond) \
    if (!(cond)) {             \
      abort();                 \
    }
#endif

#define MIN(left, right) ((left) > (right) ? (right) : (left))
#define MAX(left, right) ((left) < (right) ? (right) : (left))

typedef uint8_t indent_value;
const indent_value INVALID_INDENT_VALUE = (indent_value)~0U;

struct indent_vec {
  int32_t len;
  int32_t capacity;
  indent_value* data;
};

#define INDENT_VEC_EMPTY                  \
  {                                       \
    .len = 0, .capacity = 0, .data = NULL \
  }

_nonnull_(1) static void indent_vec_destroy(struct indent_vec* self)
{
  ts_free(self->data);
  memset(self, 0, sizeof(*self));
}

_nonnull_(1) _returns_nonnull_ static indent_value* indent_vec_at(
    struct indent_vec* self, int32_t idx)
{
  RUNTIME_ASSERT(idx >= 0 && idx < self->len);
  return &self->data[idx];
}

_nonnull_(1) static indent_value indent_vec_get(
    const struct indent_vec* self, int32_t idx)
{
  return *indent_vec_at((struct indent_vec*)self, idx);
}

_nonnull_(1) _returns_nonnull_ static indent_value* indent_vec_at_capacity(
    struct indent_vec* self, int32_t idx)
{
  RUNTIME_ASSERT(idx >= 0 && idx < self->capacity);
  return &self->data[idx];
}

_nonnull_(1) static int indent_vec_set_capacity(
    struct indent_vec* self, int32_t size)
{
  if (size < 0) {
    return -1;
  }
  if (size != self->capacity) {
    indent_value* new_data = ts_realloc(self->data, size);
    if (!new_data) {
      return -1;
    }
    self->data = new_data;
    self->capacity = size;
    self->len = MIN(self->len, size);
    for (int i = self->len; i < size; i++) {
      *indent_vec_at_capacity(self, i) = INVALID_INDENT_VALUE;
    }
  }
  return 0;
}

_nonnull_(1) static int indent_vec_set_len(
    struct indent_vec* self, int32_t size)
{
  if (size < 0) {
    return -1;
  }
  if (size > self->capacity) {
    if (indent_vec_set_capacity(self, size) < 0) {
      return -1;
    }
  }

  for (int i = self->len; i < size; i++) {
    *indent_vec_at_capacity(self, i) = INVALID_INDENT_VALUE;
  }
  self->len = size;

  return 0;
}

_nonnull_(1) static int indent_vec_push(
    struct indent_vec* self, indent_value value)
{
  if (self->len >= self->capacity) {
    int32_t new_capacity = self->len >= 2 ? self->len * 3 / 2 : self->len + 1;
    if (indent_vec_set_capacity(self, new_capacity) < 0) {
      return -1;
    }
  }

  self->len++;
  *indent_vec_at(self, self->len - 1) = value;

  return 0;
}

_nonnull_(1) static void indent_vec_pop(struct indent_vec* self)
{
  indent_vec_set_len(self, MAX(0, self->len - 1));
}

_nonnull_(1) static indent_value indent_vec_back(const struct indent_vec* self)
{
  return indent_vec_get(self, self->len - 1);
}

_nonnull_(1, 2) static unsigned indent_vec_serialize(
    const struct indent_vec* self, char* buffer, unsigned buffer_len)
{
  size_t n_bytes = self->len * sizeof(*self->data);
  if (n_bytes > buffer_len) {
    DBG_F(
        "warning: buffer is smaller than vector (%u < %zd), partially "
        "serializing",
        buffer_len, n_bytes);
  }

  // Prevents passing NULL pointer to memcpy
  if (n_bytes == 0) {
    return n_bytes;
  }

  unsigned serialize_len = MIN(buffer_len, n_bytes);
  memcpy(buffer, self->data, n_bytes);
  return serialize_len;
}

_nonnull_(1, 2) static void indent_vec_deserialize(
    struct indent_vec* self, const char* buffer, unsigned buffer_len)
{
  int32_t n_items = (int32_t)MIN(buffer_len / sizeof(*self->data), INT32_MAX);
  if (indent_vec_set_len(self, n_items) < 0) {
    DBG("cannot deserialize: set_len failed");
    return;
  }
  if (n_items > 0) {
    memcpy(self->data, buffer, n_items * sizeof(*self->data));
  }
}

_nonnull_(1) static void indent_vec_debug(const struct indent_vec* self)
{
  if (debug_mode) {
    DBG_F("current layout stack: [");
    for (int32_t i = 0; i < self->len; i++) {
      (void)dprintf(" %" PRIu8, indent_vec_get(self, i));
    }
    (void)dprintf(" ]\n");
  }
}

enum token_type {
  TOKEN_TYPE_START,
  BLOCK_COMMENT_CONTENT = TOKEN_TYPE_START,
  BLOCK_DOC_COMMENT_CONTENT,
  COMMENT_CONTENT,
  LONG_STRING_QUOTE,
  LAYOUT_START,
  LAYOUT_END,
  LAYOUT_TERMINATOR,
  LAYOUT_EMPTY,
  INHIBIT_LAYOUT_END,
  INHIBIT_KEYWORD_TERMINATION,
  COMMA,
  SYNCHRONIZE,
  INVALID_LAYOUT,
  SIGIL_OP,
  UNARY_OP,
  WANT_EXPORT_MARKER,
  OF,
  TOKEN_TYPE_LEN
};

#ifdef TREE_SITTER_INTERNAL_BUILD
const char* const TOKEN_TYPE_STR[TOKEN_TYPE_LEN] = {
    "BLOCK_COMMENT_CONTENT",
    "BLOCK_DOC_COMMENT_CONTENT",
    "COMMENT_CONTENT",
    "LONG_STRING_QUOTE",
    "LAYOUT_START",
    "LAYOUT_END",
    "LAYOUT_TERMINATOR",
    "LAYOUT_EMPTY",
    "INHIBIT_LAYOUT_END",
    "INHIBIT_KEYWORD_TERMINATION",
    "COMMA",
    "SYNCHRONIZE",
    "INVALID_LAYOUT",
    "SIGIL_OP",
    "UNARY_OP",
    "WANT_EXPORT_MARKER",
    "OF",
};
#endif

struct valid_tokens {
  uint32_t bits : TOKEN_TYPE_LEN;
};

#define TO_VT_BIT(value) 1U << (enum token_type)(value)
#define VALID_TOKENS(bits_) \
  {                         \
    .bits = (bits_)         \
  }

_nonnull_(1) _pure_ static struct valid_tokens valid_tokens_from_array(
    const bool* valid_tokens)
{
  struct valid_tokens result = {0};
  for (unsigned i = TOKEN_TYPE_START; i < TOKEN_TYPE_LEN; i++) {
    result.bits |= (unsigned)valid_tokens[i] << i;
  }
  return result;
}

_const_ static bool valid_tokens_test(
    struct valid_tokens self, enum token_type type)
{
  return (self.bits & TO_VT_BIT(type)) != 0;
}

_const_ static bool valid_tokens_any_valid(
    struct valid_tokens left, struct valid_tokens right)
{
  return (left.bits & right.bits) != 0;
}

_const_ static bool valid_tokens_is_error(struct valid_tokens self)
{
  return self.bits == ~(~0U << (enum token_type)TOKEN_TYPE_LEN);
}

static void valid_tokens_debug(struct valid_tokens self)
{
  if (debug_mode) {
    DBG_F("valid tokens: [");
    for (int i = TOKEN_TYPE_START; i < TOKEN_TYPE_LEN; i++) {
      if (valid_tokens_test(self, i)) {
        (void)dprintf(" %s", TOKEN_TYPE_STR[i]);
      }
    }
    (void)dputs(" ]\n");
  }
}

#define FLAG_AFTER_NEWLINE 1U
#define FLAG_LEN 1U

typedef uint8_t flags_storage;

struct state {
  struct indent_vec layout_stack;
};

static struct state* state_new(void)
{
  struct state* result = ts_calloc(1, sizeof(struct state));
  if (!result) {
    return NULL;
  }
  return result;
}

static void state_destroy(struct state* self)
{
  if (self) {
    indent_vec_destroy(&self->layout_stack);
    ts_free(self);
  }
}

_nonnull_(1) static void state_clear(struct state* self)
{
  indent_vec_set_len(&self->layout_stack, 0);
}

_nonnull_(1, 2) static unsigned state_serialize(
    const struct state* self, char* buffer, unsigned buffer_len)
{
  unsigned serialize_len = 0;
  serialize_len += indent_vec_serialize(
      &self->layout_stack, &buffer[serialize_len], buffer_len - serialize_len);
  DBG_F("serialized %u bytes\n", serialize_len);
  return serialize_len;
}

_nonnull_(1) static void state_deserialize(
    struct state* self, const char* buffer, unsigned buffer_len)
{
  if (!buffer && buffer_len > 0) {
    DBG("error: no buffer but buffer length > 0");
    return;
  }

  unsigned idx = 0;
  state_clear(self);
  indent_vec_deserialize(&self->layout_stack, &buffer[idx], buffer_len - idx);
}

_nonnull_(1) static void state_debug(struct state* self)
{
  indent_vec_debug(&self->layout_stack);
}

struct context {
  TSLexer* _lexer;
  struct state* state;
  uint32_t advance_counter;
  struct valid_tokens valid_tokens;
  indent_value _current_indent;
  flags_storage flags : FLAG_LEN;
};

_nonnull_(1) static void context_mark_end(struct context* self)
{
  self->_lexer->mark_end(self->_lexer);
}

_nonnull_(1) _pure_ static uint32_t context_lookahead(struct context* self)
{
  return self->_lexer->lookahead;
}

_nonnull_(1) _pure_ static bool context_eof(struct context* self)
{
  return self->_lexer->eof(self->_lexer);
}

_nonnull_(1) static uint32_t context_advance(struct context* self, bool skip)
{
  self->advance_counter += (int)!context_eof(self);
  if (!context_eof(self)) {
    self->flags &= ~FLAG_AFTER_NEWLINE;
  }
  self->_lexer->advance(self->_lexer, skip);
  return self->_lexer->lookahead;
}

_nonnull_(1) static uint32_t context_consume(struct context* self, bool skip)
{
  uint32_t result = context_advance(self, skip);
  context_mark_end(self);
  return result;
}

_nonnull_(1) static bool context_finish(
    struct context* self, enum token_type type)
{
  DBG_F("finished scanning token: %s\n", TOKEN_TYPE_STR[type]);
  self->_lexer->result_symbol = (TSSymbol)type;
  return true;
}

_nonnull_(1) static indent_value context_indent(struct context* self)
{
  if (self->flags & FLAG_AFTER_NEWLINE) {
    return self->_current_indent;
  }

  return INVALID_INDENT_VALUE;
}

#define TRY_LEX_INNER(cnt, ctx, fn, ...)                          \
  do {                                                            \
    const uint32_t last_count_##cnt = (ctx)->advance_counter;     \
    if (fn((ctx), ##__VA_ARGS__)) {                               \
      return true;                                                \
    }                                                             \
    if ((ctx)->advance_counter != last_count_##cnt) return false; \
  } while (false)
/// Try lexing with the given function.
///
/// If the function succeed, the lexer returns immediately.
/// Otherwise, if no input were consumed, the lexer will continue.
///
/// The lexer will stop immediately if input was consumed and the given lexing
/// function fails.
///
/// @param ctx - The context to monitor state with, and as input to `fn`.
/// @param fn - The lexing function
#define TRY_LEX(ctx, fn, ...) TRY_LEX_INNER(__COUNTER__, ctx, fn, ##__VA_ARGS__)
#define LEX_FN(name, ...) \
  _nonnull_(1) static bool name(struct context* ctx, ##__VA_ARGS__)

_const_ static bool is_digit(uint32_t chr) { return chr >= '0' && chr <= '9'; }

_const_ static bool is_lower(uint32_t chr) { return chr >= 'a' && chr <= 'z'; }

_const_ static bool is_upper(uint32_t chr) { return chr >= 'A' && chr <= 'Z'; }

_const_ static bool is_keyword(uint32_t chr)
{
  return is_lower(chr) || is_upper(chr) || chr == '_';
}

_const_ static bool is_identifier(uint32_t chr)
{
  return is_keyword(chr) || is_digit(chr);
}

_const_ static uint32_t to_upper(uint32_t chr)
{
  const uint32_t lower_case_bit = 1U << 5U;
  return is_lower(chr) ? chr & ~lower_case_bit : chr;
}

_nonnull_(1) static size_t scan_spaces(struct context* ctx, bool force_update)
{
  bool update_indent = force_update;
  uint8_t indent = 0;
  size_t spaces = 0;
  while (true) {
    // Need goto to break out of loop
    // NOLINTBEGIN(*-avoid-goto)
    switch (context_lookahead(ctx)) {
    case ' ':
      indent += (int)(indent != INVALID_INDENT_VALUE);
      spaces++;
      context_advance(ctx, true);
      break;
    case '\n':
    case '\r':
      update_indent = true;
      indent = 0;
      spaces++;
      context_advance(ctx, true);
      break;
    case '\0':
      if (context_eof(ctx)) {
        update_indent = true;
        indent = 0;
      }
      goto loop_end;
    default:
      goto loop_end;
    }
    // NOLINTEND(*-avoid-goto)
  }
loop_end:
  if (update_indent) {
    DBG_F("updated current indentation: %" PRIu8 "\n", indent);
    ctx->_current_indent = indent;
    ctx->flags |= FLAG_AFTER_NEWLINE;
  }

  return spaces;
}

LEX_FN(lex_long_string_quote)
{
  if (context_lookahead(ctx) != '"' ||
      !valid_tokens_test(ctx->valid_tokens, LONG_STRING_QUOTE)) {
    return false;
  }

  context_consume(ctx, false);
  uint8_t count = 1;
  while (context_lookahead(ctx) == '"' && count < 3) {
    context_advance(ctx, false);
    count++;
  }

  if (count < 3) {
    context_mark_end(ctx);
    return context_finish(ctx, LONG_STRING_QUOTE);
  }

  if (context_lookahead(ctx) == '"') {
    return context_finish(ctx, LONG_STRING_QUOTE);
  }

  return false;
}

static const struct valid_tokens COMMENT_TOKENS = VALID_TOKENS(
    TO_VT_BIT(BLOCK_COMMENT_CONTENT) | TO_VT_BIT(BLOCK_DOC_COMMENT_CONTENT) |
    TO_VT_BIT(COMMENT_CONTENT));

LEX_FN(lex_comment_content)
{
  if (!valid_tokens_any_valid(ctx->valid_tokens, COMMENT_TOKENS) ||
      valid_tokens_is_error(ctx->valid_tokens)) {
    return false;
  }

  if (valid_tokens_test(ctx->valid_tokens, COMMENT_CONTENT)) {
    while (!context_eof(ctx)) {
      switch (context_lookahead(ctx)) {
      case '\n':
      case '\r':
        goto exit_short_comment_loop;
      default:
        context_advance(ctx, false);
      }
    }

exit_short_comment_loop:
    context_mark_end(ctx);
    return context_finish(ctx, COMMENT_CONTENT);
  }

  uint32_t nesting = 0;
  while (!context_eof(ctx)) {
    if (context_lookahead(ctx) == '#' && context_advance(ctx, false) == '[') {
      nesting++;
      DBG_F("block comment nest level: %" PRIu32 "\n", nesting);
    }
    context_mark_end(ctx);
    if (context_lookahead(ctx) == ']') {
      if (context_advance(ctx, false) == '#') {
        if (nesting > 0) {
          DBG_F("block comment terminate nest level: %" PRIu32 "\n", nesting);
          nesting--;
        }
        else if (valid_tokens_test(
                     ctx->valid_tokens, BLOCK_DOC_COMMENT_CONTENT)) {
          if (context_advance(ctx, false) == '#') {
            return context_finish(ctx, BLOCK_DOC_COMMENT_CONTENT);
          }
        }
        else {
          return context_finish(ctx, BLOCK_COMMENT_CONTENT);
        }
      }
      continue;
    }
    context_advance(ctx, false);
  }

  return false;
}

LEX_FN(lex_init)
{
  if (ctx->state->layout_stack.len > 0 ||
      valid_tokens_is_error(ctx->valid_tokens) ||
      valid_tokens_any_valid(ctx->valid_tokens, COMMENT_TOKENS)) {
    return false;
  }

  scan_spaces(ctx, true);
  if (context_lookahead(ctx) == '#') {
    return false;
  }

  indent_value current_indent = context_indent(ctx);
  if (current_indent == INVALID_INDENT_VALUE) {
    DBG("no valid indentation found");
    return false;
  }
  if (indent_vec_push(&ctx->state->layout_stack, current_indent) < 0) {
    DBG("could not extend layout stack");
    return false;
  };
  return context_finish(ctx, SYNCHRONIZE);
}

_nonnull_(1) static void skip_underscore(struct context* ctx)
{
  if (context_lookahead(ctx) == '_') {
    context_advance(ctx, false);
  }
}

static bool chrcaseeq(uint32_t lhs, uint32_t rhs)
{
  return to_upper(lhs) == to_upper(rhs);
}

LEX_FN(scan_continuing_keyword, bool scan_do)
{
#define NEXT_OR_FAIL(chr)                          \
  do {                                             \
    context_advance(ctx, false);                   \
    skip_underscore(ctx);                          \
    if (!chrcaseeq(context_lookahead(ctx), chr)) { \
      return false;                                \
    }                                              \
  } while (false)

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define CONTINUE_ON(cond) \
  if (!(cond)) {          \
    return false;         \
  }

#define FINISH_IF_END                              \
  do {                                             \
    context_advance(ctx, false);                   \
    return !is_identifier(context_lookahead(ctx)); \
  } while (false)

  if (context_lookahead(ctx) == 'e') {
    context_advance(ctx, false);
    skip_underscore(ctx);
    if (chrcaseeq(context_lookahead(ctx), 'l')) {
      context_advance(ctx, false);
      skip_underscore(ctx);
      if (chrcaseeq(context_lookahead(ctx), 's')) {
        NEXT_OR_FAIL('e');
        FINISH_IF_END;
      }
      else if (chrcaseeq(context_lookahead(ctx), 'i')) {
        NEXT_OR_FAIL('f');
        FINISH_IF_END;
      }

      return false;
    }
    if (chrcaseeq(context_lookahead(ctx), 'x')) {
      NEXT_OR_FAIL('c');
      NEXT_OR_FAIL('e');
      NEXT_OR_FAIL('p');
      NEXT_OR_FAIL('t');
      FINISH_IF_END;
    }
  }

  if (context_lookahead(ctx) == 'f') {
    NEXT_OR_FAIL('i');
    NEXT_OR_FAIL('n');
    NEXT_OR_FAIL('a');
    NEXT_OR_FAIL('l');
    NEXT_OR_FAIL('l');
    NEXT_OR_FAIL('y');
    FINISH_IF_END;
  }

  if (scan_do) {
    if (context_lookahead(ctx) == 'd') {
      NEXT_OR_FAIL('o');
      FINISH_IF_END;
    }
  }

  return false;

#undef CASE_CHAR
#undef CONTINUE_ON
#undef NEXT_OR_FAIL
#undef FINISH_IF_END
}

LEX_FN(lex_case_of)
{
  if (context_lookahead(ctx) != 'o' ||
      !valid_tokens_test(ctx->valid_tokens, OF)) {
    return false;
  }

  skip_underscore(ctx);
  switch (context_advance(ctx, false)) {
  case 'f':
  case 'F':
    if (is_identifier(context_advance(ctx, false))) {
      return false;
    }
    context_mark_end(ctx);
    return context_finish(ctx, OF);
  default:
    return false;
  }
}

const struct valid_tokens NO_LAYOUT_END_CTX =
    VALID_TOKENS(TO_VT_BIT(INHIBIT_LAYOUT_END) | TO_VT_BIT(LONG_STRING_QUOTE));

// This function is big by design.
//
// NOLINTNEXTLINE(*-cognitive-complexity)
LEX_FN(lex_indent)
{
  if (context_lookahead(ctx) == '#' || ctx->state->layout_stack.len == 0) {
    return false;
  }

  indent_value current_indent = context_indent(ctx);
  if (current_indent == INVALID_INDENT_VALUE) {
    return false;
  }

  indent_value current_layout = indent_vec_back(&ctx->state->layout_stack);

  if (valid_tokens_test(ctx->valid_tokens, LAYOUT_START)) {
    if (current_indent > current_layout) {
      if (indent_vec_push(&ctx->state->layout_stack, current_indent) < 0) {
        DBG("could not extend layout stack");
        return false;
      }
      return context_finish(ctx, LAYOUT_START);
    }
  }

  if (valid_tokens_test(ctx->valid_tokens, LAYOUT_EMPTY)) {
    switch (0) {
    default:
      if (valid_tokens_is_error(ctx->valid_tokens)) {
        break;
      }
      if (current_indent <= current_layout) {
        return context_finish(ctx, LAYOUT_EMPTY);
      }
    }
  }

  if (valid_tokens_test(ctx->valid_tokens, LAYOUT_TERMINATOR)) {
    if (current_indent <= current_layout) {
      uint32_t last_count = ctx->advance_counter;
      if (current_indent == current_layout) {
        if (valid_tokens_test(ctx->valid_tokens, INHIBIT_KEYWORD_TERMINATION) &&
            scan_continuing_keyword(ctx, true)) {
          DBG("found continuing keyword");
          return false;
        }
        if (last_count == ctx->advance_counter) {
          if (lex_case_of(ctx)) {
            return true;
          }
        }
      }
      return context_finish(ctx, LAYOUT_TERMINATOR);
    }
  }

  // Implicit layout changes
  if (!valid_tokens_any_valid(ctx->valid_tokens, NO_LAYOUT_END_CTX) ||
      valid_tokens_is_error(ctx->valid_tokens) ||
      // Allow EOF to force a layout_end, which would lead to better error
      // recovery
      context_eof(ctx)) {
    // LAYOUT_END
    if (current_indent < current_layout || context_eof(ctx)) {
      if (ctx->state->layout_stack.len > 1) {
        indent_vec_pop(&ctx->state->layout_stack);
        return context_finish(ctx, LAYOUT_END);
      }
    }

    // INVALID_LAYOUT
    //
    // XXX: Need more data to reliably distinguish
    //
    // if (current_indent > current_layout && !context_eof(ctx)) {
    //   return context_finish(ctx, INVALID_LAYOUT);
    // }
  }

  TRY_LEX(ctx, lex_case_of);

  return false;
}

LEX_FN(lex_inline_layout)
{
  if (ctx->state->layout_stack.len == 0 || (ctx->flags & FLAG_AFTER_NEWLINE)) {
    return false;
  }

  switch (context_lookahead(ctx)) {
  case ',':
    if (valid_tokens_test(ctx->valid_tokens, COMMA)) {
      return false;
    }
    break;
  case ')':
  case ']':
  case '}':
    break;
  case '.':
    if (context_advance(ctx, false) == '}') {
      break;
    }
    return false;
  default:
    if (!valid_tokens_test(ctx->valid_tokens, INHIBIT_KEYWORD_TERMINATION) &&
        scan_continuing_keyword(ctx, false)) {
      break;
    }
    return false;
  }
  if (valid_tokens_test(ctx->valid_tokens, LAYOUT_TERMINATOR)) {
    DBG("terminate via inline element");
    return context_finish(ctx, LAYOUT_TERMINATOR);
  }

  if (valid_tokens_test(ctx->valid_tokens, LAYOUT_END) &&
      ctx->state->layout_stack.len > 1) {
    DBG("end layout via inline element");
    indent_vec_pop(&ctx->state->layout_stack);
    return context_finish(ctx, LAYOUT_END);
  }

  return false;
}

const struct valid_tokens UNARY_OPS =
    VALID_TOKENS(TO_VT_BIT(UNARY_OP) | TO_VT_BIT(SIGIL_OP));
const char OPERATOR_CHARS[] = {
    // OP10
    /* 0 */ '$', '^',
    // OP9
    /* 2 */ '*', '%', '\\', '/',
    // OP8
    /* 6 */ '+', '-', '~', '|',
    // OP7
    /* 10 */ '&',
    // OP6
    /* 11 */ '.',
    // OP5
    /* 12 */ '=', '<', '>', '!',
    // OP2
    /* 16 */ ':', '?', '@'};
const uint16_t UNICODE_OPERATOR_CHARS[] = {
    L'∙', L'∘', L'×', L'★', L'⊗', L'⊘', L'⊙', L'⊛', L'⊠', L'⊡', L'∩',
    L'∧', L'⊓', L'±', L'⊕', L'⊖', L'⊞', L'⊟', L'∪', L'∨', L'⊔',
};

_const_ static bool is_operator(uint32_t character)
{
  for (unsigned i = 0; i < sizeof(OPERATOR_CHARS); i++) {
    if ((uint32_t)OPERATOR_CHARS[i] == character) {
      return true;
    }
  }
  if (character > UINT8_MAX) {
    for (unsigned i = 0;
         i < sizeof(UNICODE_OPERATOR_CHARS) / sizeof(*UNICODE_OPERATOR_CHARS);
         i++) {
      if ((uint32_t)UNICODE_OPERATOR_CHARS[i] == character) {
        return true;
      }
    }
  }

  return false;
}

enum operator_scan_state {
  OS_REGULAR,
  OS_COLON,
  OS_COLON_COLON,
  OS_DOT,
  OS_EQUAL,
  OS_MINUS,
  OS_STAR,
};

#ifdef TREE_SITTER_INTERNAL_BUILD
const char* const OPERATOR_SCAN_STATE_STR[] = {
    [OS_REGULAR] = "REGULAR",
    "COLON",
    "COLON_COLON",
    "DOT",
    "EQUAL",
    "MINUS",
    "STAR"};
#endif

_nonnull_(1) static enum token_type scan_operator(
    struct context* ctx, bool immediate)
{
  if (immediate) {
    return TOKEN_TYPE_LEN;
  }

  enum operator_scan_state state = OS_REGULAR;
  const uint32_t first_character = context_lookahead(ctx);
  if (!is_operator(first_character)) {
    return TOKEN_TYPE_LEN;
  }

  switch (first_character) {
  case '.':
    context_advance(ctx, false);
    state = OS_DOT;
    break;
  case '=':
    context_advance(ctx, false);
    state = OS_EQUAL;
    break;
  case ':':
    context_advance(ctx, false);
    state = OS_COLON;
    break;
  case '-':
    context_advance(ctx, false);
    state = OS_MINUS;
    break;
  case '*':
    context_advance(ctx, false);
    state = OS_STAR;
    break;
  default:
    break;
  }

  while (is_operator(context_lookahead(ctx))) {
    switch (state) {
    case OS_STAR:
      switch (context_lookahead(ctx)) {
      case ':':
        goto loop_end;
      default:
        state = OS_REGULAR;
        break;
      }
      break;
    case OS_COLON:
      switch (context_lookahead(ctx)) {
      case ':':
        state = OS_COLON_COLON;
        context_advance(ctx, false);
        break;
      default:
        state = OS_REGULAR;
        break;
      }
      break;
    case OS_COLON_COLON:
    case OS_DOT:
    case OS_EQUAL:
    case OS_MINUS:
    case OS_REGULAR:
      state = OS_REGULAR;
      context_advance(ctx, false);
      break;
    }
  }
loop_end:
  switch (state) {
  case OS_EQUAL:
  case OS_COLON:
  case OS_COLON_COLON:
  case OS_DOT:
    return TOKEN_TYPE_LEN;
  case OS_MINUS:
    if (is_digit(context_lookahead(ctx))) {
      return TOKEN_TYPE_LEN;
    }
    break;
  case OS_STAR:
    if (valid_tokens_test(ctx->valid_tokens, WANT_EXPORT_MARKER)) {
      return TOKEN_TYPE_LEN;
    }
    break;
  default:
    break;
  }

  switch (context_lookahead(ctx)) {
  case ' ':
  case '\n':
  case '\r':
    return TOKEN_TYPE_LEN;
  default:
    return UNARY_OP;
  }
}

LEX_FN(lex_operators, bool immediate)
{
  if (!valid_tokens_any_valid(ctx->valid_tokens, UNARY_OPS)) {
    return false;
  }

  uint32_t first_character = context_lookahead(ctx);
  enum token_type result = scan_operator(ctx, immediate);
  if (result == TOKEN_TYPE_LEN) {
    if (first_character == '.') {
      TRY_LEX(ctx, lex_inline_layout);
    }
    return false;
  }

  result = first_character == '@' ? SIGIL_OP : UNARY_OP;
  DBG_F("operator final classification: %s\n", TOKEN_TYPE_STR[result]);

  if (!valid_tokens_test(ctx->valid_tokens, result)) {
    return false;
  }

  context_mark_end(ctx);
  return context_finish(ctx, result);
}

LEX_FN(lex_main)
{
  TRY_LEX(ctx, lex_init);

  TRY_LEX(ctx, lex_comment_content);
  TRY_LEX(ctx, lex_long_string_quote);

  size_t spaces = scan_spaces(ctx, false);

  TRY_LEX(ctx, lex_indent);
  TRY_LEX(ctx, lex_operators, spaces == 0);
  TRY_LEX(ctx, lex_inline_layout);

  return false;
}

void* tree_sitter_nim_external_scanner_create(void)
{
#ifdef TREE_SITTER_INTERNAL_BUILD
  debug_mode = getenv("TREE_SITTER_DEBUG");
#endif

  struct state* state = state_new();
  if (!state) {
    DBG("error: could not allocate a new state object!");
  }
  return state;
}

void tree_sitter_nim_external_scanner_destroy(void* payload)
{
  state_destroy((struct state*)payload);
}

unsigned tree_sitter_nim_external_scanner_serialize(
    void* payload, char* buffer)
{
  if (!payload || !buffer) {
    DBG("error: no payload or buffer");
    return 0;
  }
  return state_serialize(
      (struct state*)payload, buffer, TREE_SITTER_SERIALIZATION_BUFFER_SIZE);
}

void tree_sitter_nim_external_scanner_deserialize(
    void* payload, const char* buffer, unsigned length)
{
  if (!payload) {
    DBG("no payload, skipping");
    return;
  }
  state_deserialize((struct state*)payload, buffer, length);
}

bool tree_sitter_nim_external_scanner_scan(
    void* payload, TSLexer* lexer, const bool* valid_tokens)
{
  if (!payload || !lexer || !valid_tokens) {
    DBG("error: some parameters are not provided");
    return false;
  }

  DBG("begin");
  struct context ctx = {0};
  ctx._lexer = lexer;
  ctx.state = (struct state*)payload;
  ctx.valid_tokens = valid_tokens_from_array(valid_tokens);

  valid_tokens_debug(ctx.valid_tokens);
  state_debug(ctx.state);
  context_mark_end(&ctx);

  bool found = lex_main(&ctx);

  DBG(found ? "commit" : "end");
  state_debug(ctx.state);
  return found;
}
