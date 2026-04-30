#include "tree_sitter/parser.h"
#include <stdio.h>
#include <string.h>
#include "unicode.h"

// #define unreachable() fprintf(stderr, "unreachable src/scanner.c:%d\n", __LINE__)
// #define assert(a, ...) \
// 	if (!(a)) {\
// 		fprintf(stderr, __VA_ARGS__);\
// 		exit(EXIT_FAILURE);\
// 	}

#define unreachable() while (false);
#define assert(a, ...) while (false);


enum token_type {
	TOKEN_INDENT,
	TOKEN_DEDENT,
	TOKEN_REDENT,

	TOKEN_LINE_START_CHECK,

	TOKEN_CONTENT,
	TOKEN_STRONG,
	TOKEN_EMPH,
	TOKEN_BARRIER,
	TOKEN_BRACKET,
	TOKEN_SECTION,
	TOKEN_TERMINATION,

	TOKEN_INLINED_ITEM_END,
	TOKEN_INLINED_STMT_END,
	TOKEN_BLOCKED_EXPR_END,
	TOKEN_MATH_LETTER,
	TOKEN_MATH_IDENT,
	TOKEN_MATH_FRAC,
	TOKEN_MATH_GROUP_END,
	TOKEN_ELSE,
	TOKEN_UNIT,
	TOKEN_URL,
	TOKEN_ITEM,
	TOKEN_TERM,
	TOKEN_HEAD_1,
	TOKEN_HEAD_2,
	TOKEN_HEAD_3,
	TOKEN_HEAD_4,
	TOKEN_HEAD_5,
	TOKEN_HEAD_P,
	TOKEN_STRING_BLOB,
	TOKEN_RAW_SPAN_BLOB,
  TOKEN_RAW_BLCK_LDLM,
  TOKEN_RAW_BLCK_RDLM,
	TOKEN_RAW_BLCK_BLOB,
	TOKEN_RAW_LANG,
	TOKEN_IDENTIFIER,
	TOKEN_LABEL,
	TOKEN_ANTI_MARKUP,

	TOKEN_COMMENT,
	TOKEN_SPACE,

	TOKEN_IMMEDIATE_SET,
	TOKEN_IMMEDIATE_PAREN,
	TOKEN_IMMEDIATE_BRACK,
	TOKEN_IMMEDIATE_IDENT,
	TOKEN_IMMEDIATE_MATH_CALL,
	TOKEN_IMMEDIATE_MATH_APPLY,
	TOKEN_IMMEDIATE_MATH_FIELD,
	TOKEN_IMMEDIATE_MATH_PRIME,

	// error recovery state detection
	TOKEN_RECOVERY,
};

enum container {
	CONTAINER_CONTENT,
	CONTAINER_STRONG,
	CONTAINER_EMPH,
	CONTAINER_BARRIER,
	CONTAINER_BRACKET,
	CONTAINER_SECTION,
	// the SECTION container is special, as any index superior is considered a section
	// the distance to the original value correspond to the level of the section
};

// when a container ends
enum termination {
	// not the end
	TERMINATION_NONE,
	// termination character found
	TERMINATION_INCLUSIVE,
	// termination found without character
	TERMINATION_EXCLUSIVE,
};

#define lex_accept(a) \
	lexer->result_symbol = a;\
	lexer->mark_end(lexer);\
	return true;

#define lex_advance_if(a) \
	if (!(a)) { return false; }\
	lexer->advance(lexer, false);

#define lex_advance() \
	lexer->advance(lexer, false);

#define lex_next lexer->lookahead

static bool is_sp(uint32_t c) {
	return (
		c == ' ' ||
		c == '\t' ||
		c == 0x20 ||
		c == 0x1680 ||
		(c >= 0x2000 && c <= 0x200a) ||
		c == 0x202f ||
		c == 0x205f ||
		c == 0x3000
	);
}
static bool is_lb(uint32_t c) {
	return (
		c == '\n' ||
		c == '\r' ||
		c == '\v' ||
		c == '\f' ||
		c == 0x85 ||
		c == 0x2028 ||
		c == 0x2029
	);
}

#define is_id_start(c) unicode_class(ucd_table_xid_start, 0, UCD_LEN_XID_START, c)
#define is_id_continue(c) unicode_class(ucd_table_xid_continue, 0, UCD_LEN_XID_CONTINUE, c)
#define is_word_part(c) unicode_class(ucd_table_in_word, 0, UCD_LEN_IN_WORD, c)
static bool unicode_class(struct unicode_range t[], size_t min, size_t max, uint32_t c) {
	while (max - min > 1) {
		size_t mid = (min + max) / 2;
		if (c < t[mid].min) {
			max = mid;
		}
		else {
			min = mid;
		}
	}
	return t[min].min <= c && c <= t[min].max;
}

// vec<u32> ////////////////////////////////////////////////////////////////////
struct vec_u32 {
	size_t cap;
	size_t len;
	uint32_t* vec;
};
static struct vec_u32 vec_u32_new() {
	return (struct vec_u32) {.cap = 0, .len = 0, .vec = NULL};
}
static void vec_u32_drop(struct vec_u32 self) {
	if (self.vec != NULL) {
		free(self.vec);
	}
}
static void vec_u32_push(struct vec_u32* self, uint32_t value) {
	assert(self != NULL, "vec_u32_push");
	if (self->len + 1 > self->cap) {
		self->cap = self->len + 8;
		self->vec = realloc(self->vec, sizeof(uint32_t) * self->cap);
		assert(self->vec != NULL, "vec_u32_push: malloc failed");
	}
	self->vec[self->len++] = value;
}
static uint32_t vec_u32_pop(struct vec_u32* self) {
	assert(self != NULL, "vec_u32_pop");
	assert(self->len > 0, "vec_u32_pop: empty vec");
	return self->vec[self->len--];
}
static size_t vec_u32_serialize(struct vec_u32* self, char* buffer) {
	assert(self != NULL, "vec_u32_serialize");
	size_t written = 0;
	memcpy(buffer, &self->len, sizeof self->len);
	written += sizeof self->len;
	if (self->len > 0) {
		memcpy(buffer + written, self->vec, self->len * sizeof(uint32_t));
		written += self->len * sizeof(uint32_t);
	}
	return written;
}
static size_t vec_u32_deserialize(struct vec_u32* self, const char* buffer) {
	assert(self != NULL, "vec_u32_deserialize");
	size_t read = 0;
	memcpy(&self->len, buffer, sizeof self->len);
	read += sizeof self->len;
	if (self->len > self->cap) {
		self->cap = self->len;
		self->vec = realloc(self->vec, sizeof(uint32_t) * self->cap);
		assert(self->vec != NULL, "vec_u32_deserialize: malloc failed")
	}
	if (self->len > 0) {
		memcpy(self->vec, buffer + read, self->len * sizeof *self->vec);
		read += self->len * sizeof *self->vec;
	}
	return read;
}
//////////////////////////////////////////////////////////////////// vec<u32> //


struct scanner {
	struct vec_u32 indentation;
	struct vec_u32 containers;
	// this is used in URL parsing, as a stack to remember matching delimiters
	// so, it does not hold states between tokens
	struct vec_u32 worker;
	// if directly after a token which is not space or comment token
	bool immediate;
	uint8_t heading_level;
	bool line_start;
	uint8_t raw_level;
};

static void scanner_redent(struct scanner* self, uint32_t col) {
	if (self->indentation.len == 0) {
		// redent on base line
		// not supposed to happen
		unreachable();
		return;
	}
	self->indentation.vec[self->indentation.len - 1] = col;
}
static void scanner_dedent(struct scanner* self) {
	if (self->indentation.len == 0) {
		return;
	}
	vec_u32_pop(&self->indentation);
}
static void scanner_indent(struct scanner* self, uint32_t col) {
	vec_u32_push(&self->indentation, col);
}
static enum container scanner_container_at(struct scanner* self, size_t at) {
	assert(self->containers.len > at, "not inside a container");
	return self->containers.vec[self->containers.len - 1 - at];
}
static enum termination scanner_termination(struct scanner* self, TSLexer* lexer, const bool* valid, size_t at) {
	if (self->containers.len == at) {
		// no container
		return lexer->eof(lexer) ? TERMINATION_EXCLUSIVE : TERMINATION_NONE;
	}
	enum container container = scanner_container_at(self, at);
	switch (container) {
		case CONTAINER_BRACKET:
		case CONTAINER_CONTENT:
		break;
		default:
		if (lex_next == ']') return TERMINATION_EXCLUSIVE;
	}
	switch (container) {
		case CONTAINER_BRACKET: {
			if (lexer->eof(lexer)) return TERMINATION_EXCLUSIVE;
			if (lex_next == ']') return TERMINATION_INCLUSIVE;
			if (self->containers.len > 1 && scanner_termination(self, lexer, valid, at + 1) != TERMINATION_NONE) return TERMINATION_EXCLUSIVE;
			return TERMINATION_NONE;
		}

		case CONTAINER_CONTENT:
		return lex_next == ']' ? TERMINATION_INCLUSIVE : TERMINATION_NONE;

		case CONTAINER_STRONG:
		return lex_next == '*' ? TERMINATION_INCLUSIVE : TERMINATION_NONE;

		case CONTAINER_EMPH:
		return lex_next == '_' ? TERMINATION_INCLUSIVE : TERMINATION_NONE;

		case CONTAINER_BARRIER: {
			if (lex_next == ']') return TERMINATION_EXCLUSIVE;
			if (is_lb(lex_next)) {
				return TERMINATION_EXCLUSIVE;
			}
			if (lexer->eof(lexer)) {
				return TERMINATION_EXCLUSIVE;
			}
			if (self->containers.len > 1 + at) {
				switch (self->containers.vec[self->containers.len - 2 - at]) {
					case CONTAINER_EMPH:
					case CONTAINER_STRONG:
					// inside emph or strong, a new line is mandatory
					return TERMINATION_NONE;

					case CONTAINER_BARRIER:
					// not supposed to happen
					unreachable();
					return TERMINATION_NONE;

					case CONTAINER_CONTENT:
					return lex_next == ']' ? TERMINATION_EXCLUSIVE : TERMINATION_NONE;
				}
			}
			return TERMINATION_NONE;
		} break;

		case CONTAINER_SECTION:
		default: {
			if (self->containers.len > 1 && scanner_termination(self, lexer, valid, at + 1) != TERMINATION_NONE) return TERMINATION_EXCLUSIVE;
			if (lexer->eof(lexer)) return TERMINATION_EXCLUSIVE;
			if (lex_next == ']') return TERMINATION_EXCLUSIVE;
			return TERMINATION_NONE;
		}
	}
	return TERMINATION_NONE;
}

static void scanner_container_push(struct scanner* self, enum container container) {
	vec_u32_push(&self->containers, container);
}
static void scanner_container_pop(struct scanner* self) {
	if (self->containers.len == 0) {
		return;
	}
	vec_u32_pop(&self->containers);
}



void * tree_sitter_typst_external_scanner_create() {
	struct scanner* self = malloc(sizeof(struct scanner));
	assert(self != NULL, "malloc failed");
	self->indentation = vec_u32_new();
	self->containers = vec_u32_new();
	self->worker = vec_u32_new();
	self->immediate = false;
	self->heading_level = 0;
	self->line_start = false;
	self->raw_level = 0;
	return self;
}

void tree_sitter_typst_external_scanner_destroy(void *payload) {
	struct scanner* self = payload;
	vec_u32_drop(self->indentation);
	vec_u32_drop(self->containers);
	vec_u32_drop(self->worker);
	free(self);
}

unsigned tree_sitter_typst_external_scanner_serialize(
	void *payload,
	char *buffer
) {
	struct scanner* self = payload;
	size_t written = 0;
	written += vec_u32_serialize(&self->indentation, buffer + written);
	written += vec_u32_serialize(&self->containers, buffer + written);
	buffer[written++] = self->immediate;
	buffer[written++] = self->heading_level;
	buffer[written++] = self->line_start;
	buffer[written++] = self->raw_level;
	return written;
}

void tree_sitter_typst_external_scanner_deserialize(
	void *payload,
	const char *buffer,
	unsigned length
) {
	struct scanner* self = payload;
	self->indentation.len = 0;
	self->containers.len = 0;
	self->worker.len = 0;
	self->immediate = false;
	self->heading_level = 0;
	self->line_start = false;
	self->raw_level = 0;
	if (length != 0) {
		size_t read = 0;
		read += vec_u32_deserialize(&self->indentation, buffer + read);
		read += vec_u32_deserialize(&self->containers, buffer + read);
		self->immediate = buffer[read++];
		self->heading_level = buffer[read++];
		self->line_start = buffer[read++];
		self->raw_level = buffer[read++];
	}
	else {
		vec_u32_push(&self->indentation, 0);
	}
}

// SCAN ////////////////////////////////////////////////////////////////////////
static bool parse_comment(struct scanner* self, TSLexer* lexer) {
	lex_advance_if(lex_next == '/');
	if (lex_next == '/') {
		lex_advance();
		while (!lexer->eof(lexer) && !is_lb(lex_next)) {
			lex_advance();
		}
		self->immediate = false;
		lex_accept(TOKEN_COMMENT);
	}
	if (lex_next == '*') {
		lex_advance();

		unsigned int level = 0;
		while (!lexer->eof(lexer)) {
			if (lex_next == '*') {
				lex_advance();
				if (lex_next == '/') {
					lex_advance();
					if (level == 0) break;
					level -= 1;
				}
			}
			else if (lex_next == '/') {
				lex_advance();
				if (lex_next == '*') {
					lex_advance();
					level += 1;
				}
			}
			else {
				lex_advance();
			}
		}
		self->immediate = false;
		lex_accept(TOKEN_COMMENT);
	}
	return false;
}
static bool parse_space(struct scanner* self, TSLexer* lexer) {
	lex_advance_if(is_sp(lex_next));
	while (is_sp(lex_next)) {
		lex_advance();
	}
	self->immediate = false;
	lex_accept(TOKEN_SPACE);
}

bool tree_sitter_typst_external_scanner_scan(
	void *payload,
	TSLexer *lexer,
	const bool *valid_symbols
) {
	struct scanner* self = payload;
	lexer->mark_end(lexer);

	if (valid_symbols[TOKEN_RECOVERY]) {
		// the external scanner don't try any recovery
		lexer->result_symbol = TOKEN_RECOVERY;
		return true;
	}

	// must be before SPACE and COMMENT
	// this set the immediate flag to true
	if (valid_symbols[TOKEN_IMMEDIATE_SET]) {
		self->immediate = true;
		lexer->result_symbol = TOKEN_IMMEDIATE_SET;
		return true;
	}

	if (valid_symbols[TOKEN_IDENTIFIER] && (is_id_start(lex_next) || lex_next == '_')) {
		lex_advance();
		while (is_id_continue(lex_next) || lex_next == '-') {
			lex_advance();
		}
		self->immediate = true;
		lex_accept(TOKEN_IDENTIFIER);
	}

	if (valid_symbols[TOKEN_LABEL] && (is_id_continue(lex_next) || lex_next == '-')) {
		lex_advance();
    lexer->mark_end(lexer);
		while (
      is_id_continue(lex_next) ||
      lex_next == '-' ||
      lex_next == '.' ||
      lex_next == ':'
    ) {
      bool up_to = lex_next != '.' && lex_next != ':';
			lex_advance();
      if (up_to) {
        lexer->mark_end(lexer);
      }
		}
		self->immediate = true;
    lexer->result_symbol = TOKEN_LABEL;
    return true;
	}


	if (valid_symbols[TOKEN_RAW_SPAN_BLOB]) {
		while (lex_next != '`' && !lexer->eof(lexer)) {
			lex_advance();
		}
		lex_accept(TOKEN_RAW_SPAN_BLOB);
	}

	if (valid_symbols[TOKEN_RAW_LANG] && (lex_next == '_' || is_id_start(lex_next))) {
		lex_advance();
		while (lex_next == '-' || is_id_continue(lex_next)) {
			lex_advance();
		}
		lex_accept(TOKEN_RAW_LANG);
	}

	if (valid_symbols[TOKEN_RAW_BLCK_LDLM] && lex_next == '`') {
		lex_advance();
		lex_advance_if(lex_next == '`');
		lex_advance_if(lex_next == '`');
		uint8_t level = 3;
		while (lex_next == '`') {
			lex_advance();
			level += 1;
		}
		self->raw_level = level;
		assert(self->raw_level >= 3, "invalid raw level value %d", self->raw_level);
		lex_accept(TOKEN_RAW_BLCK_LDLM);
	}

	if (valid_symbols[TOKEN_RAW_BLCK_BLOB]) {
		assert(self->raw_level >= 3, "invalid raw level value %d", self->raw_level);
		while (!lexer->eof(lexer)) {
			uint8_t level = 0;
			while (lex_next == '`') {
				lex_advance();
				level += 1;
				if (level == self->raw_level) {
					lexer->result_symbol = TOKEN_RAW_BLCK_BLOB;
					return true;
				}
			}
			lex_advance();
			lexer->mark_end(lexer);
		}
		return false;
	}

	if (valid_symbols[TOKEN_RAW_BLCK_RDLM]) {
		assert(self->raw_level >= 3, "invalid raw level value %d", self->raw_level);
		for (uint8_t level = 0; level < self->raw_level; level += 1) {
			lex_advance_if(lex_next == '`');
		}
		self->raw_level = 0;
		lex_accept(TOKEN_RAW_BLCK_RDLM);
	}

	if (valid_symbols[TOKEN_URL]) {
		self->worker.len = 0;
		enum { BRACK, PAREN, };
		for (; true; lexer->advance(lexer, false)) {
			uint32_t c = lex_next;
			size_t len = self->worker.len;
			if (
				(c >= 'a' && c <= 'z') ||
				(c >= 'A' && c <= 'Z') ||
				(c >= '0' && c <= '9') ||
				(c == '!') ||
				(c == '#') ||
				(c == '$') ||
				(c == '%') ||
				(c == '&') ||
				(c == '*') ||
				(c == '+') ||
				(c == ',') ||
				(c == '-') ||
				(c == '.') ||
				(c == '/') ||
				(c == ':') ||
				(c == ';') ||
				(c == '=') ||
				(c == '?') ||
				(c == '@') ||
				(c == '_') ||
				(c == '~') ||
				(c == '\'')
			) {
			}
			else if (c == '[') {
				vec_u32_push(&self->worker, BRACK);
			}
			else if (c == '(') {
				vec_u32_push(&self->worker, PAREN);
			}
			else if (c == ']' && len > 0 && self->worker.vec[len - 1] == BRACK) {
				vec_u32_pop(&self->worker);
			}
			else if (c == ')' && len > 0 && self->worker.vec[len - 1] == PAREN) {
				vec_u32_pop(&self->worker);
			}
			else {
				lex_accept(TOKEN_URL);
			}
		}
	}

	// the end of strong, emph, content, item line, term line, heading line
	if (valid_symbols[TOKEN_TERMINATION]) {
		switch (scanner_termination(self, lexer, valid_symbols, 0)) {
			case TERMINATION_NONE: break;
			case TERMINATION_INCLUSIVE:
			lexer->advance(lexer, false);
			lexer->mark_end(lexer);
			if (scanner_container_at(self, 0) != CONTAINER_BRACKET) {
				scanner_dedent(self);
			}
			case TERMINATION_EXCLUSIVE:
			scanner_container_pop(self);
			lexer->result_symbol = TOKEN_TERMINATION;
			return true;
		}
	}

	// must be before SPACE and COMMENT
	// character in a string literal
	if (valid_symbols[TOKEN_STRING_BLOB]) {
		if (!lexer->eof(lexer) && lex_next != '\\' && lex_next != '"') {
			lex_advance();
			lex_accept(TOKEN_STRING_BLOB);
		}
	}

	// suffix to number literal
	if (valid_symbols[TOKEN_UNIT] && self->immediate) {
		if (lex_next == '%') {
			lex_advance();
			lex_accept(TOKEN_UNIT);
		}
		if (lex_next >= 'a' && lex_next <= 'z') {
			lex_advance();
			while (lex_next >= 'a' && lex_next <= 'z') {
				lex_advance();
			}
			lex_accept(TOKEN_UNIT);
		}
	}
	if (valid_symbols[TOKEN_INLINED_ITEM_END]) {
		if (self->immediate) {
			if (valid_symbols[TOKEN_IMMEDIATE_BRACK] && lex_next == '[') {
				lex_accept(TOKEN_IMMEDIATE_BRACK);
			}
			if (valid_symbols[TOKEN_IMMEDIATE_PAREN] && lex_next == '(') {
				lex_accept(TOKEN_IMMEDIATE_PAREN);
			}
		}
		if (valid_symbols[TOKEN_ELSE]) {
			if (parse_space(self, lexer)) {
				return true;
			}
			if (parse_comment(self, lexer)) {
				return true;
			}
			if (lex_next != 'e') {
				lexer->result_symbol = TOKEN_INLINED_ITEM_END;
				return true;
			}
			lex_advance();
			if (lex_next != 'l') {
				lexer->result_symbol = TOKEN_INLINED_ITEM_END;
				return true;
			}
			lex_advance();
			if (lex_next != 's') {
				lexer->result_symbol = TOKEN_INLINED_ITEM_END;
				return true;
			}
			lex_advance();
			if (lex_next != 'e') {
				lexer->result_symbol = TOKEN_INLINED_ITEM_END;
				return true;
			}
			lex_advance();
			if (!is_id_continue(lex_next) && lex_next != '-') {
				lex_accept(TOKEN_ELSE);
			}
			lexer->result_symbol = TOKEN_INLINED_ITEM_END;
			return true;
		}

		if (lex_next == '.') {
			lex_advance();
			if (lex_next == '_') {
				// Test 361 and 362
				lex_advance();
				if (is_id_continue(lex_next) || lex_next == '-') {
					return false;
				}
			}
			else if (is_id_start(lex_next)) {
				return false;
			}
			lexer->result_symbol = TOKEN_INLINED_ITEM_END;
			return true;
		}

		if (lex_next == ';') {
			lex_advance();
			lexer->mark_end(lexer);
		}

		lexer->result_symbol = TOKEN_INLINED_ITEM_END;
		return true;
	}

	// SPACE
	if (parse_space(self, lexer)) {
		return true;
	}

	if (lex_next == '/') {
		uint32_t column = lexer->get_column(lexer);

		// COMMENT
		if (parse_comment(self, lexer)) {
			return true;
		}

		if (valid_symbols[TOKEN_MATH_FRAC]) {
			lex_accept(TOKEN_MATH_FRAC);
		}

		if (is_sp(lex_next) || is_lb(lex_next)) {
			if (valid_symbols[TOKEN_LINE_START_CHECK]) {
				lexer->result_symbol = TOKEN_LINE_START_CHECK;
				self->line_start = true;
				return true;
			}
			if (valid_symbols[TOKEN_TERM] && self->line_start) {
				self->line_start = false;
				scanner_redent(self, column);
				lex_accept(TOKEN_TERM);
			}
		}
	}
	if (valid_symbols[TOKEN_LINE_START_CHECK]) {
		if (lex_next == '=') {
			while (lex_next == '=') lex_advance();
			if (
				is_sp(lex_next) ||
				is_lb(lex_next) ||
				lexer->eof(lexer)
			) {
				self->line_start = true;
			}
		}
		else if (lex_next == '-' || lex_next == '+') {
			lex_advance();
			if (
				is_sp(lex_next) ||
				is_lb(lex_next) ||
				lexer->eof(lexer)
			) {
				self->line_start = true;
			}
		}
		else if (lex_next >= '0' && lex_next <= '9') {
			while (lex_next >= '0' && lex_next <= '9') {
				lex_advance();
			}
			if (lex_next == '.') {
				lex_advance();
				if (
					is_sp(lex_next) ||
					is_lb(lex_next) ||
					lexer->eof(lexer)
				) {
					self->line_start = true;
				}
			}
		}
		lexer->result_symbol = TOKEN_LINE_START_CHECK;
		return true;
	}

	// must be after SPACE and COMMENT
	if (self->immediate) {
		if (valid_symbols[TOKEN_IMMEDIATE_BRACK] && lex_next == '[') {
			lex_accept(TOKEN_IMMEDIATE_BRACK);
		}
		if (valid_symbols[TOKEN_IMMEDIATE_PAREN] && lex_next == '(') {
			lex_accept(TOKEN_IMMEDIATE_PAREN);
		}
		if (valid_symbols[TOKEN_IMMEDIATE_IDENT] && (
			is_id_start(lex_next) || lex_next == '_'
		)) {
			lex_accept(TOKEN_IMMEDIATE_IDENT);
		}
		if (valid_symbols[TOKEN_IMMEDIATE_MATH_CALL] && lex_next == '(') {
			lex_accept(TOKEN_IMMEDIATE_MATH_CALL);
		}
		if (valid_symbols[TOKEN_IMMEDIATE_MATH_APPLY] && (
			lex_next == '(' ||
			lex_next == '[' ||
			lex_next == '{'
		)) {
			lex_accept(TOKEN_IMMEDIATE_MATH_APPLY);
		}
		if (valid_symbols[TOKEN_IMMEDIATE_MATH_PRIME] && lex_next == '\'') {
			lex_advance();
			while (lex_next == '\'') {
				lex_advance();
			}
			lex_accept(TOKEN_IMMEDIATE_MATH_PRIME);
		}
		if (valid_symbols[TOKEN_IMMEDIATE_MATH_FIELD] && lex_next == '.') {
			lex_advance();
			if (is_id_start(lex_next)) {
				lex_accept(TOKEN_IMMEDIATE_MATH_FIELD);
			}
			return false;
		}
	}

	if (valid_symbols[TOKEN_SECTION]) {
		scanner_container_push(self, CONTAINER_SECTION + self->heading_level);
		lex_accept(TOKEN_SECTION);
	}
	if (valid_symbols[TOKEN_BARRIER]) {
		scanner_container_push(self, CONTAINER_BARRIER);
		lex_accept(TOKEN_BARRIER);
	}
	if (valid_symbols[TOKEN_CONTENT] && lex_next == '[') {
		lexer->advance(lexer, false);
		scanner_indent(self, 0);
		scanner_container_push(self, CONTAINER_CONTENT);
		lex_accept(TOKEN_CONTENT);
	}
	if (valid_symbols[TOKEN_STRONG] && lex_next == '*') {
		lexer->advance(lexer, false);
		scanner_indent(self, 0);
		scanner_container_push(self, CONTAINER_STRONG);
		lex_accept(TOKEN_STRONG);
	}
	if (valid_symbols[TOKEN_EMPH] && lex_next == '_') {
		lexer->advance(lexer, false);
		scanner_indent(self, 0);
		scanner_container_push(self, CONTAINER_EMPH);
		lex_accept(TOKEN_EMPH);
	}
	if (valid_symbols[TOKEN_BRACKET] && lex_next == '[') {
		lex_advance();
		scanner_container_push(self, CONTAINER_BRACKET);
		lex_accept(TOKEN_BRACKET);
	}



	if (self->line_start && valid_symbols[TOKEN_HEAD_1] && lex_next == '=') {
		lex_advance();
		size_t count = 1;
		while (lex_next == '=') {
			lex_advance();
			count += 1;
		}
		if (
			is_sp(lex_next) ||
			is_lb(lex_next) ||
			lexer->eof(lexer)
		) {
			if (self->containers.len > 0 && scanner_container_at(self, 0) >= CONTAINER_SECTION) {
				uint8_t level = scanner_container_at(self, 0) - CONTAINER_SECTION;
				if (count <= level) {
					scanner_container_pop(self);
					lexer->result_symbol = TOKEN_TERMINATION;
					return true;
				}
			}
			self->heading_level = count;
			self->line_start = false;
			switch (count) {
				case 1: lex_accept(TOKEN_HEAD_1);
				case 2: lex_accept(TOKEN_HEAD_2);
				case 3: lex_accept(TOKEN_HEAD_3);
				case 4: lex_accept(TOKEN_HEAD_4);
				case 5: lex_accept(TOKEN_HEAD_5);
				default: lex_accept(TOKEN_HEAD_P);
			}
		}
		return false;
	}

	if (self->line_start && valid_symbols[TOKEN_ITEM]) {
		if (lex_next == '-' || lex_next == '+') {
			uint32_t column = lexer->get_column(lexer);
			lexer->advance(lexer, false);
			if (
				is_sp(lex_next) ||
				is_lb(lex_next) ||
				lexer->eof(lexer)
			) {
				self->line_start = false;
				scanner_redent(self, column);
				lex_accept(TOKEN_ITEM);
			}
			return false;
		}
		if (lex_next >= '0' && lex_next <= '9') {
			uint32_t column = lexer->get_column(lexer);
			lexer->advance(lexer, false);
			while (lex_next >= '0' && lex_next <= '9') {
				lexer->advance(lexer, false);
			}
			if (lex_next == '.') {
				lexer->advance(lexer, false);
				if (
					is_sp(lex_next) ||
					is_lb(lex_next) ||
					lexer->eof(lexer)
				) {
					self->line_start = false;
					scanner_redent(self, column);
					lex_accept(TOKEN_ITEM);
				}
			}
			return false;
		}
	}
  // this token matches `_` and `*` when they are between alphanumeric
  // characters because, in that case, they do not count as markup
	if (valid_symbols[TOKEN_ANTI_MARKUP] && is_word_part(lex_next)) {
		lex_advance();
		lex_advance_if(lex_next == '_' || lex_next == '*');
		lex_advance_if(is_word_part(lex_next));
		lex_accept(TOKEN_ANTI_MARKUP);
	}

	if (valid_symbols[TOKEN_MATH_IDENT] && is_id_start(lex_next)) {
		lexer->advance(lexer, false);
		if (
			valid_symbols[TOKEN_MATH_LETTER] && (
				lex_next == '_' ||
				!is_id_continue(lex_next)
			)
		) {
			self->immediate = true;
			lex_accept(TOKEN_MATH_LETTER);
		}
		while (lex_next != '_' && is_id_continue(lex_next)) {
			lexer->advance(lexer, false);
		}
		self->immediate = true;
		lex_accept(TOKEN_MATH_IDENT);
	}

	if (valid_symbols[TOKEN_MATH_GROUP_END]) {
		if (lex_next == ')' || lex_next == ']' || lex_next == '}') {
			lex_advance();
			lex_accept(TOKEN_MATH_GROUP_END);
		}
		if (lex_next == '$') {
			lex_accept(TOKEN_MATH_GROUP_END);
		}
		if (lex_next == '|') {
			lex_advance();
			lex_advance_if(lex_next == ']');
			lex_accept(TOKEN_MATH_GROUP_END);
		}
	}

	if (valid_symbols[TOKEN_ELSE]) {
		if (valid_symbols[TOKEN_BLOCKED_EXPR_END]) {
			lexer->result_symbol = TOKEN_BLOCKED_EXPR_END;
			while (is_sp(lex_next) || is_lb(lex_next)) {
				lex_advance();
			}
			if (lex_next != 'e') return true;
			lex_advance();
			if (lex_next != 'l') return true;
			lex_advance();
			if (lex_next != 's') return true;
			lex_advance();
			if (lex_next != 'e') return true;
			lex_advance();
			if (
				lex_next == '[' ||
				lex_next == '{' ||
				lex_next == '/' ||
				is_sp(lex_next)
			) {
				lex_accept(TOKEN_ELSE);
			}
			return true;
		}
		else {
			lex_advance_if(lex_next == 'e');
			lex_advance_if(lex_next == 'l');
			lex_advance_if(lex_next == 's');
			lex_advance_if(lex_next == 'e');
			if (
				lex_next == '[' ||
				lex_next == '{' ||
				lex_next == '/' ||
				is_sp(lex_next)
			) {
				lex_accept(TOKEN_ELSE);
			}
			return false;
		}
	}

	if (valid_symbols[TOKEN_BLOCKED_EXPR_END]) {
		if (lex_next == '}') {
			lexer->result_symbol = TOKEN_BLOCKED_EXPR_END;
			return true;
		}
		if (lex_next == ';') {
			lex_advance();
			lex_accept(TOKEN_BLOCKED_EXPR_END);
		}
		if (is_lb(lex_next)) {
			lexer->mark_end(lexer);
			lex_advance();
			while (is_lb(lex_next) || is_sp(lex_next)) {
				lexer->advance(lexer, false);
			}
			if (lex_next == '.') {
				return false;
			}
			lexer->result_symbol = TOKEN_BLOCKED_EXPR_END;
			return true;
		}
		return false;
	}

	if (valid_symbols[TOKEN_INLINED_STMT_END]) {
		while (is_sp(lex_next)) {
			lexer->advance(lexer, false);
		}
		if (lex_next == ';') {
			lexer->advance(lexer, false);
			lexer->mark_end(lexer);
			lexer->result_symbol = TOKEN_INLINED_STMT_END;
			return true;
		}
		if (lexer->eof(lexer) || lex_next == ']') {
			lexer->result_symbol = TOKEN_INLINED_STMT_END;
			return true;
		}
		if (is_lb(lex_next)) {
			lexer->result_symbol = TOKEN_INLINED_STMT_END;
			return true;
		}
		return false;
	}

	if (valid_symbols[TOKEN_INDENT] || valid_symbols[TOKEN_DEDENT]) {

		while (is_lb(lex_next) || is_sp(lex_next)) {
			lexer->advance(lexer, false);
		}

		// when a container terminates
		if (scanner_termination(self, lexer, valid_symbols, 0) != TERMINATION_NONE) {
			if (valid_symbols[TOKEN_DEDENT]) {
				scanner_dedent(self);
				lexer->result_symbol = TOKEN_DEDENT;
				return true;
			}
			return false;
		}

		// fprintf(stderr, "marker: %d\n", __LINE__);
		// FIXME: this is causing SEGFAULT
		// why????
		uint32_t column = lexer->get_column(lexer);
		// fprintf(stderr, "marker: %d\n", __LINE__);
		if (self->indentation.len == 0) {
			unreachable();
			return 0;
		}
		if (lex_next == ']') {
			if (valid_symbols[TOKEN_DEDENT]) {
				scanner_dedent(self);
				lexer->result_symbol = TOKEN_DEDENT;
				return true;
			}
			return false;
		}
		uint32_t indentation = self->indentation.vec[self->indentation.len - 1];
		// this is an indent
		if (column > indentation) {
			if (valid_symbols[TOKEN_INDENT]) {
				scanner_indent(self, column);
				lexer->result_symbol = TOKEN_INDENT;
				return true;
			}
			return false;
		}
		if (column < indentation) {
			// if this is a redent
			if (self->indentation.len > 1 && valid_symbols[TOKEN_REDENT]) {
				if (column > self->indentation.vec[self->indentation.len - 2]) {
					scanner_redent(self, column);
					lexer->result_symbol = TOKEN_REDENT;
					return true;
				}
			}
			// this is a dedent
			if (valid_symbols[TOKEN_DEDENT]) {
				scanner_dedent(self);
				lexer->result_symbol = TOKEN_DEDENT;
				return true;
			}
			return false;
		}
		return false;
	}

	return false;
}
