#include "tree_sitter/alloc.h"
#include "tree_sitter/array.h"
#include "tree_sitter/parser.h"

enum TokenType {
	INDENT,
	DEDENT,
	NEWLINE,
};

typedef struct {
	Array(uint16_t) indents;
} Scanner;

static inline void advance(TSLexer *lexer) {
	lexer->advance(lexer, false);
}

static inline void skip(TSLexer *lexer) {
	lexer->advance(lexer, true);
}

bool tree_sitter_pine_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
	Scanner *scanner = (Scanner *)payload;

	lexer->mark_end(lexer);

	bool found_end_of_line = false;
	uint32_t indent_length = 0;

	for (;;) {
		switch (lexer->lookahead) {
			case '\n': {
				found_end_of_line = true;
				indent_length = 0;
				skip(lexer);
				break;
			}
			case ' ': {
				indent_length++;
				skip(lexer);
				break;
			}
			case '\r':
			case '\f': {
				indent_length = 0;
				skip(lexer);
				break;
			}
			case '\t': {
				indent_length += 4;
				skip(lexer);
				break;
			}
			case '\\': {
				skip(lexer);
				if (lexer->lookahead == '\r') {
					skip(lexer);
				}
				if (lexer->lookahead == '\n' || lexer->eof(lexer)) {
					skip(lexer);
				} else {
					return false;
				}
				break;
			}
			default: {
				if (lexer->eof(lexer)) {
					indent_length = 0;
					found_end_of_line = true;
				}
				goto next;
			}
		}
	}

next:
	if (indent_length % 4 != 0) {
		// line continue
		return false;
	}

	if (found_end_of_line) {
		if (scanner->indents.size > 0) {
			uint16_t current_indent_length = *array_back(&scanner->indents);

			if (valid_symbols[INDENT] && indent_length > current_indent_length) {
				array_push(&scanner->indents, indent_length);
				lexer->result_symbol = INDENT;
				return true;
			}

			if (valid_symbols[DEDENT] && indent_length < current_indent_length) {
				array_pop(&scanner->indents);
				lexer->result_symbol = DEDENT;
				return true;
			}
		}

		if (valid_symbols[NEWLINE]) {
			lexer->result_symbol = NEWLINE;
			return true;
		}
	}

	return false;
}


unsigned tree_sitter_pine_external_scanner_serialize(void *payload, char *buffer) {
	Scanner *scanner = (Scanner *)payload;

	size_t size = 0;

	for (uint32_t i = 1; i < scanner->indents.size && size < TREE_SITTER_SERIALIZATION_BUFFER_SIZE; i ++) {
		buffer[size++] = (char)*array_get(&scanner->indents, i);
	}
	return size;
}

void tree_sitter_pine_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {
	Scanner *scanner = (Scanner *)payload;

	array_delete(&scanner->indents);
	array_push(&scanner->indents, 0);

	if (length <= 0) {
		return;
	}

	for (size_t size = 0; size < length; size ++) {
		array_push(&scanner->indents, (unsigned char)buffer[size]);
	}
}

void *tree_sitter_pine_external_scanner_create() {
	Scanner *scanner = (Scanner *)ts_calloc(1, sizeof(Scanner));
	array_init(&scanner->indents);
	tree_sitter_pine_external_scanner_deserialize(scanner, NULL, 0);
	return scanner;
}

void tree_sitter_pine_external_scanner_destroy(void *payload) {
	Scanner *scanner = (Scanner *)payload;
	array_delete(&scanner->indents);
	free(scanner);
}

