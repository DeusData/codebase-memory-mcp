/*
 * Scanner (lexer) for D code for use by Tree-Sitter.
 *
 * Copyright 2024 Garrett D'Amore
 *
 * Distributed under the MIT License.
 * (See accompanying file LICENSE.txt or https://opensource.org/licenses/MIT)
 * SPDX-License-Identifier: MIT
 */
#include "tree_sitter/parser.h"
#include <assert.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wctype.h>

// NB: It is very important that two things are true.
// First, this must match the externals in the grammar.js.
// Second, symbols and keywords must appear with least
// specific matches in front of more specific matches.
enum TokenType {
	DIRECTIVE, // # <to end of line>
	L_INT,
	L_FLOAT,
	L_STRING, // string literal (all forms)
	NOT_IN,
	NOT_IS,
	AFTER_EOF,
	ERROR,
};

static bool
is_eol(int c)
{
	return ((c == '\n') || (c == '\r') || (c == 0x2028) || (c == 0x2029));
}

// this looks for the optional suffix closer on various
// string literals (c, d, or w).  The assumption is that
// the caller will have already marked the end, and we
// can safely look ahead a little bit more.  It always
// succeeds because there is no case where we can fail --
// we simply either extend the match for the string, or we don't.
static void
match_string_suffix(TSLexer *lexer)
{
	int c = lexer->lookahead;
	if ((c == 'c') || (c == 'd') || (c == 'w')) {
		// special string form
		// advance so we include the suffix
		lexer->advance(lexer, false);
	}
	// and mark the end (regardless whether we did or did not)
	lexer->mark_end(lexer);
}

static bool
match_delimited_string(TSLexer *lexer, int start, int end)
{
	int  c;
	int  nest  = 0;
	bool first = true;
	lexer->advance(lexer, false); // skip opener
	while ((c = lexer->lookahead) != 0) {
		if (c == start && start != 0) {
			// nesting, increase the nest level
			nest++;
		}
		if (c == end) {
			if (nest > 0) {
				nest--;
			} else if (!first) {
				lexer->advance(lexer, false);
				if ((c = lexer->lookahead) != '"') {
					// do *not* advance, we already did
					// this ensures e.g. }}" will work
					continue;
				}
				lexer->advance(lexer, false);
				lexer->result_symbol = L_STRING;
				match_string_suffix(lexer);
				return (true);
			}
		}
		first = false;
		lexer->advance(lexer, false);
	}
	return (false);
}

static bool
match_heredoc_string(TSLexer *lexer)
{
	// this is an arbitrary, but reasonable limit
	// no identifiers longer than this
	int    identifier[256 + 2]; // +2 for closing " and null
	size_t i = 0;
	size_t j;
	int    c;

	// get the delimiter
	while (i < (sizeof(identifier) - 2)) {
		c = lexer->lookahead;
		// technically should not start with a digit, but we allow
		if (is_eol(c) || ((!iswalnum(c)) && (c != '_'))) {
			break;
		}
		identifier[i++] = c;
		lexer->advance(lexer, false);
	}
	if (i == 0) {
		return (false);
	}
	// inject the closing quote at the end of the identifier
	// this makes our logic below simpler
	identifier[i++] = '"';
	identifier[i]   = 0;

	while ((c = lexer->lookahead) != 0) {
		while ((!is_eol(c)) && (c != 0)) {
			lexer->advance(lexer, false);
			c = lexer->lookahead;
		}
		lexer->advance(lexer, false); // advance past the newline

		j = 0;
		while (((c = lexer->lookahead) != 0) && (j < i)) {
			if (c != identifier[j]) {
				// no match
				break;
			}
			lexer->advance(lexer, false);
			j++;
		}
		if (j == i) {
			// skip the quote
			match_string_suffix(lexer);
			lexer->result_symbol = L_STRING;
			return (true);
		}
	}
	return (false);
}

static bool
match_directive(TSLexer *lexer, const bool *valid)
{
	int c = lexer->lookahead;
	assert(c == '#');
	if (!valid[DIRECTIVE]) {
		return (false);
	}
	lexer->advance(lexer, false);
	c = lexer->lookahead;
	if (c == '!') {
		return (false);
	}
	while ((iswspace(c) || is_eol(c)) && (c)) {
		if (is_eol(c)) {
			return (false);
		}
		lexer->advance(lexer, false);
		c = lexer->lookahead;
	}

	while ((!is_eol(c)) && (c)) {
		lexer->advance(lexer, false);
		c = lexer->lookahead;
	}
	// consume the newline
	lexer->advance(lexer, false);
	lexer->mark_end(lexer);
	lexer->result_symbol = DIRECTIVE;
	return (true);
}

static bool
match_number_suffix(TSLexer *lexer, const bool *valid, bool is_float)
{
	int  c;
	bool seen_l = false;
	bool seen_i = false;
	bool seen_u = false;
	;
	bool seen_f = false;
	int  tok    = 0;
	bool done   = false;

	while (((c = lexer->lookahead) != 0) && !done) {
		switch (c) {
		case 'u':
		case 'U': // we can treat both u and U identically
			if (seen_u || seen_i || seen_f || is_float) {
				return (false);
			}
			seen_u = true;
			tok    = L_INT;
			break;

		case 'f':
		case 'F':
			if (seen_u || seen_f || seen_i) {
				return (false);
			}
			seen_f = true;
			tok    = L_FLOAT;
			break;

		case 'i':
			// i means its an imaginary floating point, and it
			// *must* be the last thing seen.  (It's also
			// deprecated.)
			if (seen_i || seen_u) {
				return (false);
			}
			tok    = L_FLOAT;
			seen_i = true;
			break;

		case 'L':
			// L can mean long, or it can mean real
			// it conflicts with f, F, and must appear before i.
			if (seen_l || seen_f || seen_i) {
				return (false);
			}
			seen_l = true;
			break;

		default:
			done = true;
			break;
		}
		if (!done) {
			lexer->advance(lexer, false);
		}
	}

	// what follows the suffix must *NOT* be digit or a number.
	if (iswalnum(c) || (c > 0x7f && !is_eol(c))) {
		return (false);
	}
	if (is_float) {
		tok = L_FLOAT;
	}
	if (valid[L_INT] && tok != L_FLOAT) {
		lexer->result_symbol = L_INT;
		lexer->mark_end(lexer);
		return (true);
	}
	if (valid[L_FLOAT] && tok != L_INT) {
		lexer->result_symbol = L_FLOAT;
		lexer->mark_end(lexer);
		return (true);
	}
	return (false);
}

static bool
match_number(TSLexer *lexer, const bool *valid)
{
	// starting item can be a digit or .
	// if it is '.', then it can be the start of a number,
	// the "." by itself, or "..", or "...".  We handle
	// the forms here in this one place to make it easy.
	// Note that there are ambiguities in the way DMD handles
	// decimal points.  For example, 0..stringof is illegal,
	// but so is 00.stringof, but 0.stringof is ok and so is 00.
	// Some of this feels like grammar bugs in DMD's implementation
	// and we have not elected to be bug-for-bug compatible.
	int  c = lexer->lookahead;
	int  next;
	bool is_hex    = false;
	bool is_bin    = false;
	bool has_digit = false;
	bool has_dot   = false;
	bool in_exp    = false;

	if (c == '.') {
		lexer->advance(lexer, false);
		c = lexer->lookahead;

		// at this point, we either have a digit, or
		// something that is not a valid number (and thus the sole dot
		// might apply from above).  Note that underscores are *not*
		// permitted as the first character after the decimal point.
		if (!iswdigit(c)) {
			return (false);
		}
		has_dot = true;

	} else if (c == '0') {
		// the next value can be b, B, x, X indicating a base,
		// a dot (making this a floating point number) or a digit or an
		// underscore. if it is anything else, then we have just the
		// value 0 (but it might have a suffix -- for example 0f)
		lexer->advance(lexer, false);
		c = lexer->lookahead;
		switch (c) {
		case 'b':
		case 'B':
			is_bin = true;
			lexer->advance(lexer, false);
			break;
		case 'x':
		case 'X':
			is_hex = true;
			lexer->advance(lexer, false);
			break;
		default:
			has_digit = true;
			break;
		}
	}

	if (!(valid[L_INT] || valid[L_FLOAT])) {
		return (false);
	}

	bool done = false;
	while (((next = lexer->lookahead) != 0) && (!done)) {
		c    = next;
		if ((c > 0x7f) || iswspace(c) || (c == ';')) {
			// optimization: not a valid number, that ends the
			// sequence
			break;
		}
		if ((is_bin) && ((c == '0') || (c == '1'))) {
			lexer->advance(lexer, false);
			lexer->mark_end(lexer);
			has_digit = true;
			continue;
		} else if (iswdigit(c) ||
		    (is_hex && (!in_exp) && (iswxdigit(c)))) {
			lexer->advance(lexer, false);
			lexer->mark_end(lexer);
			has_digit = true;
			continue;
		}

		switch (c) {
		case '.':
			// if we already have a decimal, or we haven't yet
			// collected one, or we are in the exponent, then this
			// dot is not part of our number.  (If we haven't seen
			// a digit yet, then this will be a failed parse.)
			// also binary numbers don't support floating point.
			if (!has_digit || has_dot || in_exp || is_bin) {
				lexer->mark_end(lexer);
				done = true;
				break;
			}
			lexer->mark_end(lexer);
			lexer->advance(lexer, false);
			c = lexer->lookahead;
			// if the next character is a valid digit (note that
			// binary doesn't support this, then we're good
			if (iswdigit(c) || (is_hex && iswxdigit(c))) {
				has_dot = true;
				continue;
			}
			// look ahead to see if the next thing looks like a
			// property. if it does, then this is a property
			// lookup, otherwise we want to consume the dot and
			// make it a floating point number. Note that lone
			// trailing periods like 1. are nuts, but this is what
			// DMD does.
			if (iswalnum(c) || c == '_' || c == '.' ||
			    (c > 0x7f && !is_eol(c))) {
				// its something like ._property or somesuch
				// in that case we just want the original int,
				// without the period.
				lexer->result_symbol = L_INT;
				return (valid[L_INT]);
			}
			lexer->result_symbol = L_FLOAT;
			lexer->mark_end(lexer);
			return (valid[L_FLOAT]);

		case '_':
			// an embedded (or possibly trailing) underscore.
			lexer->advance(lexer, false);
			continue;

		case 'e':
		case 'E':
		case 'p':
		case 'P':
			if (in_exp || is_bin) {
				return (false);
			}
			if (is_hex && (c == 'e' || c == 'E')) {
				return (false);
			}
			if ((!is_hex) && (c == 'p' || c == 'P')) {
				return (false);
			}
			lexer->advance(lexer, false);
			c = lexer->lookahead;
			if ((c == '+') || (c == '-')) {
				lexer->advance(lexer, false);
			}
			has_digit = false; // so we need
			in_exp    = true;
			continue;

		default:
			done = true;
			break;
		}
	}
	if (!has_digit) {
		return (false);
	}
	return (match_number_suffix(lexer, valid, has_dot || in_exp));
}

static bool
match_not_in_is(TSLexer *lexer, const bool *valid)
{
    int c;
    int token;
    if (!valid[NOT_IN] && !valid[NOT_IS]) {
        return (false);
    }
    assert(lexer->lookahead == '!');
    lexer->advance(lexer, false);
    // eat intervening whitespace... usually there isn't any
    while ((c = lexer->lookahead) != 0) {
        if (!iswspace(c) && !is_eol(c)) {
            break;
        }
        lexer->advance(lexer, false);
    }

    if (lexer->lookahead != 'i') {
        return (false);
    }
    lexer->advance(lexer, false);
    switch (lexer->lookahead) {
    case 'n':
        token = NOT_IN;
        break;
    case 's':
        token = NOT_IS;
        break;
    default:
        return (false);
    }
    if (!valid[token]) {
        return (false);
    }
    lexer->advance(lexer, false);
    c = lexer->lookahead;
    if (iswalnum(c) || ((c > 0x7F) && (!is_eol(c)))) {
        return (false);
    }
    lexer->result_symbol = token;
    lexer->mark_end(lexer);
    return (true);
}

void *
tree_sitter_d_external_scanner_create()
{
	return (NULL);
}

void
tree_sitter_d_external_scanner_destroy(void *arg)
{
}

unsigned
tree_sitter_d_external_scanner_serialize(void *arg, char *buffer)
{
	return (0); // we are completely stateless! :-)
}

void
tree_sitter_d_external_scanner_deserialize(
    void *arg, const char *buffer, unsigned length)
{
}

bool
tree_sitter_d_external_scanner_scan(
    void *arg, TSLexer *lexer, const bool *valid)
{
	int  c             = lexer->lookahead;
	bool start_of_line = lexer->get_column(lexer) == 0;

	if (valid[AFTER_EOF] && !valid[ERROR]) {
	   while (lexer->lookahead != 0) {
			lexer->advance(lexer, true);
		}
		lexer->mark_end(lexer);
		lexer->result_symbol = AFTER_EOF;
		return (true);
	}

	// consume whitespace -- we also skip newlines here
	while ((iswspace(c) || is_eol(c)) && (c)) {
		if (is_eol(c)) {
			start_of_line = true;
		}
		lexer->advance(lexer, true);
		c = lexer->lookahead;
	}

	if (c == '#' && start_of_line) {
		return (match_directive(lexer, valid));
	}

	start_of_line = false;

	if (lexer->eof(lexer)) { // in case we had ending whitespace
		return (false);
	}

	if (c == '.' || isdigit(c)) {
		return (match_number(lexer, valid));
	}

	// we have to treat !in and !is specially to recognize them
	// as tokens, specifically to ensure that they are tokenized
	// separately (e.g. func!int is a template parameter.)
	if (c == '!') {
	   return (match_not_in_is(lexer, valid));
	}

	if ((c == 'q') && (valid[L_STRING])) {
		lexer->advance(lexer, false);
		if (lexer->lookahead != '"') {
			return (false);
		}
		lexer->advance(lexer, false);
		switch ((c = lexer->lookahead)) {
		case '(':
			return (match_delimited_string(lexer, '(', ')'));
		case '[':
			return (match_delimited_string(lexer, '[', ']'));
		case '{':
			return (match_delimited_string(lexer, '{', '}'));
		case '<':
			return (match_delimited_string(lexer, '<', '>'));
		default:;
			if (iswalnum(c) || c == '_') {
				return (match_heredoc_string(lexer));
			}
			// non-nesting deliimted string
			return (match_delimited_string(lexer, 0, c));
		}
	}

	return (false);
}
