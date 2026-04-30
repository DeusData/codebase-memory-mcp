#include "tree_sitter/alloc.h"
#include "tree_sitter/parser.h"

#include <wctype.h>

enum TokenType {
    BLOCK_COMMENT_CONTENT,
    BLOCK_DOC_MARKER,
    ERROR_SENTINEL,
    LINE_DOC_CONTENT,
};

void *tree_sitter_wit_external_scanner_create(void) {}

void tree_sitter_wit_external_scanner_destroy(void *payload) {}

unsigned tree_sitter_wit_external_scanner_serialize(void *payload, char *buffer) {
    return 1;
}

void tree_sitter_wit_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {
}

static inline void advance(TSLexer *lexer) { lexer->advance(lexer, false); }

static inline void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

static inline bool process_line_doc_content(TSLexer *lexer) {
    lexer->result_symbol = LINE_DOC_CONTENT;
    for (;;) {
        if (lexer->eof(lexer)) {
            return true;
        }
        if (lexer->lookahead == '\n') {
            // Include the newline in the doc content node.
            // Line endings are useful for markdown injection.
            advance(lexer);
            return true;
        }
        advance(lexer);
    }
}

typedef enum {
    LeftForwardSlash,
    LeftAsterisk,
    Continuing,
} BlockCommentState;

typedef struct {
    BlockCommentState state;
    unsigned nestingDepth;
} BlockCommentProcessing;

static inline void process_left_forward_slash(BlockCommentProcessing *processing, char current) {
    if (current == '*') {
        processing->nestingDepth += 1;
    }
    processing->state = Continuing;
}

static inline void process_left_asterisk(BlockCommentProcessing *processing, char current, TSLexer *lexer) {
    if (current == '*') {
        lexer->mark_end(lexer);
        processing->state = LeftAsterisk;
        return;
    }

    if (current == '/') {
        processing->nestingDepth -= 1;
    }

    processing->state = Continuing;
}

static inline void process_continuing(BlockCommentProcessing *processing, char current) {
    switch (current) {
        case '/':
            processing->state = LeftForwardSlash;
            break;
        case '*':
            processing->state = LeftAsterisk;
            break;
    }
}

static inline bool process_block_comment(TSLexer *lexer, const bool *valid_symbols) {
    // The first character is stored so we can safely advance inside
    // these if blocks. However, because we only store one, we can only
    // safely advance 1 time.
    char first = (char)lexer->lookahead;

    if (valid_symbols[BLOCK_DOC_MARKER] && first == '*') {
        advance(lexer);
        lexer->mark_end(lexer);
        lexer->result_symbol = BLOCK_DOC_MARKER;
        // If the next token is a / that means that it's an empty block comment:
        // /**/
        if (lexer->lookahead == '/') {
            return false;
        }
        // If the next token is a * that means that this isn't a BLOCK_DOC_MARKER
        // as BLOCK_DOC_MARKER's only have 2 * not 3 or more.
        return true;
    } else {
         // Since there's a chance that an advance could
        // happen in one state, we must advance in all states to ensure that
        // the program ends up in a sane state prior to processing the block
        // comment if need be.
        advance(lexer);
    }

    if (valid_symbols[BLOCK_COMMENT_CONTENT]) {
        BlockCommentProcessing processing = {Continuing, 1};
        // Manually set the current state based on the first character
        switch (first) {
            case '*':
                processing.state = LeftAsterisk;
                if (lexer->lookahead == '/') {
                    // This case can happen in an empty doc block comment
                    // like /** */. The comment has no contents, so bail.
                    return false;
                }
                break;
            case '/':
                processing.state = LeftForwardSlash;
                break;
            default:
                processing.state = Continuing;
                break;
        }

        // For the purposes of actually parsing WIT code, this
        // is incorrect as it considers an unterminated block comment
        // to be an error. However, for the purposes of syntax highlighting
        // this should be considered successful as otherwise you are not able
        // to syntax highlight a block of code prior to closing the
        // block comment
        while (!lexer->eof(lexer) && processing.nestingDepth != 0) {
            // Set first to the current lookahead as that is the second character
            // as we force an advance in the above code when we are checking if we
            // need to handle a block comment inner or outer doc comment signifier
            // node
            first = (char)lexer->lookahead;
            switch (processing.state) {
                case LeftForwardSlash:
                    process_left_forward_slash(&processing, first);
                    break;
                case LeftAsterisk:
                    process_left_asterisk(&processing, first, lexer);
                    break;
                case Continuing:
                    lexer->mark_end(lexer);
                    process_continuing(&processing, first);
                    break;
                default:
                    break;
            }
            advance(lexer);
            if (first == '/' && processing.nestingDepth != 0) {
                lexer->mark_end(lexer);
            }
        }
        lexer->result_symbol = BLOCK_COMMENT_CONTENT;
        return true;
    }

    return false;
}


bool tree_sitter_wit_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    // The documentation states that if the lexical analysis fails for some reason
    // they will mark every state as valid and pass it to the external scanner
    // However, we can't do anything to help them recover in that case so we
    // should just fail.
    /*
      link: https://tree-sitter.github.io/tree-sitter/creating-parsers/4-external-scanners.html
      If a syntax error is encountered during regular parsing, Tree-sitter’s
      first action during error recovery will be to call the external scanner’s
      scan function with all tokens marked valid. The scanner should detect this
      case and handle it appropriately. One simple method of detection is to add
      an unused token to the end of the externals array, for example

      externals: $ => [$.token1, $.token2, $.error_sentinel],

      then check whether that token is marked valid to determine whether
      Tree-sitter is in error correction mode.
    */
    if (valid_symbols[ERROR_SENTINEL]) {
        return false;
    }

    if (valid_symbols[BLOCK_COMMENT_CONTENT] || valid_symbols[BLOCK_DOC_MARKER]) {
        return process_block_comment(lexer, valid_symbols);
    }

    if (valid_symbols[LINE_DOC_CONTENT]) {
        return process_line_doc_content(lexer);
    }

    while (iswspace(lexer->lookahead)) {
        skip(lexer);
    }

    return false;
}

