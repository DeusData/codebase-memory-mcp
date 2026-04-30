/**
 * @file scanner.c
 * @brief External scanner for tree-sitter-beancount parser
 *
 * This file implements an external scanner for the Beancount language parser.
 * It handles context-sensitive parsing of section headers and indentation tracking
 * for Org-mode style sections within Beancount files.
 */

#include <stdio.h>
#include "tree_sitter/parser.h"
#include <wctype.h>

#if !defined (UINT8_MAX)
#define UINT8_MAX 255
#endif

// Utility macros
#define MAX(a, b) ((a) > (b) ? (a) : (b))

// Configuration constants
#define INITIAL_VEC_CAPACITY 16  // Initial capacity for dynamic arrays
#define TAB_WIDTH 8              // Number of spaces equivalent to one tab

/**
 * @brief Dynamic array for storing integer values
 *
 * A resizable array implementation used for tracking indentation levels
 * and org-mode section nesting levels.
 */
typedef struct {
    uint32_t length;   // Current number of elements
    uint32_t capacity; // Maximum number of elements before reallocation needed
    int16_t *data;     // Pointer to the data array
} vec;

/**
 * @brief Resize a dynamic array to a new capacity
 * @param vec The vector to resize
 * @param _cap The new capacity (0 to free the array)
 *
 * Safely resizes the vector's data array. If allocation fails, the original
 * data is preserved. If capacity is 0, the array is freed.
 */
#define VEC_RESIZE(vec, _cap)                                                  \
    do {                                                                       \
        if ((_cap) == 0) {                                                     \
            free((vec).data);                                                  \
            (vec).data = NULL;                                                 \
            (vec).capacity = 0;                                                \
            break;                                                             \
        }                                                                      \
        void *tmp = realloc((vec).data, (_cap) * sizeof((vec).data[0]));       \
        if (tmp == NULL) {                                                     \
            /* Allocation failed - keep existing data and capacity */         \
            break;                                                             \
        }                                                                      \
        (vec).data = tmp;                                                      \
        (vec).capacity = (_cap);                                               \
    } while (0)

/**
 * @brief Grow a vector to at least the specified capacity
 * @param vec The vector to grow
 * @param _cap The minimum required capacity
 *
 * Only resizes if the current capacity is less than the requested capacity.
 */
#define VEC_GROW(vec, _cap)                                                    \
    if ((vec).capacity < (_cap)) {                                             \
        VEC_RESIZE((vec), (_cap));                                             \
    }

/**
 * @brief Push an element to the end of a vector
 * @param vec The vector to push to
 * @param el The element to push
 *
 * Automatically grows the vector if needed. Uses exponential growth strategy
 * (doubling capacity) for amortized O(1) performance. If allocation fails,
 * the push is skipped to prevent buffer overflow.
 */
#define VEC_PUSH(vec, el)                                                      \
    do {                                                                       \
        if ((vec).capacity == (vec).length) {                                 \
            uint32_t new_cap = MAX(INITIAL_VEC_CAPACITY, (vec).length * 2);   \
            VEC_RESIZE((vec), new_cap);                                        \
            if ((vec).capacity < new_cap) {                                    \
                /* Allocation failed - skip push to avoid buffer overflow */  \
                break;                                                         \
            }                                                                  \
        }                                                                      \
        (vec).data[(vec).length++] = (el);                                     \
    } while (0)

/** @brief Remove the last element from a vector (decreases length by 1) */
#define VEC_POP(vec) (vec).length--;

/** @brief Initialize a new empty vector */
#define VEC_NEW                                                                \
    { .length = 0, .capacity = 0, .data = NULL }

/** @brief Get the last element of a vector (assumes vector is not empty) */
#define VEC_BACK(vec) ((vec).data[(vec).length - 1])

/** @brief Free the memory used by a vector's data array */
#define VEC_FREE(vec)                                                          \
    {                                                                          \
        if ((vec).data != NULL)                                                \
            free((vec).data);                                                  \
    }

/** @brief Clear all elements from a vector (keeps allocated memory) */
#define VEC_CLEAR(vec) (vec).length = 0;

/**
 * @brief Token types that the external scanner can produce
 *
 * These tokens are used to handle context-sensitive parsing that
 * cannot be handled by the main grammar alone.
 */
enum TokenType {
    SECTION,     // Start of an org-mode style section (e.g., "* Section")
    SECTIONEND,  // End of a section (detected by indentation change)
    END_OF_FILE, // End of file marker
};

/**
 * @brief Scanner state for tracking parsing context
 *
 * The scanner maintains two stacks to track the current parsing state:
 * - indent_length_stack: Tracks indentation levels for proper nesting
 * - org_section_stack: Tracks org-mode section nesting levels
 * - eof_returned: Flag to prevent returning EOF multiple times (prevents infinite loops)
 */
typedef struct {
    vec indent_length_stack; // Stack of indentation levels
    vec org_section_stack;   // Stack of org-mode section levels
    bool eof_returned;       // Flag to prevent returning EOF multiple times
} Scanner;

/**
 * @brief Serialize scanner state into a buffer
 * @param scanner The scanner state to serialize
 * @param buffer The buffer to write the serialized data to
 * @return The number of bytes written to the buffer
 *
 * Serializes the scanner's indentation and section stacks for later restoration.
 * This is used by tree-sitter to maintain parsing state across incremental updates.
 *
 * Format: [eof_returned][indent_count][indent_data...][section_count][section_data...]
 */
static unsigned serialize(Scanner *scanner, char *buffer) {
    size_t i = 0;

    // Serialize EOF flag
    buffer[i++] = scanner->eof_returned ? 1 : 0;

    // Serialize indentation stack
    // Skip the first element (always 0) and limit to UINT8_MAX for safety
    size_t indent_count = scanner->indent_length_stack.length - 1;
    if (indent_count > UINT8_MAX)
        indent_count = UINT8_MAX;
    buffer[i++] = (char)indent_count;

    // Write indentation stack data (starting from index 1)
    uint32_t iter = 1;
    for (; iter < scanner->indent_length_stack.length
           && i < TREE_SITTER_SERIALIZATION_BUFFER_SIZE;
         ++iter) {
        buffer[i++] = (char)scanner->indent_length_stack.data[iter];
    }

    // Serialize org section stack
    size_t org_section_count = scanner->org_section_stack.length - 1;
    if (org_section_count > UINT8_MAX)
        org_section_count = UINT8_MAX;
    buffer[i++] = (char)org_section_count;

    // Write org section stack data (starting from index 1)
    iter = 1;
    for (; iter < scanner->org_section_stack.length
           && i < TREE_SITTER_SERIALIZATION_BUFFER_SIZE;
         ++iter) {
        buffer[i++] = (char)scanner->org_section_stack.data[iter];
    }

    return i;
}

/**
 * @brief Deserialize scanner state from a buffer
 * @param scanner The scanner to restore state into
 * @param buffer The buffer containing serialized data
 * @param length The length of the buffer in bytes
 *
 * Restores the scanner's indentation and section stacks from serialized data.
 * This is used by tree-sitter to restore parsing state during incremental updates.
 *
 * The stacks are always initialized with a base element of 0.
 */
static void deserialize(Scanner *scanner, const char *buffer, unsigned length) {
    // Reset scanner to initial state
    VEC_CLEAR(scanner->org_section_stack);
    VEC_CLEAR(scanner->indent_length_stack);
    VEC_PUSH(scanner->org_section_stack, 0);
    VEC_PUSH(scanner->indent_length_stack, 0);
    scanner->eof_returned = false;

    // Handle empty buffer case
    if (length == 0)
        return;

    size_t i = 0;

    // Deserialize EOF flag
    scanner->eof_returned = (buffer[i++] != 0);

    // Check if we have more data
    if (i >= length) return;

    // Deserialize indentation stack
    size_t indent_count = (unsigned char)buffer[i++];
    size_t end_indent = i + indent_count;
    for (; i < end_indent && i < length; i++) {
        VEC_PUSH(scanner->indent_length_stack, (unsigned char)buffer[i]);
    }

    // Check if we have more data for org section stack
    if (i >= length) return;

    // Deserialize org section stack
    size_t org_section_count = (unsigned char)buffer[i++];
    size_t end_section = i + org_section_count;
    for (; i < end_section && i < length; i++) {
        VEC_PUSH(scanner->org_section_stack, (unsigned char)buffer[i]);
    }
}

/** @brief Advance the lexer to the next character (include in parse result) */
static inline void advance(TSLexer *lexer) {
    lexer->advance(lexer, false);
}

/** @brief Skip the current character (exclude from parse result) */
static inline void skip(TSLexer *lexer) {
    lexer->advance(lexer, true);
}

/**
 * @brief Check if the parser is in error recovery mode
 * @param valid_symbols Array indicating which symbols are valid at this position
 * @return true if all scanner tokens are valid (indicates error recovery)
 *
 * When the parser is in error recovery, it will accept any token we produce.
 * We detect this by checking if all our token types are marked as valid.
 */
static bool in_error_recovery(const bool *valid_symbols) {
    return (valid_symbols[SECTION] && valid_symbols[SECTIONEND]
            && valid_symbols[END_OF_FILE]);
}

/**
 * @brief Check if a character is a headline marker
 * @param c The character to check
 * @return true if the character starts a headline ('*' or '#')
 *
 * Headlines in org-mode style sections start with '*' or markdown-style '#'.
 */
static inline bool is_headline_marker(char c) {
    return c == '*' || c == '#';
}

/**
 * @brief Count leading whitespace characters
 * @param lexer The tree-sitter lexer interface
 * @return The indentation length in spaces (tabs converted to equivalent spaces)
 *
 * Counts spaces and tabs at the beginning of a line, converting tabs to spaces
 * using the TAB_WIDTH constant. Stops at first non-whitespace character.
 */
static int16_t count_leading_whitespace(TSLexer *lexer) {
    int16_t indent_length = 0;

    while (lexer->lookahead == ' ' || lexer->lookahead == '\t') {
        if (lexer->lookahead == ' ') {
            indent_length++;
        } else if (lexer->lookahead == '\t') {
            indent_length += TAB_WIDTH;  // Convert tabs to equivalent spaces
        }
        skip(lexer); // Skip whitespace character
    }

    return indent_length;
}

/**
 * @brief Handle end-of-file detection
 * @param scanner The scanner state (for tracking eof_returned flag)
 * @param lexer The tree-sitter lexer interface
 * @param valid_symbols Array indicating which tokens are valid
 * @return true if EOF token was produced, false otherwise
 */
static bool handle_eof(Scanner *scanner, TSLexer *lexer, const bool *valid_symbols) {
    if (lexer->lookahead != '\0') {
        return false;
    }

    // SECTIONEND can be returned multiple times at EOF to close nested sections
    // The parser controls this via valid_symbols
    if (valid_symbols[SECTIONEND]) {
        lexer->result_symbol = SECTIONEND;
        return true;
    }

    // END_OF_FILE should only be returned once to prevent infinite loops
    if (valid_symbols[END_OF_FILE] && !scanner->eof_returned) {
        scanner->eof_returned = true;
        lexer->result_symbol = END_OF_FILE;
        return true;
    }

    return false;
}

/**
 * @brief Parse section header and determine section boundaries
 * @param scanner The scanner state
 * @param lexer The tree-sitter lexer interface
 * @param valid_symbols Array indicating which tokens are valid
 * @return true if section token was produced, false otherwise
 */
static bool parse_section_header(Scanner *scanner, TSLexer *lexer, const bool *valid_symbols) {
    if (!is_headline_marker(lexer->lookahead)) {
        return false;
    }

    lexer->mark_end(lexer);

    // Count consecutive headline markers (* or #)
    int16_t stars = 1;
    skip(lexer);
    while (is_headline_marker(lexer->lookahead)) {
        stars++;
        skip(lexer);
    }

    // Must be followed by whitespace to be a valid header
    if (!iswspace(lexer->lookahead)) {
        return false;
    }

    // Determine if this is a section end or section start
    if (valid_symbols[SECTIONEND] && stars > 0
        && scanner->org_section_stack.length > 0
        && stars <= VEC_BACK(scanner->org_section_stack)) {
        // This header closes a section (equal or higher level)
        VEC_POP(scanner->org_section_stack);
        lexer->result_symbol = SECTIONEND;
        return true;
    } else if (valid_symbols[SECTION]) {
        // This header starts a new section
        VEC_PUSH(scanner->org_section_stack, stars);
        lexer->result_symbol = SECTION;
        return true;
    }

    return false; // Header found but not at appropriate parsing state
}

/**
 * @brief Main scanning function for the external scanner
 * @param scanner The scanner state
 * @param lexer The tree-sitter lexer interface
 * @param valid_symbols Array indicating which tokens are valid at this position
 * @return true if a token was successfully scanned, false otherwise
 *
 * This function handles context-sensitive parsing of:
 * - Section boundaries (SECTION/SECTIONEND tokens)
 * - End of file detection
 * - Indentation tracking for proper nesting
 */
static bool scan(Scanner *scanner, TSLexer *lexer, const bool *valid_symbols) {

    // Don't produce tokens during error recovery
    if (in_error_recovery(valid_symbols))
        return false;

    // Mark the current position for potential token end
    lexer->mark_end(lexer);

    // Count leading whitespace to determine indentation level
    int16_t indent_length = count_leading_whitespace(lexer);

    // Handle end of file
    if (handle_eof(scanner, lexer, valid_symbols)) {
        return true;
    }

    // Check for org-mode style section headers (must start at column 0)
    if (indent_length == 0) {
        return parse_section_header(scanner, lexer, valid_symbols);
    }

    return false; // No special tokens found
}

/**
 * @brief Initialize a scanner with default state
 * @param scanner The scanner to initialize
 *
 * Sets up the scanner with empty stacks containing the base element (0).
 */
static void init_scanner(Scanner *scanner) {
    scanner->indent_length_stack = (vec)VEC_NEW;
    scanner->org_section_stack = (vec)VEC_NEW;
    scanner->eof_returned = false;

    // Initialize stacks with base element 0
    VEC_PUSH(scanner->indent_length_stack, 0);
    VEC_PUSH(scanner->org_section_stack, 0);
}

/**
 * @brief Create a new scanner instance
 * @return Pointer to the newly created scanner, or NULL if allocation fails
 *
 * This function is called by tree-sitter to create a new scanner instance.
 * The scanner is initialized with empty stacks.
 */
void *tree_sitter_beancount_external_scanner_create() {
    Scanner *scanner = (Scanner *)calloc(1, sizeof(Scanner));
    if (scanner == NULL) {
        return NULL; // Allocation failed
    }

    init_scanner(scanner);
    return scanner;
}

/**
 * @brief Entry point for scanning tokens
 * @param payload Pointer to the scanner instance
 * @param lexer The tree-sitter lexer interface
 * @param valid_symbols Array indicating which tokens are valid
 * @return true if a token was successfully scanned
 *
 * This is the main entry point called by tree-sitter for token scanning.
 */
bool tree_sitter_beancount_external_scanner_scan(void *payload,
                                                 TSLexer *lexer,
                                                 const bool *valid_symbols) {
    if (payload == NULL || lexer == NULL || valid_symbols == NULL) {
        return false; // Invalid parameters
    }

    Scanner *scanner = (Scanner *)payload;
    return scan(scanner, lexer, valid_symbols);
}

/**
 * @brief Serialize scanner state for incremental parsing
 * @param payload Pointer to the scanner instance
 * @param buffer Buffer to write serialized state to
 * @return Number of bytes written to the buffer
 *
 * Called by tree-sitter to save scanner state for incremental parsing.
 */
unsigned tree_sitter_beancount_external_scanner_serialize(void *payload,
                                                          char *buffer) {
    if (payload == NULL || buffer == NULL) {
        return 0; // Invalid parameters
    }

    Scanner *scanner = (Scanner *)payload;
    return serialize(scanner, buffer);
}

/**
 * @brief Deserialize scanner state from buffer
 * @param payload Pointer to the scanner instance
 * @param buffer Buffer containing serialized state
 * @param length Length of the buffer in bytes
 *
 * Called by tree-sitter to restore scanner state during incremental parsing.
 */
void tree_sitter_beancount_external_scanner_deserialize(void *payload,
                                                        const char *buffer,
                                                        unsigned length) {
    if (payload == NULL) {
        return; // Invalid parameters
    }

    Scanner *scanner = (Scanner *)payload;
    deserialize(scanner, buffer, length);
}

/**
 * @brief Destroy a scanner instance and free its memory
 * @param payload Pointer to the scanner instance to destroy
 *
 * Called by tree-sitter when the scanner is no longer needed.
 * Frees all allocated memory including the vector data arrays.
 */
void tree_sitter_beancount_external_scanner_destroy(void *payload) {
    Scanner *scanner = (Scanner *)payload;
    VEC_FREE(scanner->indent_length_stack);
    VEC_FREE(scanner->org_section_stack);
    free(scanner);
}
