#include "tree_sitter/parser.h"
#include "tree_sitter/alloc.h"
#include "tree_sitter/array.h"
#include "assert.h"
#include "limits.h"
#include "string.h"
#include "wctype.h"

/**
 * Macro; goes to the lexer state without consuming any codepoints.
 *
 * @param state_value The new lexer state.
 */
#define GO_TO_STATE(state_value)  \
  {                               \
    state = state_value;          \
    goto start;                   \
  }

/**
 * Macro; marks the given lexeme as accepted.
 *
 * @param lexeme The lexeme to mark as accepted.
 */
#define ACCEPT_LEXEME(lexeme)       \
  {                                 \
    (void)result;                   \
    result_lexeme = lexeme;         \
  }

/**
 * Macro; marks the given token as accepted without also marking the
 * current position as its end.
 *
 * @param token The token to mark as accepted.
 */
#define ACCEPT_LOOKAHEAD_TOKEN(token)     \
  {                                       \
    result = true;                        \
    lexer->result_symbol = token;         \
  }

/**
 * Macro; ends a lexer state by returning any accepted lexeme.
 */
#define END_LEX_STATE()   \
  {                       \
    return result_lexeme; \
  }

  // Tokens emitted by this external scanner.
  enum TokenType {
    LEADING_EXTRAMODULAR_TEXT,  // Freeform text at the start of the file.
    TRAILING_EXTRAMODULAR_TEXT, // Freeform text between or after modules.
    INDENT,             // Marks beginning of junction list.
    BULLET,             // New item of a junction list.
    DEDENT,             // Marks end of junction list.
    BEGIN_PROOF,        // Marks the beginning of an entire proof.
    BEGIN_PROOF_STEP,   // Marks the beginning of a proof step.
    PROOF_KEYWORD,      // The PROOF keyword.
    BY_KEYWORD,         // The BY keyword.
    OBVIOUS_KEYWORD,    // The OBVIOUS keyword.
    OMITTED_KEYWORD,    // The OMITTED keyword.
    QED_KEYWORD,        // The QED keyword.
    WEAK_FAIRNESS,      // The WF_ keyword.
    STRONG_FAIRNESS,    // The SF_ keyword.
    PCAL_START,         // Notifies scanner of start of PlusCal block.
    PCAL_END,           // Notifies scanner of end of PlusCal block.
    DOUBLE_EXCL,        // The !! infix op; lexical conflict with subexpr!
    ERROR_SENTINEL      // Only valid if in error recovery mode.
  };

  // Datatype used to record length of nested proofs & jlists.
  typedef int16_t nest_address;

  // Datatype used to record column index of jlists.
  typedef int16_t column_index;

  // Datatype used to record proof levels.
  typedef int32_t proof_level;

  // A dynamic array of chars.
  typedef Array(char) CharArray;

  /**
   * Advances the scanner while marking the codepoint as non-whitespace.
   *
   * @param lexer The tree-sitter lexing control structure.
   */
  static void advance(TSLexer* const lexer) {
    lexer->advance(lexer, false);
  }

  /**
   * Checks whether the next codepoint is the one given.
   *
   * @param lexer The tree-sitter lexing control structure.
   * @param codepoint The codepoint to check.
   * @return Whether the next codepoint is the one given.
   */
  static bool is_next_codepoint(
    const TSLexer* const lexer,
    int32_t const codepoint
  ) {
    return codepoint == lexer->lookahead;
  }

  /**
   * Checks whether there are any codepoints left in the string.
   *
   * @param lexer The tree-sitter lexing control structure.
   * @return Whether there are any codepoints left in the string.
   */
  static bool has_next(const TSLexer* const lexer) {
    return !lexer->eof(lexer);
  }

  /**
   * Checks whether the given codepoint could be used in an identifier,
   * which consist of capital ASCII letters, lowercase ASCII letters,
   * and underscores.
   *
   * @param codepoint The codepoint to check.
   * @return Whether the given codepoint could be used in an identifier.
   */
  static bool is_identifier_char(int32_t const codepoint) {
    return iswalnum(codepoint) || ('_' == codepoint);
  }

  /**
   * Consumes codepoints as long as they are the one given.
   *
   * @param lexer The tree-sitter lexing control structure.
   * @param codepoint The codepoint to consume.
   * @return The number of codepoints consumed.
   */
  static void consume_codepoint(TSLexer* const lexer, const int32_t codepoint) {
    while (has_next(lexer) && is_next_codepoint(lexer, codepoint)) {
      advance(lexer);
    }
  }

  /**
   * Checks whether the next codepoint sequence is the one given.
   * This function can change the state of the lexer.
   *
   * @param lexer The tree-sitter lexing control structure.
   * @param token The codepoint sequence to check for.
   * @return Whether the next codepoint sequence is the one given.
   */
  static bool is_next_codepoint_sequence(
    TSLexer* const lexer,
    const char codepoint_sequence[]
  ) {
    size_t sequence_length = strlen(codepoint_sequence);
    for (size_t i = 0; i < sequence_length; i++) {
      int32_t codepoint = codepoint_sequence[i];
      if (!is_next_codepoint(lexer, codepoint)) {
        return false;
      } else if (i + 1 < sequence_length) {
        advance(lexer);
      }
    }

    return true;
  }

  // Possible states for the extramodular text lexer to enter.
  enum EMTLexState {
    EMTLexState_CONSUME,
    EMTLexState_DASH,
    EMTLexState_SINGLE_LINE,
    EMTLexState_MODULE,
    EMTLexState_BLANK_BEFORE_MODULE,
    EMTLexState_END_OF_FILE,
    EMTLexState_BLANK_BEFORE_END_OF_FILE
  };

  /**
   * Scans for extramodular text, the freeform text that can be present
   * outside of TLA⁺ modules. This function skips any leading whitespace
   * to avoid extraneous extramodular text tokens given newlines at the
   * beginning or end of the file. It will consume any text up to the
   * point it performs lookahead that captures the following regex:
   *     /----[-]*[ ]*MODULE/
   * or EOF, which marks the end of the extramodular text. It is important
   * that the extramodular text does not itself include the captured module
   * start sequence, which is why this is in an external scanner rather
   * than a regex in the grammar itself.
   *
   * @param lexer The tree-sitter lexing control structure
   * @param valid_symbols Tokens possibly expected in this spot.
   * @return Whether any extramodular text was detected.
   */
  static bool scan_extramodular_text(TSLexer* const lexer, const bool* const valid_symbols) {
    bool has_consumed_any = false;
    enum EMTLexState state = EMTLexState_CONSUME;
    START_LEXER();
    eof = !has_next(lexer);
    switch (state) {
      case EMTLexState_CONSUME:
        if (eof) ADVANCE(EMTLexState_END_OF_FILE);
        if (iswspace(lookahead) && !has_consumed_any) SKIP(EMTLexState_CONSUME);
        if (iswspace(lookahead) && has_consumed_any) ADVANCE(EMTLexState_CONSUME);
        lexer->mark_end(lexer);
        if ('-' == lookahead) ADVANCE(EMTLexState_DASH);
        has_consumed_any = true;
        ADVANCE(EMTLexState_CONSUME);
        END_STATE();
      case EMTLexState_DASH:
        if (is_next_codepoint_sequence(lexer, "---")) ADVANCE(EMTLexState_SINGLE_LINE);
        has_consumed_any = true;
        GO_TO_STATE(EMTLexState_CONSUME);
        END_STATE();
      case EMTLexState_SINGLE_LINE:
        consume_codepoint(lexer, '-');
        consume_codepoint(lexer, ' ');
        if (is_next_codepoint_sequence(lexer, "MODULE")) { ADVANCE(EMTLexState_MODULE); }
        has_consumed_any = true;
        GO_TO_STATE(EMTLexState_CONSUME);
        END_STATE();
      case EMTLexState_MODULE:
        if (!has_consumed_any) GO_TO_STATE(EMTLexState_BLANK_BEFORE_MODULE);
        ACCEPT_LOOKAHEAD_TOKEN(valid_symbols[LEADING_EXTRAMODULAR_TEXT] ? LEADING_EXTRAMODULAR_TEXT : TRAILING_EXTRAMODULAR_TEXT);
        END_STATE();
      case EMTLexState_BLANK_BEFORE_MODULE:
        END_STATE();
      case EMTLexState_END_OF_FILE:
        if (!has_consumed_any) GO_TO_STATE(EMTLexState_BLANK_BEFORE_END_OF_FILE);
        if (valid_symbols[TRAILING_EXTRAMODULAR_TEXT]) { ACCEPT_TOKEN(TRAILING_EXTRAMODULAR_TEXT); }
        END_STATE();
      case EMTLexState_BLANK_BEFORE_END_OF_FILE:
        END_STATE();
      default:
        END_STATE();
    }
  }

  // Types of proof step IDs.
  enum ProofStepIdType {
    ProofStepIdType_STAR,     // <*>
    ProofStepIdType_PLUS,     // <+>
    ProofStepIdType_NUMBERED, // <1234>
    ProofStepIdType_NONE      // Invalid or nonexistent
  };

  // Data about a proof step ID.
  struct ProofStepId {
    // The proof step ID type.
    enum ProofStepIdType type;

    // The proof step ID level (-1 if not NUMBERED).
    proof_level level;
  };

    /**
     * Initializes a new instance of the ProofStepId class.
     *
     * @param raw_level The unparsed contents of the <...> lexeme.
     */
    static struct ProofStepId parse_proof_step_id(const CharArray* raw_level) {
      struct ProofStepId id;
      id.level = -1;
      if (0 == raw_level->size) {
        id.type = ProofStepIdType_NONE;
      } else if ('*' == *array_get(raw_level, 0)) {
        id.type = ProofStepIdType_STAR;
      } else if ('+' == *array_get(raw_level, 0)) {
        id.type = ProofStepIdType_PLUS;
      } else {
        id.type = ProofStepIdType_NUMBERED;
        id.level = 0;
        int32_t multiplier = 1;
        for (size_t i = 0; i < raw_level->size; i++) {
          const size_t index = raw_level->size - i - 1;
          const int8_t digit_value = *array_get(raw_level, index) - 48;
          if (0 <= digit_value && digit_value <= 9) {
            id.level += digit_value * multiplier;
            multiplier *= 10;
          } else {
            id.type = ProofStepIdType_NONE;
            id.level = -1;
            break;
          }
        }
      }
      return id;
    }

  // Lexemes recognized by this lexer.
  enum Lexeme {
    Lexeme_FORWARD_SLASH,
    Lexeme_BACKWARD_SLASH,
    Lexeme_GT,
    Lexeme_EQ,
    Lexeme_DASH,
    Lexeme_COMMA,
    Lexeme_COLON,
    Lexeme_SEMICOLON,
    Lexeme_LAND,
    Lexeme_LOR,
    Lexeme_DOUBLE_EXCL,
    Lexeme_L_PAREN,
    Lexeme_R_PAREN,
    Lexeme_R_SQUARE_BRACKET,
    Lexeme_R_CURLY_BRACE,
    Lexeme_R_ANGLE_BRACKET,
    Lexeme_RIGHT_ARROW,
    Lexeme_RIGHT_MAP_ARROW,
    Lexeme_COMMENT_START,
    Lexeme_BLOCK_COMMENT_START,
    Lexeme_SINGLE_LINE,
    Lexeme_DOUBLE_LINE,
    Lexeme_ASSUME_KEYWORD,
    Lexeme_ASSUMPTION_KEYWORD,
    Lexeme_AXIOM_KEYWORD,
    Lexeme_BY_KEYWORD,
    Lexeme_CONSTANT_KEYWORD,
    Lexeme_CONSTANTS_KEYWORD,
    Lexeme_COROLLARY_KEYWORD,
    Lexeme_ELSE_KEYWORD,
    Lexeme_IN_KEYWORD,
    Lexeme_LEMMA_KEYWORD,
    Lexeme_LOCAL_KEYWORD,
    Lexeme_OBVIOUS_KEYWORD,
    Lexeme_OMITTED_KEYWORD,
    Lexeme_PROOF_KEYWORD,
    Lexeme_PROPOSITION_KEYWORD,
    Lexeme_QED_KEYWORD,
    Lexeme_THEN_KEYWORD,
    Lexeme_THEOREM_KEYWORD,
    Lexeme_VARIABLE_KEYWORD,
    Lexeme_VARIABLES_KEYWORD,
    Lexeme_PROOF_STEP_ID,
    Lexeme_IDENTIFIER,
    Lexeme_WEAK_FAIRNESS,
    Lexeme_STRONG_FAIRNESS,
    Lexeme_OTHER,
    Lexeme_END_OF_FILE
  };

  // Possible states for the lexer to enter.
  enum LexState {
    LexState_CONSUME_LEADING_SPACE,
    LexState_FORWARD_SLASH,
    LexState_BACKWARD_SLASH,
    LexState_LT,
    LexState_GT,
    LexState_EQ,
    LexState_DASH,
    LexState_COMMA,
    LexState_COLON,
    LexState_EXCL,
    LexState_DOUBLE_EXCL,
    LexState_SEMICOLON,
    LexState_LAND,
    LexState_LOR,
    LexState_L_PAREN,
    LexState_R_PAREN,
    LexState_R_SQUARE_BRACKET,
    LexState_R_CURLY_BRACE,
    LexState_R_ANGLE_BRACKET,
    LexState_S, LexState_SF_,
    LexState_RIGHT_ARROW,
    LexState_RIGHT_MAP_ARROW,
    LexState_COMMENT_START,
    LexState_BLOCK_COMMENT_START,
    LexState_SINGLE_LINE,
    LexState_DOUBLE_LINE,
    LexState_PIPE,
    LexState_RIGHT_TURNSTILE,
    LexState_A, LexState_ASSUM, LexState_ASSUME, LexState_ASSUMPTION, LexState_AX, LexState_AXIOM,
    LexState_B, LexState_BY,
    LexState_C, LexState_CO, LexState_CON, LexState_COR, LexState_CONSTANT, LexState_CONSTANTS, LexState_COROLLARY,
    LexState_E, LexState_ELSE,
    LexState_I, LexState_IN,
    LexState_L, LexState_LE, LexState_LEMMA, LexState_LO, LexState_LOCAL,
    LexState_O, LexState_OB, LexState_OBVIOUS, LexState_OM, LexState_OMITTED,
    LexState_P, LexState_PRO, LexState_PROO, LexState_PROOF, LexState_PROP, LexState_PROPOSITION,
    LexState_Q, LexState_QED,
    LexState_T, LexState_THE, LexState_THEN, LexState_THEOREM,
    LexState_V, LexState_VARIABLE, LexState_VARIABLES,
    LexState_W, LexState_WF_,
    LexState_IDENTIFIER,
    LexState_PROOF_LEVEL_NUMBER,
    LexState_PROOF_LEVEL_STAR,
    LexState_PROOF_LEVEL_PLUS,
    LexState_PROOF_NAME,
    LexState_PROOF_ID,
    LexState_OTHER,
    LexState_END_OF_FILE
  };

  /**
   * Looks ahead to identify the next lexeme. Consumes all leading
   * whitespace. Out parameters include column of first non-whitespace
   * codepoint and the level of the proof step ID lexeme if encountered.
   *
   * @param lexer The tree-sitter lexing control structure.
   * @param lexeme_start_col The starting column of the first lexeme.
   * @param proof_step_id_level The level of the proof step ID.
   * @return The lexeme encountered.
   */
  static enum Lexeme lex_lookahead(
    TSLexer* const lexer,
    column_index* lexeme_start_col,
    CharArray* proof_step_id_level
  ) {
    enum LexState state = LexState_CONSUME_LEADING_SPACE;
    enum Lexeme result_lexeme = Lexeme_OTHER;
    START_LEXER();
    eof = !has_next(lexer);
    switch (state) {
      case LexState_CONSUME_LEADING_SPACE:
        if (iswspace(lookahead)) SKIP(LexState_CONSUME_LEADING_SPACE);
        *lexeme_start_col = lexer->get_column(lexer);
        lexer->mark_end(lexer);
        if (eof) ADVANCE(LexState_END_OF_FILE);
        if ('/' == lookahead) ADVANCE(LexState_FORWARD_SLASH);
        if ('\\' == lookahead) ADVANCE(LexState_BACKWARD_SLASH);
        if ('<' == lookahead) ADVANCE(LexState_LT);
        if ('>' == lookahead) ADVANCE(LexState_GT);
        if ('=' == lookahead) ADVANCE(LexState_EQ);
        if ('-' == lookahead) ADVANCE(LexState_DASH);
        if (',' == lookahead) ADVANCE(LexState_COMMA);
        if (':' == lookahead) ADVANCE(LexState_COLON);
        if (';' == lookahead) ADVANCE(LexState_SEMICOLON);
        if ('(' == lookahead) ADVANCE(LexState_L_PAREN);
        if (')' == lookahead) ADVANCE(LexState_R_PAREN);
        if (']' == lookahead) ADVANCE(LexState_R_SQUARE_BRACKET);
        if ('}' == lookahead) ADVANCE(LexState_R_CURLY_BRACE);
        if ('|' == lookahead) ADVANCE(LexState_PIPE);
        if ('!' == lookahead) ADVANCE(LexState_EXCL);
        if ('A' == lookahead) ADVANCE(LexState_A);
        if ('B' == lookahead) ADVANCE(LexState_B);
        if ('C' == lookahead) ADVANCE(LexState_C);
        if ('E' == lookahead) ADVANCE(LexState_E);
        if ('I' == lookahead) ADVANCE(LexState_I);
        if ('L' == lookahead) ADVANCE(LexState_L);
        if ('O' == lookahead) ADVANCE(LexState_O);
        if ('P' == lookahead) ADVANCE(LexState_P);
        if ('Q' == lookahead) ADVANCE(LexState_Q);
        if ('S' == lookahead) ADVANCE(LexState_S);
        if ('T' == lookahead) ADVANCE(LexState_T);
        if ('V' == lookahead) ADVANCE(LexState_V);
        if ('W' == lookahead) ADVANCE(LexState_W);
        if (L'\u2227' == lookahead) ADVANCE(LexState_LAND); // '∧'
        if (L'\u2228' == lookahead) ADVANCE(LexState_LOR); // '∨'
        if (L'\u3009' == lookahead) ADVANCE(LexState_R_ANGLE_BRACKET); // '〉'
        if (L'\u27E9' == lookahead) ADVANCE(LexState_R_ANGLE_BRACKET); // '⟩'
        if (L'\u27F6' == lookahead) ADVANCE(LexState_RIGHT_ARROW); // '⟶'
        if (L'\u2192' == lookahead) ADVANCE(LexState_RIGHT_ARROW); // '→'
        if (L'\u27FC' == lookahead) ADVANCE(LexState_RIGHT_MAP_ARROW); // '⟼'
        if (L'\u21A6' == lookahead) ADVANCE(LexState_RIGHT_MAP_ARROW); // '↦'
        ADVANCE(LexState_OTHER);
        END_LEX_STATE();
      case LexState_FORWARD_SLASH:
        ACCEPT_LEXEME(Lexeme_FORWARD_SLASH);
        if ('\\' == lookahead) ADVANCE(LexState_LAND);
        END_LEX_STATE();
      case LexState_BACKWARD_SLASH:
        ACCEPT_LEXEME(Lexeme_BACKWARD_SLASH);
        if ('/' == lookahead) ADVANCE(LexState_LOR);
        if ('*' == lookahead) ADVANCE(LexState_COMMENT_START);
        END_LEX_STATE();
      case LexState_LT:
        array_push(proof_step_id_level, (char)(lookahead & CHAR_MAX));
        if (iswdigit(lookahead)) ADVANCE(LexState_PROOF_LEVEL_NUMBER);
        if ('*' == lookahead) ADVANCE(LexState_PROOF_LEVEL_STAR);
        if ('+' == lookahead) ADVANCE(LexState_PROOF_LEVEL_PLUS);
        ADVANCE(LexState_OTHER);
        END_LEX_STATE();
      case LexState_GT:
        ACCEPT_LEXEME(Lexeme_GT);
        if ('>' == lookahead) ADVANCE(LexState_R_ANGLE_BRACKET);
        END_LEX_STATE();
      case LexState_EQ:
        ACCEPT_LEXEME(Lexeme_EQ);
        if (is_next_codepoint_sequence(lexer, "===")) ADVANCE(LexState_DOUBLE_LINE);
        END_LEX_STATE();
      case LexState_DASH:
        ACCEPT_LEXEME(Lexeme_DASH);
        if ('>' == lookahead) ADVANCE(LexState_RIGHT_ARROW);
        if (is_next_codepoint_sequence(lexer, "---")) ADVANCE(LexState_SINGLE_LINE);
        END_LEX_STATE();
      case LexState_COMMA:
        ACCEPT_LEXEME(Lexeme_COMMA);
        END_LEX_STATE();
      case LexState_COLON:
        ACCEPT_LEXEME(Lexeme_COLON);
        if (':' == lookahead) ADVANCE(LexState_OTHER);
        if ('=' == lookahead) ADVANCE(LexState_OTHER);
        if ('>' == lookahead) ADVANCE(LexState_OTHER);
        END_LEX_STATE();
      case LexState_SEMICOLON:
        ACCEPT_LEXEME(Lexeme_SEMICOLON);
        END_LEX_STATE();
      case LexState_LAND:
        ACCEPT_LEXEME(Lexeme_LAND);
        END_LEX_STATE();
      case LexState_LOR:
        ACCEPT_LEXEME(Lexeme_LOR);
        END_LEX_STATE();
      case LexState_L_PAREN:
        ACCEPT_LEXEME(Lexeme_L_PAREN);
        if ('*' == lookahead) ADVANCE(LexState_BLOCK_COMMENT_START);
        END_LEX_STATE();
      case LexState_R_PAREN:
        ACCEPT_LEXEME(Lexeme_R_PAREN);
        END_LEX_STATE();
      case LexState_R_SQUARE_BRACKET:
        ACCEPT_LEXEME(Lexeme_R_SQUARE_BRACKET);
        END_LEX_STATE();
      case LexState_R_CURLY_BRACE:
        ACCEPT_LEXEME(Lexeme_R_CURLY_BRACE);
        END_LEX_STATE();
      case LexState_R_ANGLE_BRACKET:
        ACCEPT_LEXEME(Lexeme_R_ANGLE_BRACKET);
        END_LEX_STATE();
      case LexState_RIGHT_ARROW:
        ACCEPT_LEXEME(Lexeme_RIGHT_ARROW);
        END_LEX_STATE();
      case LexState_RIGHT_MAP_ARROW:
        ACCEPT_LEXEME(Lexeme_RIGHT_MAP_ARROW);
        END_LEX_STATE();
      case LexState_COMMENT_START:
        ACCEPT_LEXEME(Lexeme_COMMENT_START);
        END_LEX_STATE();
      case LexState_BLOCK_COMMENT_START:
        ACCEPT_LEXEME(Lexeme_BLOCK_COMMENT_START);
        END_LEX_STATE();
      case LexState_SINGLE_LINE:
        ACCEPT_LEXEME(Lexeme_SINGLE_LINE);
        END_LEX_STATE();
      case LexState_DOUBLE_LINE:
        ACCEPT_LEXEME(Lexeme_DOUBLE_LINE);
        END_LEX_STATE();
      case LexState_PIPE:
        if ('-' == lookahead) ADVANCE(LexState_RIGHT_TURNSTILE);
        END_LEX_STATE();
      case LexState_EXCL:
        if ('!' == lookahead) ADVANCE(LexState_DOUBLE_EXCL);
        END_LEX_STATE();
      case LexState_DOUBLE_EXCL:
        ACCEPT_LEXEME(Lexeme_DOUBLE_EXCL);
        END_LEX_STATE();
      case LexState_RIGHT_TURNSTILE:
        if ('>' == lookahead) ADVANCE(LexState_RIGHT_MAP_ARROW);
        END_LEX_STATE();
      case LexState_A:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if ('X' == lookahead) ADVANCE(LexState_AX);
        if (is_next_codepoint_sequence(lexer, "SSUM")) ADVANCE(LexState_ASSUM);
        END_LEX_STATE();
      case LexState_ASSUM:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if ('E' == lookahead) ADVANCE(LexState_ASSUME);
        if (is_next_codepoint_sequence(lexer, "PTION")) ADVANCE(LexState_ASSUMPTION);
        END_LEX_STATE();
      case LexState_ASSUME:
        ACCEPT_LEXEME(Lexeme_ASSUME_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_ASSUMPTION:
        ACCEPT_LEXEME(Lexeme_ASSUMPTION_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_AX:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if (is_next_codepoint_sequence(lexer, "IOM")) ADVANCE(LexState_AXIOM);
        END_LEX_STATE();
      case LexState_AXIOM:
        ACCEPT_LEXEME(Lexeme_AXIOM_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_B:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if ('Y' == lookahead) ADVANCE(LexState_BY);
        END_LEX_STATE();
      case LexState_BY:
        ACCEPT_LEXEME(Lexeme_BY_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_C:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if ('O' == lookahead) ADVANCE(LexState_CO);
        END_LEX_STATE();
      case LexState_CO:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if ('N' == lookahead) ADVANCE(LexState_CON);
        if ('R' == lookahead) ADVANCE(LexState_COR);
        END_LEX_STATE();
      case LexState_CON:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if (is_next_codepoint_sequence(lexer, "STANT")) ADVANCE(LexState_CONSTANT);
        END_LEX_STATE();
      case LexState_CONSTANT:
        ACCEPT_LEXEME(Lexeme_CONSTANT_KEYWORD);
        if ('S' == lookahead) ADVANCE(LexState_CONSTANTS);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_CONSTANTS:
        ACCEPT_LEXEME(Lexeme_CONSTANTS_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_COR:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if (is_next_codepoint_sequence(lexer, "OLLARY")) ADVANCE(LexState_COROLLARY);
        END_LEX_STATE();
      case LexState_COROLLARY:
        ACCEPT_LEXEME(Lexeme_COROLLARY_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_E:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if (is_next_codepoint_sequence(lexer, "LSE")) ADVANCE(LexState_ELSE);
        END_LEX_STATE();
      case LexState_ELSE:
        ACCEPT_LEXEME(Lexeme_ELSE_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_I:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if ('N' == lookahead) ADVANCE(LexState_IN);
        END_LEX_STATE();
      case LexState_IN:
        ACCEPT_LEXEME(Lexeme_IN_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_L:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if ('E' == lookahead) ADVANCE(LexState_LE);
        if ('O' == lookahead) ADVANCE(LexState_LO);
        END_LEX_STATE();
      case LexState_LE:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if (is_next_codepoint_sequence(lexer, "MMA")) ADVANCE(LexState_LEMMA);
        END_LEX_STATE();
      case LexState_LEMMA:
        ACCEPT_LEXEME(Lexeme_LEMMA_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_LO:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if (is_next_codepoint_sequence(lexer, "CAL")) ADVANCE(LexState_LOCAL);
        END_LEX_STATE();
      case LexState_LOCAL:
        ACCEPT_LEXEME(Lexeme_LOCAL_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_O:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if ('B' == lookahead) ADVANCE(LexState_OB);
        if ('M' == lookahead) ADVANCE(LexState_OM);
        END_LEX_STATE();
      case LexState_OB:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if (is_next_codepoint_sequence(lexer, "VIOUS")) ADVANCE(LexState_OBVIOUS);
        END_LEX_STATE();
      case LexState_OBVIOUS:
        ACCEPT_LEXEME(Lexeme_OBVIOUS_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_OM:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if (is_next_codepoint_sequence(lexer, "ITTED")) ADVANCE(LexState_OMITTED);
        END_LEX_STATE();
      case LexState_OMITTED:
        ACCEPT_LEXEME(Lexeme_OMITTED_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_P:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if (is_next_codepoint_sequence(lexer, "RO")) ADVANCE(LexState_PRO);
        END_LEX_STATE();
      case LexState_PRO:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if ('O' == lookahead) ADVANCE(LexState_PROO);
        if ('P' == lookahead) ADVANCE(LexState_PROP);
        END_LEX_STATE();
      case LexState_PROO:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if ('F' == lookahead) ADVANCE(LexState_PROOF);
        END_LEX_STATE();
      case LexState_PROOF:
        ACCEPT_LEXEME(Lexeme_PROOF_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_PROP:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if (is_next_codepoint_sequence(lexer, "OSITION")) ADVANCE(LexState_PROPOSITION);
        END_LEX_STATE();
      case LexState_PROPOSITION:
        ACCEPT_LEXEME(Lexeme_PROPOSITION_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_Q:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if (is_next_codepoint_sequence(lexer, "ED")) ADVANCE(LexState_QED);
        END_LEX_STATE();
      case LexState_QED:
        ACCEPT_LEXEME(Lexeme_QED_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_S:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if (is_next_codepoint_sequence(lexer, "F_")) ADVANCE(LexState_SF_);
        END_LEX_STATE();
      case LexState_SF_:
        ACCEPT_LEXEME(Lexeme_STRONG_FAIRNESS);
        END_LEX_STATE();
      case LexState_T:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if (is_next_codepoint_sequence(lexer, "HE")) ADVANCE(LexState_THE);
        END_LEX_STATE();
      case LexState_THE:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if ('N' == lookahead) ADVANCE(LexState_THEN);
        if (is_next_codepoint_sequence(lexer, "OREM")) ADVANCE(LexState_THEOREM);
        END_LEX_STATE();
      case LexState_THEN:
        ACCEPT_LEXEME(Lexeme_THEN_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_THEOREM:
        ACCEPT_LEXEME(Lexeme_THEOREM_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_V:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if (is_next_codepoint_sequence(lexer, "ARIABLE")) ADVANCE(LexState_VARIABLE);
        END_LEX_STATE();
      case LexState_VARIABLE:
        ACCEPT_LEXEME(Lexeme_VARIABLE_KEYWORD);
        if ('S' == lookahead) ADVANCE(LexState_VARIABLES);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_VARIABLES:
        ACCEPT_LEXEME(Lexeme_VARIABLES_KEYWORD);
        if (is_identifier_char(lookahead)) ADVANCE(LexState_IDENTIFIER);
        END_LEX_STATE();
      case LexState_W:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        if (is_next_codepoint_sequence(lexer, "F_")) ADVANCE(LexState_WF_);
        END_LEX_STATE();
      case LexState_WF_:
        ACCEPT_LEXEME(Lexeme_WEAK_FAIRNESS);
        END_LEX_STATE();
      case LexState_PROOF_LEVEL_NUMBER:
        if (iswdigit(lookahead)) {
          array_push(proof_step_id_level, (char)(lookahead & CHAR_MAX));
          ADVANCE(LexState_PROOF_LEVEL_NUMBER);
        }
        if ('>' == lookahead) ADVANCE(LexState_PROOF_NAME);
        ADVANCE(LexState_OTHER);
        END_LEX_STATE();
      case LexState_PROOF_LEVEL_STAR:
        if ('>' == lookahead) ADVANCE(LexState_PROOF_NAME);
        ADVANCE(LexState_OTHER);
        END_LEX_STATE();
      case LexState_PROOF_LEVEL_PLUS:
        if ('>' == lookahead) ADVANCE(LexState_PROOF_NAME);
        ADVANCE(LexState_OTHER);
        END_LEX_STATE();
      case LexState_PROOF_NAME:
        ACCEPT_LEXEME(Lexeme_PROOF_STEP_ID);
        if (iswalnum(lookahead)) ADVANCE(LexState_PROOF_NAME);
        if ('.' == lookahead) ADVANCE(LexState_PROOF_ID);
        END_LEX_STATE();
      case LexState_PROOF_ID:
        ACCEPT_LEXEME(Lexeme_PROOF_STEP_ID);
        if ('.' == lookahead) ADVANCE(LexState_PROOF_ID);
        END_LEX_STATE();
      case LexState_IDENTIFIER:
        ACCEPT_LEXEME(Lexeme_IDENTIFIER);
        END_LEX_STATE();
      case LexState_OTHER:
        ACCEPT_LEXEME(Lexeme_OTHER);
        END_LEX_STATE();
      case LexState_END_OF_FILE:
        ACCEPT_LEXEME(Lexeme_END_OF_FILE);
        END_LEX_STATE();
      default:
        ACCEPT_LEXEME(Lexeme_OTHER);
        END_LEX_STATE();
    }
  }

  // Tokens recognized by this scanner.
  enum Token {
    Token_LAND,
    Token_LOR,
    Token_DOUBLE_EXCL,
    Token_RIGHT_DELIMITER,
    Token_COMMENT_START,
    Token_TERMINATOR,
    Token_PROOF_STEP_ID,
    Token_PROOF_KEYWORD,
    Token_BY_KEYWORD,
    Token_OBVIOUS_KEYWORD,
    Token_OMITTED_KEYWORD,
    Token_QED_KEYWORD,
    Token_WEAK_FAIRNESS,
    Token_STRONG_FAIRNESS,
    Token_OTHER
  };

  /**
   * Maps the given lexeme to a token.
   *
   * @param lexeme The lexeme to map to a token.
   * @return The token corresponding to the given lexeme.
   */
  static enum Token tokenize_lexeme(enum Lexeme lexeme) {
    switch (lexeme) {
      case Lexeme_FORWARD_SLASH: return Token_OTHER;
      case Lexeme_BACKWARD_SLASH: return Token_OTHER;
      case Lexeme_GT: return Token_OTHER;
      case Lexeme_EQ: return Token_OTHER;
      case Lexeme_DASH: return Token_OTHER;
      case Lexeme_COMMA: return Token_RIGHT_DELIMITER;
      case Lexeme_COLON: return Token_RIGHT_DELIMITER;
      case Lexeme_SEMICOLON: return Token_TERMINATOR;
      case Lexeme_LAND: return Token_LAND;
      case Lexeme_LOR: return Token_LOR;
      case Lexeme_DOUBLE_EXCL: return Token_DOUBLE_EXCL;
      case Lexeme_L_PAREN: return Token_OTHER;
      case Lexeme_R_PAREN: return Token_RIGHT_DELIMITER;
      case Lexeme_R_SQUARE_BRACKET: return Token_RIGHT_DELIMITER;
      case Lexeme_R_CURLY_BRACE: return Token_RIGHT_DELIMITER;
      case Lexeme_R_ANGLE_BRACKET: return Token_RIGHT_DELIMITER;
      case Lexeme_RIGHT_ARROW: return Token_RIGHT_DELIMITER;
      case Lexeme_RIGHT_MAP_ARROW: return Token_RIGHT_DELIMITER;
      case Lexeme_COMMENT_START: return Token_COMMENT_START;
      case Lexeme_BLOCK_COMMENT_START: return Token_COMMENT_START;
      case Lexeme_SINGLE_LINE: return Token_TERMINATOR;
      case Lexeme_DOUBLE_LINE: return Token_TERMINATOR;
      case Lexeme_ASSUME_KEYWORD: return Token_TERMINATOR;
      case Lexeme_ASSUMPTION_KEYWORD: return Token_TERMINATOR;
      case Lexeme_AXIOM_KEYWORD: return Token_TERMINATOR;
      case Lexeme_BY_KEYWORD: return Token_BY_KEYWORD;
      case Lexeme_CONSTANT_KEYWORD: return Token_TERMINATOR;
      case Lexeme_CONSTANTS_KEYWORD: return Token_TERMINATOR;
      case Lexeme_COROLLARY_KEYWORD: return Token_TERMINATOR;
      case Lexeme_ELSE_KEYWORD: return Token_RIGHT_DELIMITER;
      case Lexeme_IN_KEYWORD: return Token_RIGHT_DELIMITER;
      case Lexeme_LEMMA_KEYWORD: return Token_TERMINATOR;
      case Lexeme_LOCAL_KEYWORD: return Token_TERMINATOR;
      case Lexeme_OBVIOUS_KEYWORD: return Token_OBVIOUS_KEYWORD;
      case Lexeme_OMITTED_KEYWORD: return Token_OMITTED_KEYWORD;
      case Lexeme_PROOF_KEYWORD: return Token_PROOF_KEYWORD;
      case Lexeme_PROPOSITION_KEYWORD: return Token_TERMINATOR;
      case Lexeme_THEN_KEYWORD: return Token_RIGHT_DELIMITER;
      case Lexeme_THEOREM_KEYWORD: return Token_TERMINATOR;
      case Lexeme_VARIABLE_KEYWORD: return Token_TERMINATOR;
      case Lexeme_VARIABLES_KEYWORD: return Token_TERMINATOR;
      case Lexeme_PROOF_STEP_ID: return Token_PROOF_STEP_ID;
      case Lexeme_QED_KEYWORD: return Token_QED_KEYWORD;
      case Lexeme_STRONG_FAIRNESS: return Token_STRONG_FAIRNESS;
      case Lexeme_WEAK_FAIRNESS: return Token_WEAK_FAIRNESS;
      case Lexeme_IDENTIFIER: return Token_OTHER;
      case Lexeme_OTHER: return Token_OTHER;
      case Lexeme_END_OF_FILE: return Token_TERMINATOR;
      default: return Token_OTHER;
    }
  }

  // Possible types of junction list.
  enum JunctType {
    JunctType_CONJUNCTION,
    JunctType_DISJUNCTION
  };

  // Data about a jlist.
  struct JunctList {

    // The type of jlist.
    enum JunctType type;

    // The starting alignment columnt of the jlist.
    column_index alignment_column;
  };

    static struct JunctList create_junctlist(enum JunctType const type, column_index const alignment_column) {
      struct JunctList jlist;
      jlist.type = type;
      jlist.alignment_column = alignment_column;
      return jlist;
    }

    static unsigned jlist_serialize(struct JunctList* this, char* const buffer, bool const is_dry_run) {
      unsigned offset = 0;
      unsigned copied = 0;

      // Serialize junction type
      copied = sizeof(uint8_t);
      if (!is_dry_run) { buffer[offset] = (uint8_t)(this->type); }
      offset += copied;

      // Serialize alignment column
      copied = sizeof(column_index);
      if (!is_dry_run) { memcpy(&buffer[offset], &this->alignment_column, copied); }
      offset += copied;

      return offset;
    }

    static unsigned jlist_deserialize(struct JunctList* this, const char* const buffer) {
      unsigned offset = 0;
      unsigned copied = 0;

      // Deserialize junction type
      copied = sizeof(uint8_t);
      this->type = (enum JunctType)buffer[offset];
      offset += copied;

      // Deserialize alignment column
      copied = sizeof(column_index);
      memcpy(&this->alignment_column, &buffer[offset], copied);
      offset += copied;

      return offset;
    }

  /**
   * A stateful scanner used to parse junction lists and proofs.
   */
  struct Scanner {

    // The nested junction lists at the current lexer position.
    Array(struct JunctList) jlists;

    // The nested proofs at the current lexer position.
    Array(proof_level) proofs;

    // The level of the last proof.
    proof_level last_proof_level;

    // Whether we have seen a PROOF token.
    bool have_seen_proof_keyword;
};

    /**
     * Serializes the Scanner state into the given buffer.
     *
     * @param this The Scanner state to serialize.
     * @param buffer The buffer into which to serialize the scanner state.
     * @param is_dry_run Whether to actually copy the bytes to the buffer.
     * @return Number of bytes written into the buffer.
     */
    static unsigned scanner_try_serialize(
      const struct Scanner* const this,
      char* const buffer,
      const bool is_dry_run
    ) {
      unsigned offset = 0;
      unsigned copied = 0;

      const nest_address jlist_depth = (nest_address)(this->jlists.size);
      copied = sizeof(nest_address);
      if (!is_dry_run) { memcpy(&buffer[offset], &jlist_depth, copied); }
      offset += copied;
      for (nest_address i = 0; i < jlist_depth; i++) {
        char* const buffer_addr = is_dry_run ? NULL : &buffer[offset];
        copied = jlist_serialize(array_get(&this->jlists, i), buffer_addr, is_dry_run);
        offset += copied;
      }

      const nest_address proof_depth = (nest_address)(this->proofs.size);
      copied = sizeof(nest_address);
      if (!is_dry_run) { memcpy(&buffer[offset], &proof_depth, copied); }
      offset += copied;
      copied = proof_depth * sizeof(proof_level);
      if (!is_dry_run && copied > 0) { memcpy(&buffer[offset], this->proofs.contents, copied); }
      offset += copied;

      copied = sizeof(proof_level);
      if (!is_dry_run) { memcpy(&buffer[offset], &this->last_proof_level, copied); }
      offset += copied;

      copied = sizeof(uint8_t);
      if (!is_dry_run) { buffer[offset] = (uint8_t)(this->have_seen_proof_keyword); }
      offset += copied;

      return offset;
    }

    /**
     * Serializes the Scanner state into the given buffer.
     *
     * @param this The Scanner state to serialize.
     * @param buffer The buffer into which to serialize the scanner state.
     * @return Number of bytes written into the buffer.
     */
    static unsigned scanner_serialize(const struct Scanner* this, char* buffer) {
      return scanner_try_serialize(this, buffer, false);
    }

    /**
     * Calculates the serialized size of the scanner.
     *
     * @param this The Scanner state to find the deserialized size of.
     * @return The size of the Scanner when serialized.
     */
    static unsigned scanner_serialized_size(const struct Scanner* const this) {
      return scanner_try_serialize(this, NULL, true);
    }

    /**
     * Deserializes the Scanner state from the given buffer.
     *
     * @param this The Scanner state to deserialize.
     * @param buffer The buffer from which to deserialize the state.
     * @param length The bytes available to read from the buffer.
     */
    static void scanner_deserialize(struct Scanner* const this, const char* const buffer, unsigned const length) {
      // Very important to clear values of all fields here!
      // Scanner object is reused; if a variable isn't cleared, it can
      // lead to extremely strange & impossible-to-debug behavior.
      array_delete(&this->jlists);
      array_delete(&this->proofs);
      this->last_proof_level = -1;
      this->have_seen_proof_keyword = false;

      if (length > 0) {
        unsigned offset = 0;
        unsigned copied = 0;

        nest_address jlist_depth = 0;
        copied = sizeof(nest_address);
        memcpy(&jlist_depth, &buffer[offset], copied);
        array_grow_by(&(this->jlists), jlist_depth);
        offset += copied;

        for (nest_address i = 0; i < jlist_depth; i++) {
          assert(offset < length);
          copied = jlist_deserialize(array_get(&(this->jlists), i), &buffer[offset]);
          offset += copied;
        }

        nest_address proof_depth = 0;
        copied = sizeof(nest_address);
        memcpy(&proof_depth, &buffer[offset], copied);
        array_grow_by(&(this->proofs), proof_depth);
        offset += copied;

        copied = proof_depth * sizeof(proof_level);
        if (copied > 0) { memcpy(this->proofs.contents, &buffer[offset], copied); }
        offset += copied;

        copied = sizeof(proof_level);
        memcpy(&(this->last_proof_level), &buffer[offset], copied);
        offset += copied;

        copied = sizeof(uint8_t);
        this->have_seen_proof_keyword = (bool)(buffer[offset] & 1);
        offset += copied;

        assert(offset == length);
      }
    }

    /**
     * Initializes a new instance of the Scanner object.
     *
     * @return A newly-created Scanner.
     */
    static struct Scanner scanner_create() {
      struct Scanner s;
      array_init(&s.jlists);
      array_init(&s.proofs);
      s.last_proof_level = -1;
      s.have_seen_proof_keyword = false;
      return s;
    }

    /**
     * Frees all memory associated with this Scanner.
     *
     * @param this The Scanner to free.
     */
    static void scanner_free(struct Scanner* const this) {
      array_delete(&this->jlists);
      array_delete(&this->proofs);
    }

    /**
     * Whether the Scanner state indicates we are currently in a jlist.
     *
     * @param this The Scanner state.
     * @return Whether we are in a jlist.
     */
    static bool is_in_jlist(const struct Scanner* const this) {
      return this->jlists.size > 0;
    }

    /**
     * The column index of the current jlist. Returns negative number if
     * we are not currently in a jlist.
     *
     * @param this The Scanner state.
     * @return The column index of the current jlist.
     */
    static column_index get_current_jlist_column_index(const struct Scanner* const this) {
      return is_in_jlist(this) ? array_back(&this->jlists)->alignment_column : -1;
    }

    /**
     * Whether the given jlist type matches the current jlist.
     *
     * @param this The Scanner state.
     * @param type The jlist type to check.
     * @return Whether the given jlist type matches the current jlist.
     */
    static bool current_jlist_type_is(
      struct Scanner* const this,
      enum JunctType const type
    ) {
      return is_in_jlist(this) && type == array_back(&this->jlists)->type;
    }

    /**
     * Assuming the lexer is situated directly after a junct token, read
     * to the next non-whitespace char to check whether it is a , or ) token.
     * If so, this is not the start of a new jlist; instead, it is a higher-
     * level parameter to an operator, like op(x, /\) or op(\/, x).
     *
     * @param lexer The tree-sitter lexing control structure.
     */
    static bool is_junct_token_higher_level_op_parameter(TSLexer* const lexer) {
      while (iswspace(lexer->lookahead) && has_next(lexer)) {
        lexer->advance(lexer, true);
      }
      return ',' == lexer->lookahead || ')' == lexer->lookahead;
    }

    /**
     * Emits an INDENT token, recording the new jlist in the Scanner state.
     *
     * @param this The Scanner state.
     * @param lexer The tree-sitter lexing control structure.
     * @param type The type of the new jlist.
     * @param col The column position of the new jlist.
     * @return Whether an INDENT token was emitted.
     */
    static bool emit_indent(
      struct Scanner* const this,
      TSLexer* const lexer,
      enum JunctType const type,
      column_index const col
    ) {
      lexer->result_symbol = INDENT;
      struct JunctList new_list = create_junctlist(type, col);
      array_push(&this->jlists, new_list);
      return true;
    }

    /**
     * Emits a BULLET token, marking the start of a new item in the current
     * jlist.
     *
     * @param lexer The tree-sitter lexing control structure.
     * @return Whether a BULLET token was emitted.
     */
    static bool emit_bullet(TSLexer* const lexer) {
      lexer->result_symbol = BULLET;
      return true;
    }

    /**
     * Emits a DEDENT token, removing a jlist from the Scanner state.
     *
     * @param this The Scanner state.
     * @param lexer The tree-sitter lexing control structure.
     * @return Whether a DEDENT token was emitted.
     */
    static bool emit_dedent(struct Scanner* const this, TSLexer* const lexer) {
      if (is_in_jlist(this)) {
        lexer->result_symbol = DEDENT;
        (void)array_pop(&this->jlists);
        return true;
      } else {
        return false;
      }
    }

    /**
     * Jlists are identified with the column position (cpos) of the first
     * junct token in the list, and the junction type. For a given junct
     * token there are five possible interpretations:
     * 1. The junct is after the cpos of the current jlist, and an
     *    INDENT token is expected
     *    -> this is a new nested jlist, emit INDENT token
     * 2. The junct is after the cpos of the current jlist, and an
     *    INDENT token is *not* expected
     *    -> this is an infix junct operator; emit nothing
     * 3. The junct is equal to the cpos of the current jlist, and is
     *    the same junct type (conjunction or disjunction)
     *    -> this is an item of the current jlist; emit BULLET token
     * 4. The junct is equal to the cpos of the current jlist, and is
     *    a DIFFERENT junct type (conjunction vs. disjunction)
     *    -> this is an infix operator that also ends the current list
     * 5. The junct is prior to the cpos of the current jlist
     *    -> this ends the current jlist, emit DEDENT token
     *
     * @param this The Scanner state.
     * @param lexer The tree-sitter lexing control structure.
     * @param valid_symbols Tokens possibly expected in this spot.
     * @param type The type of junction encountered.
     * @param next The column position of the junct token encountered.
     * @return Whether a jlist-relevant token should be emitted.
     */
    static bool handle_junct_token(
      struct Scanner* const this,
      TSLexer* const lexer,
      const bool* const valid_symbols,
      enum JunctType const next_type,
      column_index const next_col
    ) {
      const column_index current_col = get_current_jlist_column_index(this);
      if (current_col < next_col) {
        if (valid_symbols[INDENT]) {
          if (is_junct_token_higher_level_op_parameter(lexer)) {
            /**
             * Handle case op(x, /\) or op(\/, x) - junct token is the first
             * token of a new expression and thus could be a new jlist, but
             * actually is a higher-level operator parameter.
             */
            return false;
          } else {
            /**
             * The start of a new junction list!
             */
            return emit_indent(this, lexer, next_type, next_col);
          }
        } else {
          /**
           * This is an infix junction symbol. Tree-sitter will only look for
           * a new jlist at the start of an expression rule; infix operators
           * occur when joining two expression rules together, so tree-sitter
           * is only looking for either BULLET or DEDENT rules. Examples:
           *
           *   /\ a /\ b
           *       ^ tree-sitter will NEVER look for an INDENT here
           *
           *   /\ a
           *   /\ b
           *  ^ tree-sitter WILL look for a BULLET here
           *
           *   /\ /\ a
           *     ^ tree-sitter WILL look for an INDENT here
           */
          return false;
        }
      } else if (current_col == next_col) {
        if (current_jlist_type_is(this, next_type)) {
          /**
           * This is another entry in the jlist.
           */
          return emit_bullet(lexer);
        } else {
          /**
           * Disjunct in alignment with conjunct list or vice-versa; treat
           * this as an infix operator by terminating the current list.
           */
          return emit_dedent(this, lexer);
        }
      } else {
        /**
         * Junct found prior to the alignment column of the current jlist.
         * This marks the end of the jlist.
         */
        return emit_dedent(this, lexer);
      }
    }

    /**
     * If a given right delimiter matches some left delimiter that occurred
     * *before* the beginning of the current jlist, then that ends the
     * current jlist. The concept of a delimiter is not limited (hah) to
     * (), [], <<>>, and {}; it also includes IF/THEN, THEN/ELSE, CASE/->,
     * and basically every other language construct where an expression is
     * squeezed between a known start & end token.
     *
     * Previously I implemented complicated logic using a stack to keep
     * track of all the delimiters that have been seen (and their
     * pairs) but found that tree-sitter would never trigger the
     * external scanner before encountering a right delimiter matching
     * a left delimiter that started within the scope of a jlist. Thus
     * we can assume that when we *do* see a right delimiter, it
     * matches a left delimiter that occurred prior to the start of the
     * jlist, so we can emit a DEDENT token to end the jlist. Example:
     *
     *    /\ ( a + b )
     *              ^ tree-sitter will never look for an INDENT,
     *                BULLET, or DEDENT token here; it is only
     *                looking for another infix operator or the
     *                right-delimiter.
     *
     *    ( /\ a + b )
     *              ^ tree-sitter WILL look for an INDENT, BULLET, or
     *                DEDENT token here in addition to looking for an
     *                infix operator; it also wants to see a DEDENT
     *                token before seeing the right delimiter, although
     *                error recovery is simple enough that it would
     *                barely notice its absence.
     *
     * There are a few notable exceptions to this rule; for example, the
     * empty set or empty sequence:
     *
     *    /\  { }
     *         ^
     *    /\ << >>
     *         ^ there is the option for an expression here, so tree-sitter
     *           looks for INDENT tokens and we will see a right delimiter
     *           in this external scanner.
     *
     * Another example when the code is in a non-parseable state which we
     * nonetheless wish to handle gracefully:
     *
     *    /\ [x \in S |-> ]
     *                   ^ user is about to write an expression here, but
     *                     there is a time when the code is non-parseable;
     *                     tree-sitter will again look for an INDENT token
     *                     and we will see a right delimiter in this
     *                     external scanner.
     *
     * The easy solution to these cases is to simply check whether
     * tree-sitter is looking for a DEDENT token. If so, emit one; if not,
     * emit nothing. Tree-sitter will not look for a DEDENT token inside
     * enclosing delimiters within the scope of a jlist.
     *
     * One side-effect of all this is that tree-sitter parses certain
     * arrangements of jlists and delimiters that are actually illegal
     * according to TLA⁺ syntax rules; that is okay since tree-sitter's
     * use case of error-tolerant editor tooling ensures its design
     * errs on the side of being overly-permissive. For a concrete
     * example here, tree-sitter will parse this illegal expression
     * without complaint:
     *
     *    /\ A
     *    /\ (B + C
     *  )
     *    /\ D
     *
     * This should simply be detected as an error at the semantic level.
     *
     * @param this The Scanner state.
     * @param lexer The tree-sitter lexing control structure.
     * @param valid_symbols Tokens possibly expected in this spot.
     * @return Whether a jlist-relevant token should be emitted.
     */
    static bool handle_right_delimiter_token(
      struct Scanner* const this,
      TSLexer* const lexer,
      const bool* const valid_symbols
    ) {
      return is_in_jlist(this)
        && valid_symbols[DEDENT]
        && emit_dedent(this, lexer);
    }

    /**
     * Emits a dedent token if are in jlist and have encountered a token that
     * unconditionally ends a jlist regardless of column position; these
     * include:
     * 1. New unit definition (op == expr, etc.)
     * 2. End-of-module token (====)
     * 3. End-of-file (this shouldn't happen but we will end the jlist to
     *    improve error reporting since the end-of-module token is missing)
     *
     * @param this The Scanner state.
     * @param lexer The tree-sitter lexing control structure.
     * @return Whether a jlist-relevant token should be emitted.
     */
    static bool handle_terminator_token(
      struct Scanner* const this,
      TSLexer* const lexer
    ) {
      return is_in_jlist(this) && emit_dedent(this, lexer);
    }

    /**
     * Non-junct tokens could possibly indicate the end of a jlist. Rules:
     * - If the token cpos is leq to the current jlist cpos, the jlist
     *   has ended; emit a DEDENT token (possibly multiple); example:
     *      IF  /\ P
     *          /\ Q
     *      THEN R
     *      ELSE S
     * - Otherwise the token is treated as part of the expression in that
     *   junct; for example:
     *      /\ IF e THEN P
     *              ELSE Q
     *      /\ R
     *   so emit no token.
     *
     * @param this The Scanner state.
     * @param lexer The tree-sitter lexing control structure.
     * @param next The column position of the encountered token.
     * @return Whether a jlist-relevant token should be emitted.
     */
    static bool handle_other_token(
      struct Scanner* const this,
      TSLexer* const lexer,
      column_index const next
    ) {
      return is_in_jlist(this)
        && next <= get_current_jlist_column_index(this)
        && emit_dedent(this, lexer);
    }

    /**
     * Handles the infix !! operator. This has a lexical conflict with the
     * subexpression separator !, so an expression like A!!!B will be parsed
     * incorrectly as A !! (!B) instead of (A!) !! B. Ideally tree-sitter
     * would add negative-lookahead in regexes so this wouldn't have to be
     * corrected in the external scanner, but it is what it is.
     *
     * This function first checks whether the !! token is interfering with an
     * existing jlist. If not, it checks the next character to see whether it
     * is !. If so, this function emits false - no !! token found - under the
     * assumption that this is a A!!!B scenario so this string should be
     * split into (!)(!!) instead of (!!)(!).
     *
     * Note there is a case A!!!!B which should be grouped as (A!) (!!) (!B)
     * but will not be according to these rules. The workaround for this case
     * (as with several other lexing ambiguities in this language) is to
     * insert a space for disambiguation, like A!!! !B. Humorously, this does
     * mean that !!!!!!!!!!!!!!!!!!!!!! (ad infinitum) is valid TLA+ syntax.
     *
     * @param this The Scanner state.
     * @param lexer The tree-sitter lexing control structure.
     * @param next The column position of the encountered token.
     */
    static bool handle_double_excl_token(
      struct Scanner* const this,
      TSLexer* const lexer,
      column_index const next
    ) {
      if (handle_other_token(this, lexer, next)) {
        // This token is affecting a jlist, emit dedent
        return true;
      } else {
        if ('!' == lexer->lookahead) {
          return false;
        } else {
          lexer->mark_end(lexer);
          lexer->result_symbol = DOUBLE_EXCL;
          return true;
        }
      }
    }

    /**
     * Gets whether we are currently in a proof.
     *
     * @param this The Scanner state.
     * @return Whether we are currently in a proof.
     */
    static bool is_in_proof(const struct Scanner* const this) {
      return this->proofs.size > 0;
    }

    /**
     * Gets the current proof level; -1 if none.
     *
     * @param this The Scanner state.
     * @return The current proof level.
     */
    static proof_level get_current_proof_level(const struct Scanner* const this) {
      return is_in_proof(this) ? *array_back(&this->proofs) : -1;
    }

    /**
     * Emits a token indicating the start of a new proof.
     *
     * @param this The Scanner state.
     * @param lexer The tree-sitter lexing control structure.
     * @param level The level of the new proof.
     * @return Whether a token should be emitted.
     */
    static bool emit_begin_proof(
      struct Scanner* const this,
      TSLexer* const lexer,
      proof_level level
    ) {
      lexer->result_symbol = BEGIN_PROOF;
      array_push(&this->proofs, level);
      this->last_proof_level = level;
      this->have_seen_proof_keyword = false;
      return true;
    }

    /**
     * Emits a token indicating the start of a new proof step.
     *
     * @param this The Scanner state.
     * @param lexer The tree-sitter lexing control structure.
     * @param level The level of the new proof step.
     * @return Whether a token should be emitted.
     */
    static bool emit_begin_proof_step(
      struct Scanner* const this,
      TSLexer* const lexer,
      proof_level level
    ) {
      this->last_proof_level = level;
      lexer->result_symbol = BEGIN_PROOF_STEP;
      return true;
    }

    /**
     * Handle encountering a new proof step ID. This probably marks the
     * beginning of a new proof step, but could also be a reference to a
     * prior proof step as part of an expression. There are also various
     * interactions between the proof step ID and jlists. Cases:
     * 1. A proof step token is expected
     *    -> This is a new proof step; see proof step logic below
     * 2. A proof step token is *not* expected, and a DEDENT token *is*
     *    -> This is the beginning of a proof step but there is an open
     *       jlist which must first be closed; emit a DEDENT token.
     * 3. A proof step token is not expected, and neither is a DEDENT
     *    -> This is a proof step reference, so treat as other token.
     *       P => <1>b
     *           ^ tree-sitter will only look for INDENT here
     *
     * For handling proof steps alone, there are the following cases:
     * 1. The new proof token level is greater than the current level
     *    -> This is the start of a new proof; emit BEGIN_PROOF token
     *       and push level to proof stack. Set last_proof_level to
     *       the new proof level.
     * 2. The new proof token level is equal to the current level
     *    -> This is another proof step; emit BEGIN_PROOF_STEP token.
     * 3. The new proof token level is less than the current level
     *    -> This is an error, which we will try to recover from.
     *
     * There are also rules to handle proof step IDs where the level is
     * inferred, like <+> and <*>. They are as follows:
     * 1. The proof step ID is <+>
     *    -> This is the start of a new proof; its level is one higher
     *       than last_proof_level.
     * 2. The proof step ID is <*> and we are not inside a proof
     *    -> This is the start of the very first proof; its level is one
     *       higher than last_proof_level, which should be -1; thus the
     *       proof level should be 0.
     * 3. The proof step ID is <*> and it directly follows a PROOF keyword
     *    -> This is the start of a new proof; its level is one higher
     *       than last_proof_level.
     * 4. The proof step ID is <*> and it *does not* follow a PROOF keyword
     *    -> This is another step in the same proof; its level is the
     *       same as last_proof_level.
     *
     * Proofs are ended upon encountering a QED step, which is handled
     * elsewhere.
     *
     * @param this The Scanner state.
     * @param lexer The tree-sitter lexing control structure.
     * @param valid_symbols Tokens possibly expected in this spot.
     * @param next The column position of the encountered token.
     * @return Whether a token should be emitted.
     */
    static bool handle_proof_step_id_token(
      struct Scanner* const this,
      TSLexer* const lexer,
      const bool* const valid_symbols,
      column_index const next,
      struct ProofStepId proof_step_id_token
    ) {
      assert(ProofStepIdType_NONE != proof_step_id_token.type);
      if (valid_symbols[BEGIN_PROOF] || valid_symbols[BEGIN_PROOF_STEP]) {
        proof_level next_proof_level = -1;
        const proof_level current_proof_level = get_current_proof_level(this);
        switch (proof_step_id_token.type) {
          case ProofStepIdType_STAR:
            /**
             * <*> can start a new proof only at the very first level,
             * or directly following a PROOF keyword.
             */
            next_proof_level =
              !is_in_proof(this) || this->have_seen_proof_keyword
              ? this->last_proof_level + 1
              : current_proof_level;
            break;
          case ProofStepIdType_PLUS:
            /**
             * This keeps us from entering an infinite loop when we see
             * a <+> proof step ID; the first time we encounter the <+>,
             * we will increase the level and emit a BEGIN_PROOF token.
             * The second time, we mark it as the same level and emit a
             * BEGIN_PROOF_STEP token.
             */
            next_proof_level = valid_symbols[BEGIN_PROOF]
              ? this->last_proof_level + 1
              : current_proof_level;
            break;
          case ProofStepIdType_NUMBERED:
            next_proof_level = proof_step_id_token.level;
            break;
          default:
            return false;
        }

        if (next_proof_level > current_proof_level) {
          return emit_begin_proof(this, lexer, next_proof_level);
        } else if (next_proof_level == current_proof_level) {
          if (this->have_seen_proof_keyword) {
            // This has been declared a new proof using the PROOF keyword
            // but does not have a level greater than the old; thus we've
            // detected a syntax error.
            // TODO: handle this.
            return false;
          } else {
            return emit_begin_proof_step(this, lexer, next_proof_level);
          }
        } else {
          // The next proof level is lower than the current. This is
          // invalid syntax.
          // TODO: handle this.
          return false;
        }
      } else {
        if (valid_symbols[DEDENT]) {
          // End all jlists before start of proof.
          return handle_terminator_token(this, lexer);
        } else {
          // This is a reference to a proof step in an expression.
          return handle_other_token(this, lexer, next);
        }
      }
    }

    /**
     * Handles the PROOF keyword token. We record that we've seen the
     * PROOF keyword, which modifies the interpretation of the subsequent
     * proof step ID. The PROOF token also terminates any current jlist.
     *
     * @param this The Scanner state.
     * @param lexer The tree-sitter lexing control structure.
     * @param valid_symbols Tokens possibly expected in this spot.
     * @return Whether a token should be emitted.
     */
    static bool handle_proof_keyword_token(
      struct Scanner* const this,
      TSLexer* const lexer,
      const bool* const valid_symbols
    ) {
      if (valid_symbols[PROOF_KEYWORD]) {
        this->have_seen_proof_keyword = true;
        lexer->result_symbol = PROOF_KEYWORD;
        lexer->mark_end(lexer);
        return true;
      } else {
        return handle_terminator_token(this, lexer);
      }
    }

    /**
     * Handles the BY, OBVIOUS, and OMITTED keyword tokens. We record
     * that we've seen the keyword, which negates any PROOF keyword
     * previously encountered. These tokens also terminate any current
     * jlist.
     *
     * @param this The Scanner state.
     * @param lexer The tree-sitter lexing control structure.
     * @param valid_symbols Tokens possibly expected in this spot.
     * @param keyword_type The specific keyword being handled.
     * @return Whether a token should be emitted.
     */
    static bool handle_terminal_proof_keyword_token(
      struct Scanner* const this,
      TSLexer* const lexer,
      const bool* const valid_symbols,
      enum TokenType keyword_type
    ) {
      if (valid_symbols[keyword_type]) {
        this->have_seen_proof_keyword = false;
        lexer->result_symbol = keyword_type;
        lexer->mark_end(lexer);
        return true;
      } else {
        return handle_terminator_token(this, lexer);
      }
    }

    /**
     * Handles the QED keyword token. The QED token indicates this is the
     * final step of a proof, so we modify the state accordingly. First
     * we record the current proof level in case there is a child proof
     * of this step that uses <+> or PROOF <*> for its first step. Then
     * we pop the top proof level off the stack.
     *
     * It's possible to encounter a QED keyword while not inside of a proof
     * as part of an earlier syntax error, and proof step IDs being treated
     * as sequences of < and > operators. In this case the current proof
     * state should not be modified, but a QED keyword token is still
     * returned to help the error recovery process. Not performing this
     * check previously led to a segfault; see:
     * https://github.com/tlaplus-community/tree-sitter-tlaplus/issues/60
     *
     * @param this The Scanner state.
     * @param lexer The tree-sitter lexing control structure.
     * @return Whether a token should be emitted.
     */
    static bool handle_qed_keyword_token(
      struct Scanner* const this,
      TSLexer* const lexer
    ) {
      if (is_in_proof(this)) {
        this->last_proof_level = get_current_proof_level(this);
        (void)array_pop(&this->proofs);
      }

      lexer->result_symbol = QED_KEYWORD;
      lexer->mark_end(lexer);
      return true;
    }

    /**
     * Handles the fairness tokens WF_ and SF_.
     * Need to handle this in an external scanner due to:
     * https://github.com/tree-sitter/tree-sitter/issues/1615
     *
     * @param this The Scanner state.
     * @param lexer The tree-sitter lexing control structure.
     * @param next The column position of the encountered token.
     * @param keyword_type The specific keyword being handled.
     * @return Whether a token should be emitted.
     */
    static bool handle_fairness_keyword_token(
      struct Scanner* const this,
      TSLexer* const lexer,
      column_index const next,
      enum TokenType keyword_type
    ) {
      if (handle_other_token(this, lexer, next)) {
        return true;
      } else {
        lexer->result_symbol = keyword_type;
        lexer->mark_end(lexer);
        return true;
      }
    }

    /**
     * Scans for various possible external tokens.
     *
     * @param this The Scanner state.
     * @param lexer The tree-sitter lexing control structure.
     * @param valid_symbols Tokens possibly expected in this spot.
     * @return Whether a token was encountered.
     */
    static bool scan(
      struct Scanner* const this,
      TSLexer* const lexer,
      const bool* const valid_symbols
    ) {
      // All symbols are marked as valid during error recovery.
      // We can check for this by looking at the validity of the final
      // (unused) external symbol, ERROR_SENTINEL.
      const bool is_error_recovery = valid_symbols[ERROR_SENTINEL];

      // TODO: actually function during error recovery
      // https://github.com/tlaplus-community/tree-sitter-tlaplus/issues/19
      if (is_error_recovery) {
        return false;
      }

      if(valid_symbols[LEADING_EXTRAMODULAR_TEXT] || valid_symbols[TRAILING_EXTRAMODULAR_TEXT]) {
        return scan_extramodular_text(lexer, valid_symbols);
      } else {
        column_index col = -1;
        CharArray proof_step_id_level = array_new();
        enum Token token = tokenize_lexeme(lex_lookahead(lexer, &col, &proof_step_id_level));
        struct ProofStepId proof_step_id_token = parse_proof_step_id(&proof_step_id_level);
        array_delete(&proof_step_id_level);
        switch (token) {
          case Token_LAND:
            return handle_junct_token(this, lexer, valid_symbols, JunctType_CONJUNCTION, col);
          case Token_LOR:
            return handle_junct_token(this, lexer, valid_symbols, JunctType_DISJUNCTION, col);
          case Token_RIGHT_DELIMITER:
            return handle_right_delimiter_token(this, lexer, valid_symbols);
          case Token_COMMENT_START:
            return false;
          case Token_TERMINATOR:
            return handle_terminator_token(this, lexer);
          case Token_PROOF_STEP_ID:
            return handle_proof_step_id_token(this, lexer, valid_symbols, col, proof_step_id_token);
          case Token_PROOF_KEYWORD:
            return handle_proof_keyword_token(this, lexer, valid_symbols);
          case Token_BY_KEYWORD:
            return handle_terminal_proof_keyword_token(this, lexer, valid_symbols, BY_KEYWORD);
          case Token_OBVIOUS_KEYWORD:
            return handle_terminal_proof_keyword_token(this, lexer, valid_symbols, OBVIOUS_KEYWORD);
          case Token_OMITTED_KEYWORD:
            return handle_terminal_proof_keyword_token(this, lexer, valid_symbols, OMITTED_KEYWORD);
          case Token_QED_KEYWORD:
            return handle_qed_keyword_token(this, lexer);
          case Token_WEAK_FAIRNESS:
            return handle_fairness_keyword_token(this, lexer, col, WEAK_FAIRNESS);
          case Token_STRONG_FAIRNESS:
            return handle_fairness_keyword_token(this, lexer, col, STRONG_FAIRNESS);
          case Token_DOUBLE_EXCL:
            return handle_double_excl_token(this, lexer, col);
          case Token_OTHER:
            return handle_other_token(this, lexer, col);
          default:
            return false;
        }
      }
    }

  /**
   * A hierarchy of nested stateful scanners.
   * Each time a PlusCal block is entered, a nested context is created.
   * Exiting the PlusCal block exits the context.
   * Multiply-nested PlusCal blocks are supported.
   */
  struct NestedScanner {

    // The enclosing context(s) of the PlusCal block.
    Array(CharArray) enclosing_contexts;

    // The currently-active context.
    struct Scanner current_context;

  };

    /**
     * Serialize the nested scanner into a buffer.
     *
     * @param this The NestedScanner state.
     * @param buffer The buffer to serialize the state into.
     * @return The number of bytes written into the buffer.
     */
    static unsigned nested_scanner_serialize(
      const struct NestedScanner* const this,
      char* const buffer
    ) {
      unsigned offset = 0;
      unsigned copied = 0;

      // First write number of enclosing contexts (guaranteed to be >= 1)
      nest_address const context_depth = this->enclosing_contexts.size + 1;
      copied = sizeof(nest_address);
      if (copied > 0) memcpy(&buffer[offset], &context_depth, copied);
      offset += copied;

      // Then write size of N-1 enclosing contexts
      for (int i = 0; i < context_depth - 1; i++) {
        unsigned const context_size = array_get(&this->enclosing_contexts, i)->size;
        copied = sizeof(unsigned);
        if (copied > 0) memcpy(&buffer[offset], &context_size, copied);
        offset += copied;
      }

      // Reserve space for current context size
      unsigned const current_context_size_offset = offset;
      copied = sizeof(unsigned);
      offset += copied;

      // Serialize N-1 enclosing contexts
      for (unsigned i = 0; i < this->enclosing_contexts.size; i++) {
        CharArray* context = array_get(&this->enclosing_contexts, i);
        copied = context->size;
        if (copied > 0) memcpy(&buffer[offset], context->contents, copied);
        offset += copied;
      }

      // Serialize current context
      copied = scanner_serialize(&this->current_context, &buffer[offset]);
      offset += copied;

      // Write current context size to reserved position
      memcpy(&buffer[current_context_size_offset], &copied, sizeof(unsigned));

      return offset;
    }

    /**
     * Deserialize a nested scanner.
     *
     * @param this The nested scanner instance to deserialize into.
     * @param buffer The buffer to deserialize from.
     * @param length The number of bytes in the buffer.
     */
    static void nested_scanner_deserialize(
      struct NestedScanner* const this,
      const char* const buffer,
      unsigned const length
    ) {
      for (unsigned i = 0; i < this->enclosing_contexts.size; i++) {
        array_delete(array_get(&this->enclosing_contexts, i));
      }
      array_delete(&this->enclosing_contexts);
      scanner_deserialize(&this->current_context, NULL, 0);

      if (length > 0) {
        unsigned offset = 0;
        unsigned copied = 0;

        // First item: total number of contexts (guaranteed to be >= 1)
        nest_address context_depth = 0;
        copied = sizeof(nest_address);
        memcpy(&context_depth, &buffer[offset], copied);
        assert(1 <= context_depth);
        array_grow_by(&this->enclosing_contexts, context_depth - 1);
        offset += copied;

        // Next N items: size of all contexts
        Array(unsigned) context_sizes = array_new();
        array_grow_by(&context_sizes, context_depth);
        copied = context_depth * sizeof(unsigned);
        if (copied > 0 && context_sizes.contents != NULL) memcpy(context_sizes.contents, &buffer[offset], copied);
        offset += copied;

        // Deserialize N-1 contexts as enclosing contexts
        for (int i = 0; i < context_depth - 1; i++) {
          copied = *array_get(&context_sizes, i);
          array_grow_by(array_get(&this->enclosing_contexts, i), copied);
          if (copied > 0) memcpy(array_get(&this->enclosing_contexts, i)->contents, &buffer[offset], copied);
          offset += copied;
        }

        // Final context is deserialized as current context
        copied = *array_back(&context_sizes);
        scanner_deserialize(&this->current_context, &buffer[offset], copied);
        offset += copied;

        array_delete(&context_sizes);
        assert(offset == length);
      }
    }

    /**
     * Initializes a new instance of the NestedScanner object.
     *
     * @param this The NestedScanner to initialize.
     */
    static void nested_scanner_init(struct NestedScanner* const this) {
      array_init(&this->enclosing_contexts);
      this->current_context = scanner_create();
    }

    /**
     * Frees all memory allocated by the nested scanner.
     *
     * @param this The NestedScanner to free.
     */
    static void nested_scanner_free(struct NestedScanner* const this) {
      for (unsigned i = 0; i < this->enclosing_contexts.size; i++) {
        array_delete(array_get(&this->enclosing_contexts, i));
      }
      array_delete(&this->enclosing_contexts);
      scanner_free(&this->current_context);
    }

    static bool nested_scan(
      struct NestedScanner* const this,
      TSLexer* const lexer,
      const bool* const valid_symbols) {
      // All symbols are marked as valid during error recovery.
      // We can check for this by looking at the validity of the final
      // (unused) external symbol, ERROR_SENTINEL.
      if (valid_symbols[ERROR_SENTINEL]) {
        return false;
      } else if (valid_symbols[PCAL_START]) {
        // Entering PlusCal block; push current context then clear
        unsigned const expected_size = scanner_serialized_size(&this->current_context);
        CharArray serialized_current_context = array_new();
        array_grow_by(&serialized_current_context, expected_size);
        unsigned const actual_size = scanner_serialize(&this->current_context, serialized_current_context.contents);
        assert(expected_size == actual_size);
        array_push(&this->enclosing_contexts, serialized_current_context);
        scanner_free(&this->current_context);
        this->current_context = scanner_create();
        lexer->result_symbol = PCAL_START;
        return true;
      } else if (valid_symbols[PCAL_END] && this->enclosing_contexts.size > 0) {
        // Exiting PlusCal block; rehydrate context then pop
        CharArray* next = array_back(&this->enclosing_contexts);
        scanner_deserialize(&this->current_context, next->contents, next->size);
        CharArray ctx = array_pop(&this->enclosing_contexts);
        array_delete(&ctx);
        lexer->result_symbol = PCAL_END;
        return true;
      } else {
        return scan(&this->current_context, lexer, valid_symbols);
      }
    }

  // Called once when language is set on a parser.
  // Allocates memory for storing scanner state.
  void* tree_sitter_tlaplus_external_scanner_create() {
    struct NestedScanner* scanner = ts_malloc(sizeof(struct NestedScanner));
    nested_scanner_init(scanner);
    return scanner;
  }

  // Called once parser is deleted or different language set.
  // Frees memory storing scanner state.
  void tree_sitter_tlaplus_external_scanner_destroy(void* const payload) {
    struct NestedScanner* const scanner = (struct NestedScanner*)(payload);
    nested_scanner_free(scanner);
    ts_free(scanner);
  }

  // Called whenever this scanner recognizes a token.
  // Serializes scanner state into buffer.
  unsigned tree_sitter_tlaplus_external_scanner_serialize(
    void* const payload,
    char* const buffer
  ) {
    const struct NestedScanner* const scanner = (struct NestedScanner*)(payload);
    return nested_scanner_serialize(scanner, buffer);
  }

  // Called when handling edits and ambiguities.
  // Deserializes scanner state from buffer.
  void tree_sitter_tlaplus_external_scanner_deserialize(
    void* const payload,
    const char* const buffer,
    unsigned const length
  ) {
    struct NestedScanner* const scanner = (struct NestedScanner*)(payload);
    nested_scanner_deserialize(scanner, buffer, length);
  }

  // Scans for tokens.
  bool tree_sitter_tlaplus_external_scanner_scan(
    void* const payload,
    TSLexer* const lexer,
    const bool* const valid_symbols
  ) {
    struct NestedScanner* const scanner = (struct NestedScanner*)(payload);
    return nested_scan(scanner, lexer, valid_symbols);
  }

