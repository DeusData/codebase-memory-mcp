package cypher

import (
	"fmt"
	"strings"
	"unicode"
)

// TokenType classifies a lexer token.
type TokenType int

const (
	// Keywords
	TokMatch    TokenType = iota // MATCH
	TokWhere                     // WHERE
	TokReturn                    // RETURN
	TokOrder                     // ORDER
	TokBy                        // BY
	TokLimit                     // LIMIT
	TokAnd                       // AND
	TokOr                        // OR
	TokAs                        // AS
	TokDistinct                  // DISTINCT
	TokCount                     // COUNT
	TokContains                  // CONTAINS
	TokStarts                    // STARTS
	TokWith                      // WITH
	TokNot                       // NOT
	TokAsc                       // ASC
	TokDesc                      // DESC

	// Symbols
	TokLParen    // (
	TokRParen    // )
	TokLBracket  // [
	TokRBracket  // ]
	TokDash      // -
	TokGT        // >
	TokLT        // <
	TokColon     // :
	TokDot       // .
	TokLBrace    // {
	TokRBrace    // }
	TokStar      // *
	TokComma     // ,
	TokEQ        // =
	TokRegex     // =~
	TokGTE       // >=
	TokLTE       // <=
	TokPipe      // |
	TokDotDot    // ..

	// Literals
	TokIdent  // identifier
	TokString // "..." or '...'
	TokNumber // integer

	TokEOF // end of input
)

// Token is a single lexer token.
type Token struct {
	Type    TokenType
	Value   string
	Pos     int // byte offset in the input
}

func (t Token) String() string {
	return fmt.Sprintf("Token(%d, %q, pos=%d)", t.Type, t.Value, t.Pos)
}

// keywords maps uppercase keyword strings to their token type.
var keywords = map[string]TokenType{
	"MATCH":    TokMatch,
	"WHERE":    TokWhere,
	"RETURN":   TokReturn,
	"ORDER":    TokOrder,
	"BY":       TokBy,
	"LIMIT":    TokLimit,
	"AND":      TokAnd,
	"OR":       TokOr,
	"AS":       TokAs,
	"DISTINCT": TokDistinct,
	"COUNT":    TokCount,
	"CONTAINS": TokContains,
	"STARTS":   TokStarts,
	"WITH":     TokWith,
	"NOT":      TokNot,
	"ASC":      TokAsc,
	"DESC":     TokDesc,
}

// Lexer tokenizes a Cypher query string.
type Lexer struct {
	input  string
	pos    int
	tokens []Token
}

// Lex tokenizes the input string into a slice of tokens.
func Lex(input string) ([]Token, error) {
	l := &Lexer{input: input}
	if err := l.tokenize(); err != nil {
		return nil, err
	}
	return l.tokens, nil
}

func (l *Lexer) tokenize() error {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]

		// Skip whitespace
		if unicode.IsSpace(rune(ch)) {
			l.pos++
			continue
		}

		// Skip line comments
		if ch == '/' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '/' {
			for l.pos < len(l.input) && l.input[l.pos] != '\n' {
				l.pos++
			}
			continue
		}

		// Skip block comments
		if ch == '/' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '*' {
			l.pos += 2
			for l.pos+1 < len(l.input) {
				if l.input[l.pos] == '*' && l.input[l.pos+1] == '/' {
					l.pos += 2
					break
				}
				l.pos++
			}
			continue
		}

		switch {
		case ch == '(':
			l.emit(TokLParen, "(")
		case ch == ')':
			l.emit(TokRParen, ")")
		case ch == '[':
			l.emit(TokLBracket, "[")
		case ch == ']':
			l.emit(TokRBracket, "]")
		case ch == '{':
			l.emit(TokLBrace, "{")
		case ch == '}':
			l.emit(TokRBrace, "}")
		case ch == '*':
			l.emit(TokStar, "*")
		case ch == ',':
			l.emit(TokComma, ",")
		case ch == '|':
			l.emit(TokPipe, "|")
		case ch == ':':
			l.emit(TokColon, ":")
		case ch == '.':
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '.' {
				l.pos++
				l.emit(TokDotDot, "..")
			} else {
				l.emit(TokDot, ".")
			}
		case ch == '-':
			l.emit(TokDash, "-")
		case ch == '>':
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
				l.pos++
				l.emit(TokGTE, ">=")
			} else {
				l.emit(TokGT, ">")
			}
		case ch == '<':
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
				l.pos++
				l.emit(TokLTE, "<=")
			} else {
				l.emit(TokLT, "<")
			}
		case ch == '=':
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '~' {
				l.pos++
				l.emit(TokRegex, "=~")
			} else {
				l.emit(TokEQ, "=")
			}
		case ch == '"' || ch == '\'':
			if err := l.lexString(ch); err != nil {
				return err
			}
			continue // lexString advances pos itself
		case isDigit(ch):
			l.lexNumber()
			continue // lexNumber advances pos itself
		case isIdentStart(ch):
			l.lexIdent()
			continue // lexIdent advances pos itself
		default:
			return fmt.Errorf("unexpected char %q at pos %d", string(ch), l.pos)
		}

		l.pos++
	}

	l.tokens = append(l.tokens, Token{Type: TokEOF, Value: "", Pos: l.pos})
	return nil
}

func (l *Lexer) emit(typ TokenType, val string) {
	l.tokens = append(l.tokens, Token{Type: typ, Value: val, Pos: l.pos})
}

func (l *Lexer) lexString(quote byte) error {
	start := l.pos
	l.pos++ // skip opening quote
	var sb strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\\' && l.pos+1 < len(l.input) {
			l.pos++
			sb.WriteByte(l.input[l.pos])
			l.pos++
			continue
		}
		if ch == quote {
			l.tokens = append(l.tokens, Token{Type: TokString, Value: sb.String(), Pos: start})
			l.pos++ // skip closing quote
			return nil
		}
		sb.WriteByte(ch)
		l.pos++
	}
	return fmt.Errorf("unterminated string at pos %d", start)
}

func (l *Lexer) lexNumber() {
	start := l.pos
	for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
		l.pos++
	}
	// Handle decimal point (e.g. 0.9, 3.14) — but not ".." (range operator)
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		if l.pos+1 < len(l.input) && l.input[l.pos+1] != '.' && isDigit(l.input[l.pos+1]) {
			l.pos++ // consume '.'
			for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
				l.pos++
			}
		}
	}
	l.tokens = append(l.tokens, Token{Type: TokNumber, Value: l.input[start:l.pos], Pos: start})
}

func (l *Lexer) lexIdent() {
	start := l.pos
	for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
		l.pos++
	}
	word := l.input[start:l.pos]
	upper := strings.ToUpper(word)
	if tok, ok := keywords[upper]; ok {
		l.tokens = append(l.tokens, Token{Type: tok, Value: upper, Pos: start})
	} else {
		l.tokens = append(l.tokens, Token{Type: TokIdent, Value: word, Pos: start})
	}
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentPart(ch byte) bool {
	return isIdentStart(ch) || isDigit(ch)
}
