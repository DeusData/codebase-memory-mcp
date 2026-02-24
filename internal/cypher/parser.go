package cypher

import (
	"fmt"
	"strconv"
)

// Parser converts a token stream into an AST.
type Parser struct {
	tokens []Token
	pos    int
}

// Parse tokenizes and parses a Cypher query string into an AST.
func Parse(input string) (*Query, error) {
	tokens, err := Lex(input)
	if err != nil {
		return nil, fmt.Errorf("lex: %w", err)
	}
	p := &Parser{tokens: tokens}
	return p.parseQuery()
}

func (p *Parser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokEOF}
	}
	return p.tokens[p.pos]
}

func (p *Parser) advance() Token {
	t := p.peek()
	p.pos++
	return t
}

func (p *Parser) expect(typ TokenType) (Token, error) {
	t := p.advance()
	if t.Type != typ {
		return t, fmt.Errorf("expected token %d, got %d (%q) at pos %d", typ, t.Type, t.Value, t.Pos)
	}
	return t, nil
}

func (p *Parser) parseQuery() (*Query, error) {
	q := &Query{}

	// MATCH clause (required)
	if p.peek().Type != TokMatch {
		return nil, fmt.Errorf("expected MATCH at pos %d, got %q", p.peek().Pos, p.peek().Value)
	}
	m, err := p.parseMatch()
	if err != nil {
		return nil, err
	}
	q.Match = m

	// WHERE clause (optional)
	if p.peek().Type == TokWhere {
		w, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		q.Where = w
	}

	// RETURN clause (optional but common)
	if p.peek().Type == TokReturn {
		r, err := p.parseReturn()
		if err != nil {
			return nil, err
		}
		q.Return = r
	}

	return q, nil
}

func (p *Parser) parseMatch() (*MatchClause, error) {
	if _, err := p.expect(TokMatch); err != nil {
		return nil, err
	}
	pat, err := p.parsePattern()
	if err != nil {
		return nil, fmt.Errorf("match pattern: %w", err)
	}
	return &MatchClause{Pattern: pat}, nil
}

func (p *Parser) parsePattern() (*Pattern, error) {
	pat := &Pattern{}

	// First element must be a node
	node, err := p.parseNodePattern()
	if err != nil {
		return nil, err
	}
	pat.Elements = append(pat.Elements, node)

	// Parse alternating rel-node pairs
	for p.isRelStart() {
		rel, nextNode, err := p.parseRelAndNode()
		if err != nil {
			return nil, err
		}
		pat.Elements = append(pat.Elements, rel, nextNode)
	}

	return pat, nil
}

// isRelStart checks whether the next tokens begin a relationship pattern.
// Patterns: -[...]-> or <-[...]- or -[...]-
func (p *Parser) isRelStart() bool {
	t := p.peek()
	return t.Type == TokDash || t.Type == TokLT
}

func (p *Parser) parseRelAndNode() (*RelPattern, *NodePattern, error) {
	rel := &RelPattern{MinHops: 1, MaxHops: 1}

	// Determine direction by looking at leading token
	// Possibilities:
	//   -[...]->(node)   outbound
	//   <-[...]-(node)   inbound
	//   -[...]-(node)    any

	leadingArrow := false
	if p.peek().Type == TokLT {
		leadingArrow = true
		p.advance() // consume <
	}

	// Expect dash
	if _, err := p.expect(TokDash); err != nil {
		return nil, nil, fmt.Errorf("expected '-' in relationship: %w", err)
	}

	// Optional bracket section [...]
	if p.peek().Type == TokLBracket {
		if err := p.parseRelBracket(rel); err != nil {
			return nil, nil, err
		}
	}

	// Expect dash
	if _, err := p.expect(TokDash); err != nil {
		return nil, nil, fmt.Errorf("expected '-' after relationship: %w", err)
	}

	// Check trailing arrow
	trailingArrow := false
	if p.peek().Type == TokGT {
		trailingArrow = true
		p.advance() // consume >
	}

	// Determine direction
	switch {
	case !leadingArrow && trailingArrow:
		rel.Direction = "outbound"
	case leadingArrow && !trailingArrow:
		rel.Direction = "inbound"
	default:
		rel.Direction = "any"
	}

	// Parse the next node
	node, err := p.parseNodePattern()
	if err != nil {
		return nil, nil, err
	}

	return rel, node, nil
}

func (p *Parser) parseRelBracket(rel *RelPattern) error {
	p.advance() // consume [

	// Optional variable name
	if p.peek().Type == TokIdent {
		rel.Variable = p.advance().Value
	}

	// Optional :TYPE or :TYPE1|TYPE2
	if p.peek().Type == TokColon {
		p.advance() // consume :
		types, err := p.parseRelTypes()
		if err != nil {
			return err
		}
		rel.Types = types
	}

	// Optional *min..max for variable-length
	if p.peek().Type == TokStar {
		p.advance() // consume *
		if err := p.parseHopRange(rel); err != nil {
			return err
		}
	}

	// Expect ]
	if _, err := p.expect(TokRBracket); err != nil {
		return fmt.Errorf("expected ']' to close relationship: %w", err)
	}

	return nil
}

func (p *Parser) parseRelTypes() ([]string, error) {
	var types []string
	t := p.advance()
	if t.Type != TokIdent {
		return nil, fmt.Errorf("expected relationship type name, got %q at pos %d", t.Value, t.Pos)
	}
	types = append(types, t.Value)

	// Handle TYPE1|TYPE2
	for p.peek().Type == TokPipe {
		p.advance() // consume |
		t = p.advance()
		if t.Type != TokIdent {
			return nil, fmt.Errorf("expected relationship type after '|', got %q at pos %d", t.Value, t.Pos)
		}
		types = append(types, t.Value)
	}
	return types, nil
}

func (p *Parser) parseHopRange(rel *RelPattern) error {
	// Possibilities after *:
	//   *1..3   min=1, max=3
	//   *..3    min=1, max=3
	//   *1..    min=1, max=0 (unbounded)
	//   *3      min=1, max=3 (shorthand)
	//   (empty) min=1, max=0 (unbounded)

	if p.peek().Type == TokNumber {
		n, _ := strconv.Atoi(p.advance().Value)
		if p.peek().Type == TokDotDot {
			// *N..M or *N..
			rel.MinHops = n
			p.advance() // consume ..
			if p.peek().Type == TokNumber {
				m, _ := strconv.Atoi(p.advance().Value)
				rel.MaxHops = m
			} else {
				rel.MaxHops = 0 // unbounded
			}
		} else {
			// *N (shorthand for *1..N)
			rel.MinHops = 1
			rel.MaxHops = n
		}
	} else if p.peek().Type == TokDotDot {
		// *..M
		p.advance() // consume ..
		rel.MinHops = 1
		if p.peek().Type == TokNumber {
			m, _ := strconv.Atoi(p.advance().Value)
			rel.MaxHops = m
		} else {
			rel.MaxHops = 0
		}
	} else {
		// Just * with no range: unbounded
		rel.MinHops = 1
		rel.MaxHops = 0
	}

	return nil
}

func (p *Parser) parseNodePattern() (*NodePattern, error) {
	if _, err := p.expect(TokLParen); err != nil {
		return nil, fmt.Errorf("expected '(' for node pattern: %w", err)
	}

	node := &NodePattern{}

	// Optional variable name
	if p.peek().Type == TokIdent {
		node.Variable = p.advance().Value
	}

	// Optional :Label
	if p.peek().Type == TokColon {
		p.advance() // consume :
		t := p.advance()
		if t.Type != TokIdent {
			return nil, fmt.Errorf("expected label name after ':', got %q at pos %d", t.Value, t.Pos)
		}
		node.Label = t.Value
	}

	// Optional {key: "val", ...}
	if p.peek().Type == TokLBrace {
		props, err := p.parseInlineProps()
		if err != nil {
			return nil, err
		}
		node.Props = props
	}

	if _, err := p.expect(TokRParen); err != nil {
		return nil, fmt.Errorf("expected ')' to close node pattern: %w", err)
	}

	return node, nil
}

func (p *Parser) parseInlineProps() (map[string]string, error) {
	p.advance() // consume {
	props := make(map[string]string)

	for p.peek().Type != TokRBrace {
		if len(props) > 0 {
			if _, err := p.expect(TokComma); err != nil {
				return nil, fmt.Errorf("expected ',' between properties: %w", err)
			}
		}

		// key
		keyTok := p.advance()
		if keyTok.Type != TokIdent {
			return nil, fmt.Errorf("expected property key, got %q at pos %d", keyTok.Value, keyTok.Pos)
		}

		// :
		if _, err := p.expect(TokColon); err != nil {
			return nil, fmt.Errorf("expected ':' after property key: %w", err)
		}

		// value (string)
		valTok := p.advance()
		if valTok.Type != TokString {
			return nil, fmt.Errorf("expected string value for property %q, got %q at pos %d", keyTok.Value, valTok.Value, valTok.Pos)
		}

		props[keyTok.Value] = valTok.Value
	}

	p.advance() // consume }
	return props, nil
}

func (p *Parser) parseWhere() (*WhereClause, error) {
	p.advance() // consume WHERE
	w := &WhereClause{Operator: "AND"}

	cond, err := p.parseCondition()
	if err != nil {
		return nil, err
	}
	w.Conditions = append(w.Conditions, cond)

	for p.peek().Type == TokAnd || p.peek().Type == TokOr {
		op := p.advance()
		if op.Type == TokOr {
			w.Operator = "OR"
		}
		cond, err := p.parseCondition()
		if err != nil {
			return nil, err
		}
		w.Conditions = append(w.Conditions, cond)
	}

	return w, nil
}

func (p *Parser) parseCondition() (Condition, error) {
	c := Condition{}

	// variable.property
	varTok := p.advance()
	if varTok.Type != TokIdent {
		return c, fmt.Errorf("expected variable name in condition, got %q at pos %d", varTok.Value, varTok.Pos)
	}
	c.Variable = varTok.Value

	if _, err := p.expect(TokDot); err != nil {
		return c, fmt.Errorf("expected '.' after variable in condition: %w", err)
	}

	propTok := p.advance()
	if propTok.Type != TokIdent {
		return c, fmt.Errorf("expected property name in condition, got %q at pos %d", propTok.Value, propTok.Pos)
	}
	c.Property = propTok.Value

	// Operator
	op := p.peek()
	switch op.Type {
	case TokEQ:
		c.Operator = "="
		p.advance()
	case TokRegex:
		c.Operator = "=~"
		p.advance()
	case TokGT:
		c.Operator = ">"
		p.advance()
	case TokLT:
		c.Operator = "<"
		p.advance()
	case TokGTE:
		c.Operator = ">="
		p.advance()
	case TokLTE:
		c.Operator = "<="
		p.advance()
	case TokContains:
		c.Operator = "CONTAINS"
		p.advance()
	case TokStarts:
		// STARTS WITH
		p.advance() // consume STARTS
		if p.peek().Type != TokWith {
			return c, fmt.Errorf("expected WITH after STARTS at pos %d", p.peek().Pos)
		}
		p.advance() // consume WITH
		c.Operator = "STARTS WITH"
	default:
		return c, fmt.Errorf("expected comparison operator, got %q at pos %d", op.Value, op.Pos)
	}

	// Value (string or number)
	valTok := p.advance()
	switch valTok.Type {
	case TokString:
		c.Value = valTok.Value
	case TokNumber:
		c.Value = valTok.Value
	default:
		return c, fmt.Errorf("expected value in condition, got %q at pos %d", valTok.Value, valTok.Pos)
	}

	return c, nil
}

func (p *Parser) parseReturn() (*ReturnClause, error) {
	p.advance() // consume RETURN
	r := &ReturnClause{OrderDir: "ASC"}

	// Optional DISTINCT
	if p.peek().Type == TokDistinct {
		r.Distinct = true
		p.advance()
	}

	// Parse return items
	item, err := p.parseReturnItem()
	if err != nil {
		return nil, err
	}
	r.Items = append(r.Items, item)

	for p.peek().Type == TokComma {
		p.advance() // consume ,
		item, err := p.parseReturnItem()
		if err != nil {
			return nil, err
		}
		r.Items = append(r.Items, item)
	}

	// Optional ORDER BY
	if p.peek().Type == TokOrder {
		p.advance() // consume ORDER
		if _, err := p.expect(TokBy); err != nil {
			return nil, fmt.Errorf("expected BY after ORDER: %w", err)
		}
		orderTok := p.advance()
		if orderTok.Type != TokIdent {
			return nil, fmt.Errorf("expected field name for ORDER BY, got %q", orderTok.Value)
		}
		orderField := orderTok.Value
		if p.peek().Type == TokDot {
			p.advance() // consume .
			propTok := p.advance()
			orderField = orderField + "." + propTok.Value
		}
		r.OrderBy = orderField

		// Optional ASC/DESC
		if p.peek().Type == TokAsc {
			r.OrderDir = "ASC"
			p.advance()
		} else if p.peek().Type == TokDesc {
			r.OrderDir = "DESC"
			p.advance()
		}
	}

	// Optional LIMIT
	if p.peek().Type == TokLimit {
		p.advance() // consume LIMIT
		numTok := p.advance()
		if numTok.Type != TokNumber {
			return nil, fmt.Errorf("expected number after LIMIT, got %q", numTok.Value)
		}
		n, _ := strconv.Atoi(numTok.Value)
		r.Limit = n
	}

	return r, nil
}

func (p *Parser) parseReturnItem() (ReturnItem, error) {
	item := ReturnItem{}

	// Check for COUNT(variable)
	if p.peek().Type == TokCount {
		p.advance() // consume COUNT
		item.Func = "COUNT"
		if _, err := p.expect(TokLParen); err != nil {
			return item, fmt.Errorf("expected '(' after COUNT: %w", err)
		}
		varTok := p.advance()
		if varTok.Type != TokIdent {
			return item, fmt.Errorf("expected variable in COUNT(), got %q", varTok.Value)
		}
		item.Variable = varTok.Value
		if _, err := p.expect(TokRParen); err != nil {
			return item, fmt.Errorf("expected ')' after COUNT variable: %w", err)
		}
	} else {
		// variable or variable.property
		varTok := p.advance()
		if varTok.Type != TokIdent {
			return item, fmt.Errorf("expected variable in RETURN item, got %q at pos %d", varTok.Value, varTok.Pos)
		}
		item.Variable = varTok.Value

		if p.peek().Type == TokDot {
			p.advance() // consume .
			propTok := p.advance()
			if propTok.Type != TokIdent {
				return item, fmt.Errorf("expected property after '.', got %q", propTok.Value)
			}
			item.Property = propTok.Value
		}
	}

	// Optional AS alias
	if p.peek().Type == TokAs {
		p.advance() // consume AS
		aliasTok := p.advance()
		if aliasTok.Type != TokIdent {
			return item, fmt.Errorf("expected alias after AS, got %q", aliasTok.Value)
		}
		item.Alias = aliasTok.Value
	}

	return item, nil
}
