package cypher

// Query represents a parsed Cypher query.
type Query struct {
	Match  *MatchClause
	Where  *WhereClause
	Return *ReturnClause
}

// MatchClause holds the MATCH pattern.
type MatchClause struct {
	Pattern *Pattern
}

// Pattern is a sequence of alternating nodes and relationships.
type Pattern struct {
	Elements []PatternElement
}

// PatternElement is either a NodePattern or a RelPattern.
type PatternElement interface {
	patternElement()
}

// NodePattern matches a graph node with optional label and inline properties.
type NodePattern struct {
	Variable string            // e.g. "f"
	Label    string            // e.g. "Function" (optional)
	Props    map[string]string // inline property filters (optional)
}

func (*NodePattern) patternElement() {}

// RelPattern matches a graph relationship with optional types, direction, and hops.
type RelPattern struct {
	Variable string   // (optional)
	Types    []string // relationship types, e.g. ["CALLS", "HTTP_CALLS"]
	Direction string  // "outbound", "inbound", "any"
	MinHops  int      // for variable-length, default 1
	MaxHops  int      // for variable-length, default 1 (0 means unbounded)
}

func (*RelPattern) patternElement() {}

// WhereClause holds filter conditions joined by AND/OR.
type WhereClause struct {
	Conditions []Condition
	Operator   string // "AND" or "OR"
}

// Condition is a single property comparison.
type Condition struct {
	Variable string // "f"
	Property string // "name"
	Operator string // "=", "=~", "CONTAINS", "STARTS WITH", ">", "<", ">=", "<="
	Value    string // the comparison value
}

// ReturnClause specifies which data to return from the query.
type ReturnClause struct {
	Items    []ReturnItem
	OrderBy  string // "f.name" (optional)
	OrderDir string // "ASC" or "DESC"
	Limit    int    // 0 means no limit
	Distinct bool
}

// ReturnItem is a single item in the RETURN clause.
type ReturnItem struct {
	Variable string // "f"
	Property string // "name" (empty = return whole node)
	Alias    string // "AS call_count" (optional)
	Func     string // "COUNT" (optional aggregation)
}
