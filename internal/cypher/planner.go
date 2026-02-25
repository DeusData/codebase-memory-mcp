package cypher

import "fmt"

// Plan represents an execution plan for a parsed Cypher query.
type Plan struct {
	Steps      []PlanStep
	ReturnSpec *ReturnClause
}

// PlanStep is a single step in the execution plan.
type PlanStep interface {
	stepType() string
}

// ScanNodes finds nodes matching label and/or inline property filters.
type ScanNodes struct {
	Variable string
	Label    string
	Props    map[string]string // inline property filters
}

func (*ScanNodes) stepType() string { return "scan" }

// ExpandRelationship follows edges from bound nodes to match target nodes.
type ExpandRelationship struct {
	FromVar   string // source variable (already bound)
	ToVar     string // target variable (to bind)
	RelVar    string // optional relationship variable (to bind edge)
	ToLabel   string // optional label filter on target
	ToProps   map[string]string
	EdgeTypes []string // required edge types
	Direction string   // "outbound", "inbound", "any"
	MinHops   int
	MaxHops   int
}

func (*ExpandRelationship) stepType() string { return "expand" }

// FilterWhere applies WHERE conditions to the bindings.
type FilterWhere struct {
	Conditions []Condition
	Operator   string // "AND" or "OR"
}

func (*FilterWhere) stepType() string { return "filter" }

// asNodePattern safely type-asserts a PatternElement to *NodePattern.
func asNodePattern(el PatternElement) (*NodePattern, error) {
	np, ok := el.(*NodePattern)
	if !ok {
		return nil, fmt.Errorf("expected *NodePattern, got %T", el)
	}
	return np, nil
}

// asRelPattern safely type-asserts a PatternElement to *RelPattern.
func asRelPattern(el PatternElement) (*RelPattern, error) {
	rp, ok := el.(*RelPattern)
	if !ok {
		return nil, fmt.Errorf("expected *RelPattern, got %T", el)
	}
	return rp, nil
}

// BuildPlan converts a parsed Query AST into an execution Plan.
func BuildPlan(q *Query) (*Plan, error) {
	plan := &Plan{ReturnSpec: q.Return}

	elements := q.Match.Pattern.Elements
	if len(elements) == 0 {
		return plan, nil
	}

	// First element is always a node pattern
	firstNode, err := asNodePattern(elements[0])
	if err != nil {
		return nil, fmt.Errorf("first pattern element: %w", err)
	}
	plan.Steps = append(plan.Steps, &ScanNodes{
		Variable: firstNode.Variable,
		Label:    firstNode.Label,
		Props:    firstNode.Props,
	})

	// Optimization: push WHERE conditions that reference only the first
	// scan variable BEFORE any expand steps. This dramatically reduces the
	// number of bindings that need to be expanded.
	earlyFilters, lateFilters := splitWhereFilters(q.Where, firstNode.Variable, len(elements) > 1)

	// Insert early filter right after scan
	if len(earlyFilters) > 0 {
		plan.Steps = append(plan.Steps, &FilterWhere{
			Conditions: earlyFilters,
			Operator:   "AND",
		})
	}

	// Process relationship-node pairs
	for i := 1; i+1 < len(elements); i += 2 {
		rel, err := asRelPattern(elements[i])
		if err != nil {
			return nil, fmt.Errorf("element[%d]: %w", i, err)
		}
		targetNode, err := asNodePattern(elements[i+1])
		if err != nil {
			return nil, fmt.Errorf("element[%d]: %w", i+1, err)
		}
		fromNode, err := asNodePattern(elements[i-1])
		if err != nil {
			return nil, fmt.Errorf("element[%d]: %w", i-1, err)
		}

		plan.Steps = append(plan.Steps, &ExpandRelationship{
			FromVar:   fromNode.Variable,
			ToVar:     targetNode.Variable,
			RelVar:    rel.Variable,
			ToLabel:   targetNode.Label,
			ToProps:   targetNode.Props,
			EdgeTypes: rel.Types,
			Direction: rel.Direction,
			MinHops:   rel.MinHops,
			MaxHops:   rel.MaxHops,
		})
	}

	// Late WHERE filter (conditions referencing expand variables)
	if len(lateFilters) > 0 {
		plan.Steps = append(plan.Steps, &FilterWhere{
			Conditions: lateFilters,
			Operator:   q.Where.Operator,
		})
	} else if q.Where != nil && len(earlyFilters) == 0 {
		// No split happened — add all conditions at the end
		plan.Steps = append(plan.Steps, &FilterWhere{
			Conditions: q.Where.Conditions,
			Operator:   q.Where.Operator,
		})
	}

	return plan, nil
}

// splitWhereFilters separates WHERE conditions into early (scan-only) and late filters.
func splitWhereFilters(where *WhereClause, scanVar string, hasExpand bool) (early, late []Condition) {
	if where == nil {
		return nil, nil
	}

	if hasExpand && where.Operator == "AND" {
		for _, c := range where.Conditions {
			if c.Variable == scanVar {
				early = append(early, c)
			} else {
				late = append(late, c)
			}
		}
		return early, late
	}
	// Can't split OR conditions or when there's no expand
	return nil, where.Conditions
}
