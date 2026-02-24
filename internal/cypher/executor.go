package cypher

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

const maxResultRows = 200

// Executor runs Cypher execution plans against a store.
type Executor struct {
	Store *store.Store
}

// Result holds the tabular output of a query.
type Result struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
}

// binding maps variable names to matched nodes and edges.
type binding struct {
	nodes map[string]*store.Node
	edges map[string]*store.Edge
}

func newBinding() binding {
	return binding{
		nodes: make(map[string]*store.Node),
		edges: make(map[string]*store.Edge),
	}
}

// adjacentResult pairs a matched node with the edge that reached it.
type adjacentResult struct {
	Node *store.Node
	Edge *store.Edge
}

// Execute parses, plans, and executes a Cypher query across all projects.
func (e *Executor) Execute(query string) (*Result, error) {
	q, err := Parse(query)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	plan, err := BuildPlan(q)
	if err != nil {
		return nil, fmt.Errorf("plan: %w", err)
	}
	return e.executePlan(plan)
}

func (e *Executor) executePlan(plan *Plan) (*Result, error) {
	projects, err := e.Store.ListProjects()
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}

	var allBindings []binding
	for _, proj := range projects {
		bindings, err := e.executeStepsForProject(proj.Name, plan.Steps)
		if err != nil {
			continue // skip projects that error
		}
		allBindings = append(allBindings, bindings...)
		if len(allBindings) > maxResultRows*2 {
			allBindings = allBindings[:maxResultRows*2]
			break
		}
	}

	return e.projectResults(allBindings, plan.ReturnSpec)
}

func (e *Executor) executeStepsForProject(project string, steps []PlanStep) ([]binding, error) {
	var bindings []binding

	for i, step := range steps {
		var err error
		switch s := step.(type) {
		case *ScanNodes:
			bindings, err = e.execScan(project, s, bindings)
		case *ExpandRelationship:
			bindings, err = e.execExpand(s, bindings)
		case *FilterWhere:
			bindings, err = e.execFilter(s, bindings)
		default:
			return nil, fmt.Errorf("unknown step type: %T", step)
		}
		if err != nil {
			return nil, err
		}
		// Only cap after the last step or after expand (which can explode).
		// Never cap between scan and filter — the filter needs all candidates.
		isLastStep := i == len(steps)-1
		_, isExpand := step.(*ExpandRelationship)
		if isLastStep || isExpand {
			if len(bindings) > maxResultRows*2 {
				bindings = bindings[:maxResultRows*2]
			}
		}
	}

	return bindings, nil
}

func (e *Executor) execScan(project string, s *ScanNodes, _ []binding) ([]binding, error) {
	var nodes []*store.Node
	var err error

	if s.Label != "" {
		nodes, err = e.Store.FindNodesByLabel(project, s.Label)
	} else {
		nodes, err = e.Store.AllNodes(project)
	}
	if err != nil {
		return nil, fmt.Errorf("scan nodes: %w", err)
	}

	// Apply inline property filters
	if len(s.Props) > 0 {
		nodes = filterNodesByProps(nodes, s.Props)
	}

	var bindings []binding
	for _, n := range nodes {
		b := newBinding()
		if s.Variable != "" {
			b.nodes[s.Variable] = n
		}
		bindings = append(bindings, b)
	}
	return bindings, nil
}

func (e *Executor) execExpand(s *ExpandRelationship, bindings []binding) ([]binding, error) {
	if len(bindings) == 0 {
		return nil, nil
	}

	isVariableLength := s.MinHops != 1 || s.MaxHops != 1

	var result []binding
	for _, b := range bindings {
		fromNode, ok := b.nodes[s.FromVar]
		if !ok {
			continue
		}

		if isVariableLength {
			expanded, err := e.expandVariableLength(b, fromNode, s)
			if err != nil {
				return nil, err
			}
			result = append(result, expanded...)
		} else {
			expanded, err := e.expandFixedLength(b, fromNode, s)
			if err != nil {
				return nil, err
			}
			result = append(result, expanded...)
		}

		if len(result) > maxResultRows*2 {
			result = result[:maxResultRows*2]
			break
		}
	}
	return result, nil
}

func (e *Executor) expandFixedLength(b binding, fromNode *store.Node, s *ExpandRelationship) ([]binding, error) {
	adjacents, err := e.findAdjacentNodes(fromNode.ID, s.EdgeTypes, s.Direction)
	if err != nil {
		return nil, err
	}

	var result []binding
	for _, adj := range adjacents {
		if s.ToLabel != "" && adj.Node.Label != s.ToLabel {
			continue
		}
		if len(s.ToProps) > 0 && !nodeMatchesProps(adj.Node, s.ToProps) {
			continue
		}
		newB := copyBinding(b)
		if s.ToVar != "" {
			newB.nodes[s.ToVar] = adj.Node
		}
		if s.RelVar != "" && adj.Edge != nil {
			newB.edges[s.RelVar] = adj.Edge
		}
		result = append(result, newB)
	}
	return result, nil
}

func (e *Executor) expandVariableLength(b binding, fromNode *store.Node, s *ExpandRelationship) ([]binding, error) {
	maxDepth := s.MaxHops
	if maxDepth == 0 {
		maxDepth = 10 // cap unbounded at 10
	}

	direction := s.Direction
	if direction == "" {
		direction = "outbound"
	}

	edgeTypes := s.EdgeTypes
	if len(edgeTypes) == 0 {
		edgeTypes = []string{"CALLS"} // default
	}

	bfsResult, err := e.Store.BFS(fromNode.ID, direction, edgeTypes, maxDepth, maxResultRows)
	if err != nil {
		return nil, fmt.Errorf("bfs: %w", err)
	}

	var result []binding
	for _, nh := range bfsResult.Visited {
		if nh.Hop < s.MinHops {
			continue
		}
		if s.MaxHops > 0 && nh.Hop > s.MaxHops {
			continue
		}
		if s.ToLabel != "" && nh.Node.Label != s.ToLabel {
			continue
		}
		if len(s.ToProps) > 0 && !nodeMatchesProps(nh.Node, s.ToProps) {
			continue
		}
		newB := copyBinding(b)
		if s.ToVar != "" {
			newB.nodes[s.ToVar] = nh.Node
		}
		// Note: variable-length BFS doesn't bind individual edges
		result = append(result, newB)
	}
	return result, nil
}

func (e *Executor) findAdjacentNodes(nodeID int64, edgeTypes []string, direction string) ([]adjacentResult, error) {
	var allEdges []*store.Edge

	switch direction {
	case "outbound":
		if len(edgeTypes) > 0 {
			for _, et := range edgeTypes {
				edges, err := e.Store.FindEdgesBySourceAndType(nodeID, et)
				if err != nil {
					return nil, err
				}
				allEdges = append(allEdges, edges...)
			}
		} else {
			edges, err := e.Store.FindEdgesBySource(nodeID)
			if err != nil {
				return nil, err
			}
			allEdges = edges
		}
	case "inbound":
		if len(edgeTypes) > 0 {
			for _, et := range edgeTypes {
				edges, err := e.Store.FindEdgesByTargetAndType(nodeID, et)
				if err != nil {
					return nil, err
				}
				allEdges = append(allEdges, edges...)
			}
		} else {
			edges, err := e.Store.FindEdgesByTarget(nodeID)
			if err != nil {
				return nil, err
			}
			allEdges = edges
		}
	case "any":
		outEdges, err := e.Store.FindEdgesBySource(nodeID)
		if err != nil {
			return nil, err
		}
		inEdges, err := e.Store.FindEdgesByTarget(nodeID)
		if err != nil {
			return nil, err
		}
		if len(edgeTypes) > 0 {
			typeSet := make(map[string]bool, len(edgeTypes))
			for _, et := range edgeTypes {
				typeSet[et] = true
			}
			for _, edge := range outEdges {
				if typeSet[edge.Type] {
					allEdges = append(allEdges, edge)
				}
			}
			for _, edge := range inEdges {
				if typeSet[edge.Type] {
					allEdges = append(allEdges, edge)
				}
			}
		} else {
			allEdges = append(outEdges, inEdges...)
		}
	default:
		edges, err := e.Store.FindEdgesBySource(nodeID)
		if err != nil {
			return nil, err
		}
		allEdges = edges
	}

	// Resolve edge targets/sources to nodes, preserving the edge
	seen := make(map[int64]bool)
	var results []adjacentResult
	for _, edge := range allEdges {
		var targetID int64
		switch direction {
		case "inbound":
			targetID = edge.SourceID
		case "any":
			if edge.SourceID == nodeID {
				targetID = edge.TargetID
			} else {
				targetID = edge.SourceID
			}
		default:
			targetID = edge.TargetID
		}
		if seen[targetID] {
			continue
		}
		seen[targetID] = true

		node, err := e.Store.FindNodeByID(targetID)
		if err != nil || node == nil {
			continue
		}
		results = append(results, adjacentResult{Node: node, Edge: edge})
	}
	return results, nil
}

func (e *Executor) execFilter(s *FilterWhere, bindings []binding) ([]binding, error) {
	var result []binding
	for _, b := range bindings {
		match, err := evaluateConditions(b, s.Conditions, s.Operator)
		if err != nil {
			return nil, err
		}
		if match {
			result = append(result, b)
		}
	}
	return result, nil
}

func evaluateConditions(b binding, conditions []Condition, op string) (bool, error) {
	if op == "OR" {
		for _, c := range conditions {
			ok, err := evaluateCondition(b, c)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}
	// AND (default)
	for _, c := range conditions {
		ok, err := evaluateCondition(b, c)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func evaluateCondition(b binding, c Condition) (bool, error) {
	// Try node first, then edge
	var actual any
	if node, ok := b.nodes[c.Variable]; ok {
		actual = getNodeProperty(node, c.Property)
	} else if edge, ok := b.edges[c.Variable]; ok {
		actual = getEdgeProperty(edge, c.Property)
	} else {
		return false, nil
	}

	switch c.Operator {
	case "=":
		return fmt.Sprintf("%v", actual) == c.Value, nil
	case "=~":
		s, ok := actual.(string)
		if !ok {
			return false, nil
		}
		matched, err := regexp.MatchString(c.Value, s)
		if err != nil {
			return false, fmt.Errorf("regex %q: %w", c.Value, err)
		}
		return matched, nil
	case "CONTAINS":
		s, ok := actual.(string)
		if !ok {
			return false, nil
		}
		return strings.Contains(s, c.Value), nil
	case "STARTS WITH":
		s, ok := actual.(string)
		if !ok {
			return false, nil
		}
		return strings.HasPrefix(s, c.Value), nil
	case ">", "<", ">=", "<=":
		return compareNumeric(actual, c.Value, c.Operator)
	default:
		return false, fmt.Errorf("unsupported operator: %s", c.Operator)
	}
}

func compareNumeric(actual any, expected string, op string) (bool, error) {
	expectedNum, err := strconv.ParseFloat(expected, 64)
	if err != nil {
		return false, nil
	}
	var actualNum float64
	switch v := actual.(type) {
	case int:
		actualNum = float64(v)
	case int64:
		actualNum = float64(v)
	case float64:
		actualNum = v
	case string:
		n, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return false, nil
		}
		actualNum = n
	default:
		return false, nil
	}

	switch op {
	case ">":
		return actualNum > expectedNum, nil
	case "<":
		return actualNum < expectedNum, nil
	case ">=":
		return actualNum >= expectedNum, nil
	case "<=":
		return actualNum <= expectedNum, nil
	default:
		return false, nil
	}
}

func getNodeProperty(n *store.Node, prop string) any {
	switch prop {
	case "name":
		return n.Name
	case "qualified_name":
		return n.QualifiedName
	case "label":
		return n.Label
	case "file_path":
		return n.FilePath
	case "start_line":
		return n.StartLine
	case "end_line":
		return n.EndLine
	case "id":
		return n.ID
	case "project":
		return n.Project
	default:
		if n.Properties != nil {
			if v, ok := n.Properties[prop]; ok {
				return v
			}
		}
		return nil
	}
}

// getEdgeProperty returns a property value from an edge.
func getEdgeProperty(edge *store.Edge, prop string) any {
	switch prop {
	case "type":
		return edge.Type
	case "id":
		return edge.ID
	case "source_id":
		return edge.SourceID
	case "target_id":
		return edge.TargetID
	default:
		if edge.Properties != nil {
			if v, ok := edge.Properties[prop]; ok {
				return v
			}
		}
		return nil
	}
}

func (e *Executor) projectResults(bindings []binding, ret *ReturnClause) (*Result, error) {
	if ret == nil {
		return e.defaultProjection(bindings)
	}

	// Check if we have a COUNT aggregation
	hasCount := false
	for _, item := range ret.Items {
		if item.Func == "COUNT" {
			hasCount = true
			break
		}
	}

	if hasCount {
		return e.aggregateResults(bindings, ret)
	}

	return e.simpleProjection(bindings, ret)
}

func (e *Executor) defaultProjection(bindings []binding) (*Result, error) {
	if len(bindings) == 0 {
		return &Result{Columns: []string{}, Rows: []map[string]any{}}, nil
	}

	// Collect all variable names from nodes and edges
	varSet := make(map[string]bool)
	edgeVarSet := make(map[string]bool)
	for _, b := range bindings {
		for k := range b.nodes {
			varSet[k] = true
		}
		for k := range b.edges {
			edgeVarSet[k] = true
		}
	}
	var cols []string
	for k := range varSet {
		cols = append(cols, k+".name", k+".qualified_name", k+".label")
	}
	for k := range edgeVarSet {
		cols = append(cols, k+".type")
	}
	sort.Strings(cols)

	var rows []map[string]any
	for _, b := range bindings {
		row := make(map[string]any)
		for varName, node := range b.nodes {
			row[varName+".name"] = node.Name
			row[varName+".qualified_name"] = node.QualifiedName
			row[varName+".label"] = node.Label
		}
		for varName, edge := range b.edges {
			row[varName+".type"] = edge.Type
		}
		rows = append(rows, row)
	}

	if len(rows) > maxResultRows {
		rows = rows[:maxResultRows]
	}

	return &Result{Columns: cols, Rows: rows}, nil
}

func (e *Executor) simpleProjection(bindings []binding, ret *ReturnClause) (*Result, error) {
	var cols []string
	for _, item := range ret.Items {
		col := item.Variable
		if item.Property != "" {
			col = item.Variable + "." + item.Property
		}
		if item.Alias != "" {
			col = item.Alias
		}
		cols = append(cols, col)
	}

	seen := make(map[string]bool)
	var rows []map[string]any
	for _, b := range bindings {
		row := make(map[string]any)
		for i, item := range ret.Items {
			// Try node first, then edge
			if node, ok := b.nodes[item.Variable]; ok {
				if item.Property == "" {
					row[cols[i]] = map[string]any{
						"name":           node.Name,
						"qualified_name": node.QualifiedName,
						"label":          node.Label,
						"file_path":      node.FilePath,
						"start_line":     node.StartLine,
						"end_line":       node.EndLine,
					}
				} else {
					row[cols[i]] = getNodeProperty(node, item.Property)
				}
			} else if edge, ok := b.edges[item.Variable]; ok {
				if item.Property == "" {
					row[cols[i]] = map[string]any{
						"type":      edge.Type,
						"source_id": edge.SourceID,
						"target_id": edge.TargetID,
					}
				} else {
					row[cols[i]] = getEdgeProperty(edge, item.Property)
				}
			} else {
				row[cols[i]] = nil
			}
		}

		// DISTINCT check
		if ret.Distinct {
			key := fmt.Sprintf("%v", row)
			if seen[key] {
				continue
			}
			seen[key] = true
		}

		rows = append(rows, row)
	}

	// ORDER BY
	if ret.OrderBy != "" {
		orderCol := ret.OrderBy
		// Find the matching column name
		for i, item := range ret.Items {
			if item.Alias == orderCol {
				orderCol = cols[i]
				break
			}
		}
		sortRows(rows, orderCol, ret.OrderDir)
	}

	// LIMIT
	limit := ret.Limit
	if limit <= 0 || limit > maxResultRows {
		limit = maxResultRows
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}

	return &Result{Columns: cols, Rows: rows}, nil
}

func (e *Executor) aggregateResults(bindings []binding, ret *ReturnClause) (*Result, error) {
	// Group by non-COUNT items
	var groupItems []ReturnItem
	var countItem ReturnItem
	for _, item := range ret.Items {
		if item.Func == "COUNT" {
			countItem = item
		} else {
			groupItems = append(groupItems, item)
		}
	}

	// Build grouping key -> count
	type groupEntry struct {
		key   string
		row   map[string]any
		count int
	}
	groups := make(map[string]*groupEntry)
	var order []string

	for _, b := range bindings {
		row := make(map[string]any)
		var keyParts []string
		for _, item := range groupItems {
			col := item.Variable
			if item.Property != "" {
				col = item.Variable + "." + item.Property
			}
			if item.Alias != "" {
				col = item.Alias
			}
			var val any
			if node, ok := b.nodes[item.Variable]; ok {
				val = getNodeProperty(node, item.Property)
			} else if edge, ok := b.edges[item.Variable]; ok {
				val = getEdgeProperty(edge, item.Property)
			}
			row[col] = val
			keyParts = append(keyParts, fmt.Sprintf("%v", val))
		}
		key := strings.Join(keyParts, "\x00")
		if g, ok := groups[key]; ok {
			g.count++
		} else {
			groups[key] = &groupEntry{key: key, row: row, count: 1}
			order = append(order, key)
		}
	}

	// Build columns
	var cols []string
	for _, item := range ret.Items {
		col := item.Variable
		if item.Property != "" {
			col = item.Variable + "." + item.Property
		}
		if item.Alias != "" {
			col = item.Alias
		}
		cols = append(cols, col)
	}

	// Build result rows
	countCol := countItem.Alias
	if countCol == "" {
		countCol = "COUNT(" + countItem.Variable + ")"
	}

	var rows []map[string]any
	for _, key := range order {
		g := groups[key]
		row := g.row
		row[countCol] = g.count
		rows = append(rows, row)
	}

	// ORDER BY
	if ret.OrderBy != "" {
		sortRows(rows, ret.OrderBy, ret.OrderDir)
	}

	// LIMIT
	limit := ret.Limit
	if limit <= 0 || limit > maxResultRows {
		limit = maxResultRows
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}

	return &Result{Columns: cols, Rows: rows}, nil
}

// sortRows sorts rows by the given column.
func sortRows(rows []map[string]any, col string, dir string) {
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i][col], rows[j][col]
		cmp := compareValues(a, b)
		if dir == "DESC" {
			return cmp > 0
		}
		return cmp < 0
	})
}

func compareValues(a, b any) int {
	// Try numeric
	aNum, aOK := toFloat(a)
	bNum, bOK := toFloat(b)
	if aOK && bOK {
		if aNum < bNum {
			return -1
		}
		if aNum > bNum {
			return 1
		}
		return 0
	}
	// Fall back to string
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	}
	if aStr > bStr {
		return 1
	}
	return 0
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

// copyBinding makes a shallow copy of a binding.
func copyBinding(b binding) binding {
	c := newBinding()
	for k, v := range b.nodes {
		c.nodes[k] = v
	}
	for k, v := range b.edges {
		c.edges[k] = v
	}
	return c
}

// filterNodesByProps filters nodes by inline property key-value pairs.
func filterNodesByProps(nodes []*store.Node, props map[string]string) []*store.Node {
	var filtered []*store.Node
	for _, n := range nodes {
		if nodeMatchesProps(n, props) {
			filtered = append(filtered, n)
		}
	}
	return filtered
}

// nodeMatchesProps checks if a node matches all given property filters.
func nodeMatchesProps(n *store.Node, props map[string]string) bool {
	for key, val := range props {
		actual := getNodeProperty(n, key)
		if fmt.Sprintf("%v", actual) != val {
			return false
		}
	}
	return true
}
