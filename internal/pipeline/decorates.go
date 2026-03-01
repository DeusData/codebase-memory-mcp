package pipeline

import (
	"log/slog"
	"slices"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// passDecorates creates DECORATES edges from decorated functions/classes to
// the decorator function node (if it exists in the project).
func (p *Pipeline) passDecorates() {
	slog.Info("pass.decorates")

	count := 0
	for _, label := range []string{"Function", "Method", "Class"} {
		nodes, err := p.Store.FindNodesByLabel(p.ProjectName, label)
		if err != nil {
			continue
		}
		for _, n := range nodes {
			count += p.processNodeDecorators(n)
		}
	}

	slog.Info("pass.decorates.done", "edges", count)
}

// processNodeDecorators creates DECORATES edges for a single node's decorators.
func (p *Pipeline) processNodeDecorators(n *store.Node) int {
	decs, ok := n.Properties["decorators"]
	if !ok {
		return 0
	}
	decList, ok := decs.([]any)
	if !ok {
		return 0
	}

	moduleQN := qualifiedNamePrefix(n.QualifiedName)
	importMap := p.importMaps[moduleQN]
	count := 0

	for _, d := range decList {
		decStr, ok := d.(string)
		if !ok || decStr == "" {
			continue
		}

		funcName := decoratorFunctionName(decStr)
		if funcName == "" {
			continue
		}

		targetQN := p.registry.Resolve(funcName, moduleQN, importMap)
		if targetQN == "" {
			continue
		}
		targetNode, _ := p.Store.FindNodeByQN(p.ProjectName, targetQN)
		if targetNode == nil {
			continue
		}

		_, _ = p.Store.InsertEdge(&store.Edge{
			Project:    p.ProjectName,
			SourceID:   n.ID,
			TargetID:   targetNode.ID,
			Type:       "DECORATES",
			Properties: map[string]any{"decorator": decStr},
		})
		count++
	}
	return count
}

// decoratorFunctionName extracts the function name from a decorator string.
// "@app.route('/api')" → "app.route"
// "@pytest.fixture" → "pytest.fixture"
// "@Override" → "Override"
func decoratorFunctionName(dec string) string {
	dec = strings.TrimPrefix(dec, "@")
	if idx := strings.Index(dec, "("); idx > 0 {
		dec = dec[:idx]
	}
	return strings.TrimSpace(dec)
}

type taggedNode struct {
	qn    string
	words []string
}

// collectDecoratedNodes extracts tokenized decorator words from all decorated nodes.
func (p *Pipeline) collectDecoratedNodes() (nodes []taggedNode, wordCount map[string]int) {
	wordCount = map[string]int{}

	for _, label := range []string{"Function", "Method", "Class"} {
		found, err := p.Store.FindNodesByLabel(p.ProjectName, label)
		if err != nil {
			continue
		}
		for _, n := range found {
			words := extractDecoratorWords(n)
			if len(words) > 0 {
				nodes = append(nodes, taggedNode{qn: n.QualifiedName, words: words})
				for _, w := range words {
					wordCount[w]++
				}
			}
		}
	}
	return nodes, wordCount
}

// extractDecoratorWords tokenizes all decorators on a node into unique words.
func extractDecoratorWords(n *store.Node) []string {
	decs, ok := n.Properties["decorators"]
	if !ok {
		return nil
	}
	decList, ok := decs.([]any)
	if !ok {
		return nil
	}
	seen := map[string]bool{}
	var words []string
	for _, d := range decList {
		decStr, ok := d.(string)
		if !ok {
			continue
		}
		for _, w := range tokenizeDecorator(decStr) {
			if !seen[w] {
				seen[w] = true
				words = append(words, w)
			}
		}
	}
	return words
}

// passDecoratorTags classifies decorators into semantic tags via auto-discovery.
// Words appearing on 2+ distinct nodes become tag candidates.
func (p *Pipeline) passDecoratorTags() {
	nodes, wordCount := p.collectDecoratedNodes()

	// Tag candidates: words on 2+ distinct nodes
	candidates := map[string]bool{}
	for w, c := range wordCount {
		if c >= 2 {
			candidates[w] = true
		}
	}

	if len(candidates) == 0 {
		return
	}

	tagged := 0
	for _, tn := range nodes {
		var tags []string
		for _, w := range tn.words {
			if candidates[w] {
				tags = append(tags, w)
			}
		}

		node, err := p.Store.FindNodeByQN(p.ProjectName, tn.qn)
		if err != nil || node == nil {
			continue
		}

		if len(tags) == 0 {
			// Remove stale tags from previous runs
			if _, hasTags := node.Properties["decorator_tags"]; hasTags {
				delete(node.Properties, "decorator_tags")
				_, _ = p.Store.UpsertNode(node)
			}
			continue
		}

		slices.Sort(tags)
		if node.Properties == nil {
			node.Properties = map[string]any{}
		}
		node.Properties["decorator_tags"] = tags
		_, _ = p.Store.UpsertNode(node)
		tagged++
	}

	slog.Info("pass.decorator_tags", "candidates", len(candidates), "tagged", tagged)
}
