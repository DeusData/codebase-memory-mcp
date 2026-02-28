package pipeline

import (
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// passTests derives TESTS and TESTS_FILE edges from existing CALLS/IMPORTS data.
// No AST needed — purely DB-driven post-processing.
func (p *Pipeline) passTests() {
	slog.Info("pass.tests")
	testsCount := p.createTestsEdges()
	testsFileCount := p.createTestsFileEdges()
	slog.Info("pass.tests.done", "tests", testsCount, "tests_file", testsFileCount)
}

// createTestsEdges finds CALLS edges from test functions to production functions
// and creates TESTS edges.
func (p *Pipeline) createTestsEdges() int {
	callEdges, err := p.Store.FindEdgesByType(p.ProjectName, "CALLS")
	if err != nil {
		slog.Warn("pass.tests.calls.err", "err", err)
		return 0
	}

	if len(callEdges) == 0 {
		return 0
	}

	// Collect all unique source/target node IDs
	nodeIDs := make(map[int64]bool)
	for _, e := range callEdges {
		nodeIDs[e.SourceID] = true
		nodeIDs[e.TargetID] = true
	}

	// Batch-load all nodes
	idList := make([]int64, 0, len(nodeIDs))
	for id := range nodeIDs {
		idList = append(idList, id)
	}
	nodeMap := p.batchLoadNodes(idList)

	edges := make([]*store.Edge, 0, len(callEdges)/4)
	for _, e := range callEdges {
		srcNode := nodeMap[e.SourceID]
		tgtNode := nodeMap[e.TargetID]
		if srcNode == nil || tgtNode == nil {
			continue
		}

		srcLang := langFromFilePath(srcNode.FilePath)
		tgtLang := langFromFilePath(tgtNode.FilePath)

		// Source must be in a test file, target must NOT be
		if !isTestFile(srcNode.FilePath, srcLang) {
			continue
		}
		if isTestFile(tgtNode.FilePath, tgtLang) {
			continue
		}

		// Test helper gate: only Test*/test_* named functions produce TESTS edges
		if !isTestFunction(srcNode.Name, srcLang) {
			continue
		}

		edges = append(edges, &store.Edge{
			Project:  p.ProjectName,
			SourceID: srcNode.ID,
			TargetID: tgtNode.ID,
			Type:     "TESTS",
		})
	}

	if len(edges) > 0 {
		if err := p.Store.InsertEdgeBatch(edges); err != nil {
			slog.Warn("pass.tests.insert.err", "err", err)
		}
	}
	return len(edges)
}

// createTestsFileEdges creates TESTS_FILE edges from test modules to production modules.
func (p *Pipeline) createTestsFileEdges() int {
	modules, err := p.Store.FindNodesByLabel(p.ProjectName, "Module")
	if err != nil {
		return 0
	}

	testModules := make(map[int64]*store.Node)
	prodModules := make(map[string]*store.Node) // keyed by file path
	for _, m := range modules {
		modLang := langFromFilePath(m.FilePath)
		if isTestFile(m.FilePath, modLang) {
			testModules[m.ID] = m
		} else {
			prodModules[m.FilePath] = m
		}
	}

	var edges []*store.Edge
	for _, testMod := range testModules {
		// Strategy 1: naming convention
		prodPath := testFileToProductionFile(testMod.FilePath)
		if prodPath != "" {
			if prodMod, ok := prodModules[prodPath]; ok {
				edges = append(edges, &store.Edge{
					Project:  p.ProjectName,
					SourceID: testMod.ID,
					TargetID: prodMod.ID,
					Type:     "TESTS_FILE",
				})
				continue
			}
		}

		// Strategy 2: IMPORTS from test module to production modules
		importEdges, _ := p.Store.FindEdgesBySourceAndType(testMod.ID, "IMPORTS")
		for _, ie := range importEdges {
			targetNode, _ := p.Store.FindNodeByID(ie.TargetID)
			if targetNode == nil || targetNode.Label != "Module" {
				continue
			}
			tgtLang := langFromFilePath(targetNode.FilePath)
			if !isTestFile(targetNode.FilePath, tgtLang) {
				edges = append(edges, &store.Edge{
					Project:  p.ProjectName,
					SourceID: testMod.ID,
					TargetID: targetNode.ID,
					Type:     "TESTS_FILE",
				})
			}
		}
	}

	if len(edges) > 0 {
		if err := p.Store.InsertEdgeBatch(edges); err != nil {
			slog.Warn("pass.tests_file.insert.err", "err", err)
		}
	}
	return len(edges)
}

// testFileToProductionFile returns the expected production file path for a test file.
func testFileToProductionFile(testPath string) string {
	base := filepath.Base(testPath)
	dir := filepath.Dir(testPath)
	ext := filepath.Ext(base)
	noExt := strings.TrimSuffix(base, ext)

	// Go: foo_test.go → foo.go
	if strings.HasSuffix(noExt, "_test") && ext == ".go" {
		return filepath.Join(dir, strings.TrimSuffix(noExt, "_test")+ext)
	}

	// Python: test_foo.py → foo.py
	if strings.HasPrefix(noExt, "test_") && ext == ".py" {
		return filepath.Join(dir, strings.TrimPrefix(noExt, "test_")+ext)
	}

	// JS/TS: foo.test.ts → foo.ts, foo.spec.ts → foo.ts
	for _, suffix := range []string{".test", ".spec"} {
		if strings.HasSuffix(noExt, suffix) {
			return filepath.Join(dir, strings.TrimSuffix(noExt, suffix)+ext)
		}
	}

	return ""
}

// batchLoadNodes loads nodes by ID using batch queries.
func (p *Pipeline) batchLoadNodes(ids []int64) map[int64]*store.Node {
	result := make(map[int64]*store.Node, len(ids))
	nodes, err := p.Store.FindNodesByIDs(ids)
	if err != nil {
		// Fallback to individual lookups
		for _, id := range ids {
			if n, err := p.Store.FindNodeByID(id); err == nil && n != nil {
				result[n.ID] = n
			}
		}
		return result
	}
	for _, n := range nodes {
		result[n.ID] = n
	}
	return result
}

// langFromFilePath determines the language from a file extension.
func langFromFilePath(filePath string) lang.Language {
	ext := filepath.Ext(filePath)
	l, _ := lang.LanguageForExtension(ext)
	return l
}
