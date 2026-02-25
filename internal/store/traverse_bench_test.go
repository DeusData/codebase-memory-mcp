package store

import (
	"fmt"
	"testing"
)

// populateTraverseBench creates a graph with controlled fan-out for BFS benchmarks.
// Root node calls nodes 1-fanOut, each of which calls fanOut more nodes (depth 2).
func populateTraverseBench(b *testing.B, fanOut int) (s *Store, rootID int64) {
	b.Helper()
	var err error
	s, err = OpenMemory()
	if err != nil {
		b.Fatal(err)
	}
	if err := s.UpsertProject("bench", "/tmp/bench"); err != nil {
		b.Fatal(err)
	}

	nextID := 0
	makeNode := func(name string) int64 {
		id, nodeErr := s.UpsertNode(&Node{
			Project:       "bench",
			Label:         "Function",
			Name:          name,
			QualifiedName: fmt.Sprintf("bench.pkg.%s", name),
			FilePath:      "pkg/funcs.go",
			StartLine:     nextID*5 + 1,
			EndLine:       nextID*5 + 4,
		})
		if nodeErr != nil {
			b.Fatal(nodeErr)
		}
		nextID++
		return id
	}

	rootID = makeNode("Root")

	// Depth 1: root -> children
	depth1IDs := make([]int64, fanOut)
	for i := 0; i < fanOut; i++ {
		depth1IDs[i] = makeNode(fmt.Sprintf("Child%d", i))
		if _, err := s.InsertEdge(&Edge{
			Project:  "bench",
			SourceID: rootID,
			TargetID: depth1IDs[i],
			Type:     "CALLS",
		}); err != nil {
			b.Fatal(err)
		}
	}

	// Depth 2: each depth-1 node -> fanOut leaf nodes
	for i, parentID := range depth1IDs {
		for j := 0; j < fanOut; j++ {
			leafID := makeNode(fmt.Sprintf("Leaf%d_%d", i, j))
			if _, err := s.InsertEdge(&Edge{
				Project:  "bench",
				SourceID: parentID,
				TargetID: leafID,
				Type:     "CALLS",
			}); err != nil {
				b.Fatal(err)
			}
		}
	}

	return s, rootID
}

func BenchmarkBFS50Edges(b *testing.B) {
	// fanOut=7 gives 1 + 7 + 49 = 57 nodes and 56 edges (close to 50)
	s, rootID := populateTraverseBench(b, 7)
	defer s.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := s.BFS(rootID, "outbound", []string{"CALLS"}, 3, 200)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Visited) == 0 {
			b.Fatal("expected visited nodes")
		}
	}
}

func BenchmarkBFS200Edges(b *testing.B) {
	// fanOut=14 gives 1 + 14 + 196 = 211 nodes, 210 edges
	s, rootID := populateTraverseBench(b, 14)
	defer s.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := s.BFS(rootID, "outbound", []string{"CALLS"}, 3, 300)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Visited) == 0 {
			b.Fatal("expected visited nodes")
		}
	}
}

func BenchmarkBFSInbound(b *testing.B) {
	s, err := OpenMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	if err := s.UpsertProject("bench", "/tmp/bench"); err != nil {
		b.Fatal(err)
	}

	// Create a "popular" function called by 50 callers
	targetID, _ := s.UpsertNode(&Node{
		Project:       "bench",
		Label:         "Function",
		Name:          "PopularFunc",
		QualifiedName: "bench.pkg.PopularFunc",
		FilePath:      "pkg/popular.go",
	})

	for i := 0; i < 50; i++ {
		callerID, _ := s.UpsertNode(&Node{
			Project:       "bench",
			Label:         "Function",
			Name:          fmt.Sprintf("Caller%d", i),
			QualifiedName: fmt.Sprintf("bench.callers.Caller%d", i),
			FilePath:      fmt.Sprintf("callers/caller%d.go", i),
		})
		if _, err := s.InsertEdge(&Edge{
			Project:  "bench",
			SourceID: callerID,
			TargetID: targetID,
			Type:     "CALLS",
		}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := s.BFS(targetID, "inbound", []string{"CALLS"}, 2, 200)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Visited) == 0 {
			b.Fatal("expected visited nodes")
		}
	}
}

func BenchmarkBFSDepthScaled(b *testing.B) {
	for _, depth := range []int{1, 2, 3, 5} {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			s, rootID := populateTraverseBench(b, 5)
			defer s.Close()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := s.BFS(rootID, "outbound", []string{"CALLS"}, depth, 500)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
