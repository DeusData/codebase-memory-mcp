package store

import (
	"fmt"
	"testing"
)

// populateSearchBench creates a project with ~500 nodes and ~1000 edges.
func populateSearchBench(b *testing.B) *Store {
	b.Helper()
	s, err := OpenMemory()
	if err != nil {
		b.Fatal(err)
	}
	if err := s.UpsertProject("bench", "/tmp/bench"); err != nil {
		b.Fatal(err)
	}

	labels := []string{"Function", "Method", "Class", "Module"}
	nodeIDs := make([]int64, 0, 500)

	for i := 0; i < 500; i++ {
		label := labels[i%len(labels)]
		id, err := s.UpsertNode(&Node{
			Project:       "bench",
			Label:         label,
			Name:          fmt.Sprintf("Node%d", i),
			QualifiedName: fmt.Sprintf("bench.pkg%d.Node%d", i/50, i),
			FilePath:      fmt.Sprintf("pkg%d/file%d.go", i/50, i%50),
			StartLine:     i*10 + 1,
			EndLine:       i*10 + 8,
			Properties:    map[string]any{"index": i},
		})
		if err != nil {
			b.Fatal(err)
		}
		nodeIDs = append(nodeIDs, id)
	}

	// Create ~1000 edges: each node calls 2 others, modules define functions
	for i := 0; i < 500; i++ {
		target1 := (i + 1) % 500
		target2 := (i + 7) % 500
		edgeType := "CALLS"
		if i%4 == 3 {
			edgeType = "DEFINES"
		}
		if _, err := s.InsertEdge(&Edge{
			Project:  "bench",
			SourceID: nodeIDs[i],
			TargetID: nodeIDs[target1],
			Type:     edgeType,
		}); err != nil {
			b.Fatal(err)
		}
		if _, err := s.InsertEdge(&Edge{
			Project:  "bench",
			SourceID: nodeIDs[i],
			TargetID: nodeIDs[target2],
			Type:     "CALLS",
		}); err != nil {
			b.Fatal(err)
		}
	}

	return s
}

func BenchmarkSearch100Results(b *testing.B) {
	s := populateSearchBench(b)
	defer s.Close()

	params := SearchParams{
		Project:   "bench",
		Label:     "Function",
		MinDegree: -1,
		MaxDegree: -1,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		out, err := s.Search(&params)
		if err != nil {
			b.Fatal(err)
		}
		if len(out.Results) == 0 {
			b.Fatal("expected results")
		}
	}
}

func BenchmarkSearchWithDegreeFilter(b *testing.B) {
	s := populateSearchBench(b)
	defer s.Close()

	params := SearchParams{
		Project:      "bench",
		Relationship: "CALLS",
		Direction:    "outbound",
		MinDegree:    1,
		MaxDegree:    -1,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		out, err := s.Search(&params)
		if err != nil {
			b.Fatal(err)
		}
		if len(out.Results) == 0 {
			b.Fatal("expected results")
		}
	}
}

func BenchmarkSearchNamePattern(b *testing.B) {
	s := populateSearchBench(b)
	defer s.Close()

	params := SearchParams{
		Project:     "bench",
		NamePattern: ".*Node1[0-9]{2}.*",
		MinDegree:   -1,
		MaxDegree:   -1,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		out, err := s.Search(&params)
		if err != nil {
			b.Fatal(err)
		}
		if len(out.Results) == 0 {
			b.Fatal("expected results")
		}
	}
}

func BenchmarkSearchPagination(b *testing.B) {
	s := populateSearchBench(b)
	defer s.Close()

	params := SearchParams{
		Project:   "bench",
		Limit:     20,
		Offset:    100,
		MinDegree: -1,
		MaxDegree: -1,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		out, err := s.Search(&params)
		if err != nil {
			b.Fatal(err)
		}
		if out.Total == 0 {
			b.Fatal("expected results")
		}
	}
}
