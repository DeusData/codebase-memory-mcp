package pipeline

import (
	"testing"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

func TestPassImplementsCreatesOverrideEdges(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	project := "test-proj"
	_ = s.UpsertProject(project, "/tmp/test")

	// Create interface node
	ifaceID, _ := s.UpsertNode(&store.Node{
		Project: project, Label: "Interface", Name: "Reader",
		QualifiedName: "pkg.Reader", FilePath: "pkg/reader.go",
	})

	// Create interface method nodes
	readMethodID, _ := s.UpsertNode(&store.Node{
		Project: project, Label: "Method", Name: "Read",
		QualifiedName: "pkg.Reader.Read", FilePath: "pkg/reader.go",
	})
	closeMethodID, _ := s.UpsertNode(&store.Node{
		Project: project, Label: "Method", Name: "Close",
		QualifiedName: "pkg.Reader.Close", FilePath: "pkg/reader.go",
	})

	// DEFINES_METHOD edges (interface → method)
	_, _ = s.InsertEdge(&store.Edge{
		Project: project, SourceID: ifaceID, TargetID: readMethodID, Type: "DEFINES_METHOD",
	})
	_, _ = s.InsertEdge(&store.Edge{
		Project: project, SourceID: ifaceID, TargetID: closeMethodID, Type: "DEFINES_METHOD",
	})

	// Create struct (Class) node
	_, _ = s.UpsertNode(&store.Node{
		Project: project, Label: "Class", Name: "FileReader",
		QualifiedName: "pkg.FileReader", FilePath: "pkg/filereader.go",
	})

	// Create struct method nodes with receiver property
	_, _ = s.UpsertNode(&store.Node{
		Project: project, Label: "Method", Name: "Read",
		QualifiedName: "pkg.FileReader.Read", FilePath: "pkg/filereader.go",
		Properties: map[string]any{"receiver": "(f *FileReader)"},
	})
	_, _ = s.UpsertNode(&store.Node{
		Project: project, Label: "Method", Name: "Close",
		QualifiedName: "pkg.FileReader.Close", FilePath: "pkg/filereader.go",
		Properties: map[string]any{"receiver": "(f *FileReader)"},
	})

	// Run passImplements
	p := &Pipeline{
		Store:       s,
		ProjectName: project,
	}
	p.passImplements()

	// Verify IMPLEMENTS edge exists (struct → interface)
	structNode, _ := s.FindNodeByQN(project, "pkg.FileReader")
	if structNode == nil {
		t.Fatal("struct node not found")
	}
	implEdges, _ := s.FindEdgesBySourceAndType(structNode.ID, "IMPLEMENTS")
	if len(implEdges) != 1 {
		t.Fatalf("expected 1 IMPLEMENTS edge, got %d", len(implEdges))
	}
	if implEdges[0].TargetID != ifaceID {
		t.Errorf("IMPLEMENTS target should be interface, got %d", implEdges[0].TargetID)
	}

	// Verify OVERRIDE edges (struct_method → interface_method)
	structReadNode, _ := s.FindNodeByQN(project, "pkg.FileReader.Read")
	if structReadNode == nil {
		t.Fatal("struct Read method not found")
	}
	readOverrides, _ := s.FindEdgesBySourceAndType(structReadNode.ID, "OVERRIDE")
	if len(readOverrides) != 1 {
		t.Fatalf("expected 1 OVERRIDE edge for Read, got %d", len(readOverrides))
	}
	if readOverrides[0].TargetID != readMethodID {
		t.Errorf("OVERRIDE target should be interface Read method")
	}

	structCloseNode, _ := s.FindNodeByQN(project, "pkg.FileReader.Close")
	if structCloseNode == nil {
		t.Fatal("struct Close method not found")
	}
	closeOverrides, _ := s.FindEdgesBySourceAndType(structCloseNode.ID, "OVERRIDE")
	if len(closeOverrides) != 1 {
		t.Fatalf("expected 1 OVERRIDE edge for Close, got %d", len(closeOverrides))
	}
	if closeOverrides[0].TargetID != closeMethodID {
		t.Errorf("OVERRIDE target should be interface Close method")
	}
}

func TestPassImplementsNoOverrideWithoutMatch(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	project := "test-proj"
	_ = s.UpsertProject(project, "/tmp/test")

	// Interface with 2 methods
	ifaceID, _ := s.UpsertNode(&store.Node{
		Project: project, Label: "Interface", Name: "ReadWriter",
		QualifiedName: "pkg.ReadWriter", FilePath: "pkg/rw.go",
	})
	readID, _ := s.UpsertNode(&store.Node{
		Project: project, Label: "Method", Name: "Read",
		QualifiedName: "pkg.ReadWriter.Read", FilePath: "pkg/rw.go",
	})
	writeID, _ := s.UpsertNode(&store.Node{
		Project: project, Label: "Method", Name: "Write",
		QualifiedName: "pkg.ReadWriter.Write", FilePath: "pkg/rw.go",
	})
	_, _ = s.InsertEdge(&store.Edge{
		Project: project, SourceID: ifaceID, TargetID: readID, Type: "DEFINES_METHOD",
	})
	_, _ = s.InsertEdge(&store.Edge{
		Project: project, SourceID: ifaceID, TargetID: writeID, Type: "DEFINES_METHOD",
	})

	// Struct with only Read (missing Write) — should NOT implement
	_, _ = s.UpsertNode(&store.Node{
		Project: project, Label: "Class", Name: "OnlyReader",
		QualifiedName: "pkg.OnlyReader", FilePath: "pkg/onlyreader.go",
	})
	_, _ = s.UpsertNode(&store.Node{
		Project: project, Label: "Method", Name: "Read",
		QualifiedName: "pkg.OnlyReader.Read", FilePath: "pkg/onlyreader.go",
		Properties: map[string]any{"receiver": "(o *OnlyReader)"},
	})

	p := &Pipeline{Store: s, ProjectName: project}
	p.passImplements()

	// No IMPLEMENTS or OVERRIDE edges should exist
	structNode, _ := s.FindNodeByQN(project, "pkg.OnlyReader")
	if structNode == nil {
		t.Fatal("struct not found")
	}
	implEdges, _ := s.FindEdgesBySourceAndType(structNode.ID, "IMPLEMENTS")
	if len(implEdges) != 0 {
		t.Errorf("expected 0 IMPLEMENTS edges, got %d", len(implEdges))
	}

	methodNode, _ := s.FindNodeByQN(project, "pkg.OnlyReader.Read")
	if methodNode == nil {
		t.Fatal("method not found")
	}
	overrideEdges, _ := s.FindEdgesBySourceAndType(methodNode.ID, "OVERRIDE")
	if len(overrideEdges) != 0 {
		t.Errorf("expected 0 OVERRIDE edges, got %d", len(overrideEdges))
	}
}
