package pipeline

import (
	"log/slog"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// passImplements detects Go interface satisfaction and creates IMPLEMENTS edges.
// A struct implements an interface if it has methods matching all interface methods.
func (p *Pipeline) passImplements() error {
	slog.Info("pass5.implements")

	interfaces, err := p.Store.FindNodesByLabel(p.ProjectName, "Interface")
	if err != nil || len(interfaces) == 0 {
		return nil
	}

	// Build interface -> method names map
	type ifaceInfo struct {
		node    *store.Node
		methods []string
	}
	var ifaces []ifaceInfo

	for _, iface := range interfaces {
		// Only process Go interfaces (check file extension)
		if !strings.HasSuffix(iface.FilePath, ".go") {
			continue
		}

		// Find methods defined by this interface via DEFINES_METHOD edges
		edges, err := p.Store.FindEdgesBySourceAndType(iface.ID, "DEFINES_METHOD")
		if err != nil || len(edges) == 0 {
			continue
		}

		var methodNames []string
		for _, e := range edges {
			methodNode, _ := p.Store.FindNodeByID(e.TargetID)
			if methodNode != nil {
				methodNames = append(methodNames, methodNode.Name)
			}
		}

		if len(methodNames) > 0 {
			ifaces = append(ifaces, ifaceInfo{node: iface, methods: methodNames})
		}
	}

	if len(ifaces) == 0 {
		return nil
	}

	// Build struct -> method names map from Go receiver methods.
	// Go methods are stored as "Method" nodes with a "receiver" property
	// containing the receiver type like "(h *Handlers)".
	methods, err := p.Store.FindNodesByLabel(p.ProjectName, "Method")
	if err != nil {
		return nil
	}

	// receiverType -> set of method names
	structMethods := make(map[string]map[string]bool)
	// receiverType -> one sample method's QN prefix (to find the struct node)
	structQNPrefix := make(map[string]string)

	for _, m := range methods {
		if !strings.HasSuffix(m.FilePath, ".go") {
			continue
		}
		recv, ok := m.Properties["receiver"]
		if !ok {
			continue
		}
		recvStr, ok := recv.(string)
		if !ok || recvStr == "" {
			continue
		}

		typeName := extractReceiverType(recvStr)
		if typeName == "" {
			continue
		}

		if structMethods[typeName] == nil {
			structMethods[typeName] = make(map[string]bool)
		}
		structMethods[typeName][m.Name] = true

		// Store QN prefix for finding the struct. The method QN is like
		// "project.path.module.MethodName" — we want the module QN prefix.
		if _, exists := structQNPrefix[typeName]; !exists {
			// Take everything before the last dot (the method name)
			if idx := strings.LastIndex(m.QualifiedName, "."); idx > 0 {
				structQNPrefix[typeName] = m.QualifiedName[:idx]
			}
		}
	}

	// Check each struct against each interface
	linkCount := 0
	for _, iface := range ifaces {
		for typeName, methodSet := range structMethods {
			if satisfies(iface.methods, methodSet) {
				// Find or create the struct node. In Go, structs are stored
				// as "Class" nodes (since tree-sitter classifies type_declaration
				// as a class-like node).
				structQN := structQNPrefix[typeName] + "." + typeName
				structNode, _ := p.Store.FindNodeByQN(p.ProjectName, structQN)

				// If we can't find a class node by that QN, try to find it
				// by searching Class nodes with the matching name
				if structNode == nil {
					classes, _ := p.Store.FindNodesByLabel(p.ProjectName, "Class")
					for _, c := range classes {
						if c.Name == typeName && strings.HasSuffix(c.FilePath, ".go") {
							structNode = c
							break
						}
					}
				}

				if structNode == nil {
					continue
				}

				_, _ = p.Store.InsertEdge(&store.Edge{
					Project:  p.ProjectName,
					SourceID: structNode.ID,
					TargetID: iface.node.ID,
					Type:     "IMPLEMENTS",
				})
				linkCount++
			}
		}
	}

	slog.Info("pass5.implements.done", "links", linkCount)
	return nil
}

// extractReceiverType extracts the type name from a Go receiver string.
// "(h *Handlers)" -> "Handlers", "(s Store)" -> "Store"
func extractReceiverType(recv string) string {
	recv = strings.TrimSpace(recv)
	recv = strings.Trim(recv, "()")
	parts := strings.Fields(recv)
	if len(parts) == 0 {
		return ""
	}
	// Last field is the type, possibly with * prefix
	typeName := parts[len(parts)-1]
	typeName = strings.TrimPrefix(typeName, "*")
	return typeName
}

// satisfies checks if a set of method names includes all interface methods.
func satisfies(ifaceMethods []string, structMethodSet map[string]bool) bool {
	for _, m := range ifaceMethods {
		if !structMethodSet[m] {
			return false
		}
	}
	return true
}
