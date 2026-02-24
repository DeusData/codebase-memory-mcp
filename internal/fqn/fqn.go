package fqn

import (
	"path/filepath"
	"strings"
)

// Compute returns the canonical qualified name for a node.
// Format: <project>.<rel_path_parts_dotted>.<name>
// Examples:
//   - myproject.cmd.server.main.HandleRequest
//   - myproject.pkg.service.ProcessOrder
func Compute(project, relPath, name string) string {
	// Remove file extension
	relPath = strings.TrimSuffix(relPath, filepath.Ext(relPath))
	// Convert path separators to dots
	parts := strings.Split(filepath.ToSlash(relPath), "/")

	// For Python __init__.py, drop the __init__ part
	if len(parts) > 0 && parts[len(parts)-1] == "__init__" {
		parts = parts[:len(parts)-1]
	}
	// For JS/TS index files
	if len(parts) > 0 && parts[len(parts)-1] == "index" {
		parts = parts[:len(parts)-1]
	}

	all := append([]string{project}, parts...)
	if name != "" {
		all = append(all, name)
	}
	return strings.Join(all, ".")
}

// ModuleQN returns the qualified name for a module (file without function name).
func ModuleQN(project, relPath string) string {
	return Compute(project, relPath, "")
}

// FolderQN returns the qualified name for a folder.
func FolderQN(project, relDir string) string {
	parts := strings.Split(filepath.ToSlash(relDir), "/")
	all := append([]string{project}, parts...)
	return strings.Join(all, ".")
}
