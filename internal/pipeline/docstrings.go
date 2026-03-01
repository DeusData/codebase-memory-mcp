package pipeline

import (
	"bytes"
	"strings"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/parser"
)

// extractDocstring extracts the documentation comment for a function/class node.
// Python: looks for triple-quoted string as first statement in body (PEP 257).
// Others: scans source lines backwards from the node for comment lines.
func extractDocstring(node *tree_sitter.Node, source []byte, language lang.Language) string {
	if language == lang.Python {
		return extractPythonDocstring(node, source)
	}
	return extractCommentDocstring(source, int(node.StartPosition().Row), language)
}

// extractPythonDocstring extracts a PEP 257 docstring from a function/class body.
func extractPythonDocstring(node *tree_sitter.Node, source []byte) string {
	body := node.ChildByFieldName("body")
	if body == nil {
		return ""
	}
	if body.NamedChildCount() == 0 {
		return ""
	}
	first := body.NamedChild(0)
	if first == nil || first.Kind() != "expression_statement" {
		return ""
	}
	if first.NamedChildCount() == 0 {
		return ""
	}
	strNode := first.NamedChild(0)
	if strNode == nil || strNode.Kind() != "string" {
		return ""
	}
	return cleanPythonDocstring(parser.NodeText(strNode, source))
}

// cleanPythonDocstring removes triple-quote delimiters and normalizes indentation.
func cleanPythonDocstring(s string) string {
	for _, delim := range []string{`"""`, `'''`} {
		if strings.HasPrefix(s, delim) && strings.HasSuffix(s, delim) && len(s) >= 6 {
			s = s[3 : len(s)-3]
			break
		}
	}
	lines := strings.Split(s, "\n")
	if len(lines) <= 1 {
		return strings.TrimSpace(s)
	}
	// Dedent: find minimum indentation of non-empty continuation lines.
	minIndent := -1
	for _, line := range lines[1:] {
		trimmed := strings.TrimLeft(line, " \t")
		if trimmed == "" {
			continue
		}
		indent := len(line) - len(trimmed)
		if minIndent < 0 || indent < minIndent {
			minIndent = indent
		}
	}
	if minIndent > 0 {
		for i := 1; i < len(lines); i++ {
			if len(lines[i]) >= minIndent {
				lines[i] = lines[i][minIndent:]
			}
		}
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

// extractCommentDocstring scans backwards from startLine for doc comments.
func extractCommentDocstring(source []byte, startLine int, language lang.Language) string {
	lines := bytes.Split(source, []byte("\n"))
	if startLine <= 0 || startLine > len(lines) {
		return ""
	}

	lineIdx := startLine - 1
	trimmed := strings.TrimSpace(string(lines[lineIdx]))
	if trimmed == "" {
		return ""
	}

	// Block comment ending with */
	if strings.HasSuffix(trimmed, "*/") {
		return extractBlockComment(lines, lineIdx)
	}

	// Lua block comment ending with ]]
	if language == lang.Lua && strings.HasSuffix(trimmed, "]]") {
		return extractLuaBlockComment(lines, lineIdx)
	}

	// Line comments
	prefix := docLinePrefix(language)
	if prefix != "" && strings.HasPrefix(trimmed, prefix) {
		return extractLineComments(lines, lineIdx, prefix)
	}

	return ""
}

// docLinePrefix returns the conventional doc-comment line prefix for a language.
func docLinePrefix(language lang.Language) string {
	switch language {
	case lang.Rust:
		return "///"
	case lang.CSharp:
		return "///"
	case lang.Lua:
		return "---"
	case lang.Go, lang.CPP, lang.JavaScript, lang.TypeScript, lang.TSX,
		lang.Java, lang.Scala, lang.Kotlin, lang.PHP:
		return "//"
	default:
		return ""
	}
}

// extractBlockComment scans backwards from endLineIdx to find the start of a /* or /** block.
func extractBlockComment(lines [][]byte, endLineIdx int) string {
	startIdx := endLineIdx
	for startIdx >= 0 {
		line := strings.TrimSpace(string(lines[startIdx]))
		if strings.HasPrefix(line, "/*") {
			break
		}
		startIdx--
	}
	if startIdx < 0 {
		return ""
	}

	var result []string
	for i := startIdx; i <= endLineIdx; i++ {
		result = append(result, string(lines[i]))
	}
	return cleanBlockComment(strings.Join(result, "\n"))
}

// cleanBlockComment strips /** ... */ delimiters and leading * prefixes.
func cleanBlockComment(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "/**") {
		s = s[3:]
	} else if strings.HasPrefix(s, "/*") {
		s = s[2:]
	}
	s = strings.TrimSuffix(s, "*/")

	lines := strings.Split(s, "\n")
	cleaned := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		line = strings.TrimPrefix(line, "* ")
		line = strings.TrimPrefix(line, "*")
		cleaned = append(cleaned, line)
	}
	return strings.TrimSpace(strings.Join(cleaned, "\n"))
}

// extractLuaBlockComment scans backwards from endLineIdx for --[[ ... ]].
func extractLuaBlockComment(lines [][]byte, endLineIdx int) string {
	startIdx := endLineIdx
	for startIdx >= 0 {
		line := strings.TrimSpace(string(lines[startIdx]))
		if strings.Contains(line, "--[[") {
			break
		}
		startIdx--
	}
	if startIdx < 0 {
		return ""
	}

	var result []string
	for i := startIdx; i <= endLineIdx; i++ {
		result = append(result, string(lines[i]))
	}
	text := strings.Join(result, "\n")
	text = strings.TrimSpace(text)

	if idx := strings.Index(text, "--[["); idx >= 0 {
		text = text[idx+4:]
	}
	if idx := strings.LastIndex(text, "]]"); idx >= 0 {
		text = text[:idx]
	}
	return strings.TrimSpace(text)
}

// extractLineComments collects consecutive line comments ending at startIdx.
func extractLineComments(lines [][]byte, startIdx int, prefix string) string {
	var commentLines []string
	idx := startIdx
	for idx >= 0 {
		trimmed := strings.TrimSpace(string(lines[idx]))
		if !strings.HasPrefix(trimmed, prefix) {
			break
		}
		content := strings.TrimPrefix(trimmed, prefix)
		content = strings.TrimPrefix(content, " ")
		commentLines = append(commentLines, content)
		idx--
	}
	// Reverse (scanned backwards).
	for i, j := 0, len(commentLines)-1; i < j; i, j = i+1, j-1 {
		commentLines[i], commentLines[j] = commentLines[j], commentLines[i]
	}
	return strings.TrimSpace(strings.Join(commentLines, "\n"))
}
