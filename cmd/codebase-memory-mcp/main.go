package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
	"github.com/DeusData/codebase-memory-mcp/internal/tools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		fmt.Println("codebase-memory-mcp", version)
		os.Exit(0)
	}

	if len(os.Args) >= 3 && os.Args[1] == "cli" {
		os.Exit(runCLI(os.Args[2:]))
	}

	s, err := store.Open("codebase-memory")
	if err != nil {
		log.Fatalf("store open err=%v", err)
	}

	srv := tools.NewServer(s)

	ctx, cancel := context.WithCancel(context.Background())
	srv.StartWatcher(ctx)

	runErr := srv.MCPServer().Run(ctx, &mcp.StdioTransport{})
	cancel()
	s.Close()
	if runErr != nil {
		log.Fatalf("server err=%v", runErr)
	}
}

func runCLI(args []string) int {
	// Parse flags
	raw := false
	var positional []string
	for _, a := range args {
		switch a {
		case "--raw":
			raw = true
		default:
			positional = append(positional, a)
		}
	}

	if len(positional) == 0 || positional[0] == "--help" || positional[0] == "-h" {
		s, err := store.Open("codebase-memory")
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return 1
		}
		srv := tools.NewServer(s)
		s.Close()
		fmt.Fprintf(os.Stderr, "Usage: codebase-memory-mcp cli [--raw] <tool_name> [json_args]\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n  --raw    Print full JSON output (default: human-friendly summary)\n\n")
		fmt.Fprintf(os.Stderr, "Available tools:\n  %s\n", strings.Join(srv.ToolNames(), "\n  "))
		return 0
	}

	toolName := positional[0]

	s, err := store.Open("codebase-memory")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	defer s.Close()

	srv := tools.NewServer(s)

	var argsJSON json.RawMessage
	if len(positional) > 1 {
		argsJSON = json.RawMessage(positional[1])
	}

	result, err := srv.CallTool(context.Background(), toolName, argsJSON)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}

	if result.IsError {
		for _, c := range result.Content {
			if tc, ok := c.(*mcp.TextContent); ok {
				fmt.Fprintf(os.Stderr, "error: %s\n", tc.Text)
			}
		}
		return 1
	}

	// Extract the text content
	var text string
	for _, c := range result.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			text = tc.Text
			break
		}
	}

	if raw {
		printRawJSON(text)
		return 0
	}

	// Summary mode (default): print a human-friendly summary
	printSummary(toolName, text, s.DBPath())
	return 0
}

// printRawJSON pretty-prints JSON text to stdout.
func printRawJSON(text string) {
	var buf json.RawMessage
	if json.Unmarshal([]byte(text), &buf) == nil {
		if pretty, err := json.MarshalIndent(buf, "", "  "); err == nil {
			fmt.Println(string(pretty))
			return
		}
	}
	fmt.Println(text)
}

// printSummary prints a human-friendly summary of the tool result.
func printSummary(toolName, text, dbPath string) {
	var data map[string]any
	if err := json.Unmarshal([]byte(text), &data); err != nil {
		// Not a JSON object — might be an array (e.g. list_projects)
		var arr []any
		if err2 := json.Unmarshal([]byte(text), &arr); err2 == nil {
			printArraySummary(toolName, arr, dbPath)
			return
		}
		// Plain text — print as-is
		fmt.Println(text)
		return
	}

	switch toolName {
	case "index_repository":
		printIndexSummary(data, dbPath)
	case "search_graph":
		printSearchGraphSummary(data)
	case "search_code":
		printSearchCodeSummary(data)
	case "trace_call_path":
		printTraceSummary(data)
	case "query_graph":
		printQuerySummary(data)
	case "get_graph_schema":
		printSchemaSummary(data)
	case "get_code_snippet":
		printSnippetSummary(data)
	case "delete_project":
		printDeleteSummary(data)
	case "read_file":
		printReadFileSummary(data)
	case "list_directory":
		printListDirSummary(data)
	case "ingest_traces":
		printIngestSummary(data, dbPath)
	default:
		// Fallback: pretty-print the JSON
		printRawJSON(text)
	}
}

func printArraySummary(toolName string, arr []any, dbPath string) {
	switch toolName {
	case "list_projects":
		if len(arr) == 0 {
			fmt.Println("No projects indexed.")
			fmt.Printf("  db: %s\n", dbPath)
			return
		}
		fmt.Printf("%d project(s) indexed:\n", len(arr))
		for _, item := range arr {
			if m, ok := item.(map[string]any); ok {
				name, _ := m["name"].(string)
				nodes := jsonInt(m["nodes"])
				edges := jsonInt(m["edges"])
				indexedAt, _ := m["indexed_at"].(string)
				rootPath, _ := m["root_path"].(string)
				fmt.Printf("  %-30s %d nodes, %d edges  (indexed %s)\n", name, nodes, edges, indexedAt)
				if rootPath != "" {
					fmt.Printf("  %-30s %s\n", "", rootPath)
				}
			}
		}
		fmt.Printf("\n  db: %s\n", dbPath)
	default:
		fmt.Printf("%d result(s)\n", len(arr))
		printRawJSON(mustJSON(arr))
	}
}

func printIndexSummary(data map[string]any, dbPath string) {
	project, _ := data["project"].(string)
	nodes := jsonInt(data["nodes"])
	edges := jsonInt(data["edges"])
	indexedAt, _ := data["indexed_at"].(string)
	fmt.Printf("Indexed %q: %d nodes, %d edges\n", project, nodes, edges)
	fmt.Printf("  indexed_at: %s\n", indexedAt)
	fmt.Printf("  db: %s\n", dbPath)
}

func printSearchGraphSummary(data map[string]any) {
	total := jsonInt(data["total"])
	hasMore, _ := data["has_more"].(bool)
	results, _ := data["results"].([]any)
	shown := len(results)

	fmt.Printf("%d result(s) found", total)
	if hasMore {
		fmt.Printf(" (showing %d, has_more=true)", shown)
	}
	fmt.Println()

	for _, r := range results {
		if m, ok := r.(map[string]any); ok {
			name, _ := m["name"].(string)
			label, _ := m["label"].(string)
			filePath, _ := m["file_path"].(string)
			startLine := jsonInt(m["start_line"])
			fmt.Printf("  [%s] %s", label, name)
			if filePath != "" {
				fmt.Printf("  %s:%d", filePath, startLine)
			}
			fmt.Println()
		}
	}
}

func printSearchCodeSummary(data map[string]any) {
	total := jsonInt(data["total"])
	hasMore, _ := data["has_more"].(bool)
	matches, _ := data["matches"].([]any)
	shown := len(matches)

	fmt.Printf("%d match(es) found", total)
	if hasMore {
		fmt.Printf(" (showing %d, has_more=true)", shown)
	}
	fmt.Println()

	for _, m := range matches {
		if entry, ok := m.(map[string]any); ok {
			file, _ := entry["file"].(string)
			line := jsonInt(entry["line"])
			content, _ := entry["content"].(string)
			fmt.Printf("  %s:%d  %s\n", file, line, content)
		}
	}
}

func printTraceSummary(data map[string]any) {
	root, _ := data["root"].(map[string]any)
	rootName, _ := root["name"].(string)
	totalResults := jsonInt(data["total_results"])
	edges, _ := data["edges"].([]any)
	hops, _ := data["hops"].([]any)

	fmt.Printf("Trace from %q: %d node(s), %d edge(s), %d hop(s)\n", rootName, totalResults, len(edges), len(hops))

	for _, h := range hops {
		if hop, ok := h.(map[string]any); ok {
			hopNum := jsonInt(hop["hop"])
			nodes, _ := hop["nodes"].([]any)
			fmt.Printf("  hop %d: %d node(s)\n", hopNum, len(nodes))
			for _, n := range nodes {
				if nm, ok := n.(map[string]any); ok {
					name, _ := nm["name"].(string)
					label, _ := nm["label"].(string)
					fmt.Printf("    [%s] %s\n", label, name)
				}
			}
		}
	}
}

func printQuerySummary(data map[string]any) {
	total := jsonInt(data["total"])
	columns, _ := data["columns"].([]any)
	rows, _ := data["rows"].([]any)

	colNames := make([]string, len(columns))
	for i, c := range columns {
		colNames[i], _ = c.(string)
	}

	fmt.Printf("%d row(s) returned", total)
	if len(colNames) > 0 {
		fmt.Printf("  [%s]", strings.Join(colNames, ", "))
	}
	fmt.Println()

	for _, row := range rows {
		switch r := row.(type) {
		case map[string]any:
			// Rows are maps keyed by column name
			parts := make([]string, len(colNames))
			for i, col := range colNames {
				parts[i] = fmt.Sprintf("%v", r[col])
			}
			fmt.Printf("  %s\n", strings.Join(parts, " | "))
		case []any:
			parts := make([]string, len(r))
			for i, v := range r {
				parts[i] = fmt.Sprintf("%v", v)
			}
			fmt.Printf("  %s\n", strings.Join(parts, " | "))
		}
	}
}

func printSchemaSummary(data map[string]any) {
	projects, _ := data["projects"].([]any)
	if len(projects) == 0 {
		fmt.Println("No projects indexed.")
		return
	}

	for _, p := range projects {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}
		projName, _ := pm["project"].(string)
		schema, _ := pm["schema"].(map[string]any)
		if schema == nil {
			continue
		}

		fmt.Printf("Project: %s\n", projName)
		if labels, ok := schema["node_labels"].([]any); ok {
			fmt.Printf("  Node labels (%d):\n", len(labels))
			for _, l := range labels {
				if lm, ok := l.(map[string]any); ok {
					label, _ := lm["label"].(string)
					count := jsonInt(lm["count"])
					fmt.Printf("    %-15s %d\n", label, count)
				}
			}
		}
		if rels, ok := schema["relationship_types"].([]any); ok {
			fmt.Printf("  Edge types (%d):\n", len(rels))
			for _, r := range rels {
				if rm, ok := r.(map[string]any); ok {
					relType, _ := rm["type"].(string)
					count := jsonInt(rm["count"])
					fmt.Printf("    %-25s %d\n", relType, count)
				}
			}
		}
	}
}

func printSnippetSummary(data map[string]any) {
	name, _ := data["name"].(string)
	label, _ := data["label"].(string)
	filePath, _ := data["file_path"].(string)
	startLine := jsonInt(data["start_line"])
	endLine := jsonInt(data["end_line"])
	source, _ := data["source"].(string)

	fmt.Printf("[%s] %s  (%s:%d-%d)\n\n", label, name, filePath, startLine, endLine)
	fmt.Println(source)
}

func printDeleteSummary(data map[string]any) {
	deleted, _ := data["deleted"].(string)
	fmt.Printf("Deleted project %q\n", deleted)
}

func printReadFileSummary(data map[string]any) {
	path, _ := data["path"].(string)
	totalLines := jsonInt(data["total_lines"])
	content, _ := data["content"].(string)

	fmt.Printf("%s (%d lines)\n\n", path, totalLines)
	fmt.Println(content)
}

func printListDirSummary(data map[string]any) {
	dir, _ := data["directory"].(string)
	count := jsonInt(data["count"])
	entries, _ := data["entries"].([]any)

	fmt.Printf("%s (%d entries)\n", dir, count)
	for _, e := range entries {
		if em, ok := e.(map[string]any); ok {
			name, _ := em["name"].(string)
			isDir, _ := em["is_dir"].(bool)
			if isDir {
				fmt.Printf("  %s/\n", name)
			} else {
				size := jsonInt(em["size"])
				fmt.Printf("  %-40s %d bytes\n", name, size)
			}
		}
	}
}

func printIngestSummary(data map[string]any, dbPath string) {
	matched := jsonInt(data["matched"])
	boosted := jsonInt(data["boosted"])
	total := jsonInt(data["total_spans"])
	fmt.Printf("Ingested %d span(s): %d matched, %d boosted\n", total, matched, boosted)
	fmt.Printf("  db: %s\n", dbPath)
}

// jsonInt extracts an integer from a JSON-decoded value (float64 or int).
func jsonInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	default:
		return 0
	}
}

func mustJSON(v any) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}
