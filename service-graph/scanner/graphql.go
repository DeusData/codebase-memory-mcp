package scanner

import (
	"os"
	"regexp"
	"strings"

	"github.com/Shidfar/codebase-memory-mcp/service-graph/graph"
)

var (
	reGQLOperation    = regexp.MustCompile("gql\\s*`[^`]*?(query|mutation)\\s+(\\w+)")
	reGQLMultiline    = regexp.MustCompile("(?s)gql\\s*`([^`]*?)(query|mutation)\\s+(\\w+)")
	reFederationSetup = regexp.MustCompile(`buildSubgraphSchema|ApolloServer`)
	reGatewayURL      = regexp.MustCompile(`(?i)gateway[_\-]?url`)
)

type gqlOperation struct {
	opType string // "query" or "mutation"
	name   string
	file   string
	line   int
}

// ScanGraphQL scans files for GraphQL patterns and returns endpoints and edges.
func ScanGraphQL(files []string, serviceID string) (endpoints []graph.GraphQLEndpoint, edges []graph.Edge) {
	var schemaFiles []string
	var hasServer bool
	var operations []gqlOperation

	for _, file := range files {
		// .graphql schema files
		if strings.HasSuffix(file, ".graphql") || strings.HasSuffix(file, ".gql") {
			schemaFiles = append(schemaFiles, file)
			continue
		}

		if !strings.HasSuffix(file, ".ts") && !strings.HasSuffix(file, ".js") {
			continue
		}

		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		content := string(data)
		lines := strings.Split(content, "\n")

		for i, line := range lines {
			// Federation/Apollo server setup
			if reFederationSetup.MatchString(line) {
				hasServer = true
			}

			// gql operations (single-line)
			for _, m := range reGQLOperation.FindAllStringSubmatch(line, -1) {
				operations = append(operations, gqlOperation{
					opType: m[1], name: m[2], file: file, line: i + 1,
				})
			}
		}

		// Multi-line gql blocks
		for _, m := range reGQLMultiline.FindAllStringSubmatchIndex(content, -1) {
			opType := content[m[4]:m[5]]
			name := content[m[6]:m[7]]
			lineNum := strings.Count(content[:m[0]], "\n") + 1
			// Avoid duplicates
			dup := false
			for _, op := range operations {
				if op.name == name && op.file == file {
					dup = true
					break
				}
			}
			if !dup {
				operations = append(operations, gqlOperation{
					opType: opType, name: name, file: file, line: lineNum,
				})
			}
		}
	}

	// Build endpoint
	if hasServer || len(schemaFiles) > 0 {
		ep := graph.GraphQLEndpoint{
			ID:          serviceID + ":graphql",
			SchemaFiles: schemaFiles,
		}
		endpoints = append(endpoints, ep)

		schemaFile := "unknown"
		if len(schemaFiles) > 0 {
			schemaFile = schemaFiles[0]
		}
		edges = append(edges, graph.Edge{
			Source: serviceID,
			Target: serviceID + ":graphql",
			Type:   graph.EdgeGraphQLExposes,
			File:   schemaFile,
			Line:   1,
		})
	}

	// Outgoing operations
	for _, op := range operations {
		edgeType := graph.EdgeGraphQLQuery
		if op.opType == "mutation" {
			edgeType = graph.EdgeGraphQLMutation
		}
		edges = append(edges, graph.Edge{
			Source:   serviceID,
			Target:   "gateway:" + op.name,
			Type:     edgeType,
			File:     op.file,
			Line:     op.line,
			Metadata: map[string]string{"operationName": op.name},
		})
	}

	return endpoints, edges
}
