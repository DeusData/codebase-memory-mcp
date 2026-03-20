package scanner

import (
	"os"
	"regexp"
	"strings"

	"github.com/Shidfar/codebase-memory-mcp/service-graph/graph"
)

var (
	reTableNameStatic = regexp.MustCompile("static\\s+(?:get\\s+)?tableName\\s*(?:=|\\(\\))\\s*(?:\\{?\\s*return\\s*)?[\"'`](\\w+)[\"'`]")
	reKnexTable       = regexp.MustCompile("knex\\s*\\(\\s*[\"'`](\\w+)[\"'`]\\s*\\)")
	reKnexFromInto    = regexp.MustCompile("\\.(?:from|into)\\s*\\(\\s*[\"'`](\\w+)[\"'`]\\s*\\)")
	rePoolQuery       = regexp.MustCompile("pool\\.query\\s*\\(\\s*[\"'`]([^\"'`]+)[\"'`]")
	reSQLTable        = regexp.MustCompile(`(?i)(?:FROM|INTO|UPDATE|JOIN)\s+["'` + "`" + `]?(\w+)["'` + "`" + `]?`)
	reMigrationTable  = regexp.MustCompile("\\.createTable\\s*\\(\\s*[\"'`](\\w+)[\"'`]")
	reSQLTemplate     = regexp.MustCompile(`(?i)(?:FROM|INTO|UPDATE|JOIN)\s+["'` + "`" + `]?(\w+)["'` + "`" + `]?`)
	reTemplateStart   = regexp.MustCompile(`(?:const|let|var)\s+\w+\s*=\s*` + "`")
)

var sqlKeywords = map[string]bool{
	"set": true, "where": true, "and": true, "or": true, "values": true,
	"select": true, "on": true, "as": true, "not": true, "null": true,
	"true": true, "false": true, "case": true, "when": true, "then": true,
	"else": true, "end": true, "group": true, "order": true, "by": true,
	"having": true, "limit": true, "offset": true, "distinct": true,
	"exists": true, "between": true, "like": true, "in": true,
}

// ScanDatabase scans files for database table patterns and returns tables and edges.
func ScanDatabase(files []string, serviceID string) (tables []graph.DatabaseTable, edges []graph.Edge) {
	tableMap := make(map[string]graph.DatabaseTable)

	for _, file := range files {
		if !strings.HasSuffix(file, ".ts") && !strings.HasSuffix(file, ".js") {
			continue
		}

		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		content := string(data)
		lines := strings.Split(content, "\n")
		isModel := strings.Contains(file, "/models/") || strings.Contains(file, "/model/")
		isMigration := strings.Contains(file, "/migration")

		for i, line := range lines {
			// Objection.js model — db_owns
			for _, m := range reTableNameStatic.FindAllStringSubmatch(line, -1) {
				table := m[1]
				tableMap[table] = graph.DatabaseTable{ID: table, ORM: "objection"}
				edges = append(edges, graph.Edge{
					Source: serviceID, Target: table,
					Type: graph.EdgeDBOwns, File: file, Line: i + 1,
				})
			}

			// Knex direct queries
			for _, m := range reKnexTable.FindAllStringSubmatch(line, -1) {
				table := m[1]
				if _, ok := tableMap[table]; !ok {
					tableMap[table] = graph.DatabaseTable{ID: table, ORM: "knex"}
				}
				if !isModel && !isMigration {
					edges = append(edges, graph.Edge{
						Source: serviceID, Target: table,
						Type: graph.EdgeDBReads, File: file, Line: i + 1,
					})
				}
			}

			// .from() / .into()
			for _, m := range reKnexFromInto.FindAllStringSubmatch(line, -1) {
				table := m[1]
				if _, ok := tableMap[table]; !ok {
					tableMap[table] = graph.DatabaseTable{ID: table, ORM: "knex"}
				}
			}

			// Pool queries (raw SQL)
			for _, m := range rePoolQuery.FindAllStringSubmatch(line, -1) {
				sql := m[1]
				for _, tm := range reSQLTable.FindAllStringSubmatch(sql, -1) {
					table := strings.ToLower(tm[1])
					if sqlKeywords[table] {
						continue
					}
					if _, ok := tableMap[table]; !ok {
						tableMap[table] = graph.DatabaseTable{ID: table, ORM: "pg-pool"}
					}
					edges = append(edges, graph.Edge{
						Source: serviceID, Target: table,
						Type: graph.EdgeDBReads, File: file, Line: i + 1,
					})
				}
			}

			// SQL in template literals
			if reTemplateStart.MatchString(line) {
				end := i + 20
				if end > len(lines) {
					end = len(lines)
				}
				var sb strings.Builder
				for j := i; j < end; j++ {
					sb.WriteString(lines[j])
					sb.WriteByte('\n')
					if j > i && strings.Contains(lines[j], "`") {
						break
					}
					if j == i && strings.Count(lines[j], "`") >= 2 {
						break
					}
				}
				tmpl := sb.String()
				for _, tm := range reSQLTemplate.FindAllStringSubmatch(tmpl, -1) {
					table := strings.ToLower(tm[1])
					if sqlKeywords[table] {
						continue
					}
					if _, ok := tableMap[table]; !ok {
						tableMap[table] = graph.DatabaseTable{ID: table, ORM: "pg-pool"}
					}
					edges = append(edges, graph.Edge{
						Source: serviceID, Target: table,
						Type: graph.EdgeDBReads, File: file, Line: i + 1,
					})
				}
			}

			// Migration table creation
			if isMigration {
				for _, m := range reMigrationTable.FindAllStringSubmatch(line, -1) {
					table := m[1]
					if _, ok := tableMap[table]; !ok {
						tableMap[table] = graph.DatabaseTable{ID: table, ORM: "knex"}
					}
					edges = append(edges, graph.Edge{
						Source: serviceID, Target: table,
						Type: graph.EdgeDBOwns, File: file, Line: i + 1,
						Metadata: map[string]string{"migration": "true"},
					})
				}
			}
		}
	}

	tables = make([]graph.DatabaseTable, 0, len(tableMap))
	for _, t := range tableMap {
		tables = append(tables, t)
	}
	return tables, edges
}
