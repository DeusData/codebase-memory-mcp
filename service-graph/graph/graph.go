package graph

import (
	"time"
)

// Scanner function types — the caller wires in concrete implementations
// (from the scanner package) to avoid an import cycle.
type (
	// DiscoverFilesFunc discovers source files in a repository.
	DiscoverFilesFunc func(repoPath string) ([]string, error)

	// InferServiceIDFunc derives a service identifier from a repo path.
	InferServiceIDFunc func(repoPath string) string

	// CollectTopicDeclarationsFunc gathers topic declarations from all files.
	CollectTopicDeclarationsFunc func(files []string) ([]TopicNode, map[string]string)

	// ResolveEdgesFunc resolves Pub/Sub edges for a service using the global class→topic map.
	ResolveEdgesFunc func(files []string, serviceID string, classToTopic map[string]string, topicMap map[string]TopicNode) []Edge

	// ScanGraphQLFunc scans files for GraphQL schema/usage patterns.
	ScanGraphQLFunc func(files []string, serviceID string) ([]GraphQLEndpoint, []Edge)

	// ScanDatabaseFunc scans files for database table patterns.
	ScanDatabaseFunc func(files []string, serviceID string) ([]DatabaseTable, []Edge)
)

// Scanners bundles the scanner functions required by ScanAllRepos.
type Scanners struct {
	DiscoverFiles          DiscoverFilesFunc
	InferServiceID         InferServiceIDFunc
	CollectTopicDeclarations CollectTopicDeclarationsFunc
	ResolveEdges           ResolveEdgesFunc
	ScanGraphQL            ScanGraphQLFunc
	ScanDatabase           ScanDatabaseFunc
}

// ScanAllRepos performs a cross-repo scan with two-phase Pub/Sub resolution.
//
// Phase 1: Discover files in all repos, collect topic declarations globally.
// Phase 2: Resolve Pub/Sub edges per-repo using the global class→topic map.
// Other scanners (GraphQL, DB) run per-repo.
func ScanAllRepos(repoPaths []string, sc Scanners, store *Store) (*ServiceGraph, error) {
	var services []ServiceNode
	allEndpoints := make(map[string]GraphQLEndpoint)
	allTables := make(map[string]DatabaseTable)
	var allEdges []Edge

	// ── Phase 1: Discover all files, build global class→topic map ──
	repoFiles := make(map[string][]string)
	var allFiles []string

	for _, repoPath := range repoPaths {
		files, err := sc.DiscoverFiles(repoPath)
		if err != nil {
			continue
		}
		repoFiles[repoPath] = files
		allFiles = append(allFiles, files...)
	}

	topics, classToTopic := sc.CollectTopicDeclarations(allFiles)
	topicMap := make(map[string]TopicNode)
	for _, t := range topics {
		topicMap[t.ID] = t
	}

	// ── Phase 2: Per-repo scanning ──
	for _, repoPath := range repoPaths {
		serviceID := sc.InferServiceID(repoPath)
		files := repoFiles[repoPath]
		services = append(services, ServiceNode{ID: serviceID, RepoPath: repoPath})

		// Pub/Sub edges using global class→topic map
		pubsubEdges := sc.ResolveEdges(files, serviceID, classToTopic, topicMap)
		allEdges = append(allEdges, pubsubEdges...)

		// GraphQL scanner
		endpoints, gqlEdges := sc.ScanGraphQL(files, serviceID)
		for _, ep := range endpoints {
			allEndpoints[ep.ID] = ep
		}
		allEdges = append(allEdges, gqlEdges...)

		// Database scanner
		tables, dbEdges := sc.ScanDatabase(files, serviceID)
		for _, t := range tables {
			allTables[t.ID] = t
		}
		allEdges = append(allEdges, dbEdges...)
	}

	// Build final graph
	epSlice := make([]GraphQLEndpoint, 0, len(allEndpoints))
	for _, ep := range allEndpoints {
		epSlice = append(epSlice, ep)
	}
	tblSlice := make([]DatabaseTable, 0, len(allTables))
	for _, t := range allTables {
		tblSlice = append(tblSlice, t)
	}
	topicSlice := make([]TopicNode, 0, len(topicMap))
	for _, t := range topicMap {
		topicSlice = append(topicSlice, t)
	}

	g := &ServiceGraph{
		Services:         services,
		Topics:           topicSlice,
		GraphQLEndpoints: epSlice,
		Tables:           tblSlice,
		Edges:            allEdges,
		ScannedAt:        time.Now().UTC().Format(time.RFC3339),
	}

	// Save to store
	if store != nil {
		if err := store.SaveGraph(g); err != nil {
			return g, err
		}
	}

	return g, nil
}

// DanglingEntry represents a resource with one-sided edges.
type DanglingEntry struct {
	Resource string   `json:"resource"`
	Services []string `json:"services"`
}

// DanglingResult holds all dangling leaf categories.
type DanglingResult struct {
	SubscribedNotPublished []DanglingEntry `json:"subscribedNotPublished"`
	PublishedNotSubscribed []DanglingEntry `json:"publishedNotSubscribed"`
	ReadNotOwned           []DanglingEntry `json:"readNotOwned"`
}

// FindDanglingLeaves computes resources with one-sided edges.
func FindDanglingLeaves(g *ServiceGraph) DanglingResult {
	result := DanglingResult{}

	for _, topic := range g.Topics {
		var publishers, subscribers []string
		for _, e := range g.Edges {
			if e.Target == topic.ID {
				if e.Type == EdgePublishes {
					publishers = append(publishers, e.Source)
				} else if e.Type == EdgeSubscribes {
					subscribers = append(subscribers, e.Source)
				}
			}
		}
		uniquePubs := unique(publishers)
		uniqueSubs := unique(subscribers)

		if len(uniqueSubs) > 0 && len(uniquePubs) == 0 {
			result.SubscribedNotPublished = append(result.SubscribedNotPublished, DanglingEntry{
				Resource: topic.ID, Services: uniqueSubs,
			})
		}
		if len(uniquePubs) > 0 && len(uniqueSubs) == 0 {
			result.PublishedNotSubscribed = append(result.PublishedNotSubscribed, DanglingEntry{
				Resource: topic.ID, Services: uniquePubs,
			})
		}
	}

	for _, table := range g.Tables {
		var owners, readers []string
		for _, e := range g.Edges {
			if e.Target == table.ID {
				if e.Type == EdgeDBOwns {
					owners = append(owners, e.Source)
				} else if e.Type == EdgeDBReads {
					readers = append(readers, e.Source)
				}
			}
		}
		uniqueOwners := unique(owners)
		uniqueReaders := unique(readers)

		if len(uniqueReaders) > 0 && len(uniqueOwners) == 0 {
			result.ReadNotOwned = append(result.ReadNotOwned, DanglingEntry{
				Resource: table.ID, Services: uniqueReaders,
			})
		}
	}

	return result
}

func unique(s []string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, v := range s {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}
