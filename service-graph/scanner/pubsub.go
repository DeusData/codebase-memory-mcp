package scanner

import (
	"os"
	"regexp"
	"strings"

	"github.com/Shidfar/codebase-memory-mcp/service-graph/graph"
)

var (
	reTopicDecl        = regexp.MustCompile(`pubsub\.topic\(\s*["'` + "`" + `]([^"'` + "`" + `]+)["'` + "`" + `]`)
	reTopicNameConst   = regexp.MustCompile(`static\s+(?:readonly\s+)?topicName\s*=\s*["'` + "`" + `]([^"'` + "`" + `]+)["'` + "`" + `]`)
	rePublishCall      = regexp.MustCompile(`(\w+Event)\.publish\s*\(`)
	reSubscribeCall    = regexp.MustCompile(`(\w+Event)\.subscribe\s*\(`)
	reSubscriptionDecl = regexp.MustCompile(`pubsub\.subscription\(\s*["'` + "`" + `]([^"'` + "`" + `]+)["'` + "`" + `]`)
	reEventClassDecl   = regexp.MustCompile(`class\s+(\w+)\s+extends\s+\w*Event`)
)

// TopicMatch holds a topic declaration found during scanning.
type TopicMatch struct {
	TopicName string
	ClassName string
	File      string
	Line      int
}

// CollectTopicDeclarations runs Phase 1: scans all files to build topic and class-to-topic maps.
func CollectTopicDeclarations(files []string) (topics []graph.TopicNode, classToTopic map[string]string) {
	topicMap := make(map[string]graph.TopicNode)
	classToTopic = make(map[string]string)
	fileTopics := make(map[string][]TopicMatch)

	// First pass: collect topic declarations
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
		var matches []TopicMatch

		for i, line := range lines {
			// Direct topic declaration
			for _, m := range reTopicDecl.FindAllStringSubmatch(line, -1) {
				topicName := m[1]
				matches = append(matches, TopicMatch{TopicName: topicName, File: file, Line: i + 1})
				topicMap[topicName] = graph.TopicNode{ID: topicName}
			}

			// Indirect via static topicName
			for _, m := range reTopicNameConst.FindAllStringSubmatch(line, -1) {
				topicName := m[1]
				matches = append(matches, TopicMatch{TopicName: topicName, File: file, Line: i + 1})
				topicMap[topicName] = graph.TopicNode{ID: topicName}
			}

			// Class declarations — associate with file's topic
			for _, m := range reEventClassDecl.FindAllStringSubmatch(line, -1) {
				className := m[1]
				for _, mt := range matches {
					if mt.File == file {
						classToTopic[className] = mt.TopicName
						break
					}
				}
			}
		}

		if len(matches) > 0 {
			fileTopics[file] = matches
		}
	}

	// Second pass: resolve classes found before their topic in the same file
	for _, file := range files {
		if !strings.HasSuffix(file, ".ts") && !strings.HasSuffix(file, ".js") {
			continue
		}
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			for _, m := range reEventClassDecl.FindAllStringSubmatch(line, -1) {
				className := m[1]
				if _, ok := classToTopic[className]; !ok {
					if fileMatches, ok := fileTopics[file]; ok && len(fileMatches) > 0 {
						classToTopic[className] = fileMatches[0].TopicName
					}
				}
			}
		}
	}

	topics = make([]graph.TopicNode, 0, len(topicMap))
	for _, t := range topicMap {
		topics = append(topics, t)
	}
	return topics, classToTopic
}

// ResolveEdges runs Phase 2: resolves publish/subscribe edges for a single repo
// using the global classToTopic map from Phase 1.
func ResolveEdges(files []string, serviceID string, classToTopic map[string]string, topicMap map[string]graph.TopicNode) []graph.Edge {
	var edges []graph.Edge

	for _, file := range files {
		if !strings.HasSuffix(file, ".ts") && !strings.HasSuffix(file, ".js") {
			continue
		}
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		lines := strings.Split(string(data), "\n")

		for i, line := range lines {
			// Publish calls
			for _, m := range rePublishCall.FindAllStringSubmatch(line, -1) {
				className := m[1]
				if topicName, ok := classToTopic[className]; ok {
					edges = append(edges, graph.Edge{
						Source:   serviceID,
						Target:   topicName,
						Type:     graph.EdgePublishes,
						File:     file,
						Line:     i + 1,
						Metadata: map[string]string{"eventClass": className},
					})
				}
			}

			// Subscribe calls
			for _, m := range reSubscribeCall.FindAllStringSubmatch(line, -1) {
				className := m[1]
				if topicName, ok := classToTopic[className]; ok {
					edges = append(edges, graph.Edge{
						Source:   serviceID,
						Target:   topicName,
						Type:     graph.EdgeSubscribes,
						File:     file,
						Line:     i + 1,
						Metadata: map[string]string{"eventClass": className},
					})
				}
			}

			// Direct subscription declarations
			for _, m := range reSubscriptionDecl.FindAllStringSubmatch(line, -1) {
				subName := m[1]
				topicName := subName
				if idx := strings.LastIndex(subName, "."); idx >= 0 {
					topicName = subName[:idx]
				}
				if _, ok := topicMap[topicName]; ok {
					edges = append(edges, graph.Edge{
						Source:   serviceID,
						Target:   topicName,
						Type:     graph.EdgeSubscribes,
						File:     file,
						Line:     i + 1,
						Metadata: map[string]string{"subscriptionName": subName},
					})
				}
			}
		}
	}

	return edges
}
