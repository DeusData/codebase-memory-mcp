package graph

// EdgeType constants
const (
	EdgePublishes      = "publishes"
	EdgeSubscribes     = "subscribes"
	EdgeGraphQLQuery   = "graphql_query"
	EdgeGraphQLMutation = "graphql_mutation"
	EdgeGraphQLExposes = "graphql_exposes"
	EdgeDBOwns         = "db_owns"
	EdgeDBReads        = "db_reads"
)

// ServiceNode represents a microservice in the graph.
type ServiceNode struct {
	ID          string `json:"id"`
	RepoPath    string `json:"repoPath"`
	Description string `json:"description,omitempty"`
}

// TopicNode represents a pub/sub topic (e.g. Kafka, Google Pub/Sub).
type TopicNode struct {
	ID      string            `json:"id"`
	Options map[string]string `json:"options,omitempty"`
}

// GraphQLEndpoint represents a GraphQL schema endpoint.
type GraphQLEndpoint struct {
	ID          string   `json:"id"`
	SchemaFiles []string `json:"schemaFiles"`
	GatewayURL  string   `json:"gatewayUrl,omitempty"`
}

// DatabaseTable represents a database table used by a service.
type DatabaseTable struct {
	ID  string `json:"id"`
	ORM string `json:"orm,omitempty"`
}

// Edge represents a directed relationship between two nodes.
type Edge struct {
	Source   string            `json:"source"`
	Target   string            `json:"target"`
	Type     string            `json:"type"`
	File     string            `json:"file"`
	Line     int               `json:"line"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ServiceGraph holds the entire dependency graph produced by a scan.
type ServiceGraph struct {
	Services          []ServiceNode     `json:"services"`
	Topics            []TopicNode       `json:"topics"`
	GraphQLEndpoints  []GraphQLEndpoint `json:"graphqlEndpoints"`
	Tables            []DatabaseTable   `json:"tables"`
	Edges             []Edge            `json:"edges"`
	ScannedAt         string            `json:"scannedAt"`
}

// ScanResult holds the artefacts discovered when scanning a single service.
type ScanResult struct {
	Topics           []TopicNode       `json:"topics"`
	GraphQLEndpoints []GraphQLEndpoint `json:"graphqlEndpoints"`
	Tables           []DatabaseTable   `json:"tables"`
	Edges            []Edge            `json:"edges"`
}
