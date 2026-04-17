package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/mcp"
)

// ToolCaller is the subset of MCP client behavior discovery needs.
type ToolCaller interface {
	CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error)
}

// Options tunes candidate narrowing and scoring depth.
type Options struct {
	MaxBM25Candidates  int
	MaxGraphCandidates int
	RequestTimeout     time.Duration
}

type indexedProject struct {
	Name     string `json:"name"`
	RootPath string `json:"root_path"`
	Nodes    int    `json:"nodes"`
	Edges    int    `json:"edges"`
}

type listProjectsPayload struct {
	Projects []indexedProject `json:"projects"`
}

type searchGraphPayload struct {
	Total           int                `json:"total"`
	Results         []searchGraphHit   `json:"results"`
	SemanticResults []semanticGraphHit `json:"semantic_results"`
}

type searchGraphHit struct {
	Name          string  `json:"name"`
	QualifiedName string  `json:"qualified_name"`
	Label         string  `json:"label"`
	FilePath      string  `json:"file_path"`
	Rank          float64 `json:"rank"`
}

type semanticGraphHit struct {
	Name          string  `json:"name"`
	QualifiedName string  `json:"qualified_name"`
	Label         string  `json:"label"`
	FilePath      string  `json:"file_path"`
	Score         float64 `json:"score"`
}

type architecturePayload struct {
	Project    string      `json:"project"`
	TotalNodes int         `json:"total_nodes"`
	TotalEdges int         `json:"total_edges"`
	NodeLabels []labelStat `json:"node_labels"`
}

type labelStat struct {
	Label string `json:"label"`
	Count int    `json:"count"`
}

type catalogEntry struct {
	Project  string
	RepoSlug string
	RootPath string
	Nodes    int
	Edges    int
	Team     string
	Type     string
	Tags     []string
}

type candidateScore struct {
	Candidate
	indexed catalogEntry
}

// Discoverer implements the discovery Service.
type Discoverer struct {
	caller   ToolCaller
	manifest manifest.Manifest
	opts     Options

	mu      sync.RWMutex
	catalog []catalogEntry
}

// NewService constructs a discoverer with sane defaults.
func NewService(caller ToolCaller, m manifest.Manifest, opts Options) *Discoverer {
	if opts.MaxBM25Candidates <= 0 {
		opts.MaxBM25Candidates = 5
	}
	if opts.MaxGraphCandidates <= 0 {
		opts.MaxGraphCandidates = 3
	}
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = 5 * time.Second
	}
	return &Discoverer{
		caller:   caller,
		manifest: m,
		opts:     opts,
	}
}

func (d *Discoverer) Definition() ToolDefinition {
	return NewDefinition()
}

// Invalidate clears the in-memory project catalog so the next request refreshes it.
func (d *Discoverer) Invalidate() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.catalog = nil
}

func (d *Discoverer) DiscoverProjects(ctx context.Context, req Request) (Response, error) {
	if strings.TrimSpace(req.Query) == "" {
		return Response{}, errors.New("query is required")
	}
	if req.Limit <= 0 {
		req.Limit = 5
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.opts.RequestTimeout)
		defer cancel()
	}

	catalog, err := d.ensureCatalog(ctx)
	if err != nil {
		return Response{}, err
	}
	if len(catalog) == 0 {
		return Response{Query: req.Query}, nil
	}

	queryTokens := tokenize(req.Query)
	candidates := d.initialCandidates(req.Query, queryTokens, catalog)
	if len(candidates) == 0 {
		return Response{Query: req.Query}, nil
	}

	if err := d.applyBM25Scores(ctx, req, queryTokens, candidates); err != nil {
		return Response{}, err
	}
	if req.IncludeGraphConfidence {
		if err := d.applyGraphConfidence(ctx, candidates); err != nil {
			return Response{}, err
		}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Score == candidates[j].Score {
			return candidates[i].RepoSlug < candidates[j].RepoSlug
		}
		return candidates[i].Score > candidates[j].Score
	})

	resp := Response{Query: req.Query}
	topScore := candidates[0].Score
	primaryCutoff := math.Max(0.55, topScore-0.12)
	for _, cand := range candidates {
		cand.Confidence = confidenceFromScore(cand.Score)
		if len(resp.PrimaryRepos) == 0 || (cand.Score >= primaryCutoff && len(resp.PrimaryRepos) < min(req.Limit, 3)) {
			resp.PrimaryRepos = append(resp.PrimaryRepos, cand.Candidate)
			continue
		}
		if cand.Score >= 0.30 && len(resp.PrimaryRepos)+len(resp.RelatedRepos) < req.Limit {
			resp.RelatedRepos = append(resp.RelatedRepos, cand.Candidate)
		}
	}
	resp.CrossRepo = len(resp.PrimaryRepos)+len(resp.RelatedRepos) > 1
	return resp, nil
}

func (d *Discoverer) ensureCatalog(ctx context.Context) ([]catalogEntry, error) {
	d.mu.RLock()
	if d.catalog != nil {
		cached := append([]catalogEntry(nil), d.catalog...)
		d.mu.RUnlock()
		return cached, nil
	}
	d.mu.RUnlock()
	return d.refreshCatalog(ctx)
}

func (d *Discoverer) refreshCatalog(ctx context.Context) ([]catalogEntry, error) {
	result, err := d.caller.CallTool(ctx, "list_projects", nil)
	if err != nil {
		return nil, fmt.Errorf("list_projects: %w", err)
	}

	var payload listProjectsPayload
	if err := decodeToolPayload(result, &payload); err != nil {
		return nil, fmt.Errorf("decode list_projects: %w", err)
	}

	manifestByName := make(map[string]manifest.Repo, len(d.manifest.Repos))
	for _, repo := range d.manifest.Repos {
		manifestByName[strings.ToLower(repo.Name)] = repo
	}

	catalog := make([]catalogEntry, 0, len(payload.Projects))
	for _, project := range payload.Projects {
		slug := deriveRepoSlug(project.Name, project.RootPath, manifestByName)
		entry := catalogEntry{
			Project:  project.Name,
			RepoSlug: slug,
			RootPath: project.RootPath,
			Nodes:    project.Nodes,
			Edges:    project.Edges,
		}
		if repo, ok := manifestByName[strings.ToLower(slug)]; ok {
			entry.Team = repo.Team
			entry.Type = repo.Type
			entry.Tags = append([]string(nil), repo.Tags...)
		}
		catalog = append(catalog, entry)
	}

	d.mu.Lock()
	d.catalog = append([]catalogEntry(nil), catalog...)
	d.mu.Unlock()
	return catalog, nil
}

func deriveRepoSlug(projectName, rootPath string, manifestByName map[string]manifest.Repo) string {
	if base := strings.TrimSpace(filepath.Base(rootPath)); base != "" && base != "." && base != string(filepath.Separator) {
		return base
	}
	lowerProject := strings.ToLower(projectName)
	if _, ok := manifestByName[lowerProject]; ok {
		return projectName
	}
	prefixes := []string{
		"app-fleet-cache-",
		"data-fleet-cache-",
		"tmp-fleet-cache-",
		"fleet-cache-",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(lowerProject, prefix) {
			return projectName[len(prefix):]
		}
	}
	return projectName
}

func (d *Discoverer) initialCandidates(query string, queryTokens []string, catalog []catalogEntry) []candidateScore {
	candidates := make([]candidateScore, 0, len(catalog))
	for _, entry := range catalog {
		score, reasons := metadataScore(query, queryTokens, entry)
		candidates = append(candidates, candidateScore{
			Candidate: Candidate{
				Project:  entry.Project,
				RepoSlug: entry.RepoSlug,
				Score:    score,
				Reasons:  reasons,
			},
			indexed: entry,
		})
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Score == candidates[j].Score {
			return healthScore(candidates[i].indexed) > healthScore(candidates[j].indexed)
		}
		return candidates[i].Score > candidates[j].Score
	})

	limit := min(len(candidates), d.opts.MaxBM25Candidates)
	if limit == 0 {
		return nil
	}

	selected := append([]candidateScore(nil), candidates[:limit]...)
	allZero := true
	for _, candidate := range selected {
		if candidate.Score > 0 {
			allZero = false
			break
		}
	}
	if allZero {
		sort.SliceStable(candidates, func(i, j int) bool {
			return healthScore(candidates[i].indexed) > healthScore(candidates[j].indexed)
		})
		selected = append([]candidateScore(nil), candidates[:limit]...)
	}
	return selected
}

func metadataScore(query string, queryTokens []string, entry catalogEntry) (float64, []string) {
	var score float64
	var reasons []string

	lowerQuery := strings.ToLower(query)
	lowerSlug := strings.ToLower(entry.RepoSlug)
	if lowerSlug != "" && strings.Contains(lowerQuery, lowerSlug) {
		score += 0.35
		reasons = append(reasons, "repo slug appears directly in task")
	}

	slugTokens := tokenSet(tokenize(lowerSlug))
	tagTokens := tokenSet(entry.Tags)
	for _, token := range queryTokens {
		if _, ok := slugTokens[token]; ok {
			score += 0.12
			reasons = append(reasons, fmt.Sprintf("name token match: %s", token))
			continue
		}
		if _, ok := tagTokens[token]; ok {
			score += 0.08
			reasons = append(reasons, fmt.Sprintf("tag match: %s", token))
			continue
		}
		if token == strings.ToLower(entry.Team) || token == strings.ToLower(entry.Type) {
			score += 0.04
			reasons = append(reasons, fmt.Sprintf("metadata match: %s", token))
		}
	}

	if entry.Nodes > 0 && entry.Edges > 0 {
		score += 0.03
	}
	if entry.Nodes <= 1 || entry.Edges == 0 {
		score -= 0.15
		reasons = append(reasons, "indexed project is shallow")
	}

	return clamp(score, 0, 0.75), dedupeStrings(reasons)
}

func (d *Discoverer) applyBM25Scores(ctx context.Context, req Request, queryTokens []string, candidates []candidateScore) error {
	for i := range candidates {
		args := map[string]interface{}{
			"project": candidates[i].Project,
			"query":   req.Query,
			"limit":   8,
		}
		if req.IncludeSemantic {
			if semanticKeywords := semanticKeywords(queryTokens); len(semanticKeywords) > 0 {
				args["semantic_query"] = semanticKeywords
			}
		}

		result, err := d.caller.CallTool(ctx, "search_graph", args)
		if err != nil {
			return fmt.Errorf("search_graph %s: %w", candidates[i].Project, err)
		}

		var payload searchGraphPayload
		if err := decodeToolPayload(result, &payload); err != nil {
			return fmt.Errorf("decode search_graph %s: %w", candidates[i].Project, err)
		}

		add, reasons := bm25Score(payload)
		candidates[i].Score = clamp(candidates[i].Score+add, 0, 1.0)
		candidates[i].Reasons = dedupeStrings(append(candidates[i].Reasons, reasons...))

		if req.IncludeSemantic {
			semAdd, semReasons := semanticScore(payload)
			candidates[i].Score = clamp(candidates[i].Score+semAdd, 0, 1.0)
			candidates[i].Reasons = dedupeStrings(append(candidates[i].Reasons, semReasons...))
		}
	}
	return nil
}

func bm25Score(payload searchGraphPayload) (float64, []string) {
	if payload.Total <= 0 || len(payload.Results) == 0 {
		return 0, []string{"no BM25 code hits"}
	}

	score := math.Min(float64(payload.Total), 8) / 8 * 0.30
	best := payload.Results[0]
	score += labelWeight(best.Label)

	reasons := []string{
		fmt.Sprintf("BM25 hit count: %d", payload.Total),
		fmt.Sprintf("top hit label: %s", best.Label),
	}
	return clamp(score, 0, 0.50), reasons
}

func semanticScore(payload searchGraphPayload) (float64, []string) {
	if len(payload.SemanticResults) == 0 {
		return 0, nil
	}

	best := payload.SemanticResults[0].Score
	score := clamp(best*0.08, 0, 0.08)
	reasons := []string{fmt.Sprintf("semantic hits: %d", len(payload.SemanticResults))}
	return score, reasons
}

func (d *Discoverer) applyGraphConfidence(ctx context.Context, candidates []candidateScore) error {
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Score > candidates[j].Score })

	limit := min(len(candidates), d.opts.MaxGraphCandidates)
	for i := 0; i < limit; i++ {
		result, err := d.caller.CallTool(ctx, "get_architecture", map[string]interface{}{
			"project": candidates[i].Project,
		})
		if err != nil {
			return fmt.Errorf("get_architecture %s: %w", candidates[i].Project, err)
		}

		var payload architecturePayload
		if err := decodeToolPayload(result, &payload); err != nil {
			return fmt.Errorf("decode get_architecture %s: %w", candidates[i].Project, err)
		}

		add, reasons := graphConfidenceScore(payload)
		candidates[i].Score = clamp(candidates[i].Score+add, 0, 1.0)
		candidates[i].Reasons = dedupeStrings(append(candidates[i].Reasons, reasons...))
	}
	return nil
}

func graphConfidenceScore(payload architecturePayload) (float64, []string) {
	if payload.TotalNodes <= 1 || payload.TotalEdges == 0 {
		return -0.40, []string{"graph confidence penalty: project-only or placeholder index"}
	}

	score := 0.0
	reasons := []string{
		fmt.Sprintf("graph depth: %d nodes / %d edges", payload.TotalNodes, payload.TotalEdges),
	}

	if payload.TotalNodes > 100 && payload.TotalEdges > 100 {
		score += 0.10
	}

	for _, label := range payload.NodeLabels {
		switch label.Label {
		case "Function", "Method", "Route", "Class", "Component":
			if label.Count > 0 {
				score += 0.05
				reasons = append(reasons, fmt.Sprintf("architecture contains %s nodes", label.Label))
				return clamp(score, -0.40, 0.15), dedupeStrings(reasons)
			}
		}
	}
	return clamp(score, -0.40, 0.15), dedupeStrings(reasons)
}

func decodeToolPayload(result *mcp.ToolResult, out interface{}) error {
	if result == nil {
		return errors.New("missing tool result")
	}
	if result.IsError {
		msg := "tool returned error"
		if len(result.Content) > 0 {
			msg = result.Content[0].Text
		}
		return errors.New(msg)
	}
	for _, item := range result.Content {
		if item.Type != "text" || strings.TrimSpace(item.Text) == "" {
			continue
		}
		return json.Unmarshal([]byte(item.Text), out)
	}
	return errors.New("missing JSON text content")
}

func tokenize(input string) []string {
	replacer := strings.NewReplacer("-", " ", "_", " ", "/", " ", ".", " ", ":", " ")
	normalized := strings.ToLower(replacer.Replace(input))
	fields := strings.Fields(normalized)
	tokens := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		tokens = append(tokens, field)
	}
	return dedupeStrings(tokens)
}

func semanticKeywords(tokens []string) []string {
	stop := map[string]struct{}{
		"add": {}, "for": {}, "the": {}, "and": {}, "flow": {}, "in": {}, "a": {}, "an": {},
	}
	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if _, ok := stop[token]; ok {
			continue
		}
		out = append(out, token)
		if len(out) == 5 {
			break
		}
	}
	return out
}

func tokenSet(tokens []string) map[string]struct{} {
	set := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		token = strings.ToLower(strings.TrimSpace(token))
		if token == "" {
			continue
		}
		set[token] = struct{}{}
	}
	return set
}

func labelWeight(label string) float64 {
	switch label {
	case "Function", "Method":
		return 0.15
	case "Route":
		return 0.13
	case "Class", "Interface", "Type", "Enum":
		return 0.10
	case "Component":
		return 0.08
	default:
		return 0.03
	}
}

func healthScore(entry catalogEntry) int {
	return entry.Nodes + entry.Edges
}

func confidenceFromScore(score float64) string {
	switch {
	case score >= 0.75:
		return "high"
	case score >= 0.50:
		return "medium"
	default:
		return "low"
	}
}

func dedupeStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func clamp(value, minValue, maxValue float64) float64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
