package orgdb

import "fmt"

// DependencyResult represents a package dependency relationship.
type DependencyResult struct {
	RepoName    string
	Scope       string
	PackageName string
	DepType     string
	VersionSpec string
}

// BlastRadiusResult represents the cross-repo impact of a change.
type BlastRadiusResult struct {
	AffectedRepos []AffectedRepo
	TotalRepos    int
}

// AffectedRepo is one repo affected in a blast radius analysis.
type AffectedRepo struct {
	Name       string
	Team       string
	Reason     string  // "depends_on_package", "api_consumer", "event_consumer"
	Confidence float64
}

// FlowStep represents one hop in a cross-service flow trace.
type FlowStep struct {
	FromRepo   string
	ToRepo     string
	EdgeType   string  // "api_contract", "event_contract", "package_dep"
	Detail     string  // path or topic name
	Confidence float64
}

// TeamInfo represents a team's topology in the org.
type TeamInfo struct {
	Team     string
	Repos    []RepoSummary
	DepTeams []string // teams this team depends on
}

// RepoSummary is a brief description of a repo within a team.
type RepoSummary struct {
	Name      string
	Type      string
	NodeCount int
	EdgeCount int
}

// RepoSearchResult represents a repo found by search.
type RepoSearchResult struct {
	Name      string
	Team      string
	Type      string
	Languages string
	Score     float64
	Reason    string
}

// QueryDependents finds all repos that depend on a specific package.
func (d *DB) QueryDependents(packageScope, packageName string) ([]DependencyResult, error) {
	rows, err := d.db.Query(`
		SELECT r.name, p.scope, p.name, rd.dep_type, rd.version_spec
		FROM repo_dependencies rd
		JOIN repos r ON rd.repo_id = r.id
		JOIN packages p ON rd.package_id = p.id
		WHERE p.scope = ? AND p.name = ?
		ORDER BY r.name
	`, packageScope, packageName)
	if err != nil {
		return nil, fmt.Errorf("orgdb: query dependents %s/%s: %w", packageScope, packageName, err)
	}
	defer rows.Close()

	var results []DependencyResult
	for rows.Next() {
		var r DependencyResult
		if err := rows.Scan(&r.RepoName, &r.Scope, &r.PackageName, &r.DepType, &r.VersionSpec); err != nil {
			return nil, fmt.Errorf("orgdb: scan dependent: %w", err)
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

// QueryBlastRadius finds all repos affected by a change in the given repo.
// It checks package dependents, API consumers, and event consumers.
func (d *DB) QueryBlastRadius(repoName string) (BlastRadiusResult, error) {
	rows, err := d.db.Query(`
		SELECT DISTINCT name, team, reason FROM (
			SELECT DISTINCT r.name, r.team, 'depends_on_package' as reason
			FROM repo_dependencies rd
			JOIN repos r ON rd.repo_id = r.id
			JOIN packages p ON rd.package_id = p.id
			WHERE p.provider_repo = ?

			UNION

			SELECT DISTINCT consumer_repo, '', 'api_consumer'
			FROM api_contracts
			WHERE provider_repo = ? AND consumer_repo IS NOT NULL AND consumer_repo != ''

			UNION

			SELECT DISTINCT consumer_repo, '', 'event_consumer'
			FROM event_contracts
			WHERE producer_repo = ? AND consumer_repo IS NOT NULL AND consumer_repo != ''
		)
		ORDER BY name
	`, repoName, repoName, repoName)
	if err != nil {
		return BlastRadiusResult{}, fmt.Errorf("orgdb: query blast radius %q: %w", repoName, err)
	}
	defer rows.Close()

	var result BlastRadiusResult
	for rows.Next() {
		var ar AffectedRepo
		if err := rows.Scan(&ar.Name, &ar.Team, &ar.Reason); err != nil {
			return BlastRadiusResult{}, fmt.Errorf("orgdb: scan blast radius: %w", err)
		}
		ar.Confidence = 1.0
		result.AffectedRepos = append(result.AffectedRepos, ar)
	}
	if err := rows.Err(); err != nil {
		return BlastRadiusResult{}, err
	}
	result.TotalRepos = len(result.AffectedRepos)
	return result, nil
}

// TraceFlow traces a flow starting from a trigger repo.
// direction: "downstream" (who does this call) or "upstream" (who calls this).
// maxHops limits recursion depth (default 3, max 4).
func (d *DB) TraceFlow(trigger string, direction string, maxHops int) ([]FlowStep, error) {
	if maxHops <= 0 {
		maxHops = 3
	}
	if maxHops > 4 {
		maxHops = 4
	}

	var query string
	if direction == "upstream" {
		query = `
			WITH RECURSIVE flow(from_repo, to_repo, edge_type, detail, confidence, depth) AS (
				SELECT provider_repo, consumer_repo, 'api_contract', path, confidence, 1
				FROM api_contracts WHERE consumer_repo = ? AND provider_repo != ''
				UNION ALL
				SELECT producer_repo, consumer_repo, 'event_contract', topic, 1.0, 1
				FROM event_contracts WHERE consumer_repo = ? AND producer_repo != ''
				UNION ALL
				SELECT ac.provider_repo, f.from_repo, 'api_contract', ac.path, ac.confidence, f.depth + 1
				FROM flow f
				JOIN api_contracts ac ON ac.consumer_repo = f.from_repo
				WHERE f.depth < ? AND ac.provider_repo != '' AND ac.provider_repo != f.to_repo
				UNION ALL
				SELECT ec.producer_repo, f.from_repo, 'event_contract', ec.topic, 1.0, f.depth + 1
				FROM flow f
				JOIN event_contracts ec ON ec.consumer_repo = f.from_repo
				WHERE f.depth < ? AND ec.producer_repo != '' AND ec.producer_repo != f.to_repo
			)
			SELECT DISTINCT from_repo, to_repo, edge_type, detail, confidence FROM flow
		`
	} else {
		query = `
			WITH RECURSIVE flow(from_repo, to_repo, edge_type, detail, confidence, depth) AS (
				SELECT provider_repo, consumer_repo, 'api_contract', path, confidence, 1
				FROM api_contracts WHERE provider_repo = ? AND consumer_repo != ''
				UNION ALL
				SELECT producer_repo, consumer_repo, 'event_contract', topic, 1.0, 1
				FROM event_contracts WHERE producer_repo = ? AND consumer_repo != ''
				UNION ALL
				SELECT f.to_repo, ac.consumer_repo, 'api_contract', ac.path, ac.confidence, f.depth + 1
				FROM flow f
				JOIN api_contracts ac ON ac.provider_repo = f.to_repo
				WHERE f.depth < ? AND ac.consumer_repo != '' AND ac.consumer_repo != f.from_repo
				UNION ALL
				SELECT f.to_repo, ec.consumer_repo, 'event_contract', ec.topic, 1.0, f.depth + 1
				FROM flow f
				JOIN event_contracts ec ON ec.producer_repo = f.to_repo
				WHERE f.depth < ? AND ec.consumer_repo != '' AND ec.consumer_repo != f.from_repo
			)
			SELECT DISTINCT from_repo, to_repo, edge_type, detail, confidence FROM flow
		`
	}

	rows, err := d.db.Query(query, trigger, trigger, maxHops, maxHops)
	if err != nil {
		return nil, fmt.Errorf("orgdb: trace flow %q %s: %w", trigger, direction, err)
	}
	defer rows.Close()

	var steps []FlowStep
	for rows.Next() {
		var s FlowStep
		if err := rows.Scan(&s.FromRepo, &s.ToRepo, &s.EdgeType, &s.Detail, &s.Confidence); err != nil {
			return nil, fmt.Errorf("orgdb: scan flow step: %w", err)
		}
		steps = append(steps, s)
	}
	return steps, rows.Err()
}

// TeamTopology returns a team's repos and inter-team dependencies.
func (d *DB) TeamTopology(team string) (TeamInfo, error) {
	info := TeamInfo{Team: team}

	// Get team's repos
	rows, err := d.db.Query(
		`SELECT name, type, node_count, edge_count FROM repos WHERE team = ? ORDER BY name`,
		team,
	)
	if err != nil {
		return info, fmt.Errorf("orgdb: team topology repos %q: %w", team, err)
	}
	defer rows.Close()

	for rows.Next() {
		var r RepoSummary
		if err := rows.Scan(&r.Name, &r.Type, &r.NodeCount, &r.EdgeCount); err != nil {
			return info, fmt.Errorf("orgdb: scan repo summary: %w", err)
		}
		info.Repos = append(info.Repos, r)
	}
	if err := rows.Err(); err != nil {
		return info, err
	}

	// Get dependent teams via package dependencies
	depRows, err := d.db.Query(`
		SELECT DISTINCT r2.team FROM repo_dependencies rd
		JOIN repos r1 ON rd.repo_id = r1.id
		JOIN packages p ON rd.package_id = p.id
		JOIN repos r2 ON p.provider_repo = r2.name
		WHERE r1.team = ? AND r2.team != ? AND r2.team != ''
		ORDER BY r2.team
	`, team, team)
	if err != nil {
		return info, fmt.Errorf("orgdb: team topology deps %q: %w", team, err)
	}
	defer depRows.Close()

	for depRows.Next() {
		var depTeam string
		if err := depRows.Scan(&depTeam); err != nil {
			return info, fmt.Errorf("orgdb: scan dep team: %w", err)
		}
		info.DepTeams = append(info.DepTeams, depTeam)
	}
	if err := depRows.Err(); err != nil {
		return info, err
	}

	// Ensure non-nil slices for consistent behavior
	if info.Repos == nil {
		info.Repos = []RepoSummary{}
	}
	if info.DepTeams == nil {
		info.DepTeams = []string{}
	}

	return info, nil
}

// SearchRepos searches repos by name/team with optional type and team filters.
func (d *DB) SearchRepos(query string, scope string, team string, limit int) ([]RepoSearchResult, error) {
	if limit <= 0 {
		limit = 20
	}

	rows, err := d.db.Query(`
		SELECT name, team, type, languages, 1.0 as score
		FROM repos
		WHERE (name LIKE '%' || ? || '%' OR team LIKE '%' || ? || '%')
		AND (? = '' OR ? = 'all' OR type = ?)
		AND (? = '' OR team = ?)
		ORDER BY name
		LIMIT ?
	`, query, query, scope, scope, scope, team, team, limit)
	if err != nil {
		return nil, fmt.Errorf("orgdb: search repos %q: %w", query, err)
	}
	defer rows.Close()

	var results []RepoSearchResult
	for rows.Next() {
		var r RepoSearchResult
		var languages *string
		if err := rows.Scan(&r.Name, &r.Team, &r.Type, &languages, &r.Score); err != nil {
			return nil, fmt.Errorf("orgdb: scan search result: %w", err)
		}
		if languages != nil {
			r.Languages = *languages
		}
		results = append(results, r)
	}
	return results, rows.Err()
}
