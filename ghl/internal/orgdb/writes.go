package orgdb

import (
	"fmt"
	"log/slog"
	"strings"
)

// APIContract represents a detected HTTP API dependency between two repos.
type APIContract struct {
	ProviderRepo   string
	ConsumerRepo   string
	Method         string  // GET, POST, etc.
	Path           string
	ProviderSymbol string
	ConsumerSymbol string
	Confidence     float64
}

// EventContract represents a detected event-based dependency between two repos.
type EventContract struct {
	Topic          string
	EventType      string // pubsub, cdc, cloudtask
	ProducerRepo   string
	ConsumerRepo   string
	ProducerSymbol string
	ConsumerSymbol string
}

// SetPackageProvider sets the provider_repo for a package identified by scope and name.
// The package row is created if it doesn't already exist.
func (d *DB) SetPackageProvider(scope, name, providerRepo string) error {
	_, err := d.db.Exec(`
		INSERT INTO packages (scope, name, provider_repo) VALUES (?, ?, ?)
		ON CONFLICT(scope, name) DO UPDATE SET provider_repo = excluded.provider_repo
	`, scope, name, providerRepo)
	if err != nil {
		return fmt.Errorf("orgdb: set package provider %s/%s → %s: %w", scope, name, providerRepo, err)
	}
	return nil
}

// InferPackageProviders sets provider_repo on packages by matching package names
// against repo names. For example, package "base-service" in scope "@platform-core"
// is likely provided by a repo whose name contains "base-service".
// This works without MCP tool calls — pure SQL on existing data.
// Returns the number of packages updated.
func (d *DB) InferPackageProviders() (int, error) {
	// Strategy: For each package that has no provider_repo set,
	// find a repo whose name ends with the package name or contains it
	// as a hyphen-delimited suffix. We prefer exact suffix match.
	//
	// Examples:
	//   package "base-service" → repo "platform-core-base-service" or "base-service"
	//   package "ghl-ui" → repo "ghl-ui" or "platform-ui-ghl-ui"
	//   package "logger" → repo "platform-core-logger" or "logger"
	result, err := d.db.Exec(`
		UPDATE packages SET provider_repo = (
			SELECT r.name FROM repos r
			WHERE r.name LIKE '%' || packages.name
			   OR r.name LIKE '%-' || packages.name
			   OR r.name = packages.name
			ORDER BY
				CASE WHEN r.name = packages.name THEN 0
				     WHEN r.name LIKE '%-' || packages.name THEN 1
				     ELSE 2
				END,
				length(r.name)
			LIMIT 1
		)
		WHERE (provider_repo IS NULL OR provider_repo = '')
		AND name != ''
	`)
	if err != nil {
		return 0, fmt.Errorf("orgdb: infer package providers: %w", err)
	}
	rows, _ := result.RowsAffected()
	return int(rows), nil
}

// ClearRepoData deletes all enrichment data for a repo across dependency,
// contract, event, deployment, and team_ownership tables.
// It does NOT delete from the repos table (UpsertRepo handles that).
func (d *DB) ClearRepoData(repoName string) error {
	queries := []struct {
		sql  string
		args []any
	}{
		{
			sql:  `DELETE FROM repo_dependencies WHERE repo_id IN (SELECT id FROM repos WHERE name = ?)`,
			args: []any{repoName},
		},
		{
			sql:  `DELETE FROM api_contracts WHERE provider_repo = ? OR consumer_repo = ?`,
			args: []any{repoName, repoName},
		},
		{
			sql:  `DELETE FROM event_contracts WHERE producer_repo = ? OR consumer_repo = ?`,
			args: []any{repoName, repoName},
		},
		{
			sql:  `DELETE FROM deployments WHERE repo_name = ?`,
			args: []any{repoName},
		},
		{
			sql:  `DELETE FROM team_ownership WHERE repo_name = ?`,
			args: []any{repoName},
		},
	}
	for _, q := range queries {
		if _, err := d.db.Exec(q.sql, q.args...); err != nil {
			return fmt.Errorf("orgdb: clear repo data %q: %w", repoName, err)
		}
	}
	return nil
}

// UpsertPackageDep inserts or updates a package dependency link for a repo.
// It creates the package row if it doesn't exist.
func (d *DB) UpsertPackageDep(repoName string, dep Dep) error {
	// Ensure package exists
	if _, err := d.db.Exec(
		`INSERT OR IGNORE INTO packages (scope, name) VALUES (?, ?)`,
		dep.Scope, dep.Name,
	); err != nil {
		return fmt.Errorf("orgdb: upsert package %s/%s: %w", dep.Scope, dep.Name, err)
	}

	// Get package_id
	var packageID int64
	if err := d.db.QueryRow(
		`SELECT id FROM packages WHERE scope = ? AND name = ?`,
		dep.Scope, dep.Name,
	).Scan(&packageID); err != nil {
		return fmt.Errorf("orgdb: get package id %s/%s: %w", dep.Scope, dep.Name, err)
	}

	// Get repo_id
	var repoID int64
	if err := d.db.QueryRow(
		`SELECT id FROM repos WHERE name = ?`, repoName,
	).Scan(&repoID); err != nil {
		return fmt.Errorf("orgdb: get repo id %q: %w", repoName, err)
	}

	// Upsert dependency link
	if _, err := d.db.Exec(`
		INSERT INTO repo_dependencies (repo_id, package_id, dep_type, version_spec)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(repo_id, package_id) DO UPDATE SET
			dep_type     = excluded.dep_type,
			version_spec = excluded.version_spec
	`, repoID, packageID, dep.DepType, dep.VersionSpec); err != nil {
		return fmt.Errorf("orgdb: upsert dep %q -> %s/%s: %w", repoName, dep.Scope, dep.Name, err)
	}

	return nil
}

// InsertAPIContract inserts an API contract record.
func (d *DB) InsertAPIContract(contract APIContract) error {
	if _, err := d.db.Exec(`
		INSERT INTO api_contracts (provider_repo, consumer_repo, method, path, provider_symbol, consumer_symbol, confidence)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, contract.ProviderRepo, contract.ConsumerRepo, contract.Method, contract.Path,
		contract.ProviderSymbol, contract.ConsumerSymbol, contract.Confidence,
	); err != nil {
		return fmt.Errorf("orgdb: insert api contract %s %s: %w", contract.Method, contract.Path, err)
	}
	return nil
}

// InsertEventContract inserts an event contract record.
func (d *DB) InsertEventContract(contract EventContract) error {
	if _, err := d.db.Exec(`
		INSERT INTO event_contracts (topic, event_type, producer_repo, consumer_repo, producer_symbol, consumer_symbol)
		VALUES (?, ?, ?, ?, ?, ?)
	`, contract.Topic, contract.EventType, contract.ProducerRepo, contract.ConsumerRepo,
		contract.ProducerSymbol, contract.ConsumerSymbol,
	); err != nil {
		return fmt.Errorf("orgdb: insert event contract %q: %w", contract.Topic, err)
	}
	return nil
}

// CountRepoDependencies returns the number of internal package dependencies for a repo.
func (d *DB) CountRepoDependencies(repoName string) int {
	var count int
	d.db.QueryRow(`SELECT COUNT(*) FROM repo_dependencies rd JOIN repos r ON rd.repo_id = r.id WHERE r.name = ?`, repoName).Scan(&count)
	return count
}

// CountRepoContracts returns the number of API contracts where the repo is provider or consumer.
func (d *DB) CountRepoContracts(repoName string) int {
	var count int
	d.db.QueryRow(`SELECT COUNT(*) FROM api_contracts WHERE provider_repo = ? OR consumer_repo = ?`, repoName, repoName).Scan(&count)
	return count
}

// FixRoutePaths converts __ path separators to / in api_contracts paths.
// The C binary's route qualified names use __ (e.g. "contacts__list"),
// but cross-referencing needs / (e.g. "contacts/list") to match consumer paths.
func (d *DB) FixRoutePaths() (int, error) {
	result, err := d.db.Exec(`
		UPDATE api_contracts SET path = REPLACE(path, '__', '/')
		WHERE INSTR(path, '__') > 0 AND provider_repo != '' AND (consumer_repo IS NULL OR consumer_repo = '')
	`)
	if err != nil {
		return 0, fmt.Errorf("orgdb: fix route paths: %w", err)
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// CrossReferenceContracts matches consumer-only API contracts (from InternalRequest
// calls) with provider-only contracts (from @Controller routes) by method and
// route (last path segment). The serviceName in InternalRequest (e.g. CONTACTS_API)
// differs from the controller path (e.g. contacts), so we match on the route
// portion only. Matched contracts get the provider_repo/symbol filled in and
// confidence bumped to 0.7. Returns the number of contracts updated.
func (d *DB) CrossReferenceContracts() (int, error) {
	// Extract the last path segment for comparison:
	// provider path "/contacts/list" → route "list"
	// consumer path "/CONTACTS_API/list" → route "list"
	// SQLite: substr(path, instr(reverse(path), '/')) doesn't exist,
	// so we use a Go-side approach: read both sides, match, write back.

	type contract struct {
		id             int64
		providerRepo   string
		consumerRepo   string
		method         string
		path           string
		providerSymbol string
		consumerSymbol string
		route          string // last path segment
		prefix         string // normalized first path segment (service prefix)
	}

	// Load provider-only contracts
	provRows, err := d.db.Query(`
		SELECT id, provider_repo, method, path, provider_symbol
		FROM api_contracts
		WHERE provider_repo != '' AND (consumer_repo IS NULL OR consumer_repo = '')
	`)
	if err != nil {
		return 0, fmt.Errorf("orgdb: cross-ref read providers: %w", err)
	}
	defer provRows.Close()

	var providers []contract
	for provRows.Next() {
		var c contract
		if err := provRows.Scan(&c.id, &c.providerRepo, &c.method, &c.path, &c.providerSymbol); err != nil {
			return 0, fmt.Errorf("orgdb: cross-ref scan provider: %w", err)
		}
		c.route = lastSegment(c.path)
		c.prefix = extractServiceIdentifier(c.path)
		providers = append(providers, c)
	}

	// Load consumer-only contracts
	consRows, err := d.db.Query(`
		SELECT id, consumer_repo, method, path, consumer_symbol
		FROM api_contracts
		WHERE consumer_repo != '' AND (provider_repo IS NULL OR provider_repo = '')
	`)
	if err != nil {
		return 0, fmt.Errorf("orgdb: cross-ref read consumers: %w", err)
	}
	defer consRows.Close()

	var consumers []contract
	for consRows.Next() {
		var c contract
		if err := consRows.Scan(&c.id, &c.consumerRepo, &c.method, &c.path, &c.consumerSymbol); err != nil {
			return 0, fmt.Errorf("orgdb: cross-ref scan consumer: %w", err)
		}
		c.route = lastSegment(c.path)
		c.prefix = extractServiceIdentifier(c.path)
		consumers = append(consumers, c)
	}

	// Debug: log counts and prefix overlap analysis
	provPrefixes := make(map[string]int)
	for _, p := range providers {
		if p.prefix != "" {
			provPrefixes[p.prefix]++
		}
	}
	consPrefixes := make(map[string]int)
	consOverlap := 0
	for _, c := range consumers {
		if c.prefix != "" {
			consPrefixes[c.prefix]++
			if provPrefixes[c.prefix] > 0 {
				consOverlap++
			}
		}
	}
	// Log up to 10 consumer prefixes
	consKeys := make([]string, 0, len(consPrefixes))
	for k := range consPrefixes {
		consKeys = append(consKeys, k)
	}
	if len(consKeys) > 10 {
		consKeys = consKeys[:10]
	}
	slog.Info("cross-ref: loaded contracts",
		"providers", len(providers), "consumers", len(consumers),
		"prov_prefixes", len(provPrefixes), "cons_prefixes", len(consPrefixes),
		"prefix_overlap", consOverlap, "sample_cons_prefixes", strings.Join(consKeys, ","))
	// Log first consumer that overlaps
	for _, c := range consumers {
		if c.prefix != "" && provPrefixes[c.prefix] > 0 {
			slog.Info("cross-ref: overlapping consumer",
				"repo", c.consumerRepo, "method", c.method, "path", c.path,
				"route", c.route, "prefix", c.prefix)
			// Find matching provider
			for _, p := range providers {
				if p.prefix == c.prefix {
					slog.Info("cross-ref: matching provider candidate",
						"repo", p.providerRepo, "method", p.method, "path", p.path,
						"route", p.route, "prefix", p.prefix)
					break
				}
			}
			break
		}
	}

	// Build provider index: key = "prefix:route" → list of providers
	type provKey struct{ prefix, route string }
	provIndex := make(map[provKey][]contract)
	for _, prov := range providers {
		if prov.route == "" || prov.prefix == "" {
			continue
		}
		key := provKey{prov.prefix, prov.route}
		provIndex[key] = append(provIndex[key], prov)
	}

	// Match by route (last path segment) + normalized service prefix.
	// Method matching: ANY matches any method, otherwise exact match.
	matched := 0
	for _, cons := range consumers {
		if cons.route == "" || cons.prefix == "" {
			continue
		}
		key := provKey{cons.prefix, cons.route}
		candidates := provIndex[key]
		for _, prov := range candidates {
			methodMatch := cons.method == prov.method ||
				prov.method == "ANY" || cons.method == "ANY"
			if methodMatch {
				_, err := d.db.Exec(`
					UPDATE api_contracts SET
						provider_repo   = ?,
						provider_symbol = ?,
						confidence      = 0.7
					WHERE id = ?
				`, prov.providerRepo, prov.providerSymbol, cons.id)
				if err != nil {
					return matched, fmt.Errorf("orgdb: cross-ref update consumer %d: %w", cons.id, err)
				}
				matched++
				break // first match wins
			}
		}
	}

	return matched, nil
}

// CrossReferenceEventContracts matches producer-only and consumer-only event contracts
// by topic. When a producer and consumer share the same topic, the consumer row gets
// the producer_repo/symbol filled in. Returns the number of contracts updated.
func (d *DB) CrossReferenceEventContracts() (int, error) {
	type eventContract struct {
		id             int64
		topic          string
		producerRepo   string
		consumerRepo   string
		producerSymbol string
		consumerSymbol string
	}

	// Load producer-only event contracts
	prodRows, err := d.db.Query(`
		SELECT id, topic, producer_repo, producer_symbol
		FROM event_contracts
		WHERE producer_repo != '' AND (consumer_repo IS NULL OR consumer_repo = '')
	`)
	if err != nil {
		return 0, fmt.Errorf("orgdb: cross-ref events read producers: %w", err)
	}
	defer prodRows.Close()

	var producers []eventContract
	for prodRows.Next() {
		var c eventContract
		if err := prodRows.Scan(&c.id, &c.topic, &c.producerRepo, &c.producerSymbol); err != nil {
			return 0, fmt.Errorf("orgdb: cross-ref events scan producer: %w", err)
		}
		producers = append(producers, c)
	}

	// Load consumer-only event contracts
	consRows, err := d.db.Query(`
		SELECT id, topic, consumer_repo, consumer_symbol
		FROM event_contracts
		WHERE consumer_repo != '' AND (producer_repo IS NULL OR producer_repo = '')
	`)
	if err != nil {
		return 0, fmt.Errorf("orgdb: cross-ref events read consumers: %w", err)
	}
	defer consRows.Close()

	var consumers []eventContract
	for consRows.Next() {
		var c eventContract
		if err := consRows.Scan(&c.id, &c.topic, &c.consumerRepo, &c.consumerSymbol); err != nil {
			return 0, fmt.Errorf("orgdb: cross-ref events scan consumer: %w", err)
		}
		consumers = append(consumers, c)
	}

	// Match by topic
	matched := 0
	for _, cons := range consumers {
		for _, prod := range producers {
			if cons.topic == prod.topic {
				_, err := d.db.Exec(`
					UPDATE event_contracts SET
						producer_repo   = ?,
						producer_symbol = ?
					WHERE id = ?
				`, prod.producerRepo, prod.producerSymbol, cons.id)
				if err != nil {
					return matched, fmt.Errorf("orgdb: cross-ref events update consumer %d: %w", cons.id, err)
				}
				matched++
				break // first match wins
			}
		}
	}

	return matched, nil
}

// lastSegment returns the last path segment: "/contacts/list" → "list".
func lastSegment(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[i+1:]
		}
	}
	return path
}

// extractServiceIdentifier extracts the service name from a path, handling both:
//   - Provider paths: "/contacts/list", "/api/v1/contacts/list", "/api/contacts/list"
//   - Consumer paths: "/CONTACTS_API/list"
//
// It strips common API prefixes (api, api/v1, api/v2, ...) to find the real
// service segment, then normalizes it.
func extractServiceIdentifier(path string) string {
	p := strings.TrimPrefix(path, "/")
	parts := strings.Split(p, "/")
	if len(parts) == 0 {
		return ""
	}

	// Skip leading "api" and version segments like "v1", "v2"
	i := 0
	if i < len(parts) && strings.EqualFold(parts[i], "api") {
		i++
	}
	if i < len(parts) && len(parts[i]) >= 2 && (parts[i][0] == 'v' || parts[i][0] == 'V') {
		// Check if rest is digits: "v1", "v2", "v3"
		allDigits := true
		for _, c := range parts[i][1:] {
			if c < '0' || c > '9' {
				allDigits = false
				break
			}
		}
		if allDigits {
			i++
		}
	}

	// The next segment is the service identifier
	if i < len(parts) && parts[i] != "" {
		return normalizeServicePrefix(parts[i])
	}

	// Fallback: use the first segment
	return normalizeServicePrefix(parts[0])
}

// normalizeServicePrefix strips _API/_SERVICE/_WORKER suffixes, lowercases,
// and removes hyphens so "CONTACTS_API" and "contacts" both normalize to "contacts".
func normalizeServicePrefix(s string) string {
	s = strings.ToLower(s)
	for _, suffix := range []string{"_api", "_service", "_worker"} {
		s = strings.TrimSuffix(s, suffix)
	}
	// Also normalize underscores to match hyphenated names:
	// "social_media" → "social-media" style normalization not needed,
	// but ensure consistent comparison
	return s
}
