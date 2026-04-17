package orgdb

import "fmt"

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
