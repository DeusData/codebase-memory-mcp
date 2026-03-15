package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
)

// ConfigStore provides persistent key-value configuration backed by SQLite.
// Stored in _config.db in the cache directory (separate from per-project DBs).
type ConfigStore struct {
	db *sql.DB
}

// OpenConfig opens or creates the global config database.
func OpenConfig() (*ConfigStore, error) {
	dir, err := cacheDir()
	if err != nil {
		return nil, err
	}
	return OpenConfigInDir(dir)
}

// OpenConfigInDir opens the config database in a specific directory (for testing).
func OpenConfigInDir(dir string) (*ConfigStore, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}
	dbPath := filepath.Join(dir, "_config.db")
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=2000")
	if err != nil {
		return nil, fmt.Errorf("open config db: %w", err)
	}
	db.SetMaxOpenConns(1)

	_, err = db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS config (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("init config schema: %w", err)
	}

	return &ConfigStore{db: db}, nil
}

// Get returns the value for a key, or defaultVal if not set.
func (c *ConfigStore) Get(key, defaultVal string) string {
	var val string
	err := c.db.QueryRowContext(context.Background(),
		"SELECT value FROM config WHERE key = ?", key).Scan(&val)
	if err != nil {
		return defaultVal
	}
	return val
}

// GetBool returns a boolean config value (stored as "true"/"false").
func (c *ConfigStore) GetBool(key string, defaultVal bool) bool {
	raw := c.Get(key, "")
	if raw == "" {
		return defaultVal
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		return defaultVal
	}
	return b
}

// GetInt returns an integer config value.
func (c *ConfigStore) GetInt(key string, defaultVal int) int {
	raw := c.Get(key, "")
	if raw == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return defaultVal
	}
	return n
}

// GetFloat64 returns a float64 config value.
func (c *ConfigStore) GetFloat64(key string, defaultVal float64) float64 {
	raw := c.Get(key, "")
	if raw == "" {
		return defaultVal
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return defaultVal
	}
	return f
}

// Set stores a key-value pair (upsert).
func (c *ConfigStore) Set(key, value string) error {
	_, err := c.db.ExecContext(context.Background(),
		"INSERT INTO config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
		key, value)
	return err
}

// Delete removes a config key.
func (c *ConfigStore) Delete(key string) error {
	_, err := c.db.ExecContext(context.Background(),
		"DELETE FROM config WHERE key = ?", key)
	return err
}

// All returns all config key-value pairs.
func (c *ConfigStore) All() (map[string]string, error) {
	rows, err := c.db.QueryContext(context.Background(),
		"SELECT key, value FROM config ORDER BY key")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			continue
		}
		result[k] = v
	}
	return result, rows.Err()
}

// Close closes the config database.
func (c *ConfigStore) Close() error {
	return c.db.Close()
}

// Known config keys and their defaults.
const (
	// ConfigAutoIndex controls whether the server auto-indexes on startup.
	// Default: false (off). Enable with: codebase-memory-mcp config set auto_index true
	ConfigAutoIndex = "auto_index"

	// ConfigAutoIndexLimit is the max file count for auto-indexing new projects.
	// Default: 50000. Projects above this limit require explicit index_repository.
	ConfigAutoIndexLimit = "auto_index_limit"

	// ConfigMemLimit sets GOMEMLIMIT for the server process.
	// Accepts human-readable sizes: "2G", "512M", "4096M".
	// Default: empty (no limit). Applied on server startup.
	ConfigMemLimit = "mem_limit"

	// ConfigMetricsEnabled controls whether token savings estimation is computed
	// and included in tool responses. Default: true.
	// Set to false with: codebase-memory-mcp config set metrics_enabled false
	ConfigMetricsEnabled = "metrics_enabled"

	// ConfigPricingModel selects the token pricing model for cost estimation.
	// Supported values: "claude-sonnet" (default), "claude-opus", "gpt-4o", "custom".
	// When "custom", ConfigCustomPricePerToken is used.
	ConfigPricingModel = "pricing_model"

	// ConfigCustomPricePerToken is the USD cost per output token used when
	// ConfigPricingModel is "custom". Example: 0.000015 for $15/M tokens.
	ConfigCustomPricePerToken = "custom_price_per_token"
)
