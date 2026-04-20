package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

const githubAPIVersion = "2022-11-28"

// GitHubConfig configures bearer-token validation against GitHub.
type GitHubConfig struct {
	BaseURL     string
	AllowedOrgs []string
	HTTPClient  *http.Client
	CacheTTL    time.Duration
}

// GitHubAuthenticator validates incoming bearer tokens against GitHub APIs.
type GitHubAuthenticator struct {
	baseURL     string
	allowedOrgs []string
	client      *http.Client
	cacheTTL    time.Duration

	mu    sync.Mutex
	cache map[string]cacheEntry
}

type cacheEntry struct {
	expiresAt time.Time
	err       error
}

type githubUser struct {
	Login string `json:"login"`
}

type githubMembership struct {
	State string `json:"state"`
}

// NewGitHubAuthenticator constructs a GitHub-backed token authenticator.
func NewGitHubAuthenticator(cfg GitHubConfig) *GitHubAuthenticator {
	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		baseURL = "https://api.github.com"
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	cacheTTL := cfg.CacheTTL
	if cacheTTL <= 0 {
		cacheTTL = 5 * time.Minute
	}
	return &GitHubAuthenticator{
		baseURL:     strings.TrimRight(baseURL, "/"),
		allowedOrgs: append([]string(nil), cfg.AllowedOrgs...),
		client:      client,
		cacheTTL:    cacheTTL,
		cache:       make(map[string]cacheEntry),
	}
}

// Authenticate validates the bearer token against GitHub and optional org membership.
func (a *GitHubAuthenticator) Authenticate(ctx context.Context, bearerToken string) error {
	token := strings.TrimSpace(bearerToken)
	if token == "" {
		return errors.New("missing github token")
	}

	cacheKey := hashToken(token)
	if err, ok := a.cached(cacheKey); ok {
		return err
	}

	err := a.authenticateUncached(ctx, token)
	if err == nil {
		a.store(cacheKey, nil)
	}
	return err
}

func (a *GitHubAuthenticator) authenticateUncached(ctx context.Context, token string) error {
	user, err := a.fetchUser(ctx, token)
	if err != nil {
		return err
	}
	if len(a.allowedOrgs) == 0 {
		return nil
	}
	for _, org := range a.allowedOrgs {
		ok, err := a.isActiveOrgMember(ctx, token, org, user.Login)
		if err == nil && ok {
			return nil
		}
	}
	return fmt.Errorf("github user %q is not an active member of allowed orgs", user.Login)
}

func (a *GitHubAuthenticator) fetchUser(ctx context.Context, token string) (*githubUser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.baseURL+"/user", nil)
	if err != nil {
		return nil, err
	}
	addGitHubHeaders(req, token)

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("github /user request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("github /user returned %d", resp.StatusCode)
	}

	var user githubUser
	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return nil, fmt.Errorf("decode github /user: %w", err)
	}
	if user.Login == "" {
		return nil, errors.New("github /user missing login")
	}
	return &user, nil
}

func (a *GitHubAuthenticator) isActiveOrgMember(ctx context.Context, token, org, _ string) (bool, error) {
	org = strings.TrimSpace(org)
	if org == "" {
		return false, nil
	}

	// Use /user/orgs — lists all orgs the authenticated user belongs to.
	// Works with any token scope. Check if the target org is in the list.
	reqURL := a.baseURL + "/user/orgs?per_page=100"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return false, err
	}
	addGitHubHeaders(req, token)

	resp, err := a.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("github /user/orgs request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("github /user/orgs returned %d", resp.StatusCode)
	}

	var orgs []struct {
		Login string `json:"login"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&orgs); err != nil {
		return false, fmt.Errorf("decode github /user/orgs: %w", err)
	}
	for _, o := range orgs {
		if strings.EqualFold(o.Login, org) {
			return true, nil
		}
	}
	return false, nil
}

func addGitHubHeaders(req *http.Request, token string) {
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
	req.Header.Set("User-Agent", "codebase-memory-mcp-ghl")
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (a *GitHubAuthenticator) cached(key string) (error, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry, ok := a.cache[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.expiresAt) {
		delete(a.cache, key)
		return nil, false
	}
	return entry.err, true
}

func (a *GitHubAuthenticator) store(key string, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cache[key] = cacheEntry{
		expiresAt: time.Now().Add(a.cacheTTL),
		err:       err,
	}
}
