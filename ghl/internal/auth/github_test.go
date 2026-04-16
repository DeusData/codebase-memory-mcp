package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestGitHubAuthenticatorAcceptsValidUserToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/user":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"login":"octocat"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	auth := NewGitHubAuthenticator(GitHubConfig{
		BaseURL:  server.URL,
		CacheTTL: time.Minute,
	})

	if err := auth.Authenticate(context.Background(), "ghp-valid"); err != nil {
		t.Fatalf("Authenticate: unexpected error: %v", err)
	}
}

func TestGitHubAuthenticatorRejectsUserOutsideAllowedOrg(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/user":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"login":"octocat"}`))
		case "/user/memberships/orgs/GoHighLevel":
			http.Error(w, "not found", http.StatusNotFound)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	auth := NewGitHubAuthenticator(GitHubConfig{
		BaseURL:     server.URL,
		AllowedOrgs: []string{"GoHighLevel"},
		CacheTTL:    time.Minute,
	})

	if err := auth.Authenticate(context.Background(), "ghp-valid"); err == nil {
		t.Fatal("Authenticate: expected org membership error, got nil")
	}
}

func TestGitHubAuthenticatorAcceptsActiveOrgMember(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/user":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"login":"octocat"}`))
		case "/user/memberships/orgs/GoHighLevel":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"state":"active"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	auth := NewGitHubAuthenticator(GitHubConfig{
		BaseURL:     server.URL,
		AllowedOrgs: []string{"GoHighLevel"},
		CacheTTL:    time.Minute,
	})

	if err := auth.Authenticate(context.Background(), "ghp-valid"); err != nil {
		t.Fatalf("Authenticate: unexpected error: %v", err)
	}
}

func TestGitHubAuthenticatorCachesSuccessfulValidation(t *testing.T) {
	var userCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/user":
			userCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"login":"octocat"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	auth := NewGitHubAuthenticator(GitHubConfig{
		BaseURL:  server.URL,
		CacheTTL: time.Minute,
	})

	if err := auth.Authenticate(context.Background(), "ghp-valid"); err != nil {
		t.Fatalf("Authenticate first: unexpected error: %v", err)
	}
	if err := auth.Authenticate(context.Background(), "ghp-valid"); err != nil {
		t.Fatalf("Authenticate second: unexpected error: %v", err)
	}
	if got := userCalls.Load(); got != 1 {
		t.Fatalf("/user calls: want 1, got %d", got)
	}
}

func TestGitHubAuthenticatorDoesNotCacheTransientFailures(t *testing.T) {
	var userCalls atomic.Int32
	var failFirst atomic.Bool
	failFirst.Store(true)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/user":
			userCalls.Add(1)
			if failFirst.CompareAndSwap(true, false) {
				http.Error(w, "temporary failure", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"login":"octocat"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	auth := NewGitHubAuthenticator(GitHubConfig{
		BaseURL:  server.URL,
		CacheTTL: time.Minute,
	})

	if err := auth.Authenticate(context.Background(), "ghp-valid"); err == nil {
		t.Fatal("Authenticate first: expected transient failure, got nil")
	}
	if err := auth.Authenticate(context.Background(), "ghp-valid"); err != nil {
		t.Fatalf("Authenticate second: unexpected error: %v", err)
	}
	if got := userCalls.Load(); got != 2 {
		t.Fatalf("/user calls: want 2 after transient failure retry, got %d", got)
	}
}

func TestGitHubAuthenticatorAcceptsUserInAnyAllowedOrg(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/user":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"login":"octocat"}`))
		case "/user/memberships/orgs/OrgOne":
			http.Error(w, "not found", http.StatusNotFound)
		case "/user/memberships/orgs/OrgTwo":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"state":"active"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	auth := NewGitHubAuthenticator(GitHubConfig{
		BaseURL:     server.URL,
		AllowedOrgs: []string{"OrgOne", "OrgTwo"},
		CacheTTL:    time.Minute,
	})

	if err := auth.Authenticate(context.Background(), "ghp-valid"); err != nil {
		t.Fatalf("Authenticate: unexpected error: %v", err)
	}
}
