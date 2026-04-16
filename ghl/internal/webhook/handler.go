// Package webhook handles incoming GitHub push events and triggers repo re-indexing.
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// Config configures the webhook handler.
type Config struct {
	// Secret is the HMAC-SHA256 key configured on the GitHub webhook.
	// If nil, signature validation is skipped (development mode only).
	Secret []byte

	// OnPush is called asynchronously when a valid push to a default branch is received.
	// The argument is the repository slug (repository.name from the payload).
	OnPush func(repoSlug string)
}

// Handler is an http.Handler that processes GitHub webhook events.
type Handler struct {
	cfg Config
}

// NewHandler creates a new webhook Handler with the given configuration.
func NewHandler(cfg Config) *Handler {
	return &Handler{cfg: cfg}
}

// pushPayload is the subset of a GitHub push event we care about.
type pushPayload struct {
	Ref   string `json:"ref"`
	After string `json:"after"`
	Repository struct {
		Name     string `json:"name"`
		FullName string `json:"full_name"`
		CloneURL string `json:"clone_url"`
	} `json:"repository"`
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MB cap
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	// Validate HMAC-SHA256 signature if a secret is configured
	if len(h.cfg.Secret) > 0 {
		sig := r.Header.Get("X-Hub-Signature-256")
		if sig == "" {
			http.Error(w, "missing X-Hub-Signature-256", http.StatusUnauthorized)
			return
		}
		if !validateSignature(h.cfg.Secret, body, sig) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
	}

	// Only process push events
	event := r.Header.Get("X-GitHub-Event")
	if event != "push" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Parse payload
	var payload pushPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "invalid JSON payload", http.StatusBadRequest)
		return
	}

	// Only handle pushes to default branches (master or main)
	ref := payload.Ref
	if !strings.HasSuffix(ref, "/master") && !strings.HasSuffix(ref, "/main") {
		w.WriteHeader(http.StatusOK)
		return
	}

	repoSlug := payload.Repository.Name
	if repoSlug == "" {
		http.Error(w, "missing repository.name", http.StatusBadRequest)
		return
	}

	// Fire-and-forget — respond 202 immediately
	if h.cfg.OnPush != nil {
		go h.cfg.OnPush(repoSlug)
	}

	w.WriteHeader(http.StatusAccepted)
}

// validateSignature checks the X-Hub-Signature-256 header using a constant-time comparison.
func validateSignature(secret, body []byte, signature string) bool {
	if !strings.HasPrefix(signature, "sha256=") {
		return false
	}
	got, err := hex.DecodeString(strings.TrimPrefix(signature, "sha256="))
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	expected := mac.Sum(nil)
	return hmac.Equal(got, expected)
}
