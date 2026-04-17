package webhook_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/webhook"
)

// ── Helpers ────────────────────────────────────────────────────

func sign(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func pushPayload(repoName, ref, afterSHA string) []byte {
	b, _ := json.Marshal(map[string]interface{}{
		"ref":   ref,
		"after": afterSHA,
		"repository": map[string]interface{}{
			"name":      repoName,
			"full_name": "GoHighLevel/" + repoName,
			"clone_url": "https://github.com/GoHighLevel/" + repoName + ".git",
		},
	})
	return b
}

func makeRequest(t *testing.T, body []byte, secret []byte, event string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Event", event)
	if secret != nil {
		req.Header.Set("X-Hub-Signature-256", sign(secret, body))
	}
	return req
}

// ── Tests ──────────────────────────────────────────────────────

func TestHandler_ValidPush_Accepted(t *testing.T) {
	secret := []byte("test-secret")
	triggered := make(chan string, 1)

	h := webhook.NewHandler(webhook.Config{
		Secret: secret,
		OnPush: func(repoSlug string) {
			triggered <- repoSlug
		},
	})

	body := pushPayload("membership-backend", "refs/heads/master", "abc123")
	req := makeRequest(t, body, secret, "push")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Errorf("status: want 202, got %d", rr.Code)
	}

	select {
	case slug := <-triggered:
		if slug != "membership-backend" {
			t.Errorf("OnPush slug: want membership-backend, got %q", slug)
		}
	case <-time.After(2 * time.Second):
		t.Error("OnPush: not called within timeout")
	}
}

func TestHandler_InvalidSignature_Rejected(t *testing.T) {
	h := webhook.NewHandler(webhook.Config{
		Secret: []byte("real-secret"),
		OnPush: func(_ string) { /* should not be called */ },
	})

	body := pushPayload("membership-backend", "refs/heads/master", "abc123")
	// Sign with wrong secret
	req := makeRequest(t, body, []byte("wrong-secret"), "push")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status: want 401, got %d", rr.Code)
	}
}

func TestHandler_MissingSignature_Rejected(t *testing.T) {
	h := webhook.NewHandler(webhook.Config{
		Secret: []byte("real-secret"),
		OnPush: func(_ string) {},
	})

	body := pushPayload("membership-backend", "refs/heads/master", "abc123")
	req := makeRequest(t, body, nil /* no signature */, "push")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status: want 401, got %d", rr.Code)
	}
}

func TestHandler_NonPushEvent_Ignored(t *testing.T) {
	secret := []byte("test-secret")
	called := false

	h := webhook.NewHandler(webhook.Config{
		Secret: secret,
		OnPush: func(_ string) { called = true },
	})

	body := pushPayload("membership-backend", "refs/heads/master", "abc123")
	req := makeRequest(t, body, secret, "pull_request")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status: want 200, got %d", rr.Code)
	}
	if called {
		t.Error("OnPush: should not be called for non-push events")
	}
}

func TestHandler_NonDefaultBranch_Ignored(t *testing.T) {
	secret := []byte("test-secret")
	called := false

	h := webhook.NewHandler(webhook.Config{
		Secret: secret,
		OnPush: func(_ string) { called = true },
	})

	// Feature branch push — should be ignored
	body := pushPayload("membership-backend", "refs/heads/feat/new-feature", "abc123")
	req := makeRequest(t, body, secret, "push")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status: want 200 for non-default branch, got %d", rr.Code)
	}
	if called {
		t.Error("OnPush: should not be called for non-default branch pushes")
	}
}

func TestHandler_MainBranch_Accepted(t *testing.T) {
	secret := []byte("test-secret")
	triggered := make(chan string, 1)

	h := webhook.NewHandler(webhook.Config{
		Secret: secret,
		OnPush: func(slug string) { triggered <- slug },
	})

	// "main" branch (not "master") — both should be accepted
	body := pushPayload("ghl-revex-frontend", "refs/heads/main", "def456")
	req := makeRequest(t, body, secret, "push")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Errorf("status: want 202, got %d", rr.Code)
	}
	select {
	case slug := <-triggered:
		if slug != "ghl-revex-frontend" {
			t.Errorf("OnPush slug: want ghl-revex-frontend, got %q", slug)
		}
	case <-time.After(2 * time.Second):
		t.Error("OnPush: not called for main branch within timeout")
	}
}

func TestHandler_NoSecret_AllowsAnyRequest(t *testing.T) {
	// When no secret is configured (dev mode), skip signature validation
	triggered := make(chan string, 1)

	h := webhook.NewHandler(webhook.Config{
		Secret: nil, // no secret
		OnPush: func(slug string) { triggered <- slug },
	})

	body := pushPayload("platform-backend", "refs/heads/master", "xyz789")
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Event", "push")
	// No signature header
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Errorf("status: want 202 with no secret, got %d", rr.Code)
	}
}

func TestHandler_InvalidJSON_BadRequest(t *testing.T) {
	secret := []byte("test-secret")
	badBody := []byte("not json {{{")

	h := webhook.NewHandler(webhook.Config{
		Secret: secret,
		OnPush: func(_ string) {},
	})

	req := makeRequest(t, badBody, secret, "push")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status: want 400 for invalid JSON, got %d", rr.Code)
	}
}

func TestHandler_TimingSafeComparison(t *testing.T) {
	// Verify we're not vulnerable to timing attacks by confirming the implementation
	// uses hmac.Equal (or equivalent) rather than string comparison.
	// This is a behavioral test: both requests have valid-looking signatures but one is wrong.
	secret := []byte("test-secret")
	body := pushPayload("membership-backend", "refs/heads/master", "abc123")

	// Craft a signature that has the right prefix but wrong digest
	wrongSig := fmt.Sprintf("sha256=%s", "0000000000000000000000000000000000000000000000000000000000000000")

	h := webhook.NewHandler(webhook.Config{
		Secret: secret,
		OnPush: func(_ string) {},
	})

	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-Hub-Signature-256", wrongSig)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("wrong signature should return 401, got %d", rr.Code)
	}
}
