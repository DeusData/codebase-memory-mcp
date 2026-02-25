package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

func createTempFile(t *testing.T, dir, name, content string) {
	t.Helper()
	fullPath := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fullPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestScanDockerfileEnvURLs(t *testing.T) {
	dir := t.TempDir()

	createTempFile(t, dir, "Dockerfile", `FROM python:3.9-slim
ENV ORDER_URL=https://api.example.com/api/orders
ENV DB_HOST=localhost
ARG WEBHOOK_URL=https://hooks.example.com/webhook
`)

	bindings := ScanProjectEnvURLs(dir)

	found := map[string]string{}
	for _, b := range bindings {
		found[b.Key] = b.Value
	}

	if v, ok := found["ORDER_URL"]; !ok || v != "https://api.example.com/api/orders" {
		t.Errorf("expected ORDER_URL=https://api.example.com/api/orders, got %q", v)
	}
	if v, ok := found["WEBHOOK_URL"]; !ok || v != "https://hooks.example.com/webhook" {
		t.Errorf("expected WEBHOOK_URL=https://hooks.example.com/webhook, got %q", v)
	}
	if _, ok := found["DB_HOST"]; ok {
		t.Error("DB_HOST=localhost should not be extracted (not a URL)")
	}
}

func TestScanYAMLEnvURLs(t *testing.T) {
	dir := t.TempDir()

	createTempFile(t, dir, "config.yaml", `
service:
  service_url: "https://api.internal.com/api/process"
  timeout: 30
  callback_url: "https://hooks.internal.com/callback"
`)

	bindings := ScanProjectEnvURLs(dir)

	found := map[string]string{}
	for _, b := range bindings {
		found[b.Key] = b.Value
	}

	if v, ok := found["service_url"]; !ok || v != "https://api.internal.com/api/process" {
		t.Errorf("expected service_url, got %q (ok=%v)", v, ok)
	}
	if v, ok := found["callback_url"]; !ok || v != "https://hooks.internal.com/callback" {
		t.Errorf("expected callback_url, got %q (ok=%v)", v, ok)
	}
}

func TestScanShellEnvURLs(t *testing.T) {
	dir := t.TempDir()

	createTempFile(t, dir, "setup.sh", `#!/bin/bash
export DB_URL="https://db.example.com/api/sync"
APP_NAME="my-service"
CALLBACK_URL=https://hooks.example.com/notify
`)

	bindings := ScanProjectEnvURLs(dir)

	found := map[string]string{}
	for _, b := range bindings {
		found[b.Key] = b.Value
	}

	if v, ok := found["DB_URL"]; !ok || v != "https://db.example.com/api/sync" {
		t.Errorf("expected DB_URL=https://db.example.com/api/sync, got %q", v)
	}
	if v, ok := found["CALLBACK_URL"]; !ok || v != "https://hooks.example.com/notify" {
		t.Errorf("expected CALLBACK_URL=https://hooks.example.com/notify, got %q", v)
	}
	if _, ok := found["APP_NAME"]; ok {
		t.Error("APP_NAME=my-service should not be extracted (not a URL)")
	}
}

func TestScanEnvFileURLs(t *testing.T) {
	dir := t.TempDir()

	createTempFile(t, dir, ".env", `
API_URL=https://api.example.com/v1
DEBUG=true
SERVICE_URL=https://service.example.com/api
`)

	bindings := ScanProjectEnvURLs(dir)

	found := map[string]string{}
	for _, b := range bindings {
		found[b.Key] = b.Value
	}

	if v, ok := found["API_URL"]; !ok || v != "https://api.example.com/v1" {
		t.Errorf("expected API_URL=https://api.example.com/v1, got %q", v)
	}
	if v, ok := found["SERVICE_URL"]; !ok || v != "https://service.example.com/api" {
		t.Errorf("expected SERVICE_URL=https://service.example.com/api, got %q", v)
	}
	if _, ok := found["DEBUG"]; ok {
		t.Error("DEBUG=true should not be extracted (not a URL)")
	}
}

func TestScanTerraformURLs(t *testing.T) {
	dir := t.TempDir()

	createTempFile(t, dir, "variables.tf", `
variable "webhook_url" {
  description = "Webhook endpoint"
  default     = "https://api.example.com/webhook"
}

variable "region" {
  default = "us-east-1"
}

resource "aws_lambda_function" "handler" {
  environment {
    variables = {
      API_ENDPOINT = "https://api.example.com/v2"
    }
  }
}
`)

	bindings := ScanProjectEnvURLs(dir)

	if len(bindings) == 0 {
		t.Fatal("expected at least 1 binding from terraform file")
	}

	foundURL := false
	for _, b := range bindings {
		if b.Value == "https://api.example.com/webhook" {
			foundURL = true
		}
	}
	if !foundURL {
		t.Error("expected to find https://api.example.com/webhook in terraform bindings")
	}
}

func TestScanTomlURLs(t *testing.T) {
	dir := t.TempDir()

	createTempFile(t, dir, "config.toml", `
[service]
base_url = "https://api.example.com"
name = "my-service"
callback_url = "https://hooks.example.com/notify"
`)

	bindings := ScanProjectEnvURLs(dir)

	found := map[string]string{}
	for _, b := range bindings {
		found[b.Key] = b.Value
	}

	if v, ok := found["base_url"]; !ok || v != "https://api.example.com" {
		t.Errorf("expected base_url=https://api.example.com, got %q", v)
	}
	if v, ok := found["callback_url"]; !ok || v != "https://hooks.example.com/notify" {
		t.Errorf("expected callback_url=https://hooks.example.com/notify, got %q", v)
	}
	if _, ok := found["name"]; ok {
		t.Error("name=my-service should not be extracted (not a URL)")
	}
}

func TestScanPropertiesURLs(t *testing.T) {
	dir := t.TempDir()

	createTempFile(t, dir, "app.properties", `
api.url=https://api.example.com/health
app.name=myapp
service.endpoint=https://service.example.com/api
`)

	bindings := ScanProjectEnvURLs(dir)

	found := map[string]string{}
	for _, b := range bindings {
		// Properties patterns use dot-separated keys; the regex captures the last key segment
		found[b.Key] = b.Value
	}

	foundHealthURL := false
	foundServiceURL := false
	for _, b := range bindings {
		if b.Value == "https://api.example.com/health" {
			foundHealthURL = true
		}
		if b.Value == "https://service.example.com/api" {
			foundServiceURL = true
		}
	}

	if !foundHealthURL {
		t.Error("expected to find https://api.example.com/health")
	}
	if !foundServiceURL {
		t.Error("expected to find https://service.example.com/api")
	}
}

func TestSecretKeyExclusion(t *testing.T) {
	dir := t.TempDir()

	createTempFile(t, dir, "Dockerfile", `FROM node:18
ENV SECRET_TOKEN=https://api.example.com/api
ENV API_KEY=https://api.example.com/v1
ENV PASSWORD=https://auth.example.com/login
ENV NORMAL_URL=https://api.example.com/orders
`)

	bindings := ScanProjectEnvURLs(dir)

	for _, b := range bindings {
		if b.Key == "SECRET_TOKEN" || b.Key == "API_KEY" || b.Key == "PASSWORD" {
			t.Errorf("secret key %q should have been excluded", b.Key)
		}
	}

	found := false
	for _, b := range bindings {
		if b.Key == "NORMAL_URL" {
			found = true
		}
	}
	if !found {
		t.Error("NORMAL_URL should not be excluded")
	}
}

func TestSecretValueExclusion(t *testing.T) {
	dir := t.TempDir()

	createTempFile(t, dir, "deploy.sh", `#!/bin/bash
export GH_URL="https://ghp_abcdefghijklmnopqrstuvwxyz1234567890@github.com/repo"
export NORMAL_ENDPOINT="https://api.example.com/orders"
`)

	bindings := ScanProjectEnvURLs(dir)

	for _, b := range bindings {
		if b.Key == "GH_URL" {
			t.Error("value containing ghp_ token should have been excluded")
		}
	}

	found := false
	for _, b := range bindings {
		if b.Key == "NORMAL_ENDPOINT" {
			found = true
		}
	}
	if !found {
		t.Error("NORMAL_ENDPOINT should not be excluded")
	}
}

func TestSecretFileExclusion(t *testing.T) {
	dir := t.TempDir()

	// Create a file that matches secret file patterns
	createTempFile(t, dir, "credentials.sh", `#!/bin/bash
export API_URL="https://api.example.com/v1"
`)

	// Also create a normal file for comparison
	createTempFile(t, dir, "setup.sh", `#!/bin/bash
export API_URL="https://api.example.com/v1"
`)

	bindings := ScanProjectEnvURLs(dir)

	for _, b := range bindings {
		if b.FilePath == "credentials.sh" {
			t.Error("files matching secret patterns (credentials.sh) should be skipped")
		}
	}

	found := false
	for _, b := range bindings {
		if b.FilePath == "setup.sh" {
			found = true
		}
	}
	if !found {
		t.Error("setup.sh should be scanned normally")
	}
}

func TestSkipsIgnoredDirs(t *testing.T) {
	dir := t.TempDir()

	// File inside .git directory should be skipped
	createTempFile(t, dir, ".git/config.sh", `#!/bin/bash
export API_URL="https://api.example.com/v1"
`)

	// File inside node_modules should be skipped
	createTempFile(t, dir, "node_modules/pkg/config.sh", `#!/bin/bash
export API_URL="https://api.example.com/v1"
`)

	// File at root level should be scanned
	createTempFile(t, dir, "deploy.sh", `#!/bin/bash
export API_URL="https://api.example.com/v1"
`)

	bindings := ScanProjectEnvURLs(dir)

	for _, b := range bindings {
		if filepath.Dir(b.FilePath) == ".git" || filepath.Dir(b.FilePath) == "node_modules/pkg" {
			t.Errorf("file in ignored directory should be skipped: %s", b.FilePath)
		}
	}

	found := false
	for _, b := range bindings {
		if b.FilePath == "deploy.sh" {
			found = true
		}
	}
	if !found {
		t.Error("deploy.sh at root level should be scanned")
	}
}

func TestNonURLValuesSkipped(t *testing.T) {
	dir := t.TempDir()

	createTempFile(t, dir, "Dockerfile", `FROM python:3.9
ENV APP_NAME=my-service
ENV PORT=8080
ENV DEBUG=true
ENV LOG_LEVEL=info
`)

	createTempFile(t, dir, "config.sh", `#!/bin/bash
export REGION="us-east-1"
export COUNT=42
`)

	bindings := ScanProjectEnvURLs(dir)

	if len(bindings) != 0 {
		t.Errorf("expected 0 bindings for non-URL values, got %d", len(bindings))
		for _, b := range bindings {
			t.Logf("  unexpected: %s=%s (%s)", b.Key, b.Value, b.FilePath)
		}
	}
}
