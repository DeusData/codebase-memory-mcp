package pipeline

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// --- Dockerfile parser tests ---

func TestParseDockerfile(t *testing.T) {
	tests := []struct {
		name    string
		content string
		check   func(t *testing.T, props map[string]any)
	}{
		{
			name: "multi-stage with all directives",
			content: `FROM golang:1.23-alpine AS builder
WORKDIR /app
ARG SSH_PRIVATE_KEY
RUN go build -o server .

FROM alpine:3.19
WORKDIR /usr/app
ENV PORT=8080
ENV PYTHONUNBUFFERED=1
EXPOSE 8080 443
USER appuser
CMD ["./server"]
HEALTHCHECK CMD wget http://localhost:8080/health
`,
			check: func(t *testing.T, props map[string]any) {
				t.Helper()
				assertEqual(t, props["infra_type"], "dockerfile")
				assertEqual(t, props["base_image"], "alpine:3.19")
				assertSliceContains(t, props["base_images"], "golang:1.23-alpine")
				assertSliceContains(t, props["base_images"], "alpine:3.19")
				assertSliceContains(t, props["exposed_ports"], "8080")
				assertSliceContains(t, props["exposed_ports"], "443")
				assertEqual(t, props["workdir"], "/usr/app")
				assertEqual(t, props["user"], "appuser")
				assertEqual(t, props["cmd"], "./server")
				assertEqual(t, props["healthcheck"], "wget http://localhost:8080/health")

				envVars := requireMapStringString(t, props["env_vars"])
				assertEqual(t, envVars["PORT"], "8080")
				assertEqual(t, envVars["PYTHONUNBUFFERED"], "1")

				assertSliceContains(t, props["build_args"], "SSH_PRIVATE_KEY")
			},
		},
		{
			name: "single stage with entrypoint",
			content: `FROM python:3.9-slim
ENTRYPOINT ["python", "main.py"]
`,
			check: func(t *testing.T, props map[string]any) {
				t.Helper()
				assertEqual(t, props["base_image"], "python:3.9-slim")
				assertEqual(t, props["entrypoint"], "python main.py")

				stages, ok := props["stages"].([]map[string]string)
				if !ok {
					t.Fatal("stages is not []map[string]string")
				}
				if len(stages) != 1 {
					t.Fatalf("stages len: got %d, want 1", len(stages))
				}
			},
		},
		{
			name: "secret env vars filtered",
			content: `FROM node:20
ENV API_KEY=sk-1234567890abcdef12345
ENV DATABASE_URL=https://db.example.com
ENV JWT_SECRET=supersecret
`,
			check: func(t *testing.T, props map[string]any) {
				t.Helper()
				envVars := requireMapStringString(t, props["env_vars"])
				if _, ok := envVars["API_KEY"]; ok {
					t.Error("API_KEY should be filtered as secret value")
				}
				if _, ok := envVars["JWT_SECRET"]; ok {
					t.Error("JWT_SECRET should be filtered as secret key")
				}
				assertEqual(t, envVars["DATABASE_URL"], "https://db.example.com")
			},
		},
		{
			name: "expose with protocol suffix",
			content: `FROM nginx:latest
EXPOSE 80/tcp 443/tcp
`,
			check: func(t *testing.T, props map[string]any) {
				t.Helper()
				assertSliceContains(t, props["exposed_ports"], "80")
				assertSliceContains(t, props["exposed_ports"], "443")
			},
		},
		{
			name: "ENV space-separated format",
			content: `FROM python:3.9
ENV PYTHONPATH /usr/app
`,
			check: func(t *testing.T, props map[string]any) {
				t.Helper()
				envVars := requireMapStringString(t, props["env_vars"])
				assertEqual(t, envVars["PYTHONPATH"], "/usr/app")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTempFile(t, "Dockerfile", tt.content)
			result := parseDockerfile(path, "Dockerfile")
			if len(result) == 0 {
				t.Fatal("expected at least one result")
			}
			tt.check(t, result[0].properties)
		})
	}
}

func TestParseDockerfileEmpty(t *testing.T) {
	path := writeTempFile(t, "Dockerfile", "# just a comment\n")
	result := parseDockerfile(path, "Dockerfile")
	if len(result) != 0 {
		t.Errorf("expected empty result for comment-only Dockerfile, got %d", len(result))
	}
}

// --- docker-compose parser tests ---

func TestParseComposeFile(t *testing.T) {
	content := `
services:
  auth-service:
    build:
      context: ./auth-service
    ports:
      - "8080:8080"
    environment:
      PORT: "8080"
      JWT_SECRET: "sk-1234567890abcdefghijkl"
    depends_on:
      - db
    networks:
      - backend
    container_name: my-auth
    volumes:
      - ./data:/app/data

  db:
    image: postgres:15-alpine
    expose:
      - "5432"
    environment:
      - POSTGRES_DB=mydb
      - POSTGRES_PASSWORD=secret-should-filter
`

	path := writeTempFile(t, "docker-compose.yml", content)
	results := parseComposeFile(path, "docker-compose.yml")
	if len(results) != 2 {
		t.Fatalf("expected 2 services, got %d", len(results))
	}

	// Find auth-service
	var auth, db *infraFile
	for i := range results {
		sn, ok := results[i].properties["service_name"].(string)
		if !ok {
			t.Fatal("service_name is not a string")
		}
		switch sn {
		case "auth-service":
			auth = &results[i]
		case "db":
			db = &results[i]
		}
	}

	if auth == nil || db == nil {
		t.Fatal("missing expected services")
	}

	// auth-service checks
	assertEqual(t, auth.properties["infra_type"], "compose-service")
	assertEqual(t, auth.properties["build_context"], "./auth-service")
	assertSliceContains(t, auth.properties["ports"], "8080:8080")
	assertEqual(t, auth.properties["container_name"], "my-auth")

	authEnv := requireMapStringString(t, auth.properties["environment"])
	assertEqual(t, authEnv["PORT"], "8080")
	if _, ok := authEnv["JWT_SECRET"]; ok {
		t.Error("JWT_SECRET should be filtered")
	}

	assertSliceContains(t, auth.properties["depends_on"], "db")
	assertSliceContains(t, auth.properties["networks"], "backend")

	// db checks
	assertEqual(t, db.properties["image"], "postgres:15-alpine")
	assertSliceContains(t, db.properties["expose"], "5432")
}

func TestParseComposeMapDependsOn(t *testing.T) {
	content := `
services:
  web:
    image: nginx:latest
    depends_on:
      api:
        condition: service_healthy
      cache:
        condition: service_started
`
	path := writeTempFile(t, "docker-compose.yml", content)
	results := parseComposeFile(path, "docker-compose.yml")
	if len(results) != 1 {
		t.Fatalf("expected 1 service, got %d", len(results))
	}
	deps, ok := results[0].properties["depends_on"].([]string)
	if !ok {
		t.Fatal("depends_on is not []string")
	}
	if len(deps) != 2 {
		t.Fatalf("expected 2 deps, got %d", len(deps))
	}
}

func TestParseComposeStringBuild(t *testing.T) {
	content := `
services:
  app:
    build: ./myapp
`
	path := writeTempFile(t, "docker-compose.yml", content)
	results := parseComposeFile(path, "docker-compose.yml")
	if len(results) != 1 {
		t.Fatalf("expected 1 service, got %d", len(results))
	}
	assertEqual(t, results[0].properties["build_context"], "./myapp")
}

// --- cloudbuild parser tests ---

func TestParseCloudbuildFile(t *testing.T) {
	content := `
substitutions:
  _SERVICE_NAME: order-processor
  _REGION: europe-west3

steps:
  - name: gcr.io/cloud-builders/docker
    args: ['build', '-t', 'eu.gcr.io/$PROJECT_ID/$_SERVICE_NAME', '.']

  - name: gcr.io/google.com/cloudsdktool/cloud-sdk
    args:
      - gcloud
      - run
      - deploy
      - $_SERVICE_NAME
      - --image=eu.gcr.io/$PROJECT_ID/$_SERVICE_NAME
      - --region=$_REGION
      - --cpu=2
      - --memory=4Gi
      - --concurrency=1
      - --max-instances=1
      - --timeout=3600s
      - --ingress=internal
      - --set-env-vars=SOURCE_ENTITY=$_SERVICE_NAME,LOG_LEVEL=INFO

images:
  - eu.gcr.io/$PROJECT_ID/$_SERVICE_NAME
`

	path := writeTempFile(t, "cloudbuild.yaml", content)
	results := parseCloudbuildFile(path, "cloudbuild.yaml")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	props := results[0].properties
	assertEqual(t, props["infra_type"], "cloudbuild")
	assertEqual(t, props["service_name"], "order-processor")
	assertEqual(t, props["deploy_region"], "$_REGION")
	assertEqual(t, props["deploy_cpu"], "2")
	assertEqual(t, props["deploy_memory"], "4Gi")
	assertEqual(t, props["deploy_concurrency"], "1")
	assertEqual(t, props["deploy_max_instances"], "1")
	assertEqual(t, props["deploy_timeout"], "3600s")
	assertEqual(t, props["deploy_ingress"], "internal")
	assertEqual(t, props["image_registry"], "eu.gcr.io/$PROJECT_ID/$_SERVICE_NAME")

	envVars := requireMapStringString(t, props["deploy_env_vars"])
	assertEqual(t, envVars["SOURCE_ENTITY"], "$_SERVICE_NAME")
	assertEqual(t, envVars["LOG_LEVEL"], "INFO")
}

func TestParseCloudbuildSeparateFlags(t *testing.T) {
	content := `
steps:
  - name: gcr.io/google.com/cloudsdktool/cloud-sdk
    args:
      - gcloud
      - run
      - deploy
      - my-service
      - --region
      - us-central1
      - --memory
      - 1Gi
`
	path := writeTempFile(t, "cloudbuild.yaml", content)
	results := parseCloudbuildFile(path, "cloudbuild.yaml")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	props := results[0].properties
	assertEqual(t, props["deploy_region"], "us-central1")
	assertEqual(t, props["deploy_memory"], "1Gi")
}

// --- .env file parser tests ---

func TestParseDotenvFile(t *testing.T) {
	content := `# Database config
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_NAME=mydb
API_SECRET=should-not-appear
PLAIN_VALUE=hello world
`
	path := writeTempFile(t, ".env", content)
	results := parseDotenvFile(path, ".env")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	envVars := requireMapStringString(t, results[0].properties["env_vars"])
	assertEqual(t, envVars["DATABASE_HOST"], "localhost")
	assertEqual(t, envVars["DATABASE_PORT"], "5432")
	assertEqual(t, envVars["PLAIN_VALUE"], "hello world")

	if _, ok := envVars["API_SECRET"]; ok {
		t.Error("API_SECRET should be filtered")
	}
}

func TestParseDotenvQuotedValues(t *testing.T) {
	content := `KEY1="quoted value"
KEY2='single quoted'
`
	path := writeTempFile(t, ".env", content)
	results := parseDotenvFile(path, ".env")
	if len(results) != 1 {
		t.Fatal("expected 1 result")
	}
	envVars := requireMapStringString(t, results[0].properties["env_vars"])
	assertEqual(t, envVars["KEY1"], "quoted value")
	assertEqual(t, envVars["KEY2"], "single quoted")
}

// --- File identification tests ---

func TestIsComposeFile(t *testing.T) {
	tests := []struct {
		name   string
		expect bool
	}{
		{"docker-compose.yml", true},
		{"docker-compose.yaml", true},
		{"docker-compose.prod.yml", true},
		{"compose.yml", true},
		{"compose.yaml", true},
		{"mycompose.yml", false},
		{"docker-compose.txt", false},
		{"Dockerfile", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isComposeFile(tt.name); got != tt.expect {
				t.Errorf("isComposeFile(%q) = %v, want %v", tt.name, got, tt.expect)
			}
		})
	}
}

func TestIsCloudbuildFile(t *testing.T) {
	tests := []struct {
		name   string
		expect bool
	}{
		{"cloudbuild.yaml", true},
		{"cloudbuild.yml", true},
		{"cloudbuild-prod.yaml", true},
		{"Cloudbuild.yml", true},
		{"build.yaml", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isCloudbuildFile(tt.name); got != tt.expect {
				t.Errorf("isCloudbuildFile(%q) = %v, want %v", tt.name, got, tt.expect)
			}
		})
	}
}

// --- Terraform parser tests ---

func TestParseTerraformFile(t *testing.T) {
	content := `
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.35.0"
    }
  }
  backend "gcs" {
    bucket = "hoepke-tf"
    prefix = "state"
  }
}

variable "project_id" {
  description = "The GCP project ID"
  type        = string
  default     = "hoepke-cloud"
}

variable "region" {
  description = "The region"
  type        = string
}

resource "google_cloud_run_service" "main" {
  name     = "my-service"
  location = var.region
}

resource "google_compute_address" "nat_ip" {
  name   = "nat-ip"
  region = var.region
}

output "service_url" {
  value = google_cloud_run_service.main.status[0].url
}

data "google_project" "project" {
}

module "vpc" {
  source = "./modules/vpc"
}

locals {
  env = "prod"
}
`
	path := writeTempFile(t, "main.tf", content)
	results := parseTerraformFile(path, "opentofu/main.tf")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	props := results[0].properties
	assertEqual(t, props["infra_type"], "terraform")
	assertEqual(t, props["backend"], "gcs")

	t.Run("resources", func(t *testing.T) { verifyTFResources(t, props) })
	t.Run("variables", func(t *testing.T) { verifyTFVariables(t, props) })
	t.Run("other_blocks", func(t *testing.T) { verifyTFOtherBlocks(t, props) })
}

func verifyTFResources(t *testing.T, props map[string]any) {
	t.Helper()
	resources, ok := props["resources"].([]map[string]string)
	if !ok {
		t.Fatal("resources is not []map[string]string")
	}
	if len(resources) != 2 {
		t.Fatalf("expected 2 resources, got %d", len(resources))
	}
	foundCloudRun := false
	for _, r := range resources {
		if r["type"] == "google_cloud_run_service" && r["name"] == "main" {
			foundCloudRun = true
		}
	}
	if !foundCloudRun {
		t.Error("missing google_cloud_run_service.main resource")
	}
}

func verifyTFVariables(t *testing.T, props map[string]any) {
	t.Helper()
	variables, ok := props["variables"].([]map[string]string)
	if !ok {
		t.Fatal("variables is not []map[string]string")
	}
	if len(variables) != 2 {
		t.Fatalf("expected 2 variables, got %d", len(variables))
	}
	for _, v := range variables {
		if v["name"] == "project_id" {
			assertEqual(t, v["default"], "hoepke-cloud")
			assertEqual(t, v["type"], "string")
			assertEqual(t, v["description"], "The GCP project ID")
			return
		}
	}
	t.Error("missing project_id variable")
}

func verifyTFOtherBlocks(t *testing.T, props map[string]any) {
	t.Helper()
	assertSliceContains(t, props["outputs"], "service_url")

	dataSources, ok := props["data_sources"].([]map[string]string)
	if !ok {
		t.Fatal("data_sources is not []map[string]string")
	}
	if len(dataSources) != 1 {
		t.Fatalf("expected 1 data source, got %d", len(dataSources))
	}

	modules, ok := props["modules"].([]map[string]string)
	if !ok {
		t.Fatal("modules is not []map[string]string")
	}
	if len(modules) != 1 {
		t.Fatalf("expected 1 module, got %d", len(modules))
	}
	assertEqual(t, modules[0]["name"], "vpc")
	assertEqual(t, modules[0]["source"], "./modules/vpc")

	if props["has_locals"] != true {
		t.Error("expected has_locals=true")
	}
}

func TestParseTerraformVariablesOnly(t *testing.T) {
	content := `
variable "project_id" {
  description = "The GCP project ID"
  type        = string
  default     = "hoepke-cloud"
}

variable "secret_key" {
  description = "A secret"
  type        = string
  default     = "sk-1234567890abcdef12345"
}
`
	path := writeTempFile(t, "variables.tf", content)
	results := parseTerraformFile(path, "opentofu/variables.tf")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	variables, ok := results[0].properties["variables"].([]map[string]string)
	if !ok {
		t.Fatal("variables is not []map[string]string")
	}

	// Secret default should be filtered
	for _, v := range variables {
		if v["name"] == "secret_key" {
			if _, hasDefault := v["default"]; hasDefault {
				t.Error("secret_key default should be filtered")
			}
		}
	}
}

func TestParseTerraformEmpty(t *testing.T) {
	path := writeTempFile(t, "empty.tf", "# just comments\n")
	results := parseTerraformFile(path, "empty.tf")
	if len(results) != 0 {
		t.Errorf("expected empty result for comment-only .tf, got %d", len(results))
	}
}

// --- Shell script parser tests ---

func TestParseShellScript(t *testing.T) {
	content := `#!/bin/bash
set -e

# Configuration
YOUR_CONTAINER_NAME="order-email-extractor-endpoint"
DOCKERFILE_PATH="/path/to/dockerfile"

export ENVIRONMENT="development"
export USE_STACKDRIVER="false"

# Shut down existing containers
./shut-down-docker-container.sh

docker build -t "$YOUR_CONTAINER_NAME" "$DOCKERFILE_PATH"
docker run -d --name "$YOUR_CONTAINER_NAME" "$YOUR_CONTAINER_NAME"
docker-compose up -d
`
	path := writeTempFile(t, "run.sh", content)
	results := parseShellScript(path, "scripts/run.sh")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	props := results[0].properties
	assertEqual(t, props["infra_type"], "shell")
	assertEqual(t, props["shebang"], "/bin/bash")

	envVars := requireMapStringString(t, props["env_vars"])
	assertEqual(t, envVars["ENVIRONMENT"], "development")
	assertEqual(t, envVars["YOUR_CONTAINER_NAME"], "order-email-extractor-endpoint")

	assertSliceContains(t, props["docker_commands"], "docker build")
	assertSliceContains(t, props["docker_commands"], "docker run")
	assertSliceContains(t, props["docker_commands"], "docker-compose up")
}

func TestParseShellScriptWithSource(t *testing.T) {
	content := `#!/usr/bin/env bash
source ./config.sh
. /etc/profile.d/env.sh
`
	path := writeTempFile(t, "init.sh", content)
	results := parseShellScript(path, "init.sh")
	if len(results) != 1 {
		t.Fatal("expected 1 result")
	}

	props := results[0].properties
	assertEqual(t, props["shebang"], "/usr/bin/env bash")
	assertSliceContains(t, props["sources"], "./config.sh")
	assertSliceContains(t, props["sources"], "/etc/profile.d/env.sh")
}

func TestParseShellScriptSecretFiltered(t *testing.T) {
	content := `#!/bin/bash
export API_SECRET="should-not-appear"
export DATABASE_URL="https://db.example.com"
`
	path := writeTempFile(t, "env.sh", content)
	results := parseShellScript(path, "env.sh")
	if len(results) != 1 {
		t.Fatal("expected 1 result")
	}

	envVars := requireMapStringString(t, results[0].properties["env_vars"])
	if _, ok := envVars["API_SECRET"]; ok {
		t.Error("API_SECRET should be filtered")
	}
	assertEqual(t, envVars["DATABASE_URL"], "https://db.example.com")
}

func TestParseShellScriptShebanOnly(t *testing.T) {
	// A script with only shebang still produces a result (shebang is useful metadata)
	path := writeTempFile(t, "empty.sh", "#!/bin/bash\n# just comments\n")
	results := parseShellScript(path, "empty.sh")
	if len(results) != 1 {
		t.Fatalf("expected 1 result for shebang-only .sh, got %d", len(results))
	}
	assertEqual(t, results[0].properties["shebang"], "/bin/bash")
}

func TestParseShellScriptTrulyEmpty(t *testing.T) {
	path := writeTempFile(t, "empty.sh", "# no shebang, just comments\n")
	results := parseShellScript(path, "empty.sh")
	if len(results) != 0 {
		t.Errorf("expected empty result for no-shebang .sh, got %d", len(results))
	}
}

func TestIsShellScript(t *testing.T) {
	tests := []struct {
		name   string
		ext    string
		expect bool
	}{
		{"run.sh", ".sh", true},
		{"deploy.bash", ".bash", true},
		{"init.zsh", ".zsh", true},
		{"main.py", ".py", false},
		{"Dockerfile", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isShellScript(tt.name, tt.ext); got != tt.expect {
				t.Errorf("isShellScript(%q, %q) = %v, want %v", tt.name, tt.ext, got, tt.expect)
			}
		})
	}
}

// --- Integration test: passInfraFiles creates nodes ---

func TestPassInfraFilesIntegration(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Create a temp repo with infra files
	tmpDir := t.TempDir()
	writeFile(t, filepath.Join(tmpDir, "Dockerfile"), `FROM alpine:3.19
EXPOSE 8080
`)
	writeFile(t, filepath.Join(tmpDir, ".env"), `APP_PORT=8080
`)
	writeFile(t, filepath.Join(tmpDir, "docker-compose.yml"), `
services:
  web:
    image: nginx:latest
    ports:
      - "80:80"
`)

	p := New(s, tmpDir)
	err = s.WithTransaction(func(txStore *store.Store) error {
		p.Store = txStore
		_ = txStore.UpsertProject(p.ProjectName, tmpDir)
		p.passInfraFiles()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify InfraFile nodes were created
	infraNodes, err := s.FindNodesByLabel(p.ProjectName, "InfraFile")
	if err != nil {
		t.Fatal(err)
	}
	if len(infraNodes) < 3 {
		t.Errorf("expected at least 3 InfraFile nodes, got %d", len(infraNodes))
	}

	// Verify File nodes were also created (for search_code)
	fileNodes, err := s.FindNodesByLabel(p.ProjectName, "File")
	if err != nil {
		t.Fatal(err)
	}
	if len(fileNodes) < 3 {
		t.Errorf("expected at least 3 File nodes, got %d", len(fileNodes))
	}

	// Verify no Module nodes were created (InfraFile, not Module)
	moduleNodes, err := s.FindNodesByLabel(p.ProjectName, "Module")
	if err != nil {
		t.Fatal(err)
	}
	if len(moduleNodes) != 0 {
		t.Errorf("expected 0 Module nodes from infra scan, got %d", len(moduleNodes))
	}

	// Check InfraFile properties
	for _, n := range infraNodes {
		infraType, ok := n.Properties["infra_type"].(string)
		if !ok {
			t.Errorf("node %s missing infra_type", n.QualifiedName)
			continue
		}
		switch infraType {
		case "dockerfile":
			assertEqual(t, n.Properties["base_image"], "alpine:3.19")
		case "env":
			envVars, eOk := n.Properties["env_vars"].(map[string]any)
			if !eOk {
				t.Fatal("env_vars is not map[string]any")
			}
			assertEqual(t, envVars["APP_PORT"], "8080")
		case "compose-service":
			assertEqual(t, n.Properties["service_name"], "web")
		}
	}
}

// TestPassInfraFilesIdempotent verifies that re-running passInfraFiles
// does not create duplicate nodes (upsert behavior).
func TestPassInfraFilesIdempotent(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	tmpDir := t.TempDir()
	writeFile(t, filepath.Join(tmpDir, "Dockerfile"), `FROM alpine:3.19
EXPOSE 8080
`)

	p := New(s, tmpDir)
	err = s.WithTransaction(func(txStore *store.Store) error {
		p.Store = txStore
		_ = txStore.UpsertProject(p.ProjectName, tmpDir)
		p.passInfraFiles()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	countBefore, _ := s.CountNodes(p.ProjectName)

	// Run again — should delete and recreate (same count)
	err = s.WithTransaction(func(txStore *store.Store) error {
		p.Store = txStore
		_ = txStore.DeleteNodesByLabel(p.ProjectName, "InfraFile")
		p.passInfraFiles()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	countAfter, _ := s.CountNodes(p.ProjectName)
	if countBefore != countAfter {
		t.Errorf("node count changed: before=%d, after=%d", countBefore, countAfter)
	}
}

// --- cleanJSONBrackets tests ---

func TestCleanJSONBrackets(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`["./server"]`, `./server`},
		{`["python", "main.py"]`, `python main.py`},
		{`./server`, `./server`},
		{`["./app", "--flag", "value"]`, `./app --flag value`},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := cleanJSONBrackets(tt.input)
			if got != tt.want {
				t.Errorf("cleanJSONBrackets(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// --- infraQN tests ---

func TestInfraQN(t *testing.T) {
	p := &Pipeline{ProjectName: "myproject"}

	// Regular infra file
	qn := p.infraQN("docker-images/service/Dockerfile", map[string]any{"infra_type": "dockerfile"})
	if !strings.Contains(qn, ".__infra__") {
		t.Errorf("expected __infra__ suffix, got %s", qn)
	}

	// Compose service gets per-service suffix
	qn = p.infraQN("docker-compose.yml", map[string]any{
		"infra_type":   "compose-service",
		"service_name": "web",
	})
	if !strings.Contains(qn, "::web") {
		t.Errorf("expected ::web suffix, got %s", qn)
	}
}

// --- Bug fix regression tests ---

func TestParseComposeNonStringEnvValues(t *testing.T) {
	content := `
services:
  auth-service:
    image: auth:latest
    environment:
      PORT: 8090
      DEBUG: true
      RATE_LIMIT: 1.5
      NAME: "string-value"
`
	path := writeTempFile(t, "docker-compose.yml", content)
	results := parseComposeFile(path, "docker-compose.yml")
	if len(results) != 1 {
		t.Fatalf("expected 1 service, got %d", len(results))
	}

	envVars := requireMapStringString(t, results[0].properties["environment"])
	assertEqual(t, envVars["PORT"], "8090")
	assertEqual(t, envVars["DEBUG"], "true")
	assertEqual(t, envVars["RATE_LIMIT"], "1.5")
	assertEqual(t, envVars["NAME"], "string-value")
}

func TestParseComposeSecretKeyNotFiltered(t *testing.T) {
	// Key names like JWT_PRIVATE_KEY_ID are config references, not secrets.
	// Only actual secret VALUES should be filtered.
	content := `
services:
  auth-service:
    image: auth:latest
    environment:
      JWT_PRIVATE_KEY_ID: jwt-private-key
      GOOGLE_CLIENT_SECRET_ID: google-oauth-secret
      ACCESS_TOKEN_TTL: 15m
      ACTUAL_SECRET: "sk-1234567890abcdef12345"
`
	path := writeTempFile(t, "docker-compose.yml", content)
	results := parseComposeFile(path, "docker-compose.yml")
	if len(results) != 1 {
		t.Fatalf("expected 1 service, got %d", len(results))
	}

	envVars := requireMapStringString(t, results[0].properties["environment"])
	// These should NOT be filtered — key name matches pattern but value is a reference
	assertEqual(t, envVars["JWT_PRIVATE_KEY_ID"], "jwt-private-key")
	assertEqual(t, envVars["GOOGLE_CLIENT_SECRET_ID"], "google-oauth-secret")
	assertEqual(t, envVars["ACCESS_TOKEN_TTL"], "15m")

	// This SHOULD be filtered — value matches secret pattern (sk-...)
	if _, ok := envVars["ACTUAL_SECRET"]; ok {
		t.Error("ACTUAL_SECRET should be filtered (value matches secret pattern)")
	}
}

func TestParseCloudbuildBashScript(t *testing.T) {
	content := `
substitutions:
  _SERVICE_NAME: my-service

steps:
  - name: gcr.io/cloud-builders/docker
    args: ['build', '-t', 'eu.gcr.io/$PROJECT_ID/$_SERVICE_NAME', '.']

  - name: gcr.io/google.com/cloudsdktool/cloud-sdk
    entrypoint: bash
    args:
      - "-c"
      - |
        gcloud run deploy $_SERVICE_NAME \
          --region=europe-west3 \
          --memory=2Gi \
          --cpu=2 \
          --concurrency=80 \
          --max-instances=10 \
          --ingress=internal-and-cloud-load-balancing \
          --set-env-vars=LOG_LEVEL=INFO,SOURCE_ENTITY=$_SERVICE_NAME

images:
  - eu.gcr.io/$PROJECT_ID/$_SERVICE_NAME
`
	path := writeTempFile(t, "cloudbuild.yaml", content)
	results := parseCloudbuildFile(path, "cloudbuild.yaml")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	props := results[0].properties
	assertEqual(t, props["service_name"], "my-service")
	assertEqual(t, props["deploy_region"], "europe-west3")
	assertEqual(t, props["deploy_memory"], "2Gi")
	assertEqual(t, props["deploy_cpu"], "2")
	assertEqual(t, props["deploy_concurrency"], "80")
	assertEqual(t, props["deploy_max_instances"], "10")
	assertEqual(t, props["deploy_ingress"], "internal-and-cloud-load-balancing")

	envVars := requireMapStringString(t, props["deploy_env_vars"])
	assertEqual(t, envVars["LOG_LEVEL"], "INFO")
	assertEqual(t, envVars["SOURCE_ENTITY"], "$_SERVICE_NAME")
}

// --- helpers ---

func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// requireMapStringString asserts the value is map[string]string (from parsed props)
// and returns it. Fails the test if not.
func requireMapStringString(t *testing.T, v any) map[string]string {
	t.Helper()
	m, ok := v.(map[string]string)
	if !ok {
		t.Fatalf("expected map[string]string, got %T", v)
	}
	return m
}

func assertEqual(t *testing.T, got, want any) {
	t.Helper()
	gotStr, _ := got.(string)
	wantStr, _ := want.(string)
	if gotStr != wantStr {
		t.Errorf("got %v, want %v", got, want)
	}
}

func assertSliceContains(t *testing.T, sliceAny any, want string) {
	t.Helper()
	switch s := sliceAny.(type) {
	case []string:
		for _, v := range s {
			if v == want {
				return
			}
		}
	case []any:
		for _, v := range s {
			if v == want {
				return
			}
		}
	}
	t.Errorf("slice %v does not contain %q", sliceAny, want)
}
