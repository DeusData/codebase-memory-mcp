package main

import "embed"

//go:embed assets/skills/codebase-memory-exploring/SKILL.md
var skillExploring string

//go:embed assets/skills/codebase-memory-tracing/SKILL.md
var skillTracing string

//go:embed assets/skills/codebase-memory-quality/SKILL.md
var skillQuality string

//go:embed assets/skills/codebase-memory-reference/SKILL.md
var skillReference string

//go:embed assets/codex-instructions.md
var codexInstructions string

// skillFiles maps skill directory name to embedded content.
var skillFiles = map[string]string{
	"codebase-memory-exploring": skillExploring,
	"codebase-memory-tracing":   skillTracing,
	"codebase-memory-quality":   skillQuality,
	"codebase-memory-reference": skillReference,
}

// Ensure embed import is used.
var _ embed.FS
