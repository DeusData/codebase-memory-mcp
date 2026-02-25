package pipeline

import (
	"bufio"
	"os"
	"regexp"
	"strings"
)

// --- Terraform parser (regex, line-oriented with brace tracking) ---

var (
	reResource  = regexp.MustCompile(`^resource\s+"(\S+)"\s+"(\S+)"`)
	reVariable  = regexp.MustCompile(`^variable\s+"(\S+)"`)
	reOutput    = regexp.MustCompile(`^output\s+"(\S+)"`)
	reProvider  = regexp.MustCompile(`^provider\s+"(\S+)"`)
	reTFModule  = regexp.MustCompile(`^module\s+"(\S+)"`)
	reDataSrc   = regexp.MustCompile(`^data\s+"(\S+)"\s+"(\S+)"`)
	reBackend   = regexp.MustCompile(`^\s*backend\s+"(\S+)"`)
	reTFDefault = regexp.MustCompile(`^\s*default\s*=\s*"?([^"\n]*)"?`)
	reTFType    = regexp.MustCompile(`^\s*type\s*=\s*(\S+)`)
	reTFSource  = regexp.MustCompile(`^\s*source\s*=\s*"([^"]*)"`)
	reTFDesc    = regexp.MustCompile(`^\s*description\s*=\s*"([^"]*)"`)
	reTFLocals  = regexp.MustCompile(`^locals\s*\{`)
)

// blockKind identifies what type of HCL block we're inside.
type blockKind int

const (
	blockNone blockKind = iota
	blockVariable
	blockModule
	blockTerraform
)

// tfState accumulates parsed Terraform file metadata.
type tfState struct {
	resources   []map[string]string // [{type, name}]
	variables   []map[string]string // [{name, type, default, description}]
	outputs     []string
	providers   []string
	modules     []map[string]string // [{name, source}]
	dataSources []map[string]string // [{type, name}]
	backend     string
	hasLocals   bool
}

func parseTerraformFile(absPath, relPath string) []infraFile {
	f, err := os.Open(absPath)
	if err != nil {
		return nil
	}
	defer f.Close()

	var st tfState
	var curBlock blockKind
	var curBlockData map[string]string
	braceDepth := 0

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "//") {
			continue
		}

		// Track brace depth
		braceDepth += strings.Count(trimmed, "{") - strings.Count(trimmed, "}")

		// At top level (depth 0 or entering depth 1), detect block headers
		if braceDepth <= 1 {
			if newBlock, data := st.matchBlockHeader(trimmed); newBlock != blockNone {
				curBlock = newBlock
				curBlockData = data
				continue
			}
		}

		// Inside a block, extract attributes.
		// Depth 1 for variable/module attrs, depth 1-2 for terraform (backend is nested).
		if curBlock != blockNone && braceDepth >= 1 && braceDepth <= 2 {
			st.extractBlockAttr(trimmed, curBlock, curBlockData)
		}

		// Block closed — finalize
		if braceDepth == 0 && curBlock != blockNone {
			st.finalizeBlock(curBlock, curBlockData)
			curBlock = blockNone
			curBlockData = nil
		}
	}

	props := st.toProperties()
	if len(props) <= 1 { // only infra_type, nothing useful
		return nil
	}
	return []infraFile{{relPath: relPath, infraType: "terraform", properties: props}}
}

// matchBlockHeader checks if the line starts a recognized HCL block.
// Returns the block kind and a data map to accumulate attributes into.
func (st *tfState) matchBlockHeader(line string) (kind blockKind, data map[string]string) {
	if m := reResource.FindStringSubmatch(line); m != nil {
		st.resources = append(st.resources, map[string]string{"type": m[1], "name": m[2]})
		return blockNone, nil // no attribute extraction needed
	}
	if m := reVariable.FindStringSubmatch(line); m != nil {
		return blockVariable, map[string]string{"name": m[1]}
	}
	if m := reOutput.FindStringSubmatch(line); m != nil {
		st.outputs = append(st.outputs, m[1])
		return blockNone, nil
	}
	if m := reProvider.FindStringSubmatch(line); m != nil {
		st.providers = append(st.providers, m[1])
		return blockNone, nil
	}
	if m := reTFModule.FindStringSubmatch(line); m != nil {
		return blockModule, map[string]string{"name": m[1]}
	}
	if m := reDataSrc.FindStringSubmatch(line); m != nil {
		st.dataSources = append(st.dataSources, map[string]string{"type": m[1], "name": m[2]})
		return blockNone, nil
	}
	if strings.HasPrefix(line, "terraform") && strings.Contains(line, "{") {
		return blockTerraform, nil
	}
	if reTFLocals.MatchString(line) {
		st.hasLocals = true
	}
	return blockNone, nil
}

// extractBlockAttr extracts key attributes from inside a block.
func (st *tfState) extractBlockAttr(line string, kind blockKind, data map[string]string) {
	switch kind {
	case blockVariable:
		st.extractVariableAttr(line, data)
	case blockModule:
		if m := reTFSource.FindStringSubmatch(line); m != nil {
			data["source"] = m[1]
		}
	case blockTerraform:
		if m := reBackend.FindStringSubmatch(line); m != nil {
			st.backend = m[1]
		}
	}
}

func (st *tfState) extractVariableAttr(line string, data map[string]string) {
	if m := reTFDefault.FindStringSubmatch(line); m != nil {
		val := strings.TrimSpace(m[1])
		if !isSecretBinding(data["name"], val) {
			data["default"] = val
		}
	}
	if m := reTFType.FindStringSubmatch(line); m != nil {
		data["type"] = m[1]
	}
	if m := reTFDesc.FindStringSubmatch(line); m != nil {
		data["description"] = m[1]
	}
}

// finalizeBlock saves the accumulated block data to the state.
func (st *tfState) finalizeBlock(kind blockKind, data map[string]string) {
	switch kind {
	case blockVariable:
		if data != nil {
			st.variables = append(st.variables, data)
		}
	case blockModule:
		if data != nil {
			st.modules = append(st.modules, data)
		}
	case blockTerraform:
		// backend already extracted inline
	}
}

func (st *tfState) toProperties() map[string]any {
	props := map[string]any{
		"infra_type": "terraform",
	}

	setNonEmptyMaps(props, "resources", st.resources)
	setNonEmptyMaps(props, "variables", st.variables)
	setNonEmpty(props, "outputs", st.outputs)
	setNonEmpty(props, "providers", st.providers)
	setNonEmptyMaps(props, "modules", st.modules)
	setNonEmptyMaps(props, "data_sources", st.dataSources)
	setNonEmptyStr(props, "backend", st.backend)

	if st.hasLocals {
		props["has_locals"] = true
	}

	return props
}

// setNonEmptyMaps sets a property if the slice of maps is non-empty.
func setNonEmptyMaps(m map[string]any, key string, val []map[string]string) {
	if len(val) > 0 {
		m[key] = val
	}
}
