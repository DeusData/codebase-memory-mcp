package enricher

import (
	_ "embed"
	"fmt"

	"gopkg.in/yaml.v3"
)

// defaultProductMapYAML ships with the binary. Single source of truth —
// changing data/product_map.yaml and rebuilding is the only supported way
// to refresh the map. No runtime reload, no filesystem dependency in prod.
//
//go:embed data/product_map.yaml
var defaultProductMapYAML []byte

// LoadDefaultProductMap returns the product map embedded into the binary at
// build time. Parsed once per process — callers should treat the returned
// pointer as read-only. Returns a descriptive error only if the embedded YAML
// is malformed, which would be a build-time bug, not a runtime condition.
func LoadDefaultProductMap() (*ProductMap, error) {
	var pm ProductMap
	if err := yaml.Unmarshal(defaultProductMapYAML, &pm); err != nil {
		return nil, fmt.Errorf("enricher: parse embedded product_map.yaml: %w", err)
	}
	return &pm, nil
}
