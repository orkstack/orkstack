package dsl

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"gopkg.in/yaml.v3"
)

// contentHash derives a stable version for an unpinned document.
//
// The hash covers the document's semantic content, not its bytes: the YAML is
// decoded, the `version` key is removed, values are normalized to JSON-
// compatible types, and the canonical JSON encoding is hashed. Comments,
// whitespace, and mapping key order therefore do not change the version, while
// task order (which is semantic) does.
func contentHash(source []byte) (string, error) {
	var tree any
	if err := yaml.Unmarshal(source, &tree); err != nil {
		return "", fmt.Errorf("hash: parse yaml: %w", err)
	}
	normalized := normalizeValue(tree)
	if doc, ok := normalized.(map[string]any); ok {
		delete(doc, "version")
		normalized = doc
	}
	canonical, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("hash: encode: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}
