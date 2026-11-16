package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.yaml.in/yaml/v4"
)

// maxAliasDepth bounds how far an alias is followed while looking for the
// files a document names.
const maxAliasDepth = 16

// FileSetting is one file a setting names, or one setting whose files could
// not be told.
type FileSetting struct {
	// Setting is the dotted key, Env its variable.
	Setting, Env string
	// Path is the file as the setting names it; empty when Unresolved says
	// it could not be told.
	Path string
	// EngineOnly says only `ork` reads the setting: a file the Orker should
	// not be able to open at all.
	EngineOnly bool
	// Unresolved says why the files of this setting could not all be told
	// (a value that does not parse, a shape the setting does not take, a
	// relative path whose directory is the other process's). The guard
	// fails closed on it: the Orker reports it, and refuses to start with
	// orker.refuse_readable_engine_keys.
	Unresolved string
}

func fileSetting(v Var, path, unresolved string) FileSetting {
	return FileSetting{Setting: v.Path, Env: v.Env, Path: path, EngineOnly: !v.usesBinary(BinaryOrker), Unresolved: unresolved}
}

// pathSettings turns the paths a setting names into FileSettings: a
// relative one is kept, and said unresolved, since it is relative to the
// working directory of the process that reads it.
func pathSettings(v Var, paths []string) []FileSetting {
	var out []FileSetting
	for _, p := range paths {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		reason := ""
		if !filepath.IsAbs(p) {
			reason = "a relative path, resolved against the working directory of the process that reads it, which the Orker cannot know"
		}
		out = append(out, fileSetting(v, p, reason))
	}
	return out
}

// FileSettings answers every file the settings marked File name in the
// environment getenv reads, whichever binary reads them.
func FileSettings(getenv func(string) string) []FileSetting {
	var out []FileSetting
	for _, v := range Schema {
		if !v.File {
			continue
		}
		raw := getenv(v.Env)
		if strings.TrimSpace(raw) == "" {
			continue
		}
		switch v.Kind {
		case KindList:
			out = append(out, pathSettings(v, strings.Split(raw, ","))...)
		case KindDocument:
			var node yaml.Node
			if err := yaml.Unmarshal([]byte(raw), &node); err != nil {
				out = append(out, fileSetting(v, "", fmt.Sprintf("%s does not parse: %v", v.Env, err)))
				continue
			}
			paths, odd := documentFiles(&node, v)
			out = append(out, pathSettings(v, paths)...)
			if odd != "" {
				out = append(out, fileSetting(v, "", odd))
			}
		default:
			out = append(out, pathSettings(v, []string{raw})...)
		}
	}
	return out
}

// foreignFiles answers the files a subtree of settings the binary does not
// read names, in every form the loader accepts for them: a value, a list,
// {ork: …} (the engine's own of {ork: …, orker: …}), {file: PATH} (PATH
// itself and the paths it holds), and a document's FileField. A form it
// cannot resolve is answered Unresolved, never dropped.
func foreignFiles(node *yaml.Node, snode *schemaNode, dir string) []FileSetting {
	node = resolveAlias(node, 0)
	if snode.v == nil {
		if node == nil || node.Kind != yaml.MappingNode {
			return nil
		}
		var out []FileSetting
		for i := 0; i+1 < len(node.Content); i += 2 {
			if child, ok := snode.children[node.Content[i].Value]; ok {
				out = append(out, foreignFiles(node.Content[i+1], child, dir)...)
			}
		}
		return out
	}
	v := *snode.v
	if !v.File || node == nil {
		return nil
	}
	if node.Kind == yaml.ScalarNode && node.Tag == "!!null" {
		return nil
	}
	if v.Kind == KindDocument {
		paths, odd := documentFiles(node, v)
		out := pathSettings(v, paths)
		if odd != "" {
			out = append(out, fileSetting(v, "", odd))
		}
		return out
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if v.Kind == KindList {
			return pathSettings(v, strings.Split(node.Value, ","))
		}
		return pathSettings(v, []string{node.Value})
	case yaml.SequenceNode:
		var paths []string
		for _, item := range node.Content {
			item = resolveAlias(item, 0)
			if item == nil || item.Kind != yaml.ScalarNode {
				return append(pathSettings(v, paths), fileSetting(v, "", "a list item that is not a path"))
			}
			paths = append(paths, item.Value)
		}
		return pathSettings(v, paths)
	case yaml.MappingNode:
		byKey := map[string]*yaml.Node{}
		for i := 0; i+1 < len(node.Content); i += 2 {
			byKey[node.Content[i].Value] = node.Content[i+1]
		}
		if f, ok := byKey["file"]; ok && len(byKey) == 1 {
			f = resolveAlias(f, 0)
			if f == nil || f.Kind != yaml.ScalarNode {
				return []FileSetting{fileSetting(v, "", "{file: …} that names no path")}
			}
			holder := resolveFilePath(dir, f.Value)
			out := []FileSetting{fileSetting(v, holder, "")}
			raw, err := os.ReadFile(holder)
			if err != nil {
				return append(out, fileSetting(v, "", fmt.Sprintf("the file %s holds its value and could not be read: %v", holder, err)))
			}
			value := strings.TrimRight(string(raw), "\r\n")
			if v.Kind == KindList {
				return append(out, pathSettings(v, strings.Split(value, ","))...)
			}
			return append(out, pathSettings(v, []string{value})...)
		}
		if ork, ok := byKey[string(BinaryOrk)]; ok {
			allBinaries := true
			for k := range byKey {
				if k != string(BinaryOrk) && k != string(BinaryOrker) {
					allBinaries = false
				}
			}
			if allBinaries {
				return foreignFiles(ork, snode, dir)
			}
		}
		if _, ok := byKey[string(BinaryOrker)]; ok && len(byKey) == 1 {
			return nil // the engine reads nothing of it
		}
	}
	return []FileSetting{fileSetting(v, "", fmt.Sprintf("a value of a shape %s does not take", v.Path))}
}

// resolveAlias follows an alias to its node, at most maxAliasDepth deep.
func resolveAlias(n *yaml.Node, depth int) *yaml.Node {
	for n != nil && n.Kind == yaml.AliasNode {
		if depth >= maxAliasDepth {
			return nil
		}
		n, depth = n.Alias, depth+1
	}
	return n
}

// fieldValues are the values of every mapping key named field anywhere in
// node, aliases and merge keys followed; odd says why one was not a path.
// documentFiles answers the files a document names: its FileField's
// values, and its FileSourceField's that are file:/PATH sources.
func documentFiles(node *yaml.Node, v Var) (paths []string, odd string) {
	paths, odd = fieldValues(node, v.FileField)
	if v.FileSourceField == "" {
		return paths, odd
	}
	sources, sourceOdd := fieldValues(node, v.FileSourceField)
	for _, s := range sources {
		if p, ok := strings.CutPrefix(s, "file:"); ok {
			paths = append(paths, p)
		}
	}
	if odd == "" {
		odd = sourceOdd
	}
	return paths, odd
}

func fieldValues(node *yaml.Node, field string) (paths []string, odd string) {
	seen := map[*yaml.Node]bool{}
	var walk func(*yaml.Node, int)
	walk = func(n *yaml.Node, depth int) {
		n = resolveAlias(n, depth)
		if n == nil {
			odd = "an alias too deep to follow"
			return
		}
		if seen[n] || depth > maxAliasDepth*4 {
			return
		}
		seen[n] = true
		if n.Kind == yaml.MappingNode {
			for i := 0; i+1 < len(n.Content); i += 2 {
				if n.Content[i].Value != field {
					continue
				}
				value := resolveAlias(n.Content[i+1], 0)
				if value == nil || value.Kind != yaml.ScalarNode {
					odd = fmt.Sprintf("a %s that is not a path", field)
					continue
				}
				paths = append(paths, value.Value)
			}
		}
		for _, c := range n.Content {
			walk(c, depth+1)
		}
	}
	walk(node, 0)
	return paths, odd
}
