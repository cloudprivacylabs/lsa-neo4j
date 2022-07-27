package neo4j

import (
	"fmt"
	"strings"

	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/cloudprivacylabs/opencypher/graph"
)

type Config struct {
	TermMappings      map[string]string `yaml:"termMappings"`
	NamespaceMappings map[string]string `yaml:"namespaceMappings"`
	PropertyTypes     map[string]string `yaml:"propertyTypes"`
	trie              *Trie
}

type withProperty interface {
	ForEachProperty(func(string, interface{}) bool) bool
}

type mapWithProperty map[string]interface{}

func (m mapWithProperty) ForEachProperty(f func(string, interface{}) bool) bool {
	for k, v := range m {
		if !f(k, v) {
			return false
		}
	}
	return true
}

func InitNamespaceTrie(cfg *Config) *Trie {
	root := InitTrie()
	for k, v := range cfg.NamespaceMappings {
		root.Insert(k, v)
	}
	cfg.trie = root
	return root
}

func (cfg Config) MakeProperties(x withProperty, txVars map[string]interface{}) string {
	propMap := make(map[string]*ls.PropertyValue)
	for k, v := range ls.PropertiesAsMap(x) {
		short := cfg.Shorten(k)
		if short != "" {
			propMap[short] = v
		}
	}
	props := buildDBPropertiesForSave(cfg, x, txVars, propMap, nil)
	return props
}

func (cfg Config) MakeLabels(types []string) string {
	var mapped []string
	for _, t := range types {
		short := cfg.Shorten(t)
		if short != "" {
			mapped = append(mapped, short)
		}
	}
	labels := makeLabels(nil, mapped)
	return labels
}

// GetNativePropertyValue is called during building properties for save and when the expanded property key exists in the config.
func (cfg Config) GetNativePropertyValue(node graph.Node, expandedPropertyKey, val string) interface{} {
	if _, exists := cfg.PropertyTypes[expandedPropertyKey]; exists {
		va := ls.GetValueAccessor(cfg.PropertyTypes[expandedPropertyKey])
		native, err := va.GetNativeValue(val, node)
		if err != nil {
			panic(fmt.Errorf("Cannot get native value for %v, %w", node, err))
		}
		return native
	}
	return val
}

func (cfg Config) Shorten(fullName string) string {
	if _, exists := cfg.TermMappings[fullName]; exists {
		return cfg.TermMappings[fullName]
	}
	prefix, alias, found := cfg.trie.Search(fullName)
	if found && (prefix != "" && alias != "") {
		shortName := alias + ":" + fullName[len(prefix):]
		return shortName
	}
	return fullName
}

func reverseLookup(m map[string]string, key string) (string, bool) {
	for k, v := range m {
		if key == v {
			return k, true
		}
	}
	return "", false
}

func (cfg *Config) Expand(short string) string {
	if x, exists := reverseLookup(cfg.TermMappings, short); exists {
		return x
	}
	col := strings.Index(short, ":")
	if col == -1 {
		return short
	}
	if prefix, exists := reverseLookup(cfg.NamespaceMappings, short[:col]); exists {
		suffix := short[col+1:]
		return prefix + suffix
	}
	return short
}
