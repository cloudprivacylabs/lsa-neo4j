package neo4j

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/cloudprivacylabs/opencypher/graph"
)

type Config struct {
	TermMappings         map[string]string `yaml:"termMappings"`
	NamespaceMappings    map[string]string `yaml:"namespaceMappings"`
	PropertyTypeMappings map[string]string `yaml:"propertyTypeMappings"`
	trie                 *Trie
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
	var subject withProperty
	switch x.(type) {
	case graph.Node:
		subject = x.(graph.Node)
	case graph.Edge:
		subject = x.(graph.Edge)
	}
	propMap := make(map[string]*ls.PropertyValue)
	for k, v := range ls.PropertiesAsMap(x) {
		short := cfg.Map(k)
		if short != "" {
			propMap[short] = v
		}
	}
	props := makeProperties(cfg, subject, txVars, propMap, nil)
	return props
}

func (cfg Config) MakeLabels(types []string) string {
	var mapped []string
	for _, t := range types {
		short := cfg.Map(t)
		if short != "" {
			mapped = append(mapped, short)
		}
	}
	labels := makeLabels(nil, mapped)
	return labels
}

func (cfg Config) PropertyMap(tp, val string) interface{} {
	if _, exists := cfg.PropertyTypeMappings[cfg.Map(tp)]; exists {
		cnvrt := cfg.PropertyTypeMappings[cfg.Map(tp)]
		switch cnvrt {
		case "json:boolean":
			boolVal, err := strconv.ParseBool(val)
			if err != nil {
				return fmt.Errorf("Error parsing boolean value: %s, %w", val, err)
			}
			return boolVal
		case "ls:boolean":
			boolVal, err := strconv.ParseBool(val)
			if err != nil {
				return fmt.Errorf("Error parsing boolean value: %s, %w", val, err)
			}
			return boolVal
		case "json:number":
			intVal, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return fmt.Errorf("Error parsing boolean value: %s, %w", val, err)
			}
			return intVal
		}
	}
	return val
}

func (cfg Config) Map(fullName string) string {
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
