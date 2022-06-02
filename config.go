package neo4j

import (
	"strings"

	"github.com/cloudprivacylabs/lsa/pkg/ls"
)

type Config struct {
	TermMappings       map[string]string `yaml:"termMappings"`
	termExpansion      map[string]string
	NamespaceMappings  map[string]string `yaml:"namespaceMappings"`
	namespaceExpansion map[string]string
	trie               *Trie
}

type withProperty interface {
	ForEachProperty(func(string, interface{}) bool) bool
}

func InitNamespaceTrie(cfg *Config) *Trie {
	root := InitTrie()
	cfg.namespaceExpansion = make(map[string]string)
	for k, v := range cfg.NamespaceMappings {
		cfg.namespaceExpansion[v] = k
		root.Insert(k, v)
	}
	cfg.trie = root
	return root
}

func (cfg Config) MakeProperties(x withProperty, txVars map[string]interface{}) string {
	propMap := ls.PropertiesAsMap(x)
	for k, v := range propMap {
		short := cfg.Map(k)
		if short != "" {
			propMap[short] = v
		}
	}
	props := makeProperties(txVars, propMap, nil)
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

func (cfg Config) Expand(short string) string {
	if _, exists := cfg.termExpansion[short]; exists {
		return cfg.termExpansion[short]
	}
	if _, exists := cfg.namespaceExpansion[short]; exists {
		return cfg.namespaceExpansion[short]
	}
	col := strings.Index(short, ":")
	prefix := cfg.namespaceExpansion[short[:col]]
	suffix := short[col+1:]
	return prefix + suffix
}

// func (cfg Config) MapNamespaces(exact string) string {
// 	if _, exists := cfg.NamespaceMappings[exact]; exists {
// 		return cfg.NamespaceMappings[exact]
// 	}
// 	return exact
// }

// func (cfg Config) MapLongestPrefix(prefix string) string {
// 	return cfg.Trie.AllWordsFromPrefix(prefix)[0]
// }
