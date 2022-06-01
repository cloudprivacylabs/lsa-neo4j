package neo4j

import (
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/cloudprivacylabs/opencypher/graph"
)

type Config struct {
	TermMappings      map[string]string `yaml:"termMappings"`
	termExpansion     map[string]string
	NamespaceMappings map[string]string `yaml:"namespaceMappings"`
	Trie              *Trie
}

type withProperty interface {
	ForEachProperty(func(string, interface{}) bool) bool
}

func (cfg Config) MakeProperties(x withProperty, txVars map[string]interface{}) string {
	// var mapped []string
	x.ForEachProperty(func(key string, value interface{}) bool {
		switch x.(type) {
		case graph.Node:
			x.(graph.Node).SetProperty(key, cfg.Map(ls.AsPropertyValue(value, true).AsString()))
		case graph.Edge:
			// if prop, exists := x.(graph.Edge).GetProperty(key); exists {
			// mapped = append(mapped, cfg.Map(ls.AsPropertyValue(prop, true).AsString()))
			x.(graph.Edge).SetProperty(key, cfg.Map(ls.AsPropertyValue(value, true).AsString()))
			// }
		}
		return true
	})
	props := makeProperties(txVars, ls.PropertiesAsMap(x), nil)
	return props
}

func (cfg Config) MakeLabels(types []string) string {
	var mapped []string
	for _, t := range types {
		mapped = append(mapped, cfg.Map(t))
	}
	labels := makeLabels(nil, mapped)
	return labels
}

func (cfg Config) Map(fullName string) string {
	if _, exists := cfg.TermMappings[fullName]; exists {
		return cfg.TermMappings[fullName]
	}
	prefix, alias, found := cfg.Trie.Search(fullName)
	if found {
		shortName := alias + ":" + fullName[len(prefix):]
		return shortName
	}
	return fullName
}

func (cfg Config) Expand(short string) string {
	if _, exists := cfg.termExpansion[short]; exists {
		return cfg.termExpansion[short]
	}
	return short
}

func InitNamespaceTrie(cfg Config) *Trie {
	root := InitTrie()
	for k, v := range cfg.NamespaceMappings {
		root.Insert(k, v)
	}
	return root
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