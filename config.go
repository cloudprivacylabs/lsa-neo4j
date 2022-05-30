package neo4j

import (
	"github.com/cloudprivacylabs/lsa/pkg/ls"
)

type Config struct {
	TermMappings      map[string]string `yaml:"termMappings"`
	TermExpansion     map[string]string `yaml:"termExpansion"`
	NamespaceMappings map[string]string `yaml:"namespaceMappings"`
	Trie              *Trie
	txVars            map[string]interface{}
}

type withProperty interface {
	ForEachProperty(func(string, interface{}) bool) bool
}

func (cfg Config) MakeProperties(x withProperty) string {
	props := makeProperties(cfg.txVars, ls.PropertiesAsMap(x), nil)
	return props
}

func (cfg Config) MakeLabels(types []string) string {
	labels := makeLabels(nil, types)
	return labels
}

func (cfg Config) Map(fullName string) string {
	if _, exists := cfg.TermMappings[fullName]; exists {
		return cfg.TermMappings[fullName]
	}
	return fullName
}

func (cfg Config) Expand(short string) string {
	if _, exists := cfg.TermExpansion[short]; exists {
		return cfg.TermExpansion[short]
	}
	return short
}

func InitNamespaceTrie(cfg Config) *Trie {
	root := InitTrie()
	for _, v := range cfg.NamespaceMappings {
		root.Insert(v)
	}
	return root
}

func (cfg Config) MapNamespaces(exact string) string {
	if _, exists := cfg.NamespaceMappings[exact]; exists {
		return cfg.NamespaceMappings[exact]
	}
	return exact
}

func (cfg Config) MapLongestPrefix(prefix string) string {
	return cfg.Trie.AllWordsFromPrefix(prefix)[0]
}
