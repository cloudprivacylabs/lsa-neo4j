package neo4j

import (
	"github.com/cloudprivacylabs/lsa/pkg/ls"
)

type Config struct {
	TermMappings map[string]interface{} `yaml:"termMappings"`
}

type withProperty interface {
	ForEachProperty(func(string, interface{}) bool) bool
}

func (cfg Config) MakeProperties(x withProperty) string {
	props := makeProperties(cfg.TermMappings, ls.PropertiesAsMap(x), nil)
	return props
}

func (cfg Config) MakeLabels(types []string) string {
	labels := makeLabels(cfg.TermMappings, types)
	return labels
}
