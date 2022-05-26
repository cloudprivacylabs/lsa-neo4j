package neo4j

import (
	"github.com/cloudprivacylabs/lsa/pkg/ls"
)

type Config struct {
	shorten map[string]interface{}
}

type withProperty interface {
	ForEachProperty(func(string, interface{}) bool) bool
}

func (cfg Config) MakeProperties(x withProperty) string {
	props := makeProperties(cfg.shorten, ls.PropertiesAsMap(x), nil)
	return props
}

func (cfg Config) MakeLabels(types []string) string {
	labels := makeLabels(cfg.shorten, types)
	return labels
}
