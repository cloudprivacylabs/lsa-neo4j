package neo4j

import (
	"reflect"
	"testing"

	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
)

func TestNamespace(t *testing.T) {
	var cfg Config
	if err := cmdutil.ReadJSONOrYAML("lsaneo/lsaneo.config.yaml", &cfg); err != nil {
		t.Errorf("Could not read file: %s", "lsaneo.config.yaml")
	}

	myTrie := InitNamespaceTrie(&cfg)
	table := []struct {
		pre    string
		exp    []string
		mapped string
	}{
		{"https://lschema.org/A/b", []string{"https://lschema.org/A/", "lsa"}, "lsa:b"},
		{"https://lschema.org/Y/z", []string{"https://lschema.org/Y/", "lsy"}, "lsy:z"},
		{"https://lschema.org/Y/", []string{"https://lschema.org/Y/", "lsy"}, "lsy:"},
		{"https://lschema.org/X", []string{"https://lschema.org/", "ls"}, "ls:X"},
	}
	shortToExpand := []struct {
		short  string
		expand string
	}{
		{"ls:X", "https://lschema.org/X"},
		{"lsy:", "https://lschema.org/Y/"},
	}
	for _, tt := range table {
		x, y, ok := myTrie.Search(tt.pre)
		if !ok {
			t.Errorf("Word not found")
		}
		//fmt.Println(x, y)
		if !reflect.DeepEqual([]string{x, y}, tt.exp) {
			t.Errorf("Got %v, expected %v", []string{x, y}, tt.exp)
		}
		if !reflect.DeepEqual(cfg.Map(tt.pre), tt.mapped) {
			t.Errorf("Got %v, expected %v", cfg.Map(tt.pre), tt.mapped)
		}
	}
	for _, tt := range shortToExpand {
		if !reflect.DeepEqual(cfg.Expand(tt.short), tt.expand) {
			t.Errorf("Got %v, expected %v", cfg.Expand(tt.short), tt.expand)
		}
	}
}
