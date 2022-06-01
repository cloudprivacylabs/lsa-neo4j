package neo4j

import (
	"reflect"
	"testing"

	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
)

func TestNamespace(t *testing.T) {
	var cfg Config
	if err := cmdutil.ReadJSONOrYAML("lsaneo/cmd/config.yaml", &cfg); err != nil {
		t.Errorf("Could not read file: %s", "lsaneo/cmd/config.yaml")
	}
	myTrie := InitNamespaceTrie(cfg)
	cfg.Trie = myTrie
	table := []struct {
		pre    string
		exp    []string
		mapped string
	}{
		{"https://lschema.org/A/b", []string{"https://lschema.org/A/", "lsa"}, "lsa:b"},
		{"https://lschema.org/Y/z", []string{"https://lschema.org/Y/", "lsy"}, "lsy:z"},
	}
	for _, tt := range table {
		x, y, ok := myTrie.Search(tt.pre)
		if !ok {
			t.Errorf("Word not found")
		}
		if !reflect.DeepEqual([]string{x, y}, tt.exp) {
			t.Errorf("Got %v, expected %v", []string{x, y}, tt.exp)
		}
		if !reflect.DeepEqual(cfg.Map(tt.pre), tt.mapped) {
			t.Errorf("Got %v, expected %v", cfg.Map(tt.pre), tt.mapped)
		}
	}
}
