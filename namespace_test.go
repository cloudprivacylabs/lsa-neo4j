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
		pre string
		exp []string
	}{
		{"https://lschema.org/", []string{"https://lschema.org/", "ls:"}},
		{"https://lschema.org/A", []string{"https://lschema.org/A", "ls:A"}},
		{"https://lschema.org/Y", []string{"https://lschema.org/Y", "ls:Y"}},
		{"https://lschema.org/Y/", []string{"https://lschema.org/Y/", "lsy:"}},
		{"https://lschema.org/Y/a", []string{"https://lschema.org/Y/a", "lsy:a"}},
	}
	for _, tt := range table {
		x, _, ok := myTrie.Search(tt.pre)
		if !ok {
			t.Errorf("Word not found")
		}
		//fmt.Println(cfg.Map(x))
		if !reflect.DeepEqual([]string{x, cfg.Map(x)}, tt.exp) {
			t.Errorf("Got %v, expected %v", []string{x, cfg.Map(x)}, tt.exp)
		}
	}
}
