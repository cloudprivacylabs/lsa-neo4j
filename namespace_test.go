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
	config := InitConfig(cfg)
	table := []struct {
		pre    string
		exp    []string
		mapped string
	}{
		{"https://lschema.org/A/b", []string{"https://lschema.org/A/", "lsa"}, "lsa:b"},
		{"https://lschema.org/Y/z", []string{"https://lschema.org/Y/", "lsy"}, "lsy:z"},
		{"https://lschema.org/Y/", []string{"https://lschema.org/Y/", "lsy"}, "lsy:"},
	}
	for _, tt := range table {
		x, y, ok := config.trie.Search(tt.pre)
		if !ok {
			t.Errorf("Word not found")
		}
		//fmt.Println(x, y)
		if !reflect.DeepEqual([]string{x, y}, tt.exp) {
			t.Errorf("Got %v, expected %v", []string{x, y}, tt.exp)
		}
		if !reflect.DeepEqual(config.Map(tt.pre), tt.mapped) {
			t.Errorf("Got %v, expected %v", config.Map(tt.pre), tt.mapped)
		}
	}
}
