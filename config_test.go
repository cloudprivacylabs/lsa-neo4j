package neo4j

import (
	"errors"
	"os"
	"testing"

	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
)

func TestSaveProperty(t *testing.T) {
	var cfg Config
	err := cmdutil.ReadJSONOrYAML("lsaneo.config.yaml", &cfg)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Error(err)
	}

}
