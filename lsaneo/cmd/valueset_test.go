package cmd

import (
	"errors"
	"os"
	"testing"

	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
)

func TestParseNodesetData(t *testing.T) {
	sheet, err := readSpreadsheetFile("../../testdata/apply_01.csv")
	if err != nil {
		t.Error(err)
	}
	var cfg neo.Config
	err = cmdutil.ReadJSONOrYAML("../lsaneo.config.yaml", &cfg)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Error(err)
	}
	neo.InitNamespaceTrie(&cfg)
	ssi := &spreadsheetInput{
		rows:      sheet,
		at:        0,
		headerRow: 0,
	}
	_, err = neo.ParseNodesetData(cfg, ssi)
	if err != nil {
		t.Error(err)
	}
}
