package cmd

import (
	"fmt"
	"testing"

	neo "github.com/cloudprivacylabs/lsa-neo4j"
)

type expected struct {
}

func TestParseNodesetData(t *testing.T) {
	sheet, err := ReadSpreadsheetFile("../../testdata/apply_01.csv")
	if err != nil {
		t.Error(err)
	}
	ssi := &spreadsheetInput{
		rows:      sheet,
		at:        0,
		headerRow: 0,
	}
	nodesets, err := neo.ParseNodesetData(ssi)
	if err != nil {
		t.Error(err)
	}
	for k, v := range nodesets {
		fmt.Println(k, v)
	}
}
