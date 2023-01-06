package cmd

import (
	"fmt"
	"os"
	"strings"

	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/spf13/cobra"
)

var (
	nodesetCmd = &cobra.Command{
		Use:   "nodeset",
		Short: "prompts nodeset commands: apply or delete",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			drv := getNeoDriver(cmd)
			var operation string
			input, _ := cmd.Flags().GetString("input")
			applyF, _ := cmd.Flags().GetString("apply")
			deleteF, _ := cmd.Flags().GetString("delete")
			if applyF != "" {
				operation = applyF
			} else if deleteF != "" {
				operation = deleteF
			} else {
				return fmt.Errorf("err: more than one command given")
			}
			var err error
			startRow, err := cmd.Flags().GetInt("startRow")
			if err != nil {
				return err
			}
			headerRow, err := cmd.Flags().GetInt("headerRow")
			if err != nil {
				return err
			}
			if headerRow >= startRow {
				return fmt.Errorf("Header row is ahead of start row")
			}
			data, err := readSpreadsheetFile(input)
			if err != nil {
				return err
			}
			ns := newNodeset(data)
			return nil
		},
	}
)

func newNodeset(sheet [][]string, headerStart int) neo.Nodeset {
	// range through headers, look for nodeset_id
	// scan down from nodeset_id and add row to nodeset
	ns := neo.Nodeset{
		Rows: neo.ItrRows{Rows: make([][]string, 0)},
	}
	for cIdx, header := range sheet[headerStart] {
		if header == "nodeset_id" {
			for j := cIdx; j < len(sheet); j++ {
				if sheet[j][cIdx] != "" || sheet[j][cIdx] != "nodeset_id" {
					ns.Rows.Rows = append(ns.Rows.Rows, sheet[j])
				}
			}
			break
		}
	}
	return ns
}

func readSpreadsheetFile(fileName string) ([][]string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if strings.HasSuffix(strings.ToLower(fileName), ".csv") {
		data, err := readCSV(file, os.Getenv("CSV_SEPARATOR"))
		if err != nil {
			return nil, err
		}
		return data, nil
	}
	return readExcel(file)
}

func init() {
	rootCmd.AddCommand(nodesetCmd)
	nodesetCmd.Flags().String("apply", "", "modify the database to reflect the nodeset file; performs operations create/update")
	nodesetCmd.Flags().String("delete", "", "delete the nodeset from the database")
	nodesetCmd.Flags().String("input", "csv", "input nodeset file")
	nodesetCmd.Flags().Int("startRow", 1, "Start row 0-based")
	nodesetCmd.Flags().Int("endRow", -1, "End row 0-based")
	nodesetCmd.Flags().Int("headerRow", 0, "Header row 0-based (default: 0) ")
}
