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
			var ns neo.Nodeset
			ns.input, _ = cmd.Flags().GetString("input")
			applyF, _ := cmd.Flags().GetString("apply")
			deleteF, _ := cmd.Flags().GetString("delete")
			if applyF != "" {
				ns.operation = applyF
			} else if deleteF != "" {
				ns.operation = deleteF
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
			data, err := readSpreadsheetFile(ns.input)
			if err != nil {
				return err
			}
			return nil
		},
	}
)

/*
A
A
B
C
C
A
*/

func readSpreadsheetFile(fileName string) (map[string][][]string, error) {
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
		return map[string][][]string{fileName: data}, nil
	}
	return readExcel(file)
}

func parseNodesetData(data map[string][][]string) error {

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
