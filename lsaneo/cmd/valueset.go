package cmd

import (
	"fmt"
	"io"
	"os"
	"strings"

	neo "github.com/cloudprivacylabs/lsa-neo4j"
	lsacsv "github.com/cloudprivacylabs/lsa/pkg/csv"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/spf13/cobra"
)

var (
	nodesetCmd = &cobra.Command{
		Use:   "nodeset",
		Short: "prompts nodeset commands: apply or delete",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			drv := getNeoDriver(cmd)
			session := drv.NewSession()
			defer session.Close()
			var tx neo4j.Transaction
			var err error
			tx, err = session.BeginTransaction()
			if err != nil {
				return err
			}
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
			ssi := &spreadsheetInput{
				rows:      data,
				at:        0,
				headerRow: headerRow,
			}
			nodesets, err := neo.ParseNodesetData(ssi)
			if err != nil {
				return err
			}
			if err := commitNodesetsOperation(tx, nodesets, operation); err != nil {

			}
			return nil
		},
	}
)

func commitNodesetsOperation(tx neo4j.Transaction, nodesets map[string]neo.Nodeset, operation string) error {
	if operation == "apply" {
		if err := neo.NodesetApply(tx, nodesets); err != nil {
			return err
		}
	} else {
		if err := neo.NodesetDelete(tx, nodesets); err != nil {
			return err
		}
	}
	return nil
}

type spreadsheetInput struct {
	rows      [][]string
	at        int
	headerRow int
}

func (s *spreadsheetInput) ColumnNames() []string {
	return s.rows[s.headerRow]
}

func (s *spreadsheetInput) Reset() error {
	s.at = s.headerRow + 1
	if s.at >= len(s.rows) {
		return io.EOF
	}
	return nil
}

func (s *spreadsheetInput) Next() ([]string, error) {
	if s.at >= len(s.rows) {
		return nil, io.EOF
	}
	ret := s.rows[s.at]
	s.at++
	return ret, nil
}

func readSpreadsheetFile(fileName string) ([][]string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if strings.HasSuffix(strings.ToLower(fileName), ".csv") {
		data, err := lsacsv.ReadCSV(file, os.Getenv("CSV_SEPARATOR"))
		if err != nil {
			return nil, err
		}
		return data, nil
	}
	xlsxSheet, err := lsacsv.ReadExcel(file)
	if err != nil {
		return nil, err
	}
	for _, sheet := range xlsxSheet {
		return sheet, nil
	}
	return nil, nil
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
