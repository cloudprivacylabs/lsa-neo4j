package cmd

import (
	"io"
	"os"
	"strings"

	lsacsv "github.com/cloudprivacylabs/lsa/pkg/csv"
	"github.com/spf13/cobra"
)

var (
	nodesetCmd = &cobra.Command{
		Use:   "nodeset",
		Short: "prompts nodeset commands: apply or delete",
		Args:  cobra.ExactArgs(1),
	}
)

func init() {
	rootCmd.AddCommand(nodesetCmd)
	nodesetCmd.PersistentFlags().Int("startRow", 1, "Start row 0-based")
	nodesetCmd.PersistentFlags().Int("endRow", -1, "End row 0-based")
	nodesetCmd.PersistentFlags().Int("headerRow", 0, "Header row 0-based (default: 0) ")
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
