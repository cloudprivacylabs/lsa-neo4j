package cmd

import (
	"encoding/csv"
	"io"

	"github.com/xuri/excelize/v2"
)

func readCSV(input io.Reader, csvSeparator string) ([][]string, error) {
	reader := csv.NewReader(input)
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1
	if len(csvSeparator) > 0 {
		reader.Comma = rune(csvSeparator[0])
	}
	data, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func readExcel(input io.Reader) (map[string][][]string, error) {
	f, err := excelize.OpenReader(input)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sheets := f.GetSheetList()
	ret := make(map[string][][]string)
	for _, sheet := range sheets {
		rows, err := f.GetRows(sheet)
		if err != nil {
			return nil, err
		}
		ret[sheet] = rows
	}
	return ret, nil
}
