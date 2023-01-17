package cmd

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig"
	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/spf13/cobra"
)

type TabReader struct {
	scanner *bufio.Scanner
}

func NewTabReader(in io.Reader) *TabReader {
	ret := &TabReader{scanner: bufio.NewScanner(in)}
	ret.scanner.Split(bufio.ScanLines)
	return ret
}

func (t *TabReader) Read() ([]string, error) {
	if !t.scanner.Scan() {
		return nil, io.EOF
	}
	items := strings.Split(t.scanner.Text(), "\t")
	return items, nil
}

var (
	importCmd = &cobra.Command{
		Use:   "import",
		Short: "Import data from a CSV or a TAB delimited file",
		Long: `This command reads lines from a CSV or TAB delimited file, and runs a Neo4j 
statement for each line. The statement is built using a Go template, and run with 
header fields as key, and corresponding cell value as value.

For instance, consider the CSV file:

concept_id,concept_name,domain_id,vocabulary_id,concept_class_id,standard_concept,concept_code
45756805,Pediatric Cardiology,Provider,ABMS,Physician Specialty,S


The following Go template can be used to create nodes from this file:

MERGE (c:Concept {id: $concept_id })
SET c.name = $concept_name 
SET c.code = $concept_code 
SET c.standard = CASE $standard_concept WHEN "S" THEN TRUE else null END

`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {

			dry, _ := cmd.Flags().GetBool("dry")

			tmplInput, _ := cmd.Flags().GetString("cmdtemplate")
			if len(tmplInput) == 0 {
				log.Fatal("Command template required")
			}
			if tmplInput[0] == '@' {
				data, err := ioutil.ReadFile(tmplInput[1:])
				if err != nil {
					log.Fatal(err)
				}
				tmplInput = string(data)
			}
			funcmap := sprig.TxtFuncMap()
			funcmap["esc"] = func(in string) string {
				in = strings.Replace(in, `\`, `\\`, -1)
				in = strings.Replace(in, `"`, `\"`, -1)
				return strings.Replace(in, `'`, `\'`, -1)
			}
			tmpl, err := template.New("cmd").Funcs(funcmap).Parse(tmplInput)
			if err != nil {
				log.Fatal(err)
			}

			input, err := cmdutil.StreamFileOrStdin(args)
			if err != nil {
				log.Fatal(err)
			}

			type csvReader interface {
				Read() ([]string, error)
			}
			var reader csvReader

			var drv *neo.Driver
			if !dry {
				drv = getNeoDriver(cmd)
			}
			inputFormat, _ := cmd.Flags().GetString("input")
			switch inputFormat {
			case "csv":
				reader = csv.NewReader(input)
			case "tab":
				reader = NewTabReader(input)
			}
			var session *neo.Session
			if !dry {
				session = drv.NewSession()
				defer session.Close()
			}

			batchSize, err := cmd.Flags().GetInt("batchsize")
			if err != nil {
				log.Fatal(err)
			}
			startRow, err := cmd.Flags().GetInt("startrow")
			if err != nil {
				log.Fatal(err)
			}
			endRow, err := cmd.Flags().GetInt("endrow")
			if err != nil {
				log.Fatal(err)
			}
			lineNumber := 0
			done := false
			var header []string

			processRecord := func(tx neo4j.Transaction, record []string) error {
				data := map[string]interface{}{}
				for i := range header {
					if i < len(record) {
						data[header[i]] = record[i]
					} else {
						data[header[i]] = ""
					}
				}
				var out bytes.Buffer
				if err := tmpl.Execute(&out, data); err != nil {
					return err
				}
				command := out.String()
				if dry {
					fmt.Println(command)
				} else {
					_, err := tx.Run(command, data)
					if err != nil {
						fmt.Printf("Command was: %s\n", command)
						return err
					}
				}
				return nil
			}

			txFunc := func(tx neo4j.Transaction) (interface{}, error) {
				for batchItem := 0; batchItem < batchSize; {
					record, err := reader.Read()
					if err == io.EOF {
						done = true
						return nil, nil
					}
					if err != nil {
						return nil, err
					}
					if lineNumber == 0 {
						header = record
					} else if lineNumber >= startRow && (endRow == -1 || lineNumber < endRow) {
						if len(record) > 0 {
							err := processRecord(tx, record)
							if err != nil {
								return nil, err
							}
							batchItem++
						}

					}
					lineNumber++
				}
				return nil, nil
			}

			for !done {
				if dry {
					_, err := txFunc(nil)
					if err != nil {
						log.Fatal(err)
					}
				} else {
					_, err := session.WriteTransaction(txFunc)
					if err != nil {
						log.Fatal(err)
					}
					fmt.Printf("Line %d\n", lineNumber)
				}
			}
		},
	}
)

func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.Flags().String("input", "csv", "Input format (csv, tab)")
	importCmd.Flags().Int("batchsize", 1000, "Commit batch size (1000)")
	importCmd.Flags().Int("startrow", 0, "Start row")
	importCmd.Flags().Int("endrow", -1, "End row")
	importCmd.Flags().String("cmdtemplate", "", "Command Go template string, or @file")
	importCmd.Flags().Bool("dry", false, "Dry run -- print commands")
}
