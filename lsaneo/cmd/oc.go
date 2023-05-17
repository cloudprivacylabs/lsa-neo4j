package cmd

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/spf13/cobra"
)

var (
	ocCmd = &cobra.Command{
		Use:   "oc",
		Short: "Run opencypher statement",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {

			output, _ := cmd.Flags().GetString("output")
			var query string
			qf, _ := cmd.Flags().GetString("qf")
			if len(qf) > 0 {
				data, err := os.ReadFile(qf)
				if err != nil {
					return err
				}
				query = string(data)
			} else if len(args) == 1 {
				query = args[0]
			} else {
				rd := bufio.NewReader(os.Stdin)
				last := ""
				for {
					t, err := rd.ReadString('\n')
					t = strings.TrimSpace(t)
					query += " " + t
					if err != nil {
						if err == io.EOF {
							break
						}
						return err
					}
					if t == "" && last == "" {
						break
					}
					last = t
				}
			}
			fmt.Println("Running", query)
			drv := getNeoDriver(cmd)
			ctx := ls.DefaultContext()
			session := drv.NewSession(ctx)
			defer session.Close(ctx)

			tx, err := session.BeginTransaction(ctx)
			if err != nil {
				return err
			}
			defer tx.Commit(ctx)

			result, err := tx.Run(ctx, query, nil)
			if err != nil {
				return err
			}
			keys, err := result.Keys()
			if err != nil {
				return err
			}
			var out io.Writer
			if output == "" {
				out = os.Stdout
			} else {
				f, err := os.Create(output)
				if err != nil {
					return err
				}
				defer f.Close()
				out = f
			}
			writer := csv.NewWriter(out)
			defer writer.Flush()
			writer.Write(keys)
			rec := make([]string, len(keys))
			type hasTime interface {
				Time() time.Time
			}
			for result.Next(ctx) {
				record := result.Record()
				for i, k := range keys {
					value, ok := record.Get(k)
					if ok {
						if value == nil {
							rec[i] = ""
						} else {
							switch k := value.(type) {
							case hasTime:
								rec[i] = k.Time().Format(time.RFC3339)
							default:
								rec[i] = fmt.Sprint(value)
							}
						}
					} else {
						rec[i] = ""
					}
				}
				writer.Write(rec)
			}

			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(ocCmd)
	ocCmd.Flags().String("output", "", "Output to file")
	ocCmd.Flags().String("qf", "", "Query file")
}
