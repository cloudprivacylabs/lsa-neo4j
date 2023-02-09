package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/cloudprivacylabs/lpg"
	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/spf13/cobra"
)

var (
	compactCmd = &cobra.Command{
		Use:   "compact",
		Short: "Compact a graph",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			inputFormat, _ := cmd.Flags().GetString("input")
			gch, err := cmdutil.ReadGraph(args, nil, inputFormat)
			if err != nil {
				return err
			}
			out, err := neo.Compact(gch)
			if err != nil {
				return err
			}
			j := lpg.JSON{
				PropertyMarshaler: func(key string, value interface{}) (string, json.RawMessage, error) {
					switch value.(type) {
					case int, int64, int32, int16, int8, uint, uint64, uint32, uint16, uint8, float32, float64, time.Time, bool, []interface{}:
						data, _ := json.Marshal(value)
						return key, data, nil
					default:
						data, _ := json.Marshal(fmt.Sprint(value))
						return key, data, nil
					}
				},
			}
			j.Encode(out, os.Stdout)
			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(compactCmd)
	compactCmd.Flags().String("input", "json", "Input graph format (json, jsonld)")
}
