package cmd

import (
	"os"

	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/spf13/cobra"
)

var (
	saveGraphCmd = &cobra.Command{
		Use:   "save",
		Short: "Save a graph to the db",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			drv := getNeoDriver(cmd)
			inputFormat, _ := cmd.Flags().GetString("input")
			g, err := cmdutil.ReadGraph(args, nil, inputFormat)
			if err != nil {
				return err
			}
			session := drv.NewSession()
			session.SetLogOutput(os.Stdout)
			defer session.Close()
			err = neo.SaveGraph(session, g)
			if err != nil {
				return err
			}
			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(saveGraphCmd)
	saveGraphCmd.Flags().String("input", "json", "Input graph format (json, jsonld)")
}
