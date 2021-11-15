package cmd

import (
	"log"

	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/spf13/cobra"
)

var (
	saveGraphCmd = &cobra.Command{
		Use:   "save",
		Short: "Save a graph to the db",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			drv := getNeoDriver(cmd)
			inputFormat, _ := cmd.Flags().GetString("input")
			g, err := cmdutil.ReadGraph(args, nil, inputFormat)
			if err != nil {
				log.Fatal(err)
			}
			session := drv.NewSession()
			defer session.Close()
			err = neo.SaveGraph(session, g)
			if err != nil {
				log.Fatal(err)
			}
		},
	}
)

func init() {
	rootCmd.AddCommand(saveGraphCmd)
	saveGraphCmd.Flags().String("input", "json", "Input graph format (json, jsonld)")
}
