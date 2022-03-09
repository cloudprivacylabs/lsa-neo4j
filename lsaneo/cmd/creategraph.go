package cmd

import (
	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/cloudprivacylabs/lsa/pkg/opencypher/graph"
	"github.com/spf13/cobra"
)

var (
	createGraphCmd = &cobra.Command{
		Use:   "create",
		Short: "Create a graph on the db",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			drv := getNeoDriver(cmd)
			inputFormat, _ := cmd.Flags().GetString("input")
			g, err := cmdutil.ReadGraph(args, nil, inputFormat)
			var nodes []graph.Node
			for g.GetNodes().Next() {
				nodes = append(nodes, g.GetNodes().Node())
			}
			if err != nil {
				return err
			}
			session := drv.NewSession()
			defer session.Close()
			tx, _ := session.BeginTransaction()
			_, err = neo.CreateGraph(session, tx, nodes)
			if err != nil {
				return err
			}
			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(createGraphCmd)
	createGraphCmd.Flags().String("input", "json", "Input graph format (json, jsonld)")
}
