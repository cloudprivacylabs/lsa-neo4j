package cmd

import (
	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/cloudprivacylabs/opencypher/graph"
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
			file, _ := cmd.Flags().GetString("cfg")
			var cfg neo.Config
			if err := cmdutil.ReadJSONOrYAML(file, &cfg); err != nil {
				return err
			}
			g, err := cmdutil.ReadGraph(args, nil, inputFormat)
			if err != nil {
				return err
			}
			nodeSl := make([]graph.Node, 0, g.NumNodes())
			for nodes := g.GetNodes(); nodes.Next(); {
				nodeSl = append(nodeSl, nodes.Node())
			}
			if err != nil {
				return err
			}
			session := drv.NewSession()
			defer session.Close()
			tx, err := session.BeginTransaction()
			if err != nil {
				return err
			}
			_, err = neo.CreateGraph(session, tx, nodeSl, cfg)
			if err != nil {
				tx.Rollback()
				return err
			}
			tx.Commit()
			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(createGraphCmd)
	createGraphCmd.Flags().String("input", "json", "Input graph format (json, jsonld)")
	createGraphCmd.Flags().String("cfg", "config.yaml", "configuration spec for node properties and labels")
}
