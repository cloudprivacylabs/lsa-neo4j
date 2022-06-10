package cmd

import (
	"errors"
	"os"
	"strconv"

	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/cloudprivacylabs/opencypher/graph"
	"github.com/spf13/cobra"
)

var (
	loadEntityNodesCmd = &cobra.Command{
		Use:   "load",
		Short: "load entity nodes from database",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			drv := getNeoDriver(cmd)
			inputFormat, _ := cmd.Flags().GetString("input")
			var cfg neo.Config

			if cfgfile, _ := cmd.Flags().GetString("cfg"); len(cfgfile) == 0 {
				err := cmdutil.ReadJSONOrYAML("lsaneo.config.yaml", &cfg)
				if !errors.Is(err, os.ErrNotExist) {
					return err
				}
			} else {
				if err := cmdutil.ReadJSONOrYAML(cfgfile, &cfg); err != nil {
					return err
				}
			}
			neo.InitNamespaceTrie(&cfg)
			grph, err := cmdutil.ReadGraph(args, nil, inputFormat)
			if err != nil {
				return err
			}
			session := drv.NewSession()
			defer session.Close()
			tx, err := session.BeginTransaction()
			if err != nil {
				return err
			}
			rootIds := make([]int64, 0)
			for _, arg := range args {
				id, err := strconv.ParseInt(arg, 10, 64)
				if err != nil {
					return err
				}
				rootIds = append(rootIds, id)
			}
			if allNodes, _ := cmd.Flags().GetString("allNodes"); len(allNodes) > 0 {
				err = session.LoadEntityNodes(tx, grph, rootIds, cfg, func(graph.Node) bool {
					return true
				})
			} else {
				if schema, _ := cmd.Flags().GetString("schema"); len(schema) > 0 {
					err = session.LoadEntityNodes(tx, grph, rootIds, cfg, func(nd graph.Node) bool {
						if _, ok := nd.GetProperty(schema); ok {
							return true
						}
						return false
					})
				}
			}
			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(loadEntityNodesCmd)
	loadEntityNodesCmd.Flags().String("allNodes", "", "load all nodes from database")
	loadEntityNodesCmd.Flags().String("schema", "", "load all nodes within schema from database")
	loadEntityNodesCmd.Flags().String("input", "json", "Input graph format (json, jsonld)")
}
