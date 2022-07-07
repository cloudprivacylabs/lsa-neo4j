package cmd

import (
	"strconv"

	"github.com/cloudprivacylabs/lsa/pkg/ls"
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
			session := drv.NewSession()
			defer session.Close()
			tx, err := session.BeginTransaction()
			if err != nil {
				return err
			}
			rootIds := make([]uint64, 0)
			for _, arg := range args {
				id, err := strconv.ParseInt(arg, 10, 64)
				if err != nil {
					return err
				}
				rootIds = append(rootIds, uint64(id))
			}
			cfg, err := loadConfig(cmd)
			if err != nil {
				return err
			}
			grph := ls.NewDocumentGraph()
			if allNodes, _ := cmd.Flags().GetBool("allNodes"); allNodes {
				err = session.LoadEntityNodes(tx, grph, rootIds, cfg, func(graph.Node) bool {
					return true
				})
			} else {
				if schema, _ := cmd.Flags().GetStringSlice("schema"); len(schema) > 0 {
					err = session.LoadEntityNodes(tx, grph, rootIds, cfg, func(nd graph.Node) bool {
						for ix := range schema {
							if ls.AsPropertyValue(nd.GetProperty(ls.EntitySchemaTerm)).AsString() == schema[ix] {
								return true
							}
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
	loadEntityNodesCmd.Flags().Bool("allNodes", false, "load all nodes from database")
	loadEntityNodesCmd.Flags().StringSlice("schema", []string{}, "load all nodes within schema from database")
}
