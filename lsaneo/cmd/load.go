package cmd

import (
	"os"

	"github.com/cloudprivacylabs/lpg/v2"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/spf13/cobra"
)

var (
	loadEntityNodesCmd = &cobra.Command{
		Use:   "load",
		Short: "load entity nodes from database",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := ls.DefaultContext()
			drv := getNeoDriver(cmd)
			session := drv.NewSession(ctx)
			defer session.Close(ctx)
			tx, err := session.BeginTransaction(ctx)
			if err != nil {
				return err
			}
			cfg, err := loadConfig(cmd)
			if err != nil {
				return err
			}
			nodeIds, _ := cmd.Flags().GetBool("nodeIds")
			for _, arg := range args {
				grph := ls.NewDocumentGraph()
				loadByID := func(f func(*lpg.Node) bool) error {
					// id, err := strconv.ParseInt(arg, 10, 64)
					// if err != nil {
					// 	return err
					// }
					return session.LoadEntityNodes(ctx, tx, grph, []string{arg}, cfg, f)
				}
				loadByEntityID := func(f func(*lpg.Node) bool) error {
					return session.LoadEntityNodesByEntityId(ctx, tx, grph, []string{arg}, cfg, f)
				}
				var load func(func(*lpg.Node) bool) error
				if nodeIds {
					load = loadByID
				} else {
					load = loadByEntityID
				}
				if allNodes, _ := cmd.Flags().GetBool("allNodes"); allNodes {
					err = load(func(*lpg.Node) bool {
						return true
					})
				} else {
					if schema, _ := cmd.Flags().GetStringSlice("schema"); len(schema) > 0 {
						err = load(func(nd *lpg.Node) bool {
							for ix := range schema {
								if ls.AsPropertyValue(nd.GetProperty(ls.EntitySchemaTerm)).AsString() == schema[ix] {
									return true
								}
							}
							return false
						})
					}
				}
				cmdutil.WriteGraph(cmd, grph, "json", os.Stdout)
			}
			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(loadEntityNodesCmd)
	loadEntityNodesCmd.Flags().Bool("nodeIds", false, "Use node ids instead of entity ids")
	loadEntityNodesCmd.Flags().Bool("allNodes", false, "load all nodes from database")
	loadEntityNodesCmd.Flags().StringSlice("schema", []string{}, "load all nodes within schema from database")
}
