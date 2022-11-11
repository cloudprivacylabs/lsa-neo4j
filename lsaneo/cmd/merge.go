package cmd

import (
	"context"

	"github.com/cloudprivacylabs/lpg"
	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/spf13/cobra"
)

var (
	mergeGraphCmd = &cobra.Command{
		Use:   "merge",
		Short: "merge or update an in-memory graph onto a pre-existing database graph",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			drv := getNeoDriver(cmd)
			mg, _ := cmd.Flags().GetString("memGraph")
			inputFormat, _ := cmd.Flags().GetString("input")
			cfg, err := loadConfig(cmd)
			if err != nil {
				return err
			}
			session := drv.NewSession()
			defer session.Close()
			tx, err := session.BeginTransaction()
			if err != nil {
				return err
			}
			memGraph, err := cmdutil.ReadGraph([]string{mg}, nil, inputFormat)
			if err != nil {
				return err
			}

			dbGraph, err := session.LoadDBGraph(tx, memGraph, cfg)
			if err != nil {
				return err
			}
			deltas, err := neo.Merge(memGraph, dbGraph, cfg)
			if err != nil {
				return err
			}

			ctx := ls.NewContext(context.Background())
			createNodeDeltas := neo.SelectDelta(deltas, func(d neo.Delta) bool {
				_, ok := d.(neo.CreateNodeDelta)
				return ok
			})
			nodes := make([]*lpg.Node, 0, len(createNodeDeltas))
			for _, x := range createNodeDeltas {
				nodes = append(nodes, x.(neo.CreateNodeDelta).DBNode)
			}
			if err := neo.CreateNodesUnwind(ctx, tx, nodes, dbGraph.NodeIds, cfg); err != nil {
				return err
			}

			createEdgeDeltas := neo.SelectDelta(deltas, func(d neo.Delta) bool {
				_, ok := d.(neo.CreateEdgeDelta)
				return ok
			})
			edges := make([]*lpg.Edge, 0, len(createEdgeDeltas))
			for _, c := range createEdgeDeltas {
				edges = append(edges, c.(neo.CreateEdgeDelta).DBEdge)
			}
			if err := neo.CreateEdgesUnwind(ctx, tx, edges, dbGraph.NodeIds, cfg); err != nil {
				return err
			}

			updateDeltas := neo.SelectDelta(deltas, func(d neo.Delta) bool {
				if _, ok := d.(neo.CreateNodeDelta); ok {
					return false
				}
				if _, ok := d.(neo.CreateEdgeDelta); ok {
					return false
				}
				return true
			})
			for _, c := range updateDeltas {
				if err := c.Run(tx, dbGraph.NodeIds, dbGraph.EdgeIds, cfg); err != nil {
					return err
				}
			}

			tx.Commit()
			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(mergeGraphCmd)
	mergeGraphCmd.Flags().String("input", "json", "Input graph format (json, jsonld)")
	mergeGraphCmd.Flags().String("cfg", "", "configuration spec for node properties and labels (default: lsaneo.config.yaml)")
	mergeGraphCmd.Flags().String("memGraph", "", "in-memory graph with updates")
}
