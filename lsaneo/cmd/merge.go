package cmd

import (
	"fmt"

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

			dbGraph, ids, edgeIds, err := session.LoadDBGraph(tx, memGraph, cfg)
			if err != nil {
				return err
			}

			_, ops, err := neo.Merge(memGraph, dbGraph, ids, edgeIds, cfg)
			if err != nil {
				return err
			}
			fmt.Println(ops)
			return neo.RunOperations(ls.DefaultContext(), session, tx, ops)
		},
	}
)

func init() {
	rootCmd.AddCommand(mergeGraphCmd)
	mergeGraphCmd.Flags().String("input", "json", "Input graph format (json, jsonld)")
	mergeGraphCmd.Flags().String("cfg", "", "configuration spec for node properties and labels (default: lsaneo.config.yaml)")
	mergeGraphCmd.Flags().String("memGraph", "", "in-memory graph with updates")
}