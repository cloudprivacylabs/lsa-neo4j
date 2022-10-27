package cmd

import (
	"context"
	"errors"
	"os"

	"github.com/cloudprivacylabs/lpg"
	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/spf13/cobra"
)

var (
	initGraphCmd = &cobra.Command{
		Use:   "insert",
		Short: "Create a graph on the db, this function will not check existing nodes",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			drv := getNeoDriver(cmd)
			inputFormat, _ := cmd.Flags().GetString("input")
			batchSize, _ := cmd.Flags().GetInt("batch")
			var cfg neo.Config
			if cfgfile, _ := cmd.Flags().GetString("cfg"); len(cfgfile) == 0 {
				err := cmdutil.ReadJSONOrYAML("lsaneo.config.yaml", &cfg)
				if err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
			} else {
				if err := cmdutil.ReadJSONOrYAML(cfgfile, &cfg); err != nil {
					return err
				}
			}
			gch, err := cmdutil.StreamGraph(context.Background(), args, nil, inputFormat)
			if err != nil {
				return err
			}
			for gr := range gch {
				if gr.Err != nil {
					return err
				}
				g := gr.G

				neo.InitNamespaceTrie(&cfg)

				if err != nil {
					return err
				}
				session := drv.NewSession()
				defer session.Close()
				tx, err := session.BeginTransaction()
				if err != nil {
					return err
				}

				_, err = neo.Insert(ls.DefaultContext(), session, tx, g, func(lpg.Node) bool { return true }, cfg, batchSize)
				if err != nil {
					tx.Rollback()
					return err
				}

				tx.Commit()
			}
			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(initGraphCmd)
	initGraphCmd.Flags().String("input", "json", "Input graph format (json, jsonld)")
	initGraphCmd.Flags().String("cfg", "", "configuration spec for node properties and labels (default: lsaneo.config.yaml)")
	initGraphCmd.Flags().Int("batch", 0, "batching size for creation of nodes and edges")
}
