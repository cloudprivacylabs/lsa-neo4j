package cmd

import (
	"fmt"

	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/spf13/cobra"
)

var (
	deleteCmd = &cobra.Command{
		Use:   "delete",
		Short: "delete",
		Long:  "delete the nodeset from the database",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			drv := getNeoDriver(cmd)
			ctx := ls.DefaultContext()
			session := drv.NewSession(ctx)
			defer session.Close(ctx)
			var tx neo4j.ExplicitTransaction
			var err error
			tx, err = session.BeginTransaction(ctx)
			if err != nil {
				return err
			}
			cfg, err := loadConfig(cmd)
			if err != nil {
				return err
			}
			neo.InitNamespaceTrie(&cfg)
			startRow, err := cmd.Flags().GetInt("startRow")
			if err != nil {
				return err
			}
			headerRow, err := cmd.Flags().GetInt("headerRow")
			if err != nil {
				return err
			}
			if headerRow >= startRow {
				return fmt.Errorf("Header row is ahead of start row")
			}
			data, err := readSpreadsheetFile(args[0])
			if err != nil {
				return err
			}
			ssi := &spreadsheetInput{
				rows:      data,
				at:        0,
				headerRow: headerRow,
			}
			nodesets, err := neo.ParseNodesetData(cfg, ssi)
			if err != nil {
				return err
			}
			for _, ns := range nodesets {
				db_ns, err := neo.LoadNodeset(ctx, cfg, tx, ns.ID)
				if err != nil {
					return err
				}
				rootOp, inserts, deletes, updates := neo.NodesetDiff(db_ns, neo.Nodeset{})
				if err := neo.Execute(ctx, tx, cfg, db_ns, ns, rootOp, inserts, updates, deletes); err != nil {
					tx.Rollback(ctx)
					return err
				}
			}
			tx.Commit(ctx)
			return nil
		},
	}
)

func init() {
	nodesetCmd.AddCommand(deleteCmd)
}
