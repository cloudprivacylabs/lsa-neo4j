package cmd

import (
	"fmt"

	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/spf13/cobra"
)

var (
	applyCmd = &cobra.Command{
		Use:   "apply",
		Short: "apply",
		Long:  "modify the database to reflect the nodeset file; performs operations create/update",
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
			fmt.Printf("Applying %d nodesets\n", len(nodesets))
			for _, ns := range nodesets {
				fmt.Printf("Loading %s...", ns.ID)
				db_ns, err := neo.LoadNodeset(ctx, cfg, tx, ns.ID)
				if err != nil {
					return err
				}
				fmt.Println()
				rootOp, inserts, deletes, updates := neo.NodesetDiff(db_ns, ns)
				fmt.Printf("New nodes: %d, updated nodes: %d, deleted nodes: %d", len(inserts), len(updates), len(deletes))
				if err := neo.Execute(ctx, tx, cfg, db_ns, ns, rootOp, inserts, updates, deletes); err != nil {
					tx.Rollback(ctx)
					return err
				}
				fmt.Println()
			}
			tx.Commit(ctx)
			return nil
		},
	}
)

func init() {
	nodesetCmd.AddCommand(applyCmd)
}
