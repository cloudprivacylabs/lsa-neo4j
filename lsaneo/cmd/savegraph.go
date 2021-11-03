package cmd

import (
	"log"

	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/spf13/cobra"
)

var (
	saveGraphCmd = &cobra.Command{
		Use:   "save",
		Short: "Save a graph to the db",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			drv := getNeoDriver(cmd)
			inp, err := readJSONFileOrStdin(args)
			if err != nil {
				log.Fatal(err)
			}
			session := drv.NewSession()
			defer session.Close()
			for _, x := range inp {
				g, err := ls.UnmarshalGraph(x, ls.NewInterner())
				if err != nil {
					log.Fatal(err)
				}
				err = neo.SaveGraph(session, g)
				if err != nil {
					log.Fatal(err)
				}
			}
		},
	}
)

func init() {
	rootCmd.AddCommand(saveGraphCmd)
}
