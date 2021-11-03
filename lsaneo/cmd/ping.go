package cmd

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

var (
	pingCmd = &cobra.Command{
		Use:   "ping",
		Short: "Ping connection to the DB",
		Run: func(cmd *cobra.Command, args []string) {
			drv := getDriver(cmd)
			if err := drv.VerifyConnectivity(); err != nil {
				log.Fatal(err)
			}
			fmt.Println("Connection OK")
		},
	}
)

func init() {
	rootCmd.AddCommand(pingCmd)
}
