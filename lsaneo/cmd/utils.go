package cmd

import (
	"errors"
	"os"

	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/spf13/cobra"
)

func loadConfig(cmd *cobra.Command) (neo.Config, error) {
	var cfg neo.Config

	if cfgfile, _ := cmd.Flags().GetString("cfg"); len(cfgfile) == 0 {
		err := cmdutil.ReadJSONOrYAML("lsaneo.config.yaml", &cfg)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return neo.Config{}, err
		}
	} else {
		if err := cmdutil.ReadJSONOrYAML(cfgfile, &cfg); err != nil {
			return neo.Config{}, err
		}
	}
	neo.InitNamespaceTrie(&cfg)
	return cfg, nil
}
