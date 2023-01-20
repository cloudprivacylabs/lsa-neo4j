package cmd

import (
	"errors"
	"os"

	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/spf13/cobra"
)

func initSession(cmd *cobra.Command) (*ls.Context, neo4j.ExplicitTransaction, error) {
	drv := getNeoDriver(cmd)
	ctx := ls.DefaultContext()
	session := drv.NewSession(ctx)
	defer session.Close(ctx)
	var tx neo4j.ExplicitTransaction
	var err error
	tx, err = session.BeginTransaction(ctx)
	if err != nil {
		return nil, nil, err
	}
	return ctx, tx, nil
}

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
