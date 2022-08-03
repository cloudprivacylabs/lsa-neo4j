package cmd

import (
	"errors"
	"io"
	"os"

	neo "github.com/cloudprivacylabs/lsa-neo4j"
	lscmd "github.com/cloudprivacylabs/lsa/layers/cmd"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/cloudprivacylabs/lsa/layers/cmd/pipeline"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/spf13/cobra"
)

var (
	streamPipelineCmd = &cobra.Command{
		Use:   "stream",
		Short: "save a graph to the db passing the graph from the pipeline",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			drv := getNeoDriver(cmd)
			var cfg neo.Config
			batchSize, _ := cmd.Flags().GetInt("batch")
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
			neo.InitNamespaceTrie(&cfg)
			session := drv.NewSession()
			defer session.Close()
			tx, err := session.BeginTransaction()
			pls := &neo.PlSession{
				S:         session,
				Tx:        tx,
				Cfg:       cfg,
				BatchSize: batchSize,
			}
			if err != nil {
				return err
			}
			ps := []pipeline.Step{
				&neo.PlSession{},
				lscmd.NewWriteGraphStep(cmd),
			}
			steps, err := pipeline.Run(ls.DefaultContext(), ps, nil, func() (io.ReadCloser, error) {
				stream, err := cmdutil.StreamFileOrStdin(args)
				if err != nil {
					return nil, err
				}
				return io.NopCloser(stream), nil
			})
			if err != nil {
				return err
			}

			err = pls.Run(steps)
			if err != nil {
				tx.Rollback()
				return err
			}
			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(createGraphCmd)
	streamPipelineCmd.Flags().String("cfg", "", "configuration spec for node properties and labels (default: lsaneo.config.yaml)")
	streamPipelineCmd.Flags().Int("batch", 0, "batching size for creation of nodes and edges")
	pipeline.RegisterPipelineStep("stream", func() pipeline.Step {
		return &neo.PlSession{}
	})
}
