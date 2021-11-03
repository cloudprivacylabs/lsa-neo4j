package cmd

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/bserdar/jsonstream"
	neo "github.com/cloudprivacylabs/lsa-neo4j"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "lsaneo",
		Short: "Neo4j implementation of layered schemas",
		Long:  `Use this CLI to interact with a Neo4j database.`,
	}
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().String("uri", "neo4j://localhost:7687", "DB URI")
	rootCmd.MarkFlagRequired("uri")
	rootCmd.PersistentFlags().String("user", "", "Username")
	rootCmd.PersistentFlags().String("pwd", "", "Password")
	rootCmd.PersistentFlags().String("realm", "", "Realm")
	rootCmd.PersistentFlags().String("db", "", "Database name")
}

func getDriver(cmd *cobra.Command) neo4j.Driver {
	dbUri, _ := cmd.Flags().GetString("uri")
	user, _ := cmd.Flags().GetString("user")
	pwd, _ := cmd.Flags().GetString("pwd")
	realm, _ := cmd.Flags().GetString("realm")
	var auth neo4j.AuthToken
	if len(user) > 0 {
		auth = neo4j.BasicAuth(user, pwd, realm)
	} else {
		auth = neo4j.NoAuth()
	}
	driver, err := neo4j.NewDriver(dbUri, auth)
	if err != nil {
		log.Fatal(err)
	}
	return driver
}

func getNeoDriver(cmd *cobra.Command) *neo.Driver {
	drv := getDriver(cmd)
	db, _ := cmd.Flags().GetString("db")
	return neo.NewDriver(drv, db)
}

// reads input[0] if it exists, otherwise reads from stdin
func readJSONFileOrStdin(input []string) ([]interface{}, error) {
	var rd jsonstream.ConcatReader
	if len(input) == 0 {
		rd = jsonstream.NewConcatReader(os.Stdin)
	} else {
		data, err := ioutil.ReadFile(input[0])
		if err != nil {
			return nil, err
		}
		rd = jsonstream.NewConcatReader(bytes.NewReader(data))
	}

	out := make([]interface{}, 0)
	for {
		var v interface{}
		err := rd.Unmarshal(&v)
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
}
