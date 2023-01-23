package main

import (
	"github.com/cloudprivacylabs/lsa-neo4j/lsaneo/cmd"
	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load(".env")
	cmd.Execute()
}
