package neo4j

import (
	"context"
	"fmt"

	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const username = "neo4j"
const pwd = "password"
const uri = "neo4j://34.213.163.7"
const port = 7687
const db = "neo4j"

type CloseWithContext interface {
	Close(context.Context) error
}

func Close(closer CloseWithContext, ctx context.Context, resourceName string) {
	Expect(closer.Close(ctx)).To(BeNil(), "%s should be closed", resourceName)
}

func startContainer(ctx context.Context, user, pwd string) (testcontainers.Container, error) {
	request := testcontainers.ContainerRequest{
		Image:        "neo4j:latest",
		ExposedPorts: []string{"7687/tcp"},
		Env:          map[string]string{"NEO4J_AUTH": fmt.Sprintf("%s/%s", user, pwd)},
		WaitingFor:   wait.ForLog("Bolt enabled"),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: request,
		Started:          true,
	})
}
