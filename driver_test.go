package neo4j

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
)

func TestLsaNeo4j(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "LsaNeo4j Suite")
}

func selectEntity(node *lpg.Node) bool {
	return true
}

var _ = Describe("Driver", func() {
	var ctx context.Context
	var neo4jContainer testcontainers.Container
	var session *Session
	var driver neo4j.Driver
	var tx neo4j.Transaction

	var cfg Config
	var grph *lpg.Graph
	var eids []uint64
	var err error

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		neo4jContainer, err = startContainer(ctx, username, pwd)
		Expect(err).To(BeNil(), "Container should start")
		//port, err := neo4jContainer.MappedPort(ctx, "7687")
		Expect(err).To(BeNil(), "Port should be resolved")
		address := fmt.Sprintf("%s:%d", uri, port)
		driver, err = neo4j.NewDriver(address, neo4j.BasicAuth(username, pwd, ""))
		Expect(err).To(BeNil(), "Driver should be created")
		err = cmdutil.ReadJSONOrYAML("lsaneo/lsaneo.config.yaml", &cfg)
		Expect(err).To(BeNil(), "Could not read file: %s", "lsaneo/lsaneo.config.yaml")
		InitNamespaceTrie(&cfg)
		grph, err = cmdutil.ReadJSONGraph([]string{"testdata/config_test.json"}, nil)
		Expect(err).To(BeNil(), "Could not read file: %s", "testdata/test.json")
	})

	AfterEach(func() {
		Close(driver, "Driver")
		Expect(neo4jContainer.Terminate(ctx)).To(BeNil(), "Container should stop")
	})

	It("Post to database", func() {
		drv := NewDriver(driver, db)
		session = drv.NewSession()
		defer session.Close()
		tx, err = session.BeginTransaction()
		Expect(err).To(BeNil(), "must be valid transaction")
		eids, err = SaveGraph(ls.DefaultContext(), session, tx, grph, selectEntity, cfg, 0)
		// Expect(err).To(BeNil(), "save graph error")
		// err := tx.Commit()
		// Expect(err).To(BeNil(), "unable to post graph to database")
	})

	It("Load from database", func() {
		drv := NewDriver(driver, db)
		session = drv.NewSession()
		defer session.Close()
		tx, err = session.BeginTransaction()
		Expect(err).To(BeNil(), "must be valid transaction")
		expectedGraph := ls.NewDocumentGraph()
		_, err = loadEntityNodes(tx, expectedGraph, eids, cfg, findNeighbors, selectEntity)
		Expect(err).To(BeNil(), "unable to load nodes connected to entity", err)
		// graph isomorphism
		if !lpg.CheckIsomorphism(grph, expectedGraph, checkNodeEquivalence, checkEdgeEquivalence) {
			log.Fatalf("Result:\n%s\nExpected:\n%s", testPrintGraph(grph), testPrintGraph(expectedGraph))
		}
	})
})
