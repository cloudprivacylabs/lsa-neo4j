package neo4j

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
)

func TestMergeNeo4j(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MergeNeo4j Suite")
}

var _ = Describe("Merge", func() {
	var ctx context.Context
	var neo4jContainer testcontainers.Container
	var session *Session
	var driver neo4j.Driver
	var tx neo4j.Transaction

	var cfg Config
	var memGraph *lpg.Graph
	var err error
	var deltas []Delta
	var nids = make(map[*lpg.Node]int64)
	var eids = make(map[*lpg.Edge]int64)

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
		memGraph, err = cmdutil.ReadJSONGraph([]string{"testdata/merge_11.json"}, nil)
		Expect(err).To(BeNil(), "Could not read file: %s", "testdata/merge_11.json")
	})

	AfterEach(func() {
		Close(driver, "Merge")
		Expect(neo4jContainer.Terminate(ctx)).To(BeNil(), "Container should stop")
	})

	It("Merge in-memory graph to db graph", func() {
		// post graph to empty database
		drv := NewDriver(driver, db)
		session = drv.NewSession()
		defer session.Close()
		tx, err = session.BeginTransaction()
		Expect(err).To(BeNil(), "must be valid transaction")
		deltas, err = Merge(memGraph, ls.NewDocumentGraph(), nids, eids, cfg)
		Expect(err).To(BeNil(), "unable to post graph to empty database with merge")
		// apply deltas to database objects
		for _, delta := range SelectDelta(deltas, func(d Delta) bool {
			_, ok := d.(CreateNodeDelta)
			return ok
		}) {
			err := delta.Run(tx, nids, eids, cfg)
			Expect(err).To(BeNil(), "error posting to DB with delta: %v", delta)
		}
		for _, delta := range SelectDelta(deltas, func(d Delta) bool {
			_, ok := d.(CreateNodeDelta)
			return !ok
		}) {
			err := delta.Run(tx, nids, eids, cfg)
			Expect(err).To(BeNil(), "error posting to DB with delta: %v", delta)
		}
		err := tx.Commit()
		Expect(err).To(BeNil(), "error committing transaction to DB")
		// compare new db graph to expected graph
		expectedGraph, err := testLoadGraph("testdata/merge_12.json")
		drv = NewDriver(driver, db)
		session = drv.NewSession()
		defer session.Close()
		tx, err = session.BeginTransaction()

		dbGraph, nids, eids, err := session.LoadDBGraph(tx, memGraph, cfg)
		if !lpg.CheckIsomorphism(dbGraph, expectedGraph, checkNodeEquivalence, checkEdgeEquivalence) {
			log.Fatalf("Result:\n%s\nExpected:\n%s", testPrintGraph(dbGraph), testPrintGraph(expectedGraph))
		}

		// load graph from database and merge in-memory graph onto db graph
		drv = NewDriver(driver, db)
		session = drv.NewSession()
		defer session.Close()
		tx, err = session.BeginTransaction()
		Expect(err).To(BeNil(), "must be valid transaction")
		dbGraph, nids, eids, err = session.LoadDBGraph(tx, memGraph, cfg)
		Expect(err).To(BeNil(), "cannot load graph from database")
		deltas, err = Merge(memGraph, dbGraph, nids, eids, cfg)
		Expect(err).To(BeNil(), "unable to merge memory graph onto db graph")
		// apply deltas to database objects
		for _, delta := range deltas {
			err := delta.Run(tx, nids, eids, cfg)
			Expect(err).To(BeNil(), "error posting to DB with delta: %v", delta)
		}
		err = tx.Commit()
		Expect(err).To(BeNil(), "error committing transaction to DB")

		// reload database graph and compare to expected graph
		drv = NewDriver(driver, db)
		session = drv.NewSession()
		defer session.Close()
		tx, err = session.BeginTransaction()
		dbGraph, nids, eids, err = session.LoadDBGraph(tx, memGraph, cfg)
		if !lpg.CheckIsomorphism(dbGraph, expectedGraph, checkNodeEquivalence, checkEdgeEquivalence) {
			log.Fatalf("Result:\n%s\nExpected:\n%s", testPrintGraph(dbGraph), testPrintGraph(expectedGraph))
		}
	})
})

func testLoadGraph(fname string) (*lpg.Graph, error) {
	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	g := lpg.NewGraph()
	m := ls.JSONMarshaler{}
	if err := m.Decode(g, json.NewDecoder(f)); err != nil {
		return nil, err
	}
	return g, nil
}

func testPrintGraph(g *lpg.Graph) string {
	m := ls.JSONMarshaler{}
	result, _ := m.Marshal(g)
	return string(result)
}

func testGraphMerge(memGraphFile, dbGraphFile string) (*lpg.Graph, *lpg.Graph, []Delta, map[*lpg.Node]int64, map[*lpg.Edge]int64, Config, error) {
	dbGraph, dbNodeIds, dbEdgeIds, err := mockLoadGraph(dbGraphFile)
	if err != nil {
		return nil, nil, nil, nil, nil, Config{}, err
	}
	memGraph, err := testLoadGraph(memGraphFile)
	var cfg Config

	err = cmdutil.ReadJSONOrYAML("lsaneo/lsaneo.config.yaml", &cfg)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil, nil, nil, Config{}, err
	}
	InitNamespaceTrie(&cfg)
	d, err := Merge(memGraph, dbGraph, dbNodeIds, dbEdgeIds, cfg)
	if err != nil {
		return nil, nil, nil, nil, nil, Config{}, err
	}
	return memGraph, dbGraph, d, dbNodeIds, dbEdgeIds, cfg, nil
}

func mockLoadGraph(filename string) (*lpg.Graph, map[*lpg.Node]int64, map[*lpg.Edge]int64, error) {
	grph, err := testLoadGraph(filename)
	if err != nil {
		return nil, nil, nil, err
	}
	var ix int
	mockNodeIDs := make(map[*lpg.Node]int64)
	for nodeItr := grph.GetNodes(); nodeItr.Next(); ix++ {
		node := nodeItr.Node()
		mockNodeIDs[node] = int64(ix)
	}
	ix = 0
	mockEdgeIDs := make(map[*lpg.Edge]int64)
	for edgeItr := grph.GetEdges(); edgeItr.Next(); ix++ {
		edge := edgeItr.Edge()
		mockEdgeIDs[edge] = int64(ix)
	}
	return grph, mockNodeIDs, mockEdgeIDs, nil
}

func checkNodeEquivalence(n1, n2 *lpg.Node) bool {
	return isNodeIdentical(n1, n2)
}

func checkEdgeEquivalence(e1, e2 *lpg.Edge) bool {
	if e1.GetLabel() != e2.GetLabel() {
		return false
	}
	if !ls.IsPropertiesEqual(ls.PropertiesAsMap(e1), ls.PropertiesAsMap(e2)) {
		return false
	}
	return true
}

func TestMerge11_10(t *testing.T) {
	_, dbGraph, _, _, _, _, err := testGraphMerge("testdata/merge_11.json", "testdata/merge_10.json")
	expectedGraph, err := testLoadGraph("testdata/merge_12.json")
	if err != nil {
		t.Error(err)
	}
	if !lpg.CheckIsomorphism(dbGraph, expectedGraph, checkNodeEquivalence, checkEdgeEquivalence) {
		log.Fatalf("Result:\n%s\nExpected:\n%s", testPrintGraph(dbGraph), testPrintGraph(expectedGraph))
	}
}

func TestMerge14_13(t *testing.T) {
	_, dbGraph, _, _, _, _, err := testGraphMerge("testdata/merge_14.json", "testdata/merge_13.json")
	expectedGraph, err := testLoadGraph("testdata/merge_15.json")
	if err != nil {
		t.Error(err)
	}
	if !lpg.CheckIsomorphism(dbGraph, expectedGraph, checkNodeEquivalence, checkEdgeEquivalence) {
		log.Fatalf("Result:\n%s\nExpected:\n%s", testPrintGraph(dbGraph), testPrintGraph(expectedGraph))
	}
}

func TestMerge16_17(t *testing.T) {
	_, dbGraph, _, _, _, _, err := testGraphMerge("testdata/merge_16.json", "testdata/merge_17.json")
	expectedGraph, err := testLoadGraph("testdata/merge_1617.json")
	if err != nil {
		t.Error(err)
	}
	if !lpg.CheckIsomorphism(dbGraph, expectedGraph, checkNodeEquivalence, checkEdgeEquivalence) {
		log.Fatalf("Result:\n%s\nExpected:\n%s", testPrintGraph(dbGraph), testPrintGraph(expectedGraph))
	}
}

func TestMergeG1Emp(t *testing.T) {
	mem, db, _, _, _, _, err := testGraphMerge("testdata/g1.json", "testdata/empty.json")
	if err != nil {
		t.Error(err)
	}
	if !lpg.CheckIsomorphism(mem, db, checkNodeEquivalence, checkEdgeEquivalence) {
		t.Errorf("Not eq")
	}
}

func TestMergeFromExistEmp(t *testing.T) {
	mem, db, _, _, _, _, err := testGraphMerge("testdata/from_exist.json", "testdata/empty.json")
	if err != nil {
		t.Error(err)
	}
	if !lpg.CheckIsomorphism(mem, db, checkNodeEquivalence, checkEdgeEquivalence) {
		t.Errorf("Not eq")
	}

}
