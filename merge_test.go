package neo4j

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/cloudprivacylabs/lpg/v2"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
)

func TestMergeNeo4j(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MergeNeo4j Suite")
}

var _ = Describe("Merge", func() {
	var ctx ls.Context
	var neo4jContainer testcontainers.Container
	var session *Session
	var driver neo4j.DriverWithContext
	var tx neo4j.ExplicitTransaction

	var cfg Config
	var memGraph *lpg.Graph
	var err error
	var deltas []Delta

	BeforeEach(func() {
		ctx = *ls.DefaultContext()
		var err error
		neo4jContainer, err = startContainer(ctx, username, pwd)
		Expect(err).To(BeNil(), "Container should start")
		//port, err := neo4jContainer.MappedPort(ctx, "7687")
		Expect(err).To(BeNil(), "Port should be resolved")
		address := fmt.Sprintf("%s:%d", uri, port)
		driver, err = neo4j.NewDriverWithContext(address, neo4j.BasicAuth(username, pwd, ""))
		Expect(err).To(BeNil(), "Driver should be created")
		err = cmdutil.ReadJSONOrYAML("lsaneo/lsaneo.config.yaml", &cfg)
		Expect(err).To(BeNil(), "Could not read file: %s", "lsaneo/lsaneo.config.yaml")
		InitNamespaceTrie(&cfg)
		memGraph, err = cmdutil.ReadJSONGraph([]string{"testdata/merge_11.json"}, nil)
		Expect(err).To(BeNil(), "Could not read file: %s", "testdata/merge_11.json")
	})

	AfterEach(func() {
		Close(driver, ctx, "Merge")
		Expect(neo4jContainer.Terminate(ctx)).To(BeNil(), "Container should stop")
	})

	It("Merge in-memory graph to db graph", func() {
		// post graph to empty database
		drv := NewDriver(driver, db)
		session = drv.NewSession(ctx)
		defer session.Close(ctx)
		tx, err = session.BeginTransaction(ctx)
		Expect(err).To(BeNil(), "must be valid transaction")
		dbGraph := NewDBGraph(lpg.NewGraph())
		deltas, err = Merge(memGraph, dbGraph, cfg)
		Expect(err).To(BeNil(), "unable to post graph to empty database with merge")
		// apply deltas to database objects
		for _, delta := range SelectDelta(deltas, func(d Delta) bool {
			_, ok := d.(CreateNodeDelta)
			return ok
		}) {
			err := delta.Run(&ctx, tx, session, dbGraph.NodeIds, dbGraph.EdgeIds, cfg)
			Expect(err).To(BeNil(), "error posting to DB with delta: %v", delta)
		}
		for _, delta := range SelectDelta(deltas, func(d Delta) bool {
			_, ok := d.(CreateNodeDelta)
			return !ok
		}) {
			err := delta.Run(&ctx, tx, session, dbGraph.NodeIds, dbGraph.EdgeIds, cfg)
			Expect(err).To(BeNil(), "error posting to DB with delta: %v", delta)
		}
		err := tx.Commit(ctx)
		Expect(err).To(BeNil(), "error committing transaction to DB")
		// compare new db graph to expected graph
		expectedGraph, err := testLoadGraph("testdata/merge_12.json")
		drv = NewDriver(driver, db)
		session = drv.NewSession(ctx)
		defer session.Close(ctx)
		tx, err = session.BeginTransaction(ctx)

		loadedDbGraph, err := session.LoadDBGraph(&ctx, tx, memGraph, cfg, &Neo4jCache{})
		if !lpg.CheckIsomorphism(dbGraph.G, expectedGraph, checkNodeEquivalence, checkEdgeEquivalence) {
			log.Fatalf("Result:\n%s\nExpected:\n%s", testPrintGraph(loadedDbGraph.G), testPrintGraph(expectedGraph))
		}

		// load graph from database and merge in-memory graph onto db graph
		drv = NewDriver(driver, db)
		session = drv.NewSession(ctx)
		defer session.Close(ctx)
		tx, err = session.BeginTransaction(ctx)
		Expect(err).To(BeNil(), "must be valid transaction")
		loadedDbGraph, err = session.LoadDBGraph(&ctx, tx, memGraph, cfg, &Neo4jCache{})
		Expect(err).To(BeNil(), "cannot load graph from database")
		deltas, err = Merge(memGraph, loadedDbGraph, cfg)
		Expect(err).To(BeNil(), "unable to merge memory graph onto db graph")
		// apply deltas to database objects
		for _, delta := range deltas {
			err := delta.Run(&ctx, tx, session, loadedDbGraph.NodeIds, loadedDbGraph.EdgeIds, cfg)
			Expect(err).To(BeNil(), "error posting to DB with delta: %v", delta)
		}
		err = tx.Commit(ctx)
		Expect(err).To(BeNil(), "error committing transaction to DB")

		// reload database graph and compare to expected graph
		drv = NewDriver(driver, db)
		session = drv.NewSession(ctx)
		defer session.Close(ctx)
		tx, err = session.BeginTransaction(ctx)
		loadedDbGraph, err = session.LoadDBGraph(&ctx, tx, memGraph, cfg, &Neo4jCache{})
		if !lpg.CheckIsomorphism(dbGraph.G, expectedGraph, checkNodeEquivalence, checkEdgeEquivalence) {
			log.Fatalf("Result:\n%s\nExpected:\n%s", testPrintGraph(loadedDbGraph.G), testPrintGraph(expectedGraph))
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

func testGraphMerge(memGraphFile, dbGraphFile string) (*lpg.Graph, *DBGraph, []Delta, Config, error) {
	dbGraph, err := mockLoadGraph(dbGraphFile)
	if err != nil {
		return nil, nil, nil, Config{}, err
	}
	memGraph, err := testLoadGraph(memGraphFile)
	var cfg Config

	err = cmdutil.ReadJSONOrYAML("lsaneo/lsaneo.config.yaml", &cfg)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil, Config{}, err
	}
	InitNamespaceTrie(&cfg)
	d, err := Merge(memGraph, dbGraph, cfg)
	if err != nil {
		return nil, nil, nil, Config{}, err
	}
	return memGraph, dbGraph, d, cfg, nil
}

func mockLoadGraph(filename string) (*DBGraph, error) {
	grph, err := testLoadGraph(filename)
	if err != nil {
		return nil, err
	}
	ret := NewDBGraph(grph)
	var ix int
	for nodeItr := grph.GetNodes(); nodeItr.Next(); ix++ {
		node := nodeItr.Node()
		ret.NewNode(node, strconv.Itoa(ix))
	}
	ix = 0
	for edgeItr := grph.GetEdges(); edgeItr.Next(); ix++ {
		edge := edgeItr.Edge()
		ret.NewEdge(edge, strconv.Itoa(ix))
	}
	return ret, nil
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
	_, dbGraph, _, _, err := testGraphMerge("testdata/merge_11.json", "testdata/merge_10.json")
	expectedGraph, err := testLoadGraph("testdata/merge_12.json")
	if err != nil {
		t.Error(err)
	}
	if !lpg.CheckIsomorphism(dbGraph.G, expectedGraph, checkNodeEquivalence, checkEdgeEquivalence) {
		log.Fatalf("Result:\n%s\nExpected:\n%s", testPrintGraph(dbGraph.G), testPrintGraph(expectedGraph))
	}
}

func TestMerge14_13(t *testing.T) {
	_, dbGraph, _, _, err := testGraphMerge("testdata/merge_14.json", "testdata/merge_13.json")
	expectedGraph, err := testLoadGraph("testdata/merge_15.json")
	if err != nil {
		t.Error(err)
	}
	if !lpg.CheckIsomorphism(dbGraph.G, expectedGraph, checkNodeEquivalence, checkEdgeEquivalence) {
		log.Fatalf("Result:\n%s\nExpected:\n%s", testPrintGraph(dbGraph.G), testPrintGraph(expectedGraph))
	}
}

func TestMerge16_17(t *testing.T) {
	_, dbGraph, _, _, err := testGraphMerge("testdata/merge_16.json", "testdata/merge_17.json")
	expectedGraph, err := testLoadGraph("testdata/merge_1617.json")
	if err != nil {
		t.Error(err)
	}
	if !lpg.CheckIsomorphism(dbGraph.G, expectedGraph, checkNodeEquivalence, checkEdgeEquivalence) {
		log.Fatalf("Result:\n%s\nExpected:\n%s", testPrintGraph(dbGraph.G), testPrintGraph(expectedGraph))
	}
}

func TestMergeG1Emp(t *testing.T) {
	mem, db, _, _, err := testGraphMerge("testdata/g1.json", "testdata/empty.json")
	if err != nil {
		t.Error(err)
	}
	if !lpg.CheckIsomorphism(mem, db.G, checkNodeEquivalence, checkEdgeEquivalence) {
		t.Errorf("Not eq")
	}
}

func TestMergeFromExistEmp(t *testing.T) {
	mem, db, _, _, err := testGraphMerge("testdata/from_exist.json", "testdata/empty.json")
	if err != nil {
		t.Error(err)
	}
	if !lpg.CheckIsomorphism(mem, db.G, checkNodeEquivalence, checkEdgeEquivalence) {
		t.Errorf("Not eq")
	}

}
