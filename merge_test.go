package neo4j

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"testing"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
)

const username = "neo4j"
const pwd = "password"
const uri = "neo4j://34.213.163.7"
const port = 7687
const db = "neo4j"

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

// testGraphMerge (without DB)
// var session *Session
// address := fmt.Sprintf("%s:%d", uri, port)
// driver, err := neo4j.NewDriver(address, neo4j.BasicAuth(username, pwd, ""))

// drv := NewDriver(driver, "neo4j")
// session = drv.NewSession()
// defer session.Close()
// tx, err := session.BeginTransaction()
// if err != nil {
// 	return nil, OperationQueue{}, err
// }
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
