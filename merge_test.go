package neo4j

import (
	"encoding/json"
	"errors"
	"fmt"
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
func testGraphMerge(memGraphFile, dbGraphFile string) (*lpg.Graph, []Delta, Config, error) {
	dbGraph, dbNodeIds, dbEdgeIds, err := mockLoadGraph(dbGraphFile)
	if err != nil {
		return nil, nil, Config{}, err
	}
	f, err := os.Open(memGraphFile)
	if err != nil {
		return nil, nil, Config{}, err
	}
	memGraph := lpg.NewGraph()
	m := ls.JSONMarshaler{}
	if err := m.Decode(memGraph, json.NewDecoder(f)); err != nil {
		return nil, nil, Config{}, err
	}
	var cfg Config

	err = cmdutil.ReadJSONOrYAML("lsaneo/lsaneo.config.yaml", &cfg)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, nil, Config{}, err
	}
	InitNamespaceTrie(&cfg)
	g, d, err := Merge(memGraph, dbGraph, dbNodeIds, dbEdgeIds, cfg)
	if err != nil {
		return nil, nil, Config{}, err
	}
	return g, d, cfg, nil
}

func mockLoadGraph(filename string) (*lpg.Graph, map[*lpg.Node]int64, map[*lpg.Edge]int64, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, nil, nil, err
	}
	grph := lpg.NewGraph()
	m := ls.JSONMarshaler{}
	if err := m.Decode(grph, json.NewDecoder(f)); err != nil {
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

func TestMergeQueries(t *testing.T) {
	dbGraph, deltas, config, err := testGraphMerge("examples/merge_14.json", "examples/merge_13.json")
	c, n, v := buildCreateNodeQueriesFromDeltas(deltas, config)
	if err != nil {
		t.Error(err)
	}
	if len(c) > 0 || len(n) > 0 || len(v) > 0 {
		t.Error("create nodes > 0, when only updates")
	}
	f, err := os.Open("examples/merge_15.json")
	if err != nil {
		t.Error(err)
	}
	expectedGraph := lpg.NewGraph()
	m := ls.JSONMarshaler{}
	if err := m.Decode(expectedGraph, json.NewDecoder(f)); err != nil {
		t.Error(err)
	}

	gotSources := make([]*lpg.Node, 0)
	expectedSources := make([]*lpg.Node, 0)
	for nodeItr := dbGraph.GetNodes(); nodeItr.Next(); {
		gotSources = append(gotSources, nodeItr.Node())
	}
	for nodeItr := expectedGraph.GetNodes(); nodeItr.Next(); {
		expectedSources = append(expectedSources, nodeItr.Node())
	}
	fmt.Println("dbGraph nodes:", dbGraph.NumNodes(), "expectedGraph nodes:", expectedGraph.NumNodes())
	fmt.Println("dbGraph edges:", dbGraph.NumEdges(), "expectedGraph edges:", expectedGraph.NumEdges())

	x, _ := os.Create("dbgraph.json")
	m.Encode(dbGraph, x)
	// fmt.Println()
	// fmt.Println()
	y, _ := os.Create("egraph.json")
	m.Encode(expectedGraph, y)
	fmt.Println()
	for g := range gotSources {
		matched := false
		for e := range expectedSources {
			eq := lpg.CheckIsomorphism(gotSources[g].GetGraph(), expectedSources[e].GetGraph(), func(n1, n2 *lpg.Node) bool {
				if !n1.GetLabels().IsEqual(n2.GetLabels()) {
					return false
				}
				// If only one of the source nodes match, return false
				if n1 == gotSources[g] {
					if n2 == expectedSources[e] {
						return true
					}
					return false
				}
				if n2 == expectedSources[e] {
					return false
				}

				// Expected properties must be a subset
				propertiesOK := true
				n2.ForEachProperty(func(k string, v interface{}) bool {
					pv, ok := v.(*ls.PropertyValue)
					if !ok {
						return true
					}
					v2, ok := n1.GetProperty(k)
					if !ok {
						log.Printf("Error at %s: %v: Property does not exist", k, v)
						propertiesOK = false
						return false
					}
					pv2, ok := v2.(*ls.PropertyValue)
					if !ok {
						log.Printf("Error at %s: %v: Not property value", k, v)
						propertiesOK = false
						return false
					}
					if !pv2.IsEqual(pv) {
						log.Printf("Error at %s: Got %v, Expected %v: Values are not equal", k, pv, pv2)
						propertiesOK = false
						return false
					}

					return true
				})
				if !propertiesOK {
					log.Printf("Properties not same")
					return false
				}
				log.Printf("True\n")
				return true
			}, func(e1, e2 *lpg.Edge) bool {
				return e1.GetLabel() == e2.GetLabel() && ls.IsPropertiesEqual(ls.PropertiesAsMap(e1), ls.PropertiesAsMap(e2))
			})
			if eq {
				matched = true
				break
			}
		}
		if !matched {
			m := ls.JSONMarshaler{}
			result, _ := m.Marshal(dbGraph)
			expected, _ := m.Marshal(expectedGraph)
			log.Fatalf("Result is different from the expected: Result:\n%s\nExpected:\n%s", string(result), string(expected))
		}
	}
	fmt.Println(deltas)
	// t.Fatal()
}
