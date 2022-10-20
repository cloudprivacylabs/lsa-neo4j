package neo4j

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

// in db, ls.SchemaNodeIDTerm is "schemaNodeId"
// func findNode(tx neo4j.Transaction, id int64, schemaNodeId string, parent, child *lpg.Node) (neo4j.Node, error) {
// 	idrec, err := tx.Run(fmt.Sprintf("MATCH (n) WHERE ID(n) = %d RETURN n", id), map[string]interface{}{})
// 	if err != nil {
// 		return neo4j.Node{}, err
// 	}
// 	rec, err := idrec.Collect()
// 	if err != nil {
// 		return neo4j.Node{}, err
// 	}
// 	if len(rec) == 0 {
// 		sidrec, err := tx.Run(fmt.Sprintf("MATCH (n) WHERE n.`%s` = %s RETURN n", ls.SchemaNodeIDTerm, quoteStringLiteral(schemaNodeId)), map[string]interface{}{})
// 		if err != nil {
// 			return neo4j.Node{}, err
// 		}
// 		for sidrec.Next() {
// 			r := sidrec.Record()
// 		}
// 	}
// 	return rec[0].Values[0].(neo4j.Node), nil
// }

// func compareNativeNodeToDBNode(n1 *lpg.Node, n2 neo4j.Node) bool {
// 	if !reflect.DeepEqual(n1.GetLabels().Slice(), n2.Labels) {
// 		return false
// 	}
// 	eq := n1.ForEachProperty(func(s string, i interface{}) bool {
// 		if prop, ok := n2.Props[s]; ok {
// 			if !reflect.DeepEqual(prop, i) {
// 				return false
// 			}
// 			return true
// 		}
// 		return true
// 	})
// 	if !eq {
// 		return false
// 	}
// 	return true
// }

const username = "neo4j"
const pwd = "password"
const uri = "neo4j://34.213.163.7"
const port = 7687
const db = "neo4j"

// testGraphMerge (without DB)
func testGraphMerge(memGraphFile, dbGraphFile string) (*lpg.Graph, OperationQueue, error) {
	var session *Session
	address := fmt.Sprintf("%s:%d", uri, port)
	driver, err := neo4j.NewDriver(address, neo4j.BasicAuth(username, pwd, ""))

	drv := NewDriver(driver, "neo4j")
	session = drv.NewSession()
	defer session.Close()
	tx, err := session.BeginTransaction()
	if err != nil {
		return nil, OperationQueue{}, err
	}
	dbGraph, dbNodeIds, dbEdgeIds, err := mockLoadGraph(dbGraphFile)
	if err != nil {
		return nil, OperationQueue{}, err
	}
	f, err := os.Open(memGraphFile)
	if err != nil {
		return nil, OperationQueue{}, err
	}
	memGraph := lpg.NewGraph()
	m := ls.JSONMarshaler{}
	if err := m.Decode(memGraph, json.NewDecoder(f)); err != nil {
		return nil, OperationQueue{}, err
	}
	var cfg Config

	err = cmdutil.ReadJSONOrYAML("lsaneo/lsaneo.config.yaml", &cfg)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, OperationQueue{}, err
	}
	InitNamespaceTrie(&cfg)
	return Merge(ls.DefaultContext(), session, tx, memGraph, dbGraph, dbNodeIds, dbEdgeIds, cfg)
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
	dbGraph, ops, err := testGraphMerge("examples/merge_05.json", "examples/merge_04.json")
	if err != nil {
		t.Error(err)
	}
	f, err := os.Open("examples/merge_06.json")
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

	// b, err := m.Marshal(dbGraph)
	// fmt.Println(string(b))
	// fmt.Println()
	// fmt.Println()
	// b, err = m.Marshal(expectedGraph)
	// fmt.Println(string(b))
	// fmt.Println()
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
	fmt.Println(strings.Join(ops.ops, ", "))
	// t.Fatal()
}

// func TestMerge(t *testing.T) {
// var session *Session
// var driver neo4j.Driver
// var cfg Config

// drv := NewDriver(driver, "neo4j")
// session = drv.NewSession()
// defer session.Close()
// tx, err := session.BeginTransaction()
// if err != nil {
// 	t.Error(err)
// }

// 	f, err := os.Open("examples/merge_02.json")
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	g1 := lpg.NewGraph()
// 	m := ls.JSONMarshaler{}
// 	if err := m.Decode(g1, json.NewDecoder(f)); err != nil {
// 		t.Error(err)
// 	}

// 	ids, err := loadGraphByEntities(tx, lpg.NewGraph(), nil, cfg, findNeighbors, selectEntity)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	// find difference between in-mem and db nodes
// 	for nodeItr := g1.GetNodes(); nodeItr.Next(); {
// 		node := nodeItr.Node()
// 		p, ok := node.GetProperty(ls.SchemaNodeIDTerm)
// 		if !ok {
// 			t.Errorf("must have schema node id term")
// 		}
// 		edgeItr := node.GetEdges(lpg.AnyEdge)
// 		parent := edgeItr.Edge().GetFrom()
// 		child := edgeItr.Edge().GetTo()
// 		cmpNode, err := findNode(tx, ids[node], ls.AsPropertyValue(p, true).AsString(), parent, child)
// 		if err != nil {
// 			t.Error(err)
// 		}

// 	}

// 	exp := lpg.NewGraph()
// 	m = ls.JSONMarshaler{}
// 	f, err = os.Open("examples/merge_03.json")
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	if err := m.Decode(exp, json.NewDecoder(f)); err != nil {
// 		t.Error(err)
// 	}

// 	// for nodeItr := exp.GetNodes(); nodeItr.Next(); {
// 	// 	node := nodeItr.Node()
// 	// 	if node.HasLabel("test") {
// 	// 		n := dbUpdateNode{
// 	// 			nodeID:     ids[node],
// 	// 			labels:     node.GetLabels().Slice(),
// 	// 			properties: make(map[string]interface{}),
// 	// 		}
// 	// 		node.ForEachProperty(func(s string, i interface{}) bool {
// 	// 			n.properties[s] = i
// 	// 			return true
// 	// 		})
// 	// 		q := n.writeQuery()
// 	// 		if q != "" {
// 	// 			t.Errorf("invalid query")
// 	// 		}
// 	// 	}
// 	// }

// }
