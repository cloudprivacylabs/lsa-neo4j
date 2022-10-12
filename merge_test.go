package neo4j

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type dbUpdateNode struct {
	nodeID     int64
	labels     []string
	properties map[string]interface{}
}

func (n dbUpdateNode) writeQuery() string {
	return ""
}

// in db, ls.SchemaNodeIDTerm is "schemaNodeId"
func findNode(tx neo4j.Transaction, id int64, schemaNodeId string, parent, child *lpg.Node) (neo4j.Node, error) {
	idrec, err := tx.Run(fmt.Sprintf("MATCH (n) WHERE ID(n) = %d RETURN n", id), map[string]interface{}{})
	if err != nil {
		return neo4j.Node{}, err
	}
	rec, err := idrec.Collect()
	if err != nil {
		return neo4j.Node{}, err
	}
	if len(rec) == 0 {
		sidrec, err := tx.Run(fmt.Sprintf("MATCH (n) WHERE n.`%s` = %s RETURN n", ls.SchemaNodeIDTerm, quoteStringLiteral(schemaNodeId)), map[string]interface{}{})
		if err != nil {
			return neo4j.Node{}, err
		}
		for sidrec.Next() {
			r := sidrec.Record()
		}
	}
	return rec[0].Values[0].(neo4j.Node), nil
}

func compareNativeNodeToDBNode(n1 *lpg.Node, n2 neo4j.Node) bool {
	if !reflect.DeepEqual(n1.GetLabels().Slice(), n2.Labels) {
		return false
	}
	eq := n1.ForEachProperty(func(s string, i interface{}) bool {
		if prop, ok := n2.Props[s]; ok {
			if !reflect.DeepEqual(prop, i) {
				return false
			}
			return true
		}
		return true
	})
	if !eq {
		return false
	}
	return true
}

func TestMerge(t *testing.T) {
	var session *Session
	var driver neo4j.Driver
	var cfg Config

	drv := NewDriver(driver, "neo4j")
	session = drv.NewSession()
	defer session.Close()
	tx, err := session.BeginTransaction()
	if err != nil {
		t.Error(err)
	}

	f, err := os.Open("examples/merge_02.json")
	if err != nil {
		t.Error(err)
	}
	g1 := lpg.NewGraph()
	m := ls.JSONMarshaler{}
	if err := m.Decode(g1, json.NewDecoder(f)); err != nil {
		t.Error(err)
	}

	ids, err := loadGraphByEntities(tx, lpg.NewGraph(), nil, cfg, findNeighbors, selectEntity)
	if err != nil {
		t.Error(err)
	}
	// find difference between in-mem and db nodes
	for nodeItr := g1.GetNodes(); nodeItr.Next(); {
		node := nodeItr.Node()
		p, ok := node.GetProperty(ls.SchemaNodeIDTerm)
		if !ok {
			t.Errorf("must have schema node id term")
		}
		edgeItr := node.GetEdges(lpg.AnyEdge)
		parent := edgeItr.Edge().GetFrom()
		child := edgeItr.Edge().GetTo()
		cmpNode, err := findNode(tx, ids[node], ls.AsPropertyValue(p, true).AsString(), parent, child)
		if err != nil {
			t.Error(err)
		}

	}

	exp := lpg.NewGraph()
	m = ls.JSONMarshaler{}
	f, err = os.Open("examples/merge_03.json")
	if err != nil {
		t.Error(err)
	}
	if err := m.Decode(exp, json.NewDecoder(f)); err != nil {
		t.Error(err)
	}

	// for nodeItr := exp.GetNodes(); nodeItr.Next(); {
	// 	node := nodeItr.Node()
	// 	if node.HasLabel("test") {
	// 		n := dbUpdateNode{
	// 			nodeID:     ids[node],
	// 			labels:     node.GetLabels().Slice(),
	// 			properties: make(map[string]interface{}),
	// 		}
	// 		node.ForEachProperty(func(s string, i interface{}) bool {
	// 			n.properties[s] = i
	// 			return true
	// 		})
	// 		q := n.writeQuery()
	// 		if q != "" {
	// 			t.Errorf("invalid query")
	// 		}
	// 	}
	// }

}
