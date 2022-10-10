package neo4j

import (
	"encoding/json"
	"os"
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

	f, err := os.Open("examples/mem_merge.json")
	if err != nil {
		t.Error(err)
	}
	g1 := lpg.NewGraph()
	m := ls.JSONMarshaler{}
	if err := m.Decode(g1, json.NewDecoder(f)); err != nil {
		t.Error(err)
	}
	g2, ids, err := loadGraphByEntities(tx, lpg.NewGraph(), nil, cfg, findNeighbors, selectEntity)
	if err != nil {
		t.Error(err)
	}
	for nodeItr := g2.GetNodes(); nodeItr.Next(); {
		node := nodeItr.Node()
		if node.HasLabel("test") {
			n := dbUpdateNode{
				nodeID:     ids[node],
				labels:     node.GetLabels().Slice(),
				properties: make(map[string]interface{}),
			}
			node.ForEachProperty(func(s string, i interface{}) bool {
				n.properties[s] = i
				return true
			})
			q := n.writeQuery()
			if q != "" {
				t.Errorf("invalid query")
			}
		}
	}

	grph, err := merge(g1, g2)
	if err != nil {
		t.Error()
	}
	for nodeItr := grph.GetNodes(); nodeItr.Next(); {
		node := nodeItr.Node()
		if node.HasLabel("test") {
			node.ForEachProperty(func(s string, i interface{}) bool {
				if s == "A" {
					if i != 1 {
						t.Error()
					}
				}
				if s == "B" {
					if i != 2 {
						t.Error()
					}
				}
				if s == "C" {
					if i != 3 {
						t.Error()
					}
				}
				return true
			})
		}
	}
}
