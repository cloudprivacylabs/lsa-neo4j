package neo4j

import (
	"testing"

	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/cloudprivacylabs/opencypher"
	"github.com/cloudprivacylabs/opencypher/graph"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type expectedStruct struct {
	sources []neo4jNode
	targets []neo4jNode
	edges   []neo4jEdge
}

var expected = []expectedStruct{
	{
		sources: []neo4jNode{
			{id: 60, labels: []string{ls.DocumentNodeTerm}, props: map[string]interface{}{"ls:entitySchema": "Schema for a city"}},
		},
		targets: []neo4jNode{
			{id: 61, labels: []string{ls.DocumentNodeTerm}, props: map[string]interface{}{"value": "San Francisco"}},
			{id: 62, labels: []string{ls.DocumentNodeTerm}, props: map[string]interface{}{"value": "Denver"}},
		},
		edges: []neo4jEdge{
			{id: 28, props: map[string]interface{}{"type": "city"}, startId: 60, endId: 61},
			{id: 29, props: map[string]interface{}{"type": "city"}, startId: 60, endId: 62},
		},
	},
	{
		sources: []neo4jNode{
			{id: 61, labels: []string{ls.DocumentNodeTerm}, props: map[string]interface{}{"value": "San Francisco"}},
			{id: 62, labels: []string{ls.DocumentNodeTerm}, props: map[string]interface{}{"value": "Denver"}},
		},
	},
}

func getMockFindNeighbor(expected []expectedStruct) func(tx neo4j.Transaction, ids []int64) ([]neo4jNode, []neo4jNode, []neo4jEdge, error) {
	seq := -1
	return func(tx neo4j.Transaction, ids []int64) ([]neo4jNode, []neo4jNode, []neo4jEdge, error) {
		seq++
		x := expected[seq]
		return x.sources, x.targets, x.edges, nil
	}
}

func selectEntity(node graph.Node) bool {
	return true
}

type transaction struct{}

func (transaction) Run(cypher string, params map[string]interface{}) (neo4j.Result, error) {
	return nil, nil
}
func (transaction) Commit() error   { return nil }
func (transaction) Rollback() error { return nil }
func (transaction) Close() error    { return nil }

func TestLoadEntityNodes(t *testing.T) {
	var cfg Config
	if err := cmdutil.ReadJSONOrYAML("lsaneo/config.yaml", &cfg); err != nil {
		t.Errorf("Could not read file: %s", "lsaneo/config.yaml")
	}
	InitNamespaceTrie(&cfg)
	tx := transaction{}
	roots := make([]int64, 0, len(expected[0].sources))
	for _, node := range expected[0].sources {
		roots = append(roots, node.id)
	}
	grph := ls.NewDocumentGraph()
	err := loadEntityNodes(tx, grph, roots, cfg, getMockFindNeighbor(expected), selectEntity)
	ectx := opencypher.NewEvalContext(grph)
	v, err := opencypher.ParseAndEvaluate("MATCH (n)-[e]->(m) return n,m,e", ectx)
	if err != nil {
		t.Error()
	}

	rs := v.Get().(opencypher.ResultSet)
	expectedGraph := ls.NewDocumentGraph()

	x := rs.Rows[0]["1"].Get().(graph.Node)
	y := rs.Rows[0]["2"].Get().(graph.Node)
	edge := rs.Rows[0]["3"].Get().([]graph.Edge)
	src := expectedGraph.NewNode(x.GetLabels().Slice(), ls.CloneProperties(x))
	target := expectedGraph.NewNode(y.GetLabels().Slice(), ls.CloneProperties(y))
	expectedGraph.NewEdge(src, target, edge[0].GetLabel(), ls.CloneProperties(edge[0]))

	b := rs.Rows[1]["2"].Get().(graph.Node)
	e := rs.Rows[1]["3"].Get().([]graph.Edge)
	n := expectedGraph.NewNode(y.GetLabels().Slice(), ls.CloneProperties(b))
	expectedGraph.NewEdge(src, n, edge[0].GetLabel(), ls.CloneProperties(e[0]))

	gotSources := make([]graph.Node, 0)
	expectedSources := make([]graph.Node, 0)
	for nodeItr := grph.GetNodes(); nodeItr.Next(); {
		gotSources = append(gotSources, nodeItr.Node())
	}
	for nodeItr := expectedGraph.GetNodes(); nodeItr.Next(); {
		expectedSources = append(expectedSources, nodeItr.Node())
	}
	for g := range gotSources {
		matched := false
		for e := range expectedSources {
			eq := graph.CheckIsomorphism(gotSources[g].GetGraph(), expectedSources[e].GetGraph(), func(n1, n2 graph.Node) bool {
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
						t.Logf("Error at %s: %v: Property does not exist", k, v)
						propertiesOK = false
						return false
					}
					pv2, ok := v2.(*ls.PropertyValue)
					if !ok {
						t.Logf("Error at %s: %v: Not property value", k, v)
						propertiesOK = false
						return false
					}
					if !pv2.IsEqual(pv) {
						t.Logf("Error at %s: %v: Values are not equal", k, v)
						propertiesOK = false
						return false
					}
					return true
				})
				if !propertiesOK {
					t.Logf("Properties not same")
					return false
				}
				t.Logf("True\n")
				return true
			}, func(e1, e2 graph.Edge) bool {
				return ls.IsPropertiesEqual(ls.PropertiesAsMap(e1), ls.PropertiesAsMap(e2))
			})
			if eq {
				matched = true
				break
			}
		}
		if !matched {
			m := ls.JSONMarshaler{}
			result, _ := m.Marshal(grph)
			expected, _ := m.Marshal(expectedGraph)
			t.Errorf("Result is different from the expected: Result:\n%s\nExpected:\n%s", string(result), string(expected))
		}
	}
}
