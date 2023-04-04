package neo4j

import (
	"fmt"
	"os"

	"github.com/cloudprivacylabs/lpg/v2"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
)

// Compact a graph for storing in the database. This will remove all
// nodes that do not have an id, and collapse all value nodes to
// properties
func Compact(g *lpg.Graph) (*lpg.Graph, error) {
	output := lpg.NewGraph()
	nodeMap := make(map[*lpg.Node]*lpg.Node)
	// Get all nodes with entityId, and collapse their children
	for nodes := g.GetNodesWithProperty(ls.EntityIDTerm); nodes.Next(); {
		node := nodes.Node()
		if len(ls.AsPropertyValue(node.GetProperty(ls.EntityIDTerm)).MustStringSlice()) == 0 {
			continue
		}
		if err := compactNode(output, node, nodeMap); err != nil {
			return nil, err
		}
	}

	for _, sourceNode := range lpg.Sources(g) {
		// Link all nodes
		ls.IterateDescendantsp(sourceNode, func(node *lpg.Node, path []*lpg.Node) bool {
			mappedNode := nodeMap[node]
			if mappedNode == nil {
				return true
			}
			// This node is mapped. Connect it to its predecessor
			var predecessor, mappedPredecessor *lpg.Node
			var predecessorIndex int
			for i := len(path) - 2; i >= 0; i-- {
				mappedPredecessor = nodeMap[path[i]]
				if mappedPredecessor != nil {
					predecessor = path[i]
					predecessorIndex = i
					break
				}
			}
			// Pred cannot be nil
			if predecessor == nil {
				return true
			}

			// Collect edge properties along the path
			props := make(map[string]interface{})
			// select the last label
			var label string
			for i := predecessorIndex; i < len(path)-1; i++ {
				for _, e := range lpg.EdgesBetweenNodes(path[i], path[i+1]) {
					e.ForEachProperty(func(key string, value interface{}) bool {
						props[key] = value
						return true
					})
					label = e.GetLabel()
				}
			}

			// If already linked, don't
			linked := false
			for _, e := range lpg.EdgesBetweenNodes(mappedPredecessor, mappedNode) {
				if e.GetLabel() == label {
					linked = true
					break
				}
			}
			if !linked {
				output.NewEdge(mappedPredecessor, mappedNode, label, props)
			}
			return true
		}, func(edge *lpg.Edge) ls.EdgeFuncResult {
			return ls.FollowEdgeResult
		}, false, true)
	}

	return output, nil
}

func compactNode(target *lpg.Graph, rootNode *lpg.Node, nodeMap map[*lpg.Node]*lpg.Node) error {
	// Make a new node from the root node

	makeLabels := func(labels lpg.StringSet) []string {
		l := labels.Clone()
		l.Remove(ls.DocumentNodeTerm)
		return ls.FilterNonLayerTypes(l.Slice())
	}

	cleanProps := func(props map[string]interface{}) map[string]interface{} {
		delete(props, ls.AttributeIndexTerm)
		return props
	}
	props := cleanProps(ls.CloneProperties(rootNode))
	newNode := target.NewNode(makeLabels(rootNode.GetLabels()), props)
	nodeMap[rootNode] = newNode

	buildName := func(path []*lpg.Node) string {
		return ls.GetNodeSchemaNodeID(path[len(path)-1])
	}
	// Object nodes collected in the entity will be stored here
	intermediateNodeMap := make(map[*lpg.Node]*lpg.Node)

	findTargetNode := func(path []*lpg.Node) *lpg.Node {
		for i := len(path) - 1; i >= 0; i-- {
			if intmd, ok := intermediateNodeMap[path[i]]; ok {
				return intmd
			}
		}
		return newNode
	}

	var err error
	// Scan all nodes reachable from the rootnode that are not identifiable
	ls.IterateDescendantsp(rootNode, func(node *lpg.Node, path []*lpg.Node) bool {
		if len(path) == 1 {
			return true
		}
		switch {
		case node.HasLabel(ls.AttributeTypeValue):
			// Move value nodes to the latest intermediate node
			name := buildName(path)
			targetNode := findTargetNode(path)
			val, e := ls.GetNodeValue(node)
			if e != nil {
				err = e
				return false
			}
			if val != nil {
				val = nativeValueToNeo4jValue(val)
				if v, ok := targetNode.GetProperty(name); ok {
					if v != val {
						//err = fmt.Errorf("Property overwritten: %s", name)
						//return false
						fmt.Fprintf(os.Stderr, "Property overwritten: %s: %v\n", name, val)
					}
				}
				targetNode.SetProperty(name, val)
			}

		case node.HasLabel(ls.AttributeTypeObject):
			// Object node without ID. Copy as is and link
			targetNode := findTargetNode(path)
			newNode := target.NewNode(makeLabels(node.GetLabels()), cleanProps(ls.CloneProperties(node)))
			intermediateNodeMap[node] = newNode
			nodeMap[node] = newNode

			var edge *lpg.Edge
			for edges := node.GetEdges(lpg.IncomingEdge); edges.Next(); {
				e := edges.Edge()
				if e.GetFrom() == path[len(path)-2] {
					edge = e
					break
				}
			}

			target.NewEdge(targetNode, newNode, edge.GetLabel(), cleanProps(ls.CloneProperties(edge)))

		case node.HasLabel(ls.AttributeTypeArray):
		}
		return true
	}, func(edge *lpg.Edge) ls.EdgeFuncResult {
		// Do not follow edges that go to an identifiable node
		if len(ls.AsPropertyValue(edge.GetTo().GetProperty(ls.EntityIDTerm)).MustStringSlice()) == 0 {
			return ls.FollowEdgeResult
		}
		return ls.SkipEdgeResult
	}, false, false)
	if err != nil {
		return err
	}

	return err
}
