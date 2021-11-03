package neo4j

import (
	"github.com/bserdar/digraph"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

// SaveGraph saves all the nodes and edges of a graph in a
// transaction. Any existing nodes/edges are updated.
func SaveGraph(session *Session, g *digraph.Graph) error {
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		// Find all existing nodes
		// Map of graph nodes to DB nodes
		nodeMap := map[ls.Node]ls.Node{}
		for nodes := g.GetAllNodes(); nodes.HasNext(); {
			node := nodes.Next().(ls.Node)
			nd, _, err := session.LoadNode(tx, node.GetID())
			if err != nil {
				if _, ok := err.(ls.ErrNotFound); !ok {
					return nil, err
				}
			}
			if nd == nil {
				nodeMap[node] = node
			} else {
				nodeMap[node] = nd
			}
		}

		// Insert/update nodes
		for graphNode, dbNode := range nodeMap {
			if dbNode == graphNode {
				_, err := session.CreateNode(tx, graphNode)
				if err != nil {
					return nil, err
				}
			} else {
				err := session.UpdateNode(tx, dbNode, graphNode)
				if err != nil {
					return nil, err
				}
			}
		}

		// Insert/update edges
		for graphNode, _ := range nodeMap {
			dbEdges, err := session.LoadEdges(tx, graphNode.GetID())
			if err != nil {
				return nil, err
			}
			for edges := graphNode.Out(); edges.HasNext(); {
				edge := edges.Next().(ls.Edge)

				// Is there a DB edge
				var found *Edge
				for i, dbEdge := range dbEdges {
					if dbEdge.ToID == edge.GetTo().(ls.Node).GetID() {
						found = &dbEdges[i]
						break
					}
				}
				if found != nil {
					if !ls.IsPropertiesEqual(found.Properties, edge.GetProperties()) {
						if err := session.UpdateEdge(tx, edge); err != nil {
							return nil, err
						}
					}
				} else {
					if err := session.CreateEdge(tx, edge); err != nil {
						return nil, err
					}
				}
			}
		}
		return nil, nil
	})
	return err
}
