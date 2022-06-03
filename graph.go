package neo4j

import (
	"github.com/cloudprivacylabs/opencypher/graph"
)

// SaveGraph saves all the nodes and edges of a graph in a
// transaction. Any existing nodes/edges are updated.
func SaveGraph(session *Session, g graph.Graph) error {
	session.Logf("Start: SaveGraph")
	// _, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
	// 	// Find all existing nodes
	// 	// Map of graph nodes to DB nodes
	// 	nodeIds := make(map[graph.Node]int64)
	// 	nodeMap := map[graph.Node]graph.Node{}
	// 	for nodes := g.GetNodes(); nodes.Next(); {
	// 		node := nodes.Node()
	// 		nd, _, err := session.LoadNode(tx, node.GetLabels().String())
	// 		if err != nil {
	// 			if _, ok := err.(ls.ErrNotFound); !ok {
	// 				return nil, err
	// 			}
	// 		}
	// 		if nd == nil {
	// 			nodeMap[node] = node
	// 		} else {
	// 			nodeMap[node] = nd
	// 		}
	// 	}
	// 	session.Logf("There are %d nodes", len(nodeMap))

	// 	// Insert/update nodes
	// 	for graphNode, dbNode := range nodeMap {
	// 		if dbNode == graphNode {
	// 			id, err := session.CreateNode(tx, graphNode)
	// 			if err != nil {
	// 				return nil, err
	// 			}
	// 			nodeIds[graphNode] = id
	// 		} else {
	// 			err := session.UpdateNode(tx, dbNode, graphNode)
	// 			if err != nil {
	// 				return nil, err
	// 			}
	// 		}
	// 	}

	// 	// Insert/update edges
	// 	for graphNode, _ := range nodeMap {
	// 		dbEdges, err := session.LoadEdges(tx, graphNode.GetLabels().String())
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		for edges := graphNode.GetEdges(graph.OutgoingEdge); edges.Next(); {
	// 			edge := edges.Edge()

	// 			// Is there a DB edge
	// 			var found *Edge
	// 			for i, dbEdge := range dbEdges {
	// 				if dbEdge.ToID == ls.GetNodeID(edge.GetTo()) {
	// 					found = &dbEdges[i]
	// 					break
	// 				}
	// 			}
	// 			if found != nil {
	// 				if !ls.IsPropertiesEqual(found.Properties, ls.PropertiesAsMap(edge)) {
	// 					if err := session.UpdateEdge(tx, edge); err != nil {
	// 						return nil, err
	// 					}
	// 				}
	// 			} else {
	// 				if err := session.CreateEdge(tx, edge, nodeIds); err != nil {
	// 					return nil, err
	// 				}
	// 			}
	// 		}
	// 	}
	// 	return nil, nil
	// })
	return nil
}
