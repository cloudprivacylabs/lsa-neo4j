package neo4j

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type Neo4jCache struct {
	nodes map[string]neo4j.Node
	edges map[string]neo4j.Relationship
}

func (n Neo4jCache) getNode(id string) (neo4j.Node, bool) {
	if n.nodes == nil {
		return neo4j.Node{}, false
	}
	node, exists := n.nodes[id]
	return node, exists
}

func (n Neo4jCache) getNodes(ids []string) (found []neo4j.Node, notFound []string) {
	notFound = make([]string, 0, len(ids))
	if n.nodes == nil {
		notFound = notFound[:len(ids)]
		copy(notFound, ids)
		return
	}
	found = make([]neo4j.Node, 0, len(ids))
	for _, x := range ids {
		if node, exists := n.nodes[x]; exists {
			found = append(found, node)
		} else {
			notFound = append(notFound, x)
		}
	}
	return
}

func (n Neo4jCache) getEdges(ids []string) (found []neo4j.Relationship, notFound []string) {
	notFound = make([]string, 0, len(ids))
	if n.edges == nil {
		notFound = notFound[:len(ids)]
		copy(notFound, ids)
		return
	}
	found = make([]neo4j.Relationship, 0, len(ids))
	for _, x := range ids {
		if edge, exists := n.edges[x]; exists {
			found = append(found, edge)
		} else {
			notFound = append(notFound, x)
		}
	}
	return
}

func (n Neo4jCache) getEdge(id string) (neo4j.Relationship, bool) {
	if n.edges == nil {
		return neo4j.Relationship{}, false
	}
	edge, exists := n.edges[id]
	return edge, exists
}

func (n *Neo4jCache) putNodes(nodes ...neo4j.Node) {
	if n.nodes == nil {
		n.nodes = make(map[string]neo4j.Node)
	}
	for _, x := range nodes {
		n.nodes[x.ElementId] = x
	}
}

func (n *Neo4jCache) putEdges(edges ...neo4j.Relationship) {
	if n.edges == nil {
		n.edges = make(map[string]neo4j.Relationship)
	}
	for _, x := range edges {
		n.edges[x.ElementId] = x
	}
}
