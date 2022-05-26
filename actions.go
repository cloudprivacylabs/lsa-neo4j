package neo4j

import (
	"fmt"

	"github.com/cloudprivacylabs/opencypher/graph"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type createNodeFromSourceAndTarget struct {
	Config
	edge graph.Edge
}

func (c createNodeFromSourceAndTarget) GetOCStmt(nodeIds map[graph.Node]int64) string {
	query := fmt.Sprintf("MATCH (f) WITH f MATCH (t) WHERE ID(f)=%d AND ID(t)=%d CREATE (f)-[%s %s]->(t)",
		nodeIds[c.edge.GetFrom()],
		nodeIds[c.edge.GetTo()],
		c.MakeLabels([]string{c.edge.GetLabel()}),
		c.MakeProperties(c.edge))
	return query
}

func (c createNodeFromSourceAndTarget) Run(tx neo4j.Transaction, nodeIds map[graph.Node]int64) error {
	query := c.GetOCStmt(nodeIds)
	_, err := tx.Run(query, c.TermMappings)
	if err != nil {
		return err
	}
	return nil
}

type createTargetFromSource struct {
	Config
	edge graph.Edge
}

func (c createTargetFromSource) GetOCStmt(nodeIds map[graph.Node]int64) string {
	query := fmt.Sprintf("MATCH (from) WHERE ID(from) = %d CREATE (from)-[%s %s]->(to %s %s) RETURN to",
		nodeIds[c.edge.GetFrom()],
		c.MakeLabels([]string{c.edge.GetLabel()}),
		c.MakeProperties(c.edge),
		c.MakeLabels(c.edge.GetTo().GetLabels().Slice()),
		c.MakeProperties(c.edge.GetTo()))
	return query
}

func (c createTargetFromSource) Run(tx neo4j.Transaction, nodeIds map[graph.Node]int64) error {
	query := c.GetOCStmt(nodeIds)
	idrec, err := tx.Run(query, c.TermMappings)
	if err != nil {
		return err
	}
	rec, err := idrec.Single()
	if err != nil {
		return err
	}
	nd := rec.Values[0].(neo4j.Node)
	nodeIds[c.edge.GetTo()] = nd.Id
	return nil
}

type createSourceFromTarget struct {
	Config
	edge graph.Edge
}

func (c createSourceFromTarget) GetOCStmt(nodeIds map[graph.Node]int64) string {
	query := fmt.Sprintf("MATCH (to) WHERE ID(to) = %d CREATE (to)<-[%s %s]-(from %s %s) RETURN from",
		nodeIds[c.edge.GetTo()],
		c.MakeLabels([]string{c.edge.GetLabel()}),
		c.MakeProperties(c.edge),
		c.MakeLabels(c.edge.GetFrom().GetLabels().Slice()),
		c.MakeProperties(c.edge.GetFrom()))
	return query
}

func (c createSourceFromTarget) Run(tx neo4j.Transaction, nodeIds map[graph.Node]int64) error {
	query := c.GetOCStmt(nodeIds)
	idrec, err := tx.Run(query, c.TermMappings)
	if err != nil {
		return err
	}
	rec, err := idrec.Single()
	if err != nil {
		return err
	}
	nd := rec.Values[0].(neo4j.Node)
	nodeIds[c.edge.GetFrom()] = nd.Id
	return nil
}
