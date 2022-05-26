package neo4j

import (
	"fmt"

	"github.com/cloudprivacylabs/opencypher/graph"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type Command interface {
	GetOCStmt() (string, map[string]interface{})
	Run(string, map[string]interface{}) error
}

type Creation struct {
	Config
	edge    graph.Edge
	tx      neo4j.Transaction
	nodeIds map[graph.Node]int64
}

type createNodeFromSourceAndTarget struct {
	Creation
}

func (c createNodeFromSourceAndTarget) GetOCStmt() string {
	query := fmt.Sprintf("MATCH (f) WITH f MATCH (t) WHERE ID(f)=%d AND ID(t)=%d CREATE (f)-[%s %s]->(t)",
		c.nodeIds[c.edge.GetFrom()],
		c.nodeIds[c.edge.GetTo()],
		c.MakeLabels([]string{c.edge.GetLabel()}),
		c.MakeProperties(c.edge))
	return query
}

func (c createNodeFromSourceAndTarget) Run(query string) error {
	_, err := c.tx.Run(query, c.shorten)
	if err != nil {
		return err
	}
	return nil
}

type createTargetFromSource struct {
	Creation
}

func (c createTargetFromSource) GetOCStmt() string {
	query := fmt.Sprintf("MATCH (from) WHERE ID(from) = %d CREATE (from)-[%s %s]->(to %s %s) RETURN to",
		c.nodeIds[c.edge.GetFrom()],
		c.MakeLabels([]string{c.edge.GetLabel()}),
		c.MakeProperties(c.edge),
		c.MakeLabels(c.edge.GetTo().GetLabels().Slice()),
		c.MakeProperties(c.edge.GetTo()))
	return query
}

func (c createTargetFromSource) Run(query string) error {
	idrec, err := c.tx.Run(query, c.shorten)
	if err != nil {
		return err
	}
	rec, err := idrec.Single()
	if err != nil {
		return err
	}
	nd := rec.Values[0].(neo4j.Node)
	c.nodeIds[c.edge.GetTo()] = nd.Id
	return nil
}

type createSourceFromTarget struct {
	Creation
}

func (c createSourceFromTarget) GetOCStmt() string {
	query := fmt.Sprintf("MATCH (to) WHERE ID(to) = %d CREATE (to)<-[%s %s]-(from %s %s) RETURN from",
		c.nodeIds[c.edge.GetTo()],
		c.MakeLabels([]string{c.edge.GetLabel()}),
		c.MakeProperties(c.edge),
		c.MakeLabels(c.edge.GetFrom().GetLabels().Slice()),
		c.MakeProperties(c.edge.GetFrom()))
	return query
}

func (c createSourceFromTarget) Run(query string) error {
	idrec, err := c.tx.Run(query, c.shorten)
	if err != nil {
		return err
	}
	rec, err := idrec.Single()
	if err != nil {
		return err
	}
	nd := rec.Values[0].(neo4j.Node)
	c.nodeIds[c.edge.GetFrom()] = nd.Id
	return nil
}
