package neo4j

import (
	"fmt"

	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/cloudprivacylabs/opencypher/graph"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type Command interface {
	GetOCStmt() (string, map[string]interface{})
	Run(string, map[string]interface{}) error
}

type Creation struct {
	Config
	Command
	edge    graph.Edge
	tx      neo4j.Transaction
	nodeIds map[graph.Node]int64
}

type createNodeFromSourceAndTarget struct {
	Creation
	Command
}

func (c createNodeFromSourceAndTarget) GetOCStmt() (string, map[string]interface{}) {
	vars := make(map[string]interface{})
	props := makeProperties(vars, ls.PropertiesAsMap(c.edge), nil)
	query := fmt.Sprintf("MATCH (f) WITH f MATCH (t) WHERE ID(f)=%d AND ID(t)=%d CREATE (f)-[%s %s]->(t)",
		c.nodeIds[c.edge.GetFrom()], c.nodeIds[c.edge.GetTo()], makeLabels(vars, []string{c.edge.GetLabel()}), props)
	return query, vars
}
func (c createNodeFromSourceAndTarget) Run(query string, vars map[string]interface{}) error {
	_, err := c.tx.Run(query, vars)
	if err != nil {
		return err
	}
	return nil
}

type createTargetFromSource struct {
	Creation
	Command
}

func (c createTargetFromSource) GetOCStmt() (string, map[string]interface{}) {
	vars := make(map[string]interface{})
	query := fmt.Sprintf("MATCH (from) WHERE ID(from) = %d CREATE (from)-[%s %s]->(to %s %s) RETURN to",
		c.nodeIds[c.edge.GetFrom()], makeLabels(vars, []string{c.edge.GetLabel()}), makeProperties(vars, ls.PropertiesAsMap(c.edge), nil),
		makeLabels(vars, c.edge.GetTo().GetLabels().Slice()), makeProperties(vars, ls.PropertiesAsMap(c.edge.GetTo()), nil))
	return query, vars
}
func (c createTargetFromSource) Run(query string, vars map[string]interface{}) error {
	idrec, err := c.tx.Run(query, vars)
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
	Command
}

func (c createSourceFromTarget) GetOCStmt() (string, map[string]interface{}) {
	vars := make(map[string]interface{})
	query := fmt.Sprintf("MATCH (to) WHERE ID(to) = %d CREATE (to)<-[%s %s]-(from %s %s) RETURN from",
		c.nodeIds[c.edge.GetTo()], makeLabels(vars, []string{c.edge.GetLabel()}), makeProperties(vars, ls.PropertiesAsMap(c.edge), nil),
		makeLabels(vars, c.edge.GetFrom().GetLabels().Slice()), makeProperties(vars, ls.PropertiesAsMap(c.edge.GetFrom()), nil))
	return query, vars
}

func (c createSourceFromTarget) Run(query string, vars map[string]interface{}) error {
	idrec, err := c.tx.Run(query, vars)
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
