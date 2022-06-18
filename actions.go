package neo4j

import (
	"fmt"

	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/cloudprivacylabs/opencypher/graph"
	"github.com/fatih/structs"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type neo4jAction interface {
	Run(neo4j.Transaction) error
}

var neo4jActions = map[string]neo4jAction{
	"Address":     recreateEntity{},
	"Observation": insert{},
}

type deleteEntity struct {
	Config
	graph.Graph
	entityId int64
}

type createEntity struct {
	Config
	graph.Graph
	graph.Node
}

type recreateEntity struct {
	ce createEntity
	de deleteEntity
}

func (r recreateEntity) Run(tx neo4j.Transaction) error {
	err := r.de.delete(tx)
	if err != nil {
		return err
	}
	err = r.ce.create(tx)
	if err != nil {
		return err
	}
	return nil
}

func (d deleteEntity) delete(tx neo4j.Transaction) error {
	err := loadEntityNodes(tx, d.Graph, []int64{d.entityId}, d.Config, findNeighbors, nil)
	if err != nil {
		return err
	}
	// delete entity and all nodes reachable from this entity,
	_, err = tx.Run("MATCH (m)-[*]->(n) WHERE ID(m)=$id DETACH DELETE m,n", map[string]interface{}{"id": d.entityId})
	if err != nil {
		return err
	}
	return nil
}

// used for query
func mapNodes(nodes []neo4jNode) []map[string]interface{} {
	var result = make([]map[string]interface{}, len(nodes))
	for index, item := range nodes {
		result[index] = structs.Map(item)
	}
	return result
}

func (c createEntity) create(tx neo4j.Transaction) error {
	// TODO: recreate, find all reachable nodes from this node
	// create entity
	vars := make(map[string]interface{})
	query := fmt.Sprintf("CREATE (m %s %s), return m",
		c.MakeLabels(c.Node.GetLabels().Slice()),
		c.MakeProperties(c.Node, vars))
	idrec, err := tx.Run(query, vars)
	if err != nil {
		return err
	}
	rec, err := idrec.Single()
	if err != nil {
		return err
	}
	eid := rec.Values[0].(neo4j.Node).Id

	connectedComponents := findConnectedComponents(c.Graph, c.Node, c.Config)
	query = fmt.Sprintf(`
		UNWIND $batch as item MATCH (m) where ID(m)=$eid CREATE (m)-[%s *]->(n) SET n = item`,
		c.GetNodes().Node().GetEdges(graph.EdgeDir(graph.OutgoingEdge)).Edge())
	_, err = tx.Run(query, map[string]interface{}{"batch": mapNodes(connectedComponents), "eid": eid})
	if err != nil {
		return err
	}
	return nil
}

type insert struct {
	Config
	graph.Graph
	graph.Node
	entityId   int64
	addedNodes []neo4jNode
}

// insert entity nodes in the db
// Insert should keep the nodes/edges to be inserted in a list
func (i insert) Run(tx neo4j.Transaction) error {
	connectedComponents := findConnectedComponents(i.Graph, i.Node, i.Config)
	query := fmt.Sprintf(`
		UNWIND $batch as item MATCH (m) where ID(m)=$eid CREATE (m)-[%s *]->(n) SET n = item`,
		i.GetNodes().Node().GetEdges(graph.EdgeDir(graph.OutgoingEdge)).Edge())
	_, err := tx.Run(query, map[string]interface{}{"batch": mapNodes(connectedComponents), "eid": i.entityId})
	if err != nil {
		return err
	}
	return nil
}

func findConnectedComponents(grph graph.Graph, node graph.Node, cfg Config) []neo4jNode {
	queue := make([]graph.Node, 1)
	allConnectedNodes := make([]neo4jNode, 0, grph.NumNodes())
	queue[0] = node
	visited := make(map[graph.Node]struct{})
	for len(queue) > 0 {
		curr := queue[0]
		props := make(map[string]interface{})
		vars := make(map[string]interface{})
		curr.ForEachProperty(func(s string, in interface{}) bool {
			prop := cfg.MakeProperties(curr, vars)
			props[s] = prop
			return true
		})
		node := neo4jNode{
			labels: []string{cfg.MakeLabels(curr.GetLabels().Slice())},
			props:  props,
		}
		allConnectedNodes = append(allConnectedNodes, node)
		queue = queue[1:]
		if _, seen := visited[curr]; !seen {
			visited[curr] = struct{}{}
			for edgeItr := curr.GetEdges(graph.OutgoingEdge); edgeItr.Next(); {
				edge := edgeItr.Edge()
				if _, seen := visited[edge.GetTo()]; !seen {
					if _, exists := edge.GetTo().GetProperty(ls.EntitySchemaTerm); !exists {
						queue = append(queue, edge.GetTo())
					}
				}
			}
		}
	}
	return allConnectedNodes
}

type createEdgeToSourceAndTarget struct {
	Config
	edge graph.Edge
}

func (c createEdgeToSourceAndTarget) GetOCStmt(nodeIds map[graph.Node]int64) (string, map[string]interface{}) {
	vars := make(map[string]interface{})
	query := fmt.Sprintf("MATCH (f) WITH f MATCH (t) WHERE ID(f)=%d AND ID(t)=%d CREATE (f)-[%s %s]->(t)",
		nodeIds[c.edge.GetFrom()],
		nodeIds[c.edge.GetTo()],
		c.MakeLabels([]string{c.edge.GetLabel()}),
		c.MakeProperties(c.edge, vars))
	return query, vars
}

func (c createEdgeToSourceAndTarget) Run(tx neo4j.Transaction, nodeIds map[graph.Node]int64) error {
	query, vars := c.GetOCStmt(nodeIds)
	_, err := tx.Run(query, vars)
	if err != nil {
		return err
	}
	return nil
}

type createTargetFromSource struct {
	Config
	edge graph.Edge
}

func (c createTargetFromSource) GetOCStmt(nodeIds map[graph.Node]int64) (string, map[string]interface{}) {
	vars := make(map[string]interface{})
	query := fmt.Sprintf("MATCH (from) WHERE ID(from) = %d CREATE (from)-[%s %s]->(to %s %s) RETURN to",
		nodeIds[c.edge.GetFrom()],
		c.MakeLabels([]string{c.edge.GetLabel()}),
		c.MakeProperties(c.edge, vars),
		c.MakeLabels(c.edge.GetTo().GetLabels().Slice()),
		c.MakeProperties(c.edge.GetTo(), vars))
	return query, vars
}

func (c createTargetFromSource) Run(tx neo4j.Transaction, nodeIds map[graph.Node]int64) error {
	query, vars := c.GetOCStmt(nodeIds)
	idrec, err := tx.Run(query, vars)
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

func (c createSourceFromTarget) GetOCStmt(nodeIds map[graph.Node]int64) (string, map[string]interface{}) {
	vars := make(map[string]interface{})
	query := fmt.Sprintf("MATCH (to) WHERE ID(to) = %d CREATE (to)<-[%s %s]-(from %s %s) RETURN from",
		nodeIds[c.edge.GetTo()],
		c.MakeLabels([]string{c.edge.GetLabel()}),
		c.MakeProperties(c.edge, vars),
		c.MakeLabels(c.edge.GetFrom().GetLabels().Slice()),
		c.MakeProperties(c.edge.GetFrom(), vars))
	return query, vars
}

func (c createSourceFromTarget) Run(tx neo4j.Transaction, nodeIds map[graph.Node]int64) error {
	query, vars := c.GetOCStmt(nodeIds)
	idrec, err := tx.Run(query, vars)
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

type createNodePair struct {
	Config
	edge graph.Edge
}

func (c createNodePair) GetOCStmt(nodeIds map[graph.Node]int64) (string, map[string]interface{}) {
	vars := make(map[string]interface{})
	fromLabelsClause := c.MakeLabels(c.edge.GetFrom().GetLabels().Slice())
	toLabelsClause := c.MakeLabels(c.edge.GetTo().GetLabels().Slice())
	fromPropertiesClause := c.MakeProperties(c.edge.GetFrom(), vars)
	toPropertiesClause := c.MakeProperties(c.edge.GetTo(), vars)

	var query string
	if c.edge.GetFrom() == c.edge.GetTo() {
		query = fmt.Sprintf("CREATE (n %s %s)-[%s %s]->(n) RETURN n",
			fromLabelsClause, fromPropertiesClause,
			c.MakeLabels([]string{c.edge.GetLabel()}),
			c.MakeProperties(c.edge, vars))
	} else {
		query = fmt.Sprintf("CREATE (n %s %s)-[%s %s]->(m %s %s) RETURN n, m",
			fromLabelsClause, fromPropertiesClause,
			c.MakeLabels([]string{c.edge.GetLabel()}),
			c.MakeProperties(c.edge, vars),
			toLabelsClause, toPropertiesClause)
	}
	return query, vars
}

// "CREATE (n :`Person`:`ls:documentNode` {`ls:entityId`:$p0,`ls:entitySchema`:$p1,`https://lschema.org/entityId`:$p2,`https://lschema.org/entitySchema`:$p3})-[:` ` ]->(m :`ls:documentNode` {`value`:$p4}) RETURN n, m"

func (c createNodePair) Run(tx neo4j.Transaction, nodeIds map[graph.Node]int64) error {
	query, vars := c.GetOCStmt(nodeIds)
	idrec, err := tx.Run(query, vars)
	if err != nil {
		return err
	}
	rec, err := idrec.Single()
	if err != nil {
		return err
	}
	if len(rec.Values) > 1 {
		nodeIds[c.edge.GetFrom()] = rec.Values[0].(neo4j.Node).Id
		nodeIds[c.edge.GetTo()] = rec.Values[1].(neo4j.Node).Id
		return nil
	}
	nodeIds[c.edge.GetFrom()] = rec.Values[0].(neo4j.Node).Id
	nodeIds[c.edge.GetTo()] = rec.Values[0].(neo4j.Node).Id
	return nil
}

type createNode struct {
	Config
	node graph.Node
}

func (c createNode) GetOCStmt(nodeIds map[graph.Node]int64) (string, map[string]interface{}) {
	nodeVars := make(map[string]interface{})
	labelsClause := c.MakeLabels(c.node.GetLabels().Slice())
	propertiesClause := c.MakeProperties(c.node, nodeVars)
	query := fmt.Sprintf("CREATE (n %s %s) RETURN n", labelsClause, propertiesClause)
	return query, nodeVars
}

func (c createNode) Run(tx neo4j.Transaction, nodeIds map[graph.Node]int64) error {
	query, vars := c.GetOCStmt(nodeIds)
	idrec, err := tx.Run(query, vars)
	if err != nil {
		return err
	}
	rec, err := idrec.Single()
	if err != nil {
		return err
	}
	nd := rec.Values[0].(neo4j.Node)
	nodeIds[c.node] = nd.Id
	return nil
}
