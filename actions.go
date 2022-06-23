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

type neo4jQueue interface {
	Queue(neo4j.Transaction, *JobQueue) error
}

type JobQueue struct {
	actions []neo4jAction
}

// func (q *JobQueue) Queue(ids []int64) error {
// 	q.actions = append(q.actions, &JobQueue{})
// 	return nil
// }

// // func (q *JobQueue) Run(tx neo4j.Transaction) error {
// // 	// for _, act := range q.actions {
// // 	//     if err := act.Run(tx); err != nil {
// // 	//         return err
// // 	//     }
// // 	// }
// // 	if err := q.actions[q.ix].Run(tx); err != nil {
// // 		return err
// // 	}
// // 	q.ix++
// // 	return nil
// }

// var neo4jActions = map[string]neo4jAction{
// 	"Address":     JobQueue{},
// 	"Observation": JobQueue{},
// }

type DeleteEntity struct {
	Config
	graph.Graph
	entityId int64
	dbIds    []int64
}

type CreateEntity struct {
	Config
	graph.Graph
	graph.Node
	n4jNodes []Neo4jNode
	n4jEdges []Neo4jEdge
}

// type recreateEntity struct {
// 	ce createEntity
// 	de deleteEntity
// }

// func (r recreateEntity) Run(tx neo4j.Transaction) error {
// 	err := r.de.delete(tx)
// 	if err != nil {
// 		return err
// 	}
// 	err = r.ce.create(tx)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

func (d *DeleteEntity) Queue(tx neo4j.Transaction, q *JobQueue) error {
	err, ids := loadEntityNodes(tx, d.Graph, []int64{d.entityId}, d.Config, findNeighbors, nil)
	if err != nil {
		return err
	}
	d.dbIds = ids
	q.actions = append(q.actions, d)
	return nil
	// // delete entity and all nodes reachable from this entity,
	// _, err = tx.Run("MATCH (m) where WHERE ID(m) in $ids DETACH DELETE m,n", map[string]interface{}{"ids": ids})
	// if err != nil {
	// 	return err
	// }
	// return nil
}

func (d *DeleteEntity) Run(tx neo4j.Transaction) error {
	// delete entity and all nodes reachable from this entity,
	_, err := tx.Run("MATCH (m) WHERE ID(m) in $ids DETACH DELETE m", map[string]interface{}{"ids": d.dbIds})
	if err != nil {
		return err
	}
	return nil
}

// used for query
func mapNeo4jNodes(nodes []Neo4jNode) []map[string]interface{} {
	var result = make([]map[string]interface{}, len(nodes))
	for index, item := range nodes {
		// for k, v := range item.Props {
		// 	v = v.(string)
		// 	item.Props[k] = v
		// }
		result[index] = structs.Map(item)
	}
	return result
}

func mapNeo4jEdges(edges []Neo4jEdge) []map[string]interface{} {
	var result = make([]map[string]interface{}, len(edges))
	for index, item := range edges {
		// for k, v := range item.Props {
		// 	v = v.(string)
		// 	item.Props[k] = v
		// }
		result[index] = structs.Map(item)
	}
	return result
}

func (c *CreateEntity) Queue(tx neo4j.Transaction, q *JobQueue) error {
	n4jNodes := make([]Neo4jNode, 0, c.NumNodes())
	n4jEdges := make([]Neo4jEdge, 0, c.NumEdges())
	// props := make(map[string]interface{})
	// vars := make(map[string]interface{})
	// c.Node.ForEachProperty(func(s string, in interface{}) bool {
	// 	prop := c.MakeProperties(c.Node, vars)
	// 	props[s] = prop
	// 	return true
	// })
	// entity := neo4jNode{
	// 	labels: []string{c.MakeLabels(c.Node.GetLabels().Slice())},
	// 	props:  props,
	// }
	// n4jNodes = append(n4jNodes, entity)
	ls.IterateDescendants(c.Node, func(n graph.Node) bool {
		props := make(map[string]interface{})
		vars := make(map[string]interface{})
		n.ForEachProperty(func(s string, in interface{}) bool {
			prop := c.MakeProperties(n, vars)
			props[s] = prop
			return true
		})
		node := Neo4jNode{
			Labels: []string{c.MakeLabels(n.GetLabels().Slice())},
			Props:  props,
		}
		n4jNodes = append(n4jNodes, node)
		return true
	}, func(e graph.Edge) ls.EdgeFuncResult {
		props := make(map[string]interface{})
		vars := make(map[string]interface{})
		e.ForEachProperty(func(s string, in interface{}) bool {
			prop := c.MakeProperties(e, vars)
			props[s] = prop
			return true
		})
		edge := Neo4jEdge{
			Type:  c.MakeLabels([]string{e.GetLabel()}),
			Props: props,
		}
		n4jEdges = append(n4jEdges, edge)
		return 0
	}, false)
	c.n4jNodes = n4jNodes
	c.n4jEdges = n4jEdges
	q.actions = append(q.actions, c)
	return nil
}

func (c *CreateEntity) Run(tx neo4j.Transaction) error {
	vars := make(map[string]interface{})
	query := fmt.Sprintf("CREATE (m %s %s) RETURN m",
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
	// t1 := mapNeo4jNodes(c.n4jNodes)
	// t2 := mapNeo4jEdges(c.n4jEdges)
	// fmt.Println(t1)
	// fmt.Println(t2)

	// "Property values can only be of primitive types or arrays thereof. Encountered: Map{https://lschema.org/entitySchema -> String(\"{`ls:xml/ns`:$p49,
	// "Expected value to be a map, but it was :`List{String(\":`ls:documentNode`:`ls:Object`:`ClinicalDocument`\")}
	// Exactly one relationship type must be specified for CREATE. Did you forget to prefix your relationship type with a ':'?
	// (Invalid input for function 'properties()': Expected a node, a relationship or a literal map but got List{String(":`ls:Object`:`ClinicalDocument`:`ls:documentNode`")})
	// TODO: fix query
	query = `
		UNWIND $nodeBatch AS node 
		UNWIND properties(node) AS val
		WITH val,
		[k in KEYS(val) | val[k]] AS values
		UNWIND values AS value
		UNWIND properties(value) AS prop
		UNWIND $edgeBatch AS edge 
		MATCH (m) WHERE ID(m) = $eid 
		CREATE (m)-[:test]->(n) 
		SET n.props = prop
	`
	// query = "UNWIND $nodeBatch as node UNWIND $edgeBatch as edge MATCH (m) where ID(m)=$eid CREATE (m)-[e]->(n) SET n = node SET e = edge"
	_, err = tx.Run(query, map[string]interface{}{"nodeBatch": mapNeo4jNodes(c.n4jNodes), "edgeBatch": mapNeo4jEdges(c.n4jEdges), "eid": eid})
	if err != nil {
		return err
	}

	return nil
}

// func (c createEntity) create(tx neo4j.Transaction) error {
// 	// TODO: recreate, find all reachable nodes from this node
// 	// create entity
// 	vars := make(map[string]interface{})
// 	query := fmt.Sprintf("CREATE (m %s %s), return m",
// 		c.MakeLabels(c.Node.GetLabels().Slice()),
// 		c.MakeProperties(c.Node, vars))
// 	idrec, err := tx.Run(query, vars)
// 	if err != nil {
// 		return err
// 	}
// 	rec, err := idrec.Single()
// 	if err != nil {
// 		return err
// 	}
// 	eid := rec.Values[0].(neo4j.Node).Id

// 	connectedComponents := findConnectedComponents(c.Graph, c.Node, c.Config)
// 	query = "UNWIND $nodeBatch as node UNWIND $edgeBatch as edge MATCH (m) where ID(m)=$eid CREATE (m)-[e]->(n) SET n = node, SET e = edge"
// 	_, err = tx.Run(query, map[string]interface{}{"nodeBatch": mapNeo4jObject(connectedComponents), "edgeBatch": mapNeo4jObject(edges), "eid": eid})
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// type insert struct {
// 	Config
// 	graph.Graph
// 	graph.Node
// 	entityId   int64
// 	addedNodes []neo4jNode
// }

// // insert entity nodes in the db
// // Insert should keep the nodes/edges to be inserted in a list
// func (i insert) Run(tx neo4j.Transaction) error {
// 	connectedComponents := findConnectedComponents(i.Graph, i.Node, i.Config)
// 	query := fmt.Sprintf(`
// 		UNWIND $batch as item MATCH (m) where ID(m)=$eid CREATE (m)-[%s *]->(n) SET n = item`,
// 		i.GetNodes().Node().GetEdges(graph.EdgeDir(graph.OutgoingEdge)).Edge())
// 	_, err := tx.Run(query, map[string]interface{}{"batch": mapNodes(connectedComponents), "eid": i.entityId})
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// func findConnectedComponents(grph graph.Graph, node graph.Node, cfg Config) []neo4jNode {
// 	queue := make([]graph.Node, 1)
// 	allConnectedNodes := make([]neo4jNode, 0, grph.NumNodes())
// 	queue[0] = node
// 	visited := make(map[graph.Node]struct{})
// 	for len(queue) > 0 {
// 		curr := queue[0]
// 		props := make(map[string]interface{})
// 		vars := make(map[string]interface{})
// 		curr.ForEachProperty(func(s string, in interface{}) bool {
// 			prop := cfg.MakeProperties(curr, vars)
// 			props[s] = prop
// 			return true
// 		})
// 		node := neo4jNode{
// 			labels: []string{cfg.MakeLabels(curr.GetLabels().Slice())},
// 			props:  props,
// 		}
// 		allConnectedNodes = append(allConnectedNodes, node)
// 		queue = queue[1:]
// 		if _, seen := visited[curr]; !seen {
// 			visited[curr] = struct{}{}
// 			for edgeItr := curr.GetEdges(graph.OutgoingEdge); edgeItr.Next(); {
// 				edge := edgeItr.Edge()
// 				if _, seen := visited[edge.GetTo()]; !seen {
// 					if _, exists := edge.GetTo().GetProperty(ls.EntitySchemaTerm); !exists {
// 						queue = append(queue, edge.GetTo())
// 					}
// 				}
// 			}
// 		}
// 	}
// 	return allConnectedNodes
// }

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
