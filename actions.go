package neo4j

import (
	"fmt"
	"strings"

	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/cloudprivacylabs/opencypher/graph"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type neo4jAction interface {
	Queue(neo4j.Transaction, *JobQueue) error
	Run(neo4j.Transaction, *JobQueue) error
}

// type neo4jQueue interface {
// 	Queue(neo4j.Transaction, *JobQueue) error
// }

type JobQueue struct {
	queueNodes createNodes
	queueEdges createEdges
	actions    []neo4jAction
	nodeBatch  int64
	edgeBatch  int64
}

type createNodes struct {
	nodes []graph.Node
}

type createEdges struct {
	edges []graph.Edge
}

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
	vars map[string]interface{}
}

func (q *JobQueue) Run(tx neo4j.Transaction) (map[graph.Node]int64, error) {
	// Go through the jobqueue, create all nodes in batches, get a node-id map
	// Combine all createNodes nodes, slice them by batch size, create each batch
	// call MakeLabels and MakeProperties during batch insertion
	// Create all edges in batches
	for _, a := range q.actions {
		if err := a.Run(tx, q); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (d *DeleteEntity) Queue(tx neo4j.Transaction, q *JobQueue) error {
	err, ids := loadEntityNodes(tx, d.Graph, []int64{d.entityId}, d.Config, findNeighbors, nil)
	if err != nil {
		return err
	}
	d.dbIds = ids
	// q.actions = append(q.actions, d)
	return nil
	// // delete entity and all nodes reachable from this entity,
	// _, err = tx.Run("MATCH (m) where WHERE ID(m) in $ids DETACH DELETE m,n", map[string]interface{}{"ids": ids})
	// if err != nil {
	// 	return err
	// }
	// return nil
}

func (d *DeleteEntity) Run(tx neo4j.Transaction, q *JobQueue) error {
	// delete entity and all nodes reachable from this entity,
	_, err := tx.Run("MATCH (m) WHERE ID(m) in $ids DETACH DELETE m", map[string]interface{}{"ids": d.dbIds})
	if err != nil {
		return err
	}
	return nil
}

func (c *CreateEntity) Queue(tx neo4j.Transaction, q *JobQueue) error {
	ls.IterateDescendants(c.Node, func(n graph.Node) bool {
		q.queueNodes.nodes = append(q.queueNodes.nodes, n)
		return true
	}, func(e graph.Edge) ls.EdgeFuncResult {
		q.queueEdges.edges = append(q.queueEdges.edges, e)
		return 0
	}, false)
	return nil
}

func (c *CreateEntity) Run(tx neo4j.Transaction, q *JobQueue) error {
	if len(q.queueNodes.nodes) == 0 && len(q.queueEdges.edges) == 0 {
		return nil
	}
	if q.edgeBatch == 0 {
		q.edgeBatch = int64(len(q.queueEdges.edges))
	}
	if q.nodeBatch == 0 {
		q.nodeBatch = int64(len(q.queueNodes.nodes))
	}
	hm := make(map[graph.Node]int64)
	var nodes []graph.Node
	if q.nodeBatch < int64(len(q.queueNodes.nodes)) {
		nodes = q.queueNodes.nodes[:q.nodeBatch]
		q.queueNodes.nodes = q.queueNodes.nodes[q.nodeBatch:]
	} else {
		nodes = q.queueNodes.nodes[:len(q.queueNodes.nodes)]
		q.queueNodes.nodes = q.queueNodes.nodes[len(q.queueNodes.nodes):]
	}
	createQuery := c.buildCreateQuery(nodes)
	idrec, err := tx.Run(createQuery, c.vars)
	if err != nil {
		return err
	}
	records, err := idrec.Single()
	if err != nil {
		return err
	}

	for ix, rec := range records.Values {
		hm[nodes[ix]] = rec.(int64)
	}
	var edges []graph.Edge
	if q.edgeBatch < int64(len(q.queueEdges.edges)) {
		edges = q.queueEdges.edges[:q.edgeBatch]
		q.queueEdges.edges = q.queueEdges.edges[q.edgeBatch:]
	} else {
		edges = q.queueEdges.edges[:len(q.queueEdges.edges)]
		q.queueEdges.edges = q.queueEdges.edges[len(q.queueEdges.edges):]
	}
	connectQuery := c.buildConnectQuery(edges, hm)
	_, err = tx.Run(connectQuery, c.vars)
	if err != nil {
		return err
	}
	return nil
}

func (c *CreateEntity) buildCreateQuery(nodes []graph.Node) string {
	sb := strings.Builder{}
	// {`ls:attributeName`:$p28,`ls:entityId`:$p29}{`ls:entityId`:$p30}
	for ix, node := range nodes {
		prop := c.MakeProperties(node, c.vars)
		labels := c.MakeLabels(node.GetLabels().Slice())
		if ix < len(nodes)-1 {
			sb.WriteString(fmt.Sprintf("(n%d%s %s),", ix, labels, prop))
		} else {
			sb.WriteString(fmt.Sprintf("(n%d%s %s) ", ix, labels, prop))
		}
	}
	builder := strings.Builder{}
	for ix := range nodes {
		if ix < len(nodes)-1 {
			builder.WriteString(fmt.Sprintf("ID(n%d),", ix))
		} else {
			builder.WriteString(fmt.Sprintf("ID(n%d)", ix))
		}
	}
	return fmt.Sprintf("CREATE %s RETURN %s", sb.String(), builder.String())
}

func (c *CreateEntity) buildConnectQuery(edges []graph.Edge, hm map[graph.Node]int64) string {
	sb := strings.Builder{}
	for ix, edge := range edges {
		from := hm[edge.GetFrom()]
		to := hm[edge.GetTo()]
		label := c.MakeLabels([]string{edge.GetLabel()})
		if ix < len(edges)-1 {
			sb.WriteString(fmt.Sprintf("MATCH (n%d) MATCH (m%d) WHERE ID(n%d)=%d AND ID(m%d)=%d CREATE (n%d)-[%s]->(m%d) UNION ", ix, ix, ix, from, ix, to, ix, label, ix))
		} else {
			sb.WriteString(fmt.Sprintf("MATCH (n%d) MATCH (m%d) WHERE ID(n%d)=%d AND ID(m%d)=%d CREATE (n%d)-[%s]->(m%d) ", ix, ix, ix, from, ix, to, ix, label, ix))
		}
	}
	return sb.String()
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
