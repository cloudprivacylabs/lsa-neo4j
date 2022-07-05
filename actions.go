package neo4j

import (
	"fmt"
	"strings"

	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/cloudprivacylabs/opencypher/graph"
	"github.com/fatih/structs"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type neo4jAction interface {
	Queue(neo4j.Transaction, *JobQueue) error
	Run(neo4j.Transaction) error
}

// type neo4jQueue interface {
// 	Queue(neo4j.Transaction, *JobQueue) error
// }

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
	vars     map[string]interface{}
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
	// q.actions = append(q.actions, d)
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
	type Conform struct {
		Labels []string
		Props  []string
	}
	cnf := make([]Conform, len(nodes))
	for _, node := range nodes {
		p := make([]string, 0)
		for _, v := range node.Props {
			p = append(p, v.(string))
		}
		cnf = append(cnf, Conform{Labels: node.Labels, Props: p})
	}
	var result = make([]map[string]interface{}, len(cnf))
	for index, item := range cnf {
		// for k, v := range item.Props {
		// 	v = v.(string)
		// 	item.Props[k] = v
		// }
		result[index] = structs.Map(item)
	}
	return result
}

func mapNeo4jEdges(edges []Neo4jEdge) []map[string]interface{} {
	type Conform struct {
		StartId int64
		EndId   int64
		Type    string
		Props   []string
	}
	cnf := make([]Conform, len(edges))
	for _, edge := range edges {
		p := make([]string, 0)
		for _, v := range edge.Props {
			p = append(p, v.(string))
		}
		cnf = append(cnf, Conform{StartId: edge.StartId, EndId: edge.EndId, Type: edge.Type, Props: p})
	}
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

// capture props/labels here then move vars to build query
// map[graph.Node]int64
// standard graph.Node / graph.Edge
func (c *CreateEntity) Queue(tx neo4j.Transaction, q *JobQueue) error {
	// c.vars = make(map[string]interface{})
	// n4jNodes := make([]Neo4jNode, 0, c.NumNodes())
	// n4jEdges := make([]Neo4jEdge, 0, c.NumEdges()) // []*Neo4jNode
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
		n.ForEachProperty(func(s string, in interface{}) bool {
			prop := c.MakeProperties(n, make(map[string]interface{}))
			n.SetProperty(s, prop)
			return true
		})
		// node := Neo4jNode{
		// 	Labels: []string{c.MakeLabels(n.GetLabels().Slice())},
		// 	Props:  props,
		// }
		// n4jNodes = append(n4jNodes, node)
		return true
	}, func(e graph.Edge) ls.EdgeFuncResult {
		// props := make(map[string]interface{})
		e.ForEachProperty(func(s string, in interface{}) bool {
			prop := c.MakeProperties(e, make(map[string]interface{}))
			e.SetProperty(s, prop)
			return true
		})
		// edge := Neo4jEdge{
		// 	Type:  c.MakeLabels([]string{e.GetLabel()}),
		// 	Props: props,
		// }
		// n4jEdges = append(n4jEdges, edge)
		return 0
	}, false)
	// c.n4jNodes = n4jNodes
	// c.n4jEdges = n4jEdges
	// q.actions = append(q.actions, c)
	return nil
}

func (c *CreateEntity) Run(tx neo4j.Transaction) error {
	// query := fmt.Sprintf("CREATE (m %s %s) RETURN m",
	// 	c.MakeLabels(c.Node.GetLabels().Slice()),
	// 	c.MakeProperties(c.Node, c.vars))
	// idrec, err := tx.Run(query, c.vars)
	// if err != nil {
	// 	return err
	// }
	// _, err = idrec.Single()
	// if err != nil {
	// 	return err
	// }
	// eid := rec.Values[0].(neo4j.Node).Id
	// t1 := mapNeo4jNodes(c.n4jNodes)
	// t2 := mapNeo4jEdges(c.n4jEdges)
	// fmt.Println(t1)
	// fmt.Println(t2)

	// "Property values can only be of primitive types or arrays thereof. Encountered: Map{https://lschema.org/entitySchema -> String(\"{`ls:xml/ns`:$p49,
	// "Expected value to be a map, but it was :`List{String(\":`ls:documentNode`:`ls:Object`:`ClinicalDocument`\")}
	// Expected prop to be a map, but it was :`String(":`ls:Object`:`ClinicalDocument`:`ls:documentNode`")`)
	// Exactly one relationship type must be specified for CREATE. Did you forget to prefix your relationship type with a ':'?
	// (Invalid input for function 'properties()': Expected a node, a relationship or a literal map but got List{String(":`ls:Object`:`ClinicalDocument`:`ls:documentNode`")})
	// LABELS	[:`ls:Value`:`ls:documentNode`]
	// PROPS	[{`ls:xml/ns`:$p0,`ls:attributeIndex`:$p1,`ls:attributeName`:$p2,`ls:schemaNodeId`:$p3,`ls:value`:$p4},{`ls:attributeIndex`:$p5,`ls:attributeName`:$p6,`ls:schemaNodeId`:$p7,`ls:value`:$p8,`ls:xml/ns`:$p9},{`ls:schemaNodeId`:$p10,`ls:value`:$p11,`ls:xml/ns`:$p12,`ls:attributeIndex`:$p13,`ls:attributeName`:$p14},{`ls:attributeName`:$p15,`ls:schemaNodeId`:$p16,`ls:value`:$p17,`ls:xml/ns`:$p18,`ls:attributeIndex`:$p19},{`ls:xml/ns`:$p20,`ls:attributeIndex`:$p21,`ls:attributeName`:$p22,`ls:schemaNodeId`:$p23,`ls:value`:$p24},{`ls:attributeName`:$p25,`ls:schemaNodeId`:$p26,`ls:value`:$p27,`ls:xml/ns`:$p28,`ls:attributeIndex`:$p29}]
	// TODO: fix query

	// query = `
	// 	UNWIND $nodeBatch AS node
	// 	CREATE (n)
	// 	SET n = node

	// `
	// query = "UNWIND $nodeBatch as node UNWIND $edgeBatch as edge MATCH (m) where ID(m)=$eid CREATE (m)-[e]->(n) SET n = node SET e = edge"
	nodes := make([]graph.Node, 0, c.Graph.NumNodes())
	edges := make([]graph.Edge, 0, c.Graph.NumEdges())
	for nodeItr := c.Graph.GetNodes(); nodeItr.Next(); {
		nodes = append(nodes, nodeItr.Node())
	}
	for edgeItr := c.Graph.GetEdges(); edgeItr.Next(); {
		edges = append(edges, edgeItr.Edge())
	}
	createQuery := c.buildCreateQuery(nodes)
	idrec, err := tx.Run(createQuery, c.vars)
	if err != nil {
		return err
	}
	records, err := idrec.Collect()
	if err != nil {
		return err
	}
	// hm := make(map[*Neo4jNode]int64)
	hm := make(map[graph.Node]int64)
	// for idrec.Next() {
	// 	rec := idrec.Record()
	// 	id := rec.Values[0].(int64)
	// 	n4jNode := &Neo4jNode{
	// 		Id: id,
	// 	}
	// 	hm[n4jNode] = n4jNode.Id
	// }
	// var recIx int64
	// var nodeIx int64
	// // OUTER:
	// for _, rec := range records {
	// 	for _, node := range nodes {
	// 		if _, exists := hm[node]; exists {
	// 			for out := node.GetEdges(graph.OutgoingEdge); out.Next(); {

	// 			}
	// 			id := rec.Values[0].(int64)

	// 			if _, exists := hm[node]; !exists {
	// 				hm[node] = id
	// 				recIx++
	// 				nodeIx++
	// 				// continue OUTER
	// 			}
	// 		}
	// 	}
	// }

	// var ix int
	// for _, node := range nodes {
	// 	for edges := node.GetEdges(graph.OutgoingEdge); edges.Next(); {
	// 		edge := edges.Edge()
	// 	INNER:
	// 		for _, rec := range records[ix:] {
	// 			id := rec.Values[0].(int64)
	// 			if _, exists := hm[edge.GetFrom()]; exists {
	// 				hm[edge.GetTo()] = id
	// 				ix++
	// 				continue INNER
	// 			}
	// 		}
	// 	}
	// }
	for ix, rec := range records[0].Values {
		hm[nodes[ix]] = rec.(int64)
	}
	connectQuery := c.buildConnectQuery(edges, hm)
	_, err = tx.Run(connectQuery, c.vars)
	if err != nil {
		return err
	}

	return nil
}

func (c *CreateEntity) buildCreateQuery(nodes []graph.Node) string {
	// first create
	sb := strings.Builder{}
	for ix, node := range nodes {
		prop := c.MakeProperties(node, c.vars)
		labels := c.MakeLabels(node.GetLabels().Slice())
		if ix < len(nodes)-1 {
			sb.WriteString(fmt.Sprintf("(n%d%s %s),", ix, labels, prop))
		} else {
			sb.WriteString(fmt.Sprintf("(n%d%s %s)", ix, labels, prop))
		}
	}
	var create string
	builder := strings.Builder{}
	for ix := range nodes {
		if ix < len(nodes)-1 {
			builder.WriteString(fmt.Sprintf("ID(n%d),", ix))
		} else {
			builder.WriteString(fmt.Sprintf("ID(n%d)", ix))
		}

	}
	create = fmt.Sprintf("CREATE %s RETURN %s", sb.String(), builder.String())
	return create
}

func (c *CreateEntity) buildConnectQuery(edges []graph.Edge, hm map[graph.Node]int64) string {
	// connect
	// CREATE (n1)-[lbl props]->(n2), (n3)-[lbl props]->(n4) where n1.id=$id1 n2.id=$id2…
	// MATCH (n1), (n2), …..  where ID(n1)=id1, ID(n2)=id2…. CREATE (n1) -[...]->(n2),
	// MATCH (to) WHERE ID(to) = %d CREATE (to)<-[%s %s]-(from %s %s)
	sb := strings.Builder{}
	// builder := strings.Builder{}
	// b := strings.Builder{}
	var connect string
	for ix, edge := range edges {
		// fmt.Println(edge)
		// fmt.Println(edge.GetFrom())
		// fmt.Println(edge.GetTo())
		from := hm[edge.GetFrom()]
		to := hm[edge.GetTo()]
		label := c.MakeLabels([]string{edge.GetLabel()})
		if ix < len(edges)-1 {
			sb.WriteString(fmt.Sprintf("MATCH (n%d) MATCH (m%d) WHERE ID(n%d)=%d AND ID(m%d)=%d CREATE (n%d)-[%s]->(m%d) UNION ", ix, ix, ix, from, ix, to, ix, label, ix))
			// sb.WriteString(fmt.Sprintf("(m)-[%s]->(n),", edge.Type))
			// builder.WriteString(fmt.Sprintf("WHERE ID(m%d)=%d,", ix, ids[ix]))
		} else {
			sb.WriteString(fmt.Sprintf("MATCH (n%d) MATCH (m%d) WHERE ID(n%d)=%d AND ID(m%d)=%d CREATE (n%d)-[%s]->(m%d)", ix, ix, ix, from, ix, to, ix, label, ix))
			// sb.WriteString(fmt.Sprintf("(m)-[%s]->(n)", edge.Type))
			// builder.WriteString(fmt.Sprintf("WHERE ID(m%d)=%d", ix, ids[ix]))
		}
	}
	// connect = fmt.Sprintf("CREATE %s", sb.String())
	connect = sb.String()
	return connect
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
