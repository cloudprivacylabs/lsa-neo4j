// Package neo4j is the graph storage driver using Neo4J database.
//
// This driver treats node IDs as unique identifiers in the underlying
// database. Thus if you attempt to store a node with an ID that is
// already in the database, that node is updated with the given node.
//
// The storage mapping is as follows:
//
// Node IDs are stored as the neo4j node property "neo4j_id".
//
// Node values are stored as the neo4j node property "neo4j_value"`.
//
// All node properties are stores as neo4j node properties.
//
// All node types are stored as neo4j node labels.
//
// All edge IDs are stored as the neo4j edge property "@id".
//
// Edge labels are stored as neo4j edge labels.
//
// Edge properties are stored as neo4j edge properties.
//
package neo4j

import (
	"fmt"
	"strings"

	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/cloudprivacylabs/lsa/pkg/opencypher/graph"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type Driver struct {
	drv    neo4j.Driver
	dbName string
}

type Session struct {
	neo4j.Session
}

const (
	PropNodeID = "neo4j_id"
)

type ErrMultipleFound string

func (e ErrMultipleFound) Error() string { return "Multiple found: " + string(e) }

func NewDriver(driver neo4j.Driver, databaseName string) *Driver {
	return &Driver{
		drv:    driver,
		dbName: databaseName,
	}
}

func (d *Driver) Close() {
	d.drv.Close()
}

func (d *Driver) NewSession() *Session {
	s := d.drv.NewSession(neo4j.SessionConfig{DatabaseName: d.dbName})
	return &Session{s}
}

func (s *Session) Close() {
	s.Session.Close()
}

func (s *Session) Logf(format string, a ...interface{}) {
	fmt.Println(fmt.Sprintf(format, a))
}

// LoadNode attempts to load a node with the given ID. The ID is
// looked up in id node property
func (s *Session) LoadNode(tx neo4j.Transaction, ID string) (ls.Node, int64, error) {
	ret, err := tx.Run(fmt.Sprintf("MATCH (node {%s:$id}) RETURN node", PropNodeID), map[string]interface{}{"id": ID})
	if err != nil {
		return nil, 0, err
	}
	if ret.Next() {
		record := ret.Record()
		if ret.Next() {
			return nil, 0, ErrMultipleFound(ID)
		}
		node, ok := record.Values[0].(neo4j.Node)
		if !ok {
			return nil, 0, ls.ErrNotFound(ID)
		}
		return s.MakeNode(node), node.Id, nil
	}
	return nil, 0, ls.ErrNotFound(ID)
}

// MakeNode builds a graph node from the given db node
func (s *Session) MakeNode(node neo4j.Node) ls.Node {
	retNode := ls.NewNode(fmt.Sprint(node.Props[PropNodeID]), node.Labels...)
	if x, ok := node.Props[PropNodeValue]; ok {
		if str, ok := x.(string); ok {
			retNode.SetValue(str)
		}
	}
	buildPropertyMap(node.Props, retNode.GetProperties())
	return retNode
}

func buildPropertyMap(in map[string]interface{}, out map[string]*ls.PropertyValue) {
	for k, v := range in {
		switch k {
		case PropNodeValue:
		case PropNodeID:
		default:
			switch val := v.(type) {
			case []interface{}:
				arr := make([]string, 0, len(val))
				for _, x := range val {
					arr = append(arr, fmt.Sprint(x))
				}
				out[k] = ls.StringSlicePropertyValue(arr)
			default:
				out[k] = ls.StringPropertyValue(fmt.Sprint(val))
			}
		}
	}
}

// func findConnectedSimple(nodes []graph.Node) []graph.Node {
// 	var dfs func(map[graph.Node][]graph.Node, graph.Node, []bool)
// 	dfs = func(g map[graph.Node][]graph.Node, node graph.Node, visited []bool) {
// 		visited[ls.GetNodeIndex(node)] = true
// 		for _, neighbor := range g[node] {
// 			if visited[ls.GetNodeIndex(neighbor)] == false {
// 				dfs(g, neighbor, visited)
// 			}
// 		}
// 	}
// 	n := len(nodes)
// 	grph := make(map[graph.Node][]graph.Node)
// 	for idx, node := range nodes {
// 		grph[node] = make([]graph.Node, 0)
// 		edgeItr := node.GetGraph().GetEdges()
// 		fromNode := edgeItr.Edge().GetFrom()
// 		toNode := edgeItr.Edge().GetTo()
// 		grph[fromNode] = append(grph[fromNode], toNode)
// 		ls.SetNodeIndex(fromNode, idx)
// 	}
// 	connected := make([]graph.Node, 0, n)
// 	visited := make([]bool, n)
// 	for idx, node := range nodes {
// 		if visited[idx] == false {
// 			connected = append(connected, node)
// 			dfs(grph, node, visited)
// 		}
// 	}
// 	return connected
// }

// strongly connected component
// var sccTime = 0

// func findSCC(nodes []graph.Node) []graph.Node {
// 	n := len(nodes)
// 	discoverTime := make([]int, n)
// 	low := make([]int, n)
// 	for i := range discoverTime {
// 		discoverTime[i] = -1
// 	}
// 	grph := make(map[graph.Node][]graph.Node)
// 	for idx, node := range nodes {
// 		grph[node] = make([]graph.Node, 0)
// 		edgeItr := node.GetGraph().GetEdges()
// 		fromNode := edgeItr.Edge().GetFrom()
// 		toNode := edgeItr.Edge().GetTo()
// 		grph[fromNode] = append(grph[fromNode], toNode)
// 		ls.SetNodeIndex(fromNode, idx)
// 	}
// 	var dfs func(graph.Node, []int, []int, map[graph.Node][]graph.Node, *[]graph.Node, graph.Node)
// 	dfs = func(node graph.Node, disc []int, low []int, grph map[graph.Node][]graph.Node, scc *[]graph.Node, pre graph.Node) {
// 		sccTime++
// 		disc[ls.GetNodeIndex(node)] = sccTime
// 		low[ls.GetNodeIndex(node)] = sccTime
// 		for j := 0; j < len(grph[node]); j++ {
// 			v := grph[node][j]
// 			if v == pre {
// 				continue
// 			}
// 			if disc[ls.GetNodeIndex(v)] == -1 {
// 				dfs(v, disc, low, grph, scc, node)
// 				low[ls.GetNodeIndex(node)] = min(low[ls.GetNodeIndex(node)], low[ls.GetNodeIndex(v)])
// 				if low[ls.GetNodeIndex(v)] > disc[ls.GetNodeIndex(node)] {
// 					// node -> v is critical, there is no path for v to reach back to node or previous vertices of node
// 					// *scc = append(*scc, node)
// 					*scc = append(*scc, v)
// 				}
// 			} else {
// 				low[ls.GetNodeIndex(node)] = min(low[ls.GetNodeIndex(node)], disc[ls.GetNodeIndex(v)])
// 			}
// 		}
// 	}
// 	scc := make([]graph.Node, 0, n)
// 	for i, node := range nodes {
// 		if discoverTime[i] == -1 {
// 			dfs(node, discoverTime, low, grph, &scc, node)
// 		}
// 	}
// 	sccTime = 0
// 	return scc
// }

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// CreateGraph creates a graph and returns the neo4j ID of the root node
func (s *Session) CreateGraph(tx neo4j.Transaction, nodes []graph.Node) (int64, error) {
	// connectedNodes := findSCC(nodes)
	nodeIds := make(map[graph.Node]int64)
	allNodes := make(map[graph.Node]struct{})

	// Find triples:
	for _, node := range nodes {
		allNodes[node] = struct{}{}
	}
	for node := range allNodes {
		hasEdges := false
		for edges := node.GetEdges(graph.OutgoingEdge); edges.Next(); {
			edge := edges.Edge()
			if _, exists := allNodes[edge.GetTo()]; exists {
				// Triple: edge.GetFrom(), edge, edge.GetTo()
				s.processTriple(tx, edge, nodeIds)
				hasEdges = true
			}
		}
		// Disconnected
		if !hasEdges {
			s.CreateNode(tx, node)
		}
	}
	return 0, nil
}

func (s *Session) processTriple(tx neo4j.Transaction, edge graph.Edge, nodeIds map[graph.Node]int64) error {
	// Contains both node and target nodes
	if contains(edge.GetFrom(), nodeIds) && contains(edge.GetTo(), nodeIds) {
		// (node)--edge-->(node)
		err := s.CreateEdge(tx, edge)
		if err != nil {
			return err
		}
		return nil
	}
	// contains only source node
	if contains(edge.GetFrom(), nodeIds) && !contains(edge.GetTo(), nodeIds) {
		// (match) --edge-->(newNode)
		vars := make(map[string]interface{})
		props := makeProperties(vars, ls.PropertiesAsMap(edge), nil)
		vars["toId"] = makeProperties(vars, ls.PropertiesAsMap(edge.GetTo()), nil)
		idrec, err := tx.Run(fmt.Sprintf("MATCH (from) WHERE ID(from) = %v CREATE (from)-[:%s %s]->(to {%s:$toId}) RETURN to", nodeIds[edge.GetFrom()], quoteBacktick(ls.GetNodeID(edge.GetFrom())), props, PropNodeID), vars)
		//                                                                                ^^  this should be the result of makeLabels([]string{edge.GetLabel()})
		//                                                                                    ^^ $edgeProps: makeProperties(edge properties)
		//                                                                                             ^^ (to %s $nodeProps) first s: nodeLabels, props: nodeProps
		if err != nil {
			return err
		}
		rec, err := idrec.Single()
		if err != nil {
			return err
		}
		nd := rec.Values[0].(neo4j.Node)
		nodeIds[edge.GetTo()] = nd.Id
		return nil
	}
	// contains only target node
	if !contains(edge.GetFrom(), nodeIds) && contains(edge.GetTo(), nodeIds) {
		// (newNode) --edge-->(match) --edge-->(newNode)
		vars := make(map[string]interface{})
		props := makeProperties(vars, ls.PropertiesAsMap(edge), nil)
		vars["fromId"] = makeProperties(vars, ls.PropertiesAsMap(edge.GetFrom()), nil)
		idrec, err := tx.Run(fmt.Sprintf("MATCH (to) WHERE ID(to) = %v CREATE (to)<-[:%s %s]-(from {%s:$fromId}) RETURN from", nodeIds[edge.GetTo()], quoteBacktick(ls.GetNodeID(edge.GetFrom())), props, PropNodeID), vars)
		// idrec, err := tx.Run(fmt.Sprintf("MATCH (to {%s:$toId}) MERGE (from {%s:$fromId}) CREATE (from)-[:%s %s]->(to)", PropNodeID, PropNodeID, quoteBacktick(ls.GetNodeID(edge.GetTo())), props), vars)
		if err != nil {
			return err
		}
		rec, err := idrec.Single()
		if err != nil {
			return err
		}
		nd := rec.Values[0].(neo4j.Node)
		nodeIds[edge.GetFrom()] = nd.Id
		return nil
	}
	// source,target does not exist in db
	// (newNode) --edge-->(newNode)
	fromId, err := s.CreateNode(tx, edge.GetFrom())
	if err != nil {
		return err
	}
	nodeIds[edge.GetFrom()] = fromId

	toId, err := s.CreateNode(tx, edge.GetTo())
	if err != nil {
		return err
	}
	nodeIds[edge.GetTo()] = toId

	err = s.CreateEdge(tx, edge)
	if err != nil {
		return err
	}
	return nil
}

func (s *Session) CreateNode(tx neo4j.Transaction, node graph.Node) (int64, error) {
	nodeVars := make(map[string]interface{})
	labelsClause := makeLabels(nodeVars, node.GetLabels().Slice())
	idAndValue := make(map[string]*ls.PropertyValue)
	if len(ls.GetAttributeID(node)) > 0 {
		idAndValue[PropNodeID] = ls.StringPropertyValue(ls.GetAttributeID(node))
	}
	propertiesClause := makeProperties(nodeVars, ls.PropertiesAsMap(node), idAndValue)
	idrec, err := tx.Run(fmt.Sprintf("CREATE (n %s %s) RETURN n", labelsClause, propertiesClause), nodeVars)
	//                                             $props
	if err != nil {
		return 0, err
	}
	rec, err := idrec.Single()
	if err != nil {
		return 0, err
	}
	nd := rec.Values[0].(neo4j.Node)
	return nd.Id, nil
}

// CreateEdge creates an edge. The from and to nodes of the edge must
// already be in the db. The edge should not exist in the db
func (s *Session) CreateEdge(tx neo4j.Transaction, edge graph.Edge) error {
	vars := make(map[string]interface{})
	props := makeProperties(vars, ls.PropertiesAsMap(edge), nil)
	vars["fromId"] = fmt.Sprint(ls.GetNodeID(edge.GetFrom()))
	vars["toId"] = fmt.Sprint(edge.GetTo().GetEdges(graph.OutgoingEdge).Edge().GetLabel())
	_, err := tx.Run(fmt.Sprintf("MATCH (from {%s:$fromId}), (to {%s:$toId}) CREATE (from)-[:%s %s]->(to)", PropNodeID, PropNodeID, quoteBacktick(edge.GetLabel()), props), vars)
	if err != nil {
		return err
	}
	return nil
}

func contains(node graph.Node, hm map[graph.Node]int64) bool {
	if _, exists := hm[node]; exists {
		return true
	}
	return false
}

// CreateNode creates a new node and returns its neo4j ID
// func (s *Session) CreateNode(tx neo4j.Transaction, node ls.Node) (int64, error) {
// 	vars := make(map[string]interface{})
// 	labelsClause := makeLabels(vars, node.GetTypes().Slice())
// 	idAndValue := make(map[string]*ls.PropertyValue)
// 	if len(node.GetID()) > 0 {
// 		idAndValue[PropNodeID] = ls.StringPropertyValue(node.GetID())
// 	}
// 	if node.GetValue() != nil {
// 		idAndValue[PropNodeValue] = ls.StringPropertyValue(fmt.Sprint(node.GetValue()))
// 	}
// 	propertiesClause := makeProperties(vars, node.GetProperties(), idAndValue)
// 	idrec, err := tx.Run(fmt.Sprintf("CREATE (n %s %s) RETURN n", labelsClause, propertiesClause), vars)
// 	if err != nil {
// 		return 0, err
// 	}
// 	rec, err := idrec.Single()
// 	if err != nil {
// 		return 0, err
// 	}
// 	nd := rec.Values[0].(neo4j.Node)
// 	return nd.Id, nil
// }

// UpdateNode applies the changes to the DB copy of the node. oldNode
// is the node read from the DB
func (s *Session) UpdateNode(tx neo4j.Transaction, oldNode, newNode ls.Node) error {
	// Discover modifications
	// Node IDs are the same
	var propertiesClause string
	vars := make(map[string]interface{})
	idAndValue := make(map[string]*ls.PropertyValue)
	if oldNode.GetValue() != newNode.GetValue() {
		idAndValue[PropNodeValue] = ls.StringPropertyValue(fmt.Sprint(newNode.GetValue()))
	}
	if len(idAndValue) > 0 || !ls.IsPropertiesEqual(oldNode.GetProperties(), newNode.GetProperties()) {
		propertiesClause = makeProperties(vars, newNode.GetProperties(), idAndValue)
	}
	oldTypes := oldNode.GetTypes().Slice()
	newTypes := newNode.GetTypes().Slice()
	deleteLabels := ls.StringSetSubtract(oldTypes, newTypes)
	insertLabels := ls.StringSetSubtract(newTypes, oldTypes)
	deleteLabelsClause := makeLabels(vars, deleteLabels)
	insertLabelsClause := makeLabels(vars, insertLabels)
	if len(deleteLabelsClause) == 0 && len(insertLabelsClause) == 0 && len(propertiesClause) == 0 {
		// No changes to the node
		return nil
	}
	vars[PropNodeID] = oldNode.GetID()
	stmt := fmt.Sprintf("MATCH (n {%s: $%s})", PropNodeID, PropNodeID)
	if len(deleteLabelsClause) > 0 {
		stmt += " REMOVE n" + deleteLabelsClause
	}
	if len(insertLabelsClause) > 0 {
		stmt += " SET n" + insertLabelsClause
	}
	if len(propertiesClause) > 0 {
		stmt += " SET n=" + propertiesClause
	}
	_, err := tx.Run(stmt, vars)
	return err
}

// Edge represents an edge in the database
type Edge struct {
	ID         int64
	FromID     string
	ToID       string
	Label      string
	Properties map[string]*ls.PropertyValue
}

// LoadEdges returns all outgoing edges of the given node
func (s *Session) LoadEdges(tx neo4j.Transaction, nodeID string) ([]Edge, error) {
	results, err := tx.Run(fmt.Sprintf("MATCH (from {%s: $id})-[r]-(target) RETURN r,target", PropNodeID), map[string]interface{}{"id": nodeID})
	if err != nil {
		return nil, err
	}
	ret := make([]Edge, 0)
	for results.Next() {
		rec := results.Record()
		edge := rec.Values[0].(neo4j.Relationship)
		target := rec.Values[1].(neo4j.Node)
		e := Edge{
			ID:         edge.Id,
			FromID:     nodeID,
			ToID:       fmt.Sprint(target.Props[PropNodeID]),
			Label:      edge.Type,
			Properties: make(map[string]*ls.PropertyValue),
		}
		buildPropertyMap(edge.Props, e.Properties)
		ret = append(ret, e)
	}
	return ret, nil
}

// CreateEdge creates an edge. The from and to nodes of the edge must
// already be in the db. The edge should not exist in the db
// func (s *Session) CreateEdge(tx neo4j.Transaction, edge ls.Edge) error {
// 	vars := make(map[string]interface{})
// 	props := makeProperties(vars, edge.GetProperties(), nil)
// 	vars["fromId"] = fmt.Sprint(edge.GetFrom().GetLabel())
// 	vars["toId"] = fmt.Sprint(edge.GetTo().GetLabel())
// 	_, err := tx.Run(fmt.Sprintf("MATCH (from {%s:$fromId}), (to {%s:$toId}) CREATE (from)-[:%s %s]->(to)", PropNodeID, PropNodeID, quoteBacktick(edge.GetLabelStr()), props), vars)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// UpdateEdge updates the properties of an existing edge
func (s *Session) UpdateEdge(tx neo4j.Transaction, edge ls.Edge) error {
	vars := make(map[string]interface{})
	props := makeProperties(vars, edge.GetProperties(), map[string]*ls.PropertyValue{
		"fromId": ls.StringPropertyValue(fmt.Sprint(edge.GetFrom().GetLabel())),
		"toId":   ls.StringPropertyValue(fmt.Sprint(edge.GetTo().GetLabel())),
		"lbl":    ls.StringPropertyValue(fmt.Sprint(edge.GetLabel())),
	})
	_, err := tx.Run(fmt.Sprintf("MATCH (from {%s:$fromId})-[r:$lbl]->(to {%s:$toId}) SET r=%s", PropNodeID, PropNodeID, props), vars)
	return err
}

func makeLabels(vars map[string]interface{}, types []string) string {
	out := strings.Builder{}
	for _, x := range types {
		out.WriteRune(':')
		out.WriteRune('`')
		out.WriteString(x)
		out.WriteRune('`')
	}
	return out.String()
}

func makeProperties(vars map[string]interface{}, properties map[string]*ls.PropertyValue, idAndValue map[string]*ls.PropertyValue) string {
	out := strings.Builder{}
	first := true

	buildProperties := func(m map[string]*ls.PropertyValue) {
		for k, v := range m {
			if v == nil {
				continue
			}
			if first {
				out.WriteRune('{')
				first = false
			} else {
				out.WriteRune(',')
			}
			out.WriteString(quoteBacktick(k))
			out.WriteRune(':')
			out.WriteRune('$')
			tname := fmt.Sprintf("p%d", len(vars))
			out.WriteString(tname)
			if v.IsString() {
				vars[tname] = v.AsString()
			} else if v.IsStringSlice() {
				vars[tname] = v.AsInterfaceSlice()
			}
		}
	}

	buildProperties(properties)
	buildProperties(idAndValue)

	if !first {
		out.WriteRune('}')
	}

	return out.String()
}

// Returns s quoted in backtick, for property names. Backticks are excaped using double backticks
func quoteBacktick(s string) string {
	s = strings.ReplaceAll(s, "`", "``")
	return "`" + s + "`"
}

// Returns s quoted as a string literal, in single-quotes. Any
// single-quotes are escaped with \', and \ are escaped with \\
func quoteStringLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return `'` + s + `'`
}
