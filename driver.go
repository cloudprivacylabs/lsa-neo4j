// Package neo4j is the graph storage driver using Neo4J database.
//
// The input graph node IDs are stored in `neo4j_id` property. All
// other properties and labels are stored verbatim.
//
//
package neo4j

import (
	"fmt"
	"strings"

	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/cloudprivacylabs/opencypher/graph"

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

// CreateGraph creates a graph and returns the neo4j ID of the root node
func CreateGraph(session *Session, tx neo4j.Transaction, nodes []graph.Node) (int64, error) {
	nodeIds := make(map[graph.Node]int64)
	allNodes := make(map[graph.Node]struct{})
	entityNodes := make(map[graph.Node]struct{})
	// Find triples:
	for _, node := range nodes {
		allNodes[node] = struct{}{}
	}
	for node := range allNodes {
		hasEdges := false
		if _, exists := node.GetProperty(ls.EntitySchemaTerm); exists {
			root := node
			if exists, nid, err := session.existsDB(tx, node); exists && err == nil {
				nodeIds[node] = nid
				ls.IterateDescendants(root, func(nd graph.Node) bool {
					entityNodes[nd] = struct{}{}
					return true
				}, ls.FollowEdgesInEntity, false)
			}
		}
		if _, exists := entityNodes[node]; exists {
			continue
		}
		for edges := node.GetEdges(graph.OutgoingEdge); edges.Next(); {
			edge := edges.Edge()
			if _, exists := allNodes[edge.GetTo()]; exists {
				// Triple: edge.GetFrom(), edge, edge.GetTo()
				if err := session.processTriple(tx, edge, nodeIds); err != nil {
					return 0, err
				}
				hasEdges = true
			}
		}
		if !hasEdges {
			if _, exists := nodeIds[node]; !exists {
				id, err := session.CreateNode(tx, node)
				if err != nil {
					return 0, err
				}
				nodeIds[node] = id
			}
		}
	}
	return 0, nil
}

func (s *Session) processTriple(tx neo4j.Transaction, edge graph.Edge, nodeIds map[graph.Node]int64) error {
	// Contains both node and target nodes
	if contains(edge.GetFrom(), nodeIds) && contains(edge.GetTo(), nodeIds) {
		// (node)--edge-->(node)
		err := s.CreateEdge(tx, edge, nodeIds)
		if err != nil {
			return err
		}
		return nil
	}
	// contains only source node
	if contains(edge.GetFrom(), nodeIds) && !contains(edge.GetTo(), nodeIds) {
		// (match) --edge-->(newNode)
		vars := make(map[string]interface{})
		query := fmt.Sprintf("MATCH (from) WHERE ID(from) = %d CREATE (from)-[%s %s]->(to %s %s) RETURN to",
			nodeIds[edge.GetFrom()], makeLabels(vars, []string{edge.GetLabel()}), makeProperties(vars, ls.PropertiesAsMap(edge), nil),
			makeLabels(vars, edge.GetTo().GetLabels().Slice()), makeProperties(vars, ls.PropertiesAsMap(edge.GetTo()), nil))
		idrec, err := tx.Run(query, vars)

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
		query := fmt.Sprintf("MATCH (to) WHERE ID(to) = %d CREATE (to)<-[%s %s]-(from %s %s) RETURN from",
			nodeIds[edge.GetTo()], makeLabels(vars, []string{edge.GetLabel()}), makeProperties(vars, ls.PropertiesAsMap(edge), nil),
			makeLabels(vars, edge.GetFrom().GetLabels().Slice()), makeProperties(vars, ls.PropertiesAsMap(edge.GetFrom()), nil))
		idrec, err := tx.Run(query, vars)
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
	fromId, toId, err := s.CreateNodePair(tx, edge)
	if err != nil {
		return err
	}
	nodeIds[edge.GetFrom()] = fromId
	nodeIds[edge.GetTo()] = toId
	return nil
}

func (s *Session) existsDB(tx neo4j.Transaction, node graph.Node) (bool, int64, error) {
	if node == nil {
		return false, -1, nil
	}
	vars := make(map[string]interface{})
	labelsClause := makeLabels(vars, node.GetLabels().Slice())
	prop, _ := node.GetProperty(ls.EntityIDTerm)
	if prop == nil {
		return false, -1, nil
	}
	entityProps := map[string]*ls.PropertyValue{ls.EntityIDTerm: prop.(*ls.PropertyValue)}
	propertiesClause := makeProperties(vars, entityProps, nil)
	query := fmt.Sprintf("MATCH (n %s %s) return n", labelsClause, propertiesClause)
	idrec, err := tx.Run(query, vars)
	if err != nil {
		return false, -1, err
	}
	rec, err := idrec.Single()
	if err != nil {
		return false, -1, err
	}
	nd := rec.Values[0].(neo4j.Node)
	return true, nd.Id, nil
}

func (s *Session) CreateNodePair(tx neo4j.Transaction, edge graph.Edge) (int64, int64, error) {
	vars := make(map[string]interface{})
	fromLabelsClause := makeLabels(vars, edge.GetFrom().GetLabels().Slice())
	toLabelsClause := makeLabels(vars, edge.GetTo().GetLabels().Slice())
	fromPropertiesClause := makeProperties(vars, ls.PropertiesAsMap(edge.GetFrom()), nil)
	toPropertiesClause := makeProperties(vars, ls.PropertiesAsMap(edge.GetTo()), nil)

	var idrec neo4j.Result
	var err error
	var query string
	if edge.GetFrom() == edge.GetTo() {
		query = fmt.Sprintf("CREATE (n %s %s)-[%s %s]->(n) RETURN n",
			fromLabelsClause, fromPropertiesClause,
			makeLabels(vars, []string{edge.GetLabel()}), makeProperties(vars, ls.PropertiesAsMap(edge), nil))
		idrec, err = tx.Run(query, vars)
	} else {
		query = fmt.Sprintf("CREATE (n %s %s)-[%s %s]->(m %s %s) RETURN n, m",
			fromLabelsClause, fromPropertiesClause,
			makeLabels(vars, []string{edge.GetLabel()}), makeProperties(vars, ls.PropertiesAsMap(edge), nil),
			toLabelsClause, toPropertiesClause)
		idrec, err = tx.Run(query, vars)
	}
	if err != nil {
		return 0, 0, err
	}
	rec, err := idrec.Single()
	if err != nil {
		return 0, 0, err
	}
	if len(rec.Values) > 1 {
		return rec.Values[0].(neo4j.Node).Id, rec.Values[1].(neo4j.Node).Id, err
	}
	return rec.Values[0].(neo4j.Node).Id, rec.Values[0].(neo4j.Node).Id, err
}

func (s *Session) CreateNode(tx neo4j.Transaction, node graph.Node) (int64, error) {
	nodeVars := make(map[string]interface{})
	labelsClause := makeLabels(nodeVars, node.GetLabels().Slice())
	propertiesClause := makeProperties(nodeVars, ls.PropertiesAsMap(node), nil)
	query := fmt.Sprintf("CREATE (n %s %s) RETURN n", labelsClause, propertiesClause)
	idrec, err := tx.Run(query, nodeVars)
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
func (s *Session) CreateEdge(tx neo4j.Transaction, edge graph.Edge, nodeIds map[graph.Node]int64) error {
	vars := make(map[string]interface{})
	props := makeProperties(vars, ls.PropertiesAsMap(edge), nil)
	query := fmt.Sprintf("MATCH (f) WITH f MATCH (t) WHERE ID(f)=%d AND ID(t)=%d CREATE (f)-[%s %s]->(t)",
		nodeIds[edge.GetFrom()], nodeIds[edge.GetTo()], makeLabels(vars, []string{edge.GetLabel()}), props)
	_, err := tx.Run(query, vars)
	// MATCH (from), (to) WHERE ID(from)=%v AND ID(to)=%v CREATE (from)-[:%s %s]->(to)    <--- slow performance
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
