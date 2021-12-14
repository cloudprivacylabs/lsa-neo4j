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
	"io"
	"strings"

	"github.com/cloudprivacylabs/lsa/pkg/ls"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type Driver struct {
	drv    neo4j.Driver
	dbName string
}

type Session struct {
	neo4j.Session
	logOutput io.Writer
}

const (
	PropNodeID    = "neo4j_id"
	PropNodeValue = "neo4j_value"
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
	return &Session{Session: s}
}

func (s *Session) SetLogOutput(w io.Writer) {
	s.logOutput = w
}

func (s *Session) Logf(format string, args ...interface{}) {
	if s.logOutput != nil {
		fmt.Fprintf(s.logOutput, format, args...)
		fmt.Fprintln(s.logOutput, "")
	}
}

func (s *Session) Close() {
	s.Session.Close()
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

// CreateNode creates a new node and returns its neo4j ID
func (s *Session) CreateNode(tx neo4j.Transaction, node ls.Node) (int64, error) {
	vars := make(map[string]interface{})
	labelsClause := makeLabels(vars, node.GetTypes().Slice())
	idAndValue := make(map[string]*ls.PropertyValue)
	if len(node.GetID()) > 0 {
		idAndValue[PropNodeID] = ls.StringPropertyValue(node.GetID())
	}
	if node.GetValue() != nil {
		idAndValue[PropNodeValue] = ls.StringPropertyValue(fmt.Sprint(node.GetValue()))
	}
	propertiesClause := makeProperties(vars, node.GetProperties(), idAndValue)
	idrec, err := tx.Run(fmt.Sprintf("CREATE (n %s %s) RETURN n", labelsClause, propertiesClause), vars)
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
func (s *Session) CreateEdge(tx neo4j.Transaction, edge ls.Edge) error {
	vars := make(map[string]interface{})
	props := makeProperties(vars, edge.GetProperties(), nil)
	vars["fromId"] = fmt.Sprint(edge.GetFrom().GetLabel())
	vars["toId"] = fmt.Sprint(edge.GetTo().GetLabel())
	_, err := tx.Run(fmt.Sprintf("MATCH (from {%s:$fromId}), (to {%s:$toId}) CREATE (from)-[:%s %s]->(to)", PropNodeID, PropNodeID, quoteBacktick(edge.GetLabelStr()), props), vars)
	if err != nil {
		return err
	}
	return nil
}

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
