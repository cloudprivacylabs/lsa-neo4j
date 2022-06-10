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
	fmt.Println(fmt.Sprintf(format+":%v", a))
}

// CreateGraph creates a graph and returns the neo4j ID of the root node
func CreateGraph(session *Session, tx neo4j.Transaction, nodes []graph.Node, config Config) (int64, error) {
	nodeIds := make(map[graph.Node]int64)
	allNodes := make(map[graph.Node]struct{})
	//entityNodes := make(map[graph.Node]struct{})
	// Find triples:
	for _, node := range nodes {
		allNodes[node] = struct{}{}
	}
	for node := range allNodes {
		if _, exists := node.GetProperty(ls.EntitySchemaTerm); exists {
			// TODO: Load entity nodes here
			id := ls.AsPropertyValue(node.GetProperty(ls.EntityIDTerm)).AsString()
			if len(id) > 0 {
				if exists, nid, err := session.existsDB(tx, node, config); exists && err == nil {
					nodeIds[node] = nid
				}
			}
		}
	}

	for node := range allNodes {
		hasEdges := false
		for edges := node.GetEdges(graph.OutgoingEdge); edges.Next(); {
			edge := edges.Edge()
			if _, exists := allNodes[edge.GetTo()]; exists {
				// Triple: edge.GetFrom(), edge, edge.GetTo()
				if err := session.processTriple(tx, edge, nodeIds, config); err != nil {
					return 0, err
				}
				hasEdges = true
			}
		}
		if !hasEdges {
			if _, exists := nodeIds[node]; !exists {
				c := createNode{Config: config, node: node}
				if err := c.Run(tx, nodeIds); err != nil {
					return 0, err
				}
			}
		}
	}
	return 0, nil
}

type action interface {
	Run(tx neo4j.Transaction, nodeIds map[graph.Node]int64) error
}

func (s *Session) processTriple(tx neo4j.Transaction, edge graph.Edge, nodeIds map[graph.Node]int64, cfg Config) error {
	var a action
	hasFrom := contains(edge.GetFrom(), nodeIds)
	hasTo := contains(edge.GetTo(), nodeIds)
	switch {
	case hasFrom && hasTo:
		// Contains both node and target nodes
		// (node)--edge-->(node)
		a = createEdgeToSourceAndTarget{Config: cfg, edge: edge}
	case hasFrom && !hasTo:
		// contains only source node
		// (match) --edge-->(newNode)
		a = createTargetFromSource{Config: cfg, edge: edge}
	case !hasFrom && hasTo:
		// contains only target node
		// (newNode) --edge-->(match) --edge-->(newNode)
		a = createSourceFromTarget{Config: cfg, edge: edge}
	default:
		// source,target does not exist in db
		// (newNode) --edge-->(newNode)
		a = createNodePair{Config: cfg, edge: edge}
	}
	if err := a.Run(tx, nodeIds); err != nil {
		return err
	}
	return nil
}

func (s *Session) LoadEntityNodes(tx neo4j.Transaction, grph graph.Graph, rootIds []int64, config Config, selectEntity func(graph.Node) bool) error {
	return loadEntityNodes(tx, grph, rootIds, config, findNeighbors, selectEntity)
}

type neo4jNode struct {
	id     int64
	labels []string
	props  map[string]interface{}
}

type neo4jEdge struct {
	id      int64
	startId int64
	endId   int64
	types   string
	props   map[string]interface{}
}

func newNode(ob1 neo4j.Node) neo4jNode {
	ob2 := neo4jNode{
		id:     ob1.Id,
		labels: ob1.Labels,
		props:  ob1.Props,
	}
	return ob2
}

func newEdge(ob1 neo4j.Relationship) neo4jEdge {
	ob2 := neo4jEdge{
		id:      ob1.Id,
		startId: ob1.StartId,
		endId:   ob1.EndId,
		types:   ob1.Type,
		props:   ob1.Props,
	}
	return ob2
}

// there may be more than one record returned
func findNeighbors(tx neo4j.Transaction, ids []int64) ([]neo4jNode, []neo4jNode, []neo4jEdge, error) {
	sources := make([]neo4jNode, 0)
	targets := make([]neo4jNode, 0)
	edges := make([]neo4jEdge, 0)
	idrec, err := tx.Run("MATCH (n)-[e]->(m) where id(n) in $id return n,m,e", map[string]interface{}{"id": ids})
	if err != nil {
		return sources, targets, edges, err
	}
	for idrec.Next() {
		record := idrec.Record()
		source := record.Values[0].(neo4j.Node)
		sources = append(sources, newNode(source))
		targets = append(targets, newNode(record.Values[1].(neo4j.Node)))
		edges = append(edges, newEdge(record.Values[2].(neo4j.Relationship)))
	}
	return sources, targets, edges, nil
}

func MakeProperties(input map[string]interface{}) map[string]*ls.PropertyValue {
	ret := make(map[string]*ls.PropertyValue)
	for k, v := range input {
		switch v.(type) {
		case string:
			ret[k] = ls.StringPropertyValue(v.(string))
		case []interface{}:
			isl := v.([]interface{})
			sl := make([]string, 0, len(isl))
			for _, val := range isl {
				sl = append(sl, val.(string))
			}
			ret[k] = ls.StringSlicePropertyValue(sl)
		}
	}
	return ret
}

func loadEntityNodes(tx neo4j.Transaction, grph graph.Graph, rootIds []int64, config Config, loadNeighbors func(neo4j.Transaction, []int64) ([]neo4jNode, []neo4jNode, []neo4jEdge, error), selectEntity func(graph.Node) bool) error {
	if len(rootIds) == 0 {
		return fmt.Errorf("Empty entity schema nodes")
	}
	// neo4j IDs
	visitedNode := make(map[int64]graph.Node)
	queue := make([]int64, 0, len(rootIds))
	for _, id := range rootIds {
		queue = append(queue, id)
	}
	for len(queue) > 0 {
		srcNodes, adjNodes, adjRelationships, err := loadNeighbors(tx, queue)
		if err != nil {
			return err
		}
		if len(srcNodes) == 0 || (len(adjNodes) == 0 && len(adjRelationships) == 0) {
			break
		}
		for _, srcNode := range srcNodes {
			if _, seen := visitedNode[srcNode.id]; !seen {
				src := grph.NewNode(srcNode.labels, srcNode.props)
				labels := make([]string, 0, len(srcNode.labels))
				for _, lbl := range srcNode.labels {
					labels = append(labels, config.Expand(lbl))
				}
				src.SetLabels(graph.NewStringSet(labels...))
				tmp := MakeProperties(srcNode.props)
				for k, v := range tmp {
					src.SetProperty(config.Expand(k), v)
				}
				visitedNode[srcNode.id] = src
			}
		}
		for _, node := range adjNodes {
			if _, seen := visitedNode[node.id]; !seen {
				nd := grph.NewNode(node.labels, node.props)
				labels := make([]string, 0, len(node.labels))
				for _, lbl := range node.labels {
					labels = append(labels, config.Expand(lbl))
				}
				nd.SetLabels(graph.NewStringSet(labels...))
				tmp := MakeProperties(node.props)
				for k, v := range tmp {
					nd.SetProperty(config.Expand(k), v)
				}
				visitedNode[node.id] = nd
				if selectEntity(nd) {
					queue = append(queue, node.id)
				}
			}
			if _, ok := node.props[config.Map(ls.EntitySchemaTerm)]; !ok {
				queue = append(queue, node.id)
			}
		}
		for _, edge := range adjRelationships {
			src := visitedNode[edge.startId]
			target := visitedNode[edge.endId]
			grph.NewEdge(src, target, edge.types, edge.props)
		}
		queue = queue[len(srcNodes):]
	}
	return nil
}

func (s *Session) existsDB(tx neo4j.Transaction, node graph.Node, config Config) (bool, int64, error) {
	if node == nil {
		return false, -1, nil
	}
	vars := make(map[string]interface{})
	labelsClause := config.MakeLabels(node.GetLabels().Slice())
	propertiesClause := config.MakeProperties(node, vars)
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
