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
	"time"

	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/cloudprivacylabs/lsa/pkg/types"
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

// SaveGraph creates a graph filtered by nodes with entity id term and returns the neo4j IDs of the entity nodes
func SaveGraph(session *Session, tx neo4j.Transaction, grph graph.Graph, selectEntity func(graph.Node) bool, config Config, batch int) ([]uint64, error) {
	eids := make([]uint64, 0)
	mappedEntities := make(map[graph.Node]uint64) // holds all neo4j id's of entity schema and nonempty entity id
	nonemptyEntityNodeIds := make([]string, 0)
	entities := make([]graph.Node, 0)
	allNodes := make(map[graph.Node]struct{})

	start := time.Now()

	for nodeItr := grph.GetNodesWithProperty(ls.EntityIDTerm); nodeItr.Next(); {
		node := nodeItr.Node()
		if _, exists := node.GetProperty(ls.EntitySchemaTerm); exists {
			id := ls.AsPropertyValue(node.GetProperty(ls.EntityIDTerm)).AsString()
			ids := ls.AsPropertyValue(node.GetProperty(ls.EntityIDTerm)).AsStringSlice()
			if len(ids) > 0 {
				nonemptyEntityNodeIds = append(nonemptyEntityNodeIds, strings.Join(ids, ","))
				entities = append(entities, node)
			}
			if id != "" {
				nonemptyEntityNodeIds = append(nonemptyEntityNodeIds, id)
				entities = append(entities, node)
			}
		}
	}

	entityDBIds, entityIds, err := session.entityDBIds(tx, nonemptyEntityNodeIds, config)
	if err != nil {
		return nil, err
	}

	// map DB ids
	for nodeItr := grph.GetNodes(); nodeItr.Next(); {
		node := nodeItr.Node()
		allNodes[node] = struct{}{}
		for ix := 0; ix < len(entityDBIds); ix++ {
			if _, exists := node.GetProperty(ls.EntitySchemaTerm); exists {
				id := ls.AsPropertyValue(node.GetProperty(ls.EntityIDTerm)).AsString()
				ids := ls.AsPropertyValue(node.GetProperty(ls.EntityIDTerm)).Slice()
				if len(ids) > 0 {
					eid := strings.Join(ids, ",")
					if entityIds[ix] == eid {
						mappedEntities[node] = uint64(entityDBIds[ix])
						eids = append(eids, mappedEntities[node])
					}
				} else {
					if entityIds[ix] == id {
						mappedEntities[node] = uint64(entityDBIds[ix])
						eids = append(eids, mappedEntities[node])
					}
				}
			}
		}
	}

	updates := make(map[string]struct{})
	creates := make(map[string]struct{})
	for _, id := range entityIds {
		updates[id] = struct{}{}
	}
	for _, id := range nonemptyEntityNodeIds {
		if _, exists := updates[id]; !exists {
			creates[id] = struct{}{}
		}
	}

	jobs := &JobQueue{
		createNodes: make([]graph.Node, 0),
		createEdges: make([]graph.Edge, 0),
		deleteNodes: make([]uint64, 0),
		deleteEdges: make([]uint64, 0),
	}
	for _, entity := range entities {
		id := ls.AsPropertyValue(entity.GetProperty(ls.EntityIDTerm)).AsString()
		ids := ls.AsPropertyValue(entity.GetProperty(ls.EntityIDTerm)).AsStringSlice()
		if id == "" {
			if len(ids) < 0 {
				continue
			}
			id = strings.Join(ids, ",")
		}
		if _, exists := updates[id]; exists {
			d := &DeleteEntity{Config: config, Graph: grph, entityId: mappedEntities[entity]}
			if err := d.Queue(tx, jobs, selectEntity); err != nil {
				return nil, err
			}
			c := &CreateEntity{Config: config, Graph: grph, Node: entity}
			if err := c.Queue(tx, jobs); err != nil {
				return nil, err
			}

		} else if _, exists = creates[id]; exists {
			c := &CreateEntity{Config: config, Graph: grph, Node: entity}
			if err := c.Queue(tx, jobs); err != nil {
				return nil, err
			}

		}
	}
	if err := jobs.Run(tx, config, mappedEntities, batch); err != nil {
		return nil, err
	}

	duration := time.Since(start)
	fmt.Println(fmt.Sprintf("time elapsed for graph creation: %v", duration))

	// Link nodes
	for node := range allNodes {
		if _, exists := node.GetProperty(ls.EntitySchemaTerm); exists {
			if err := LinkNodesForNewEntity(tx, config, node, mappedEntities); err != nil {
				return nil, err
			}
		}
	}
	return eids, nil
}

func (s *Session) LoadEntityNodes(tx neo4j.Transaction, grph graph.Graph, rootIds []uint64, config Config, selectEntity func(graph.Node) bool) error {
	_, err := loadEntityNodes(tx, grph, rootIds, config, findNeighbors, selectEntity)
	return err
}

func (s *Session) LoadEntityNodesByEntityId(tx neo4j.Transaction, grph graph.Graph, rootIds []string, config Config, selectEntity func(graph.Node) bool) error {

	idTerm := config.Map(ls.EntityIDTerm)
	res, err := tx.Run(fmt.Sprintf("match (root) where root.`%s` in $ids return id(root)", idTerm), map[string]interface{}{"ids": rootIds})
	if err != nil {
		return err
	}
	ids := make([]uint64, 0)
	for res.Next() {
		record := res.Record()
		v := record.Values[0].(int64)
		ids = append(ids, uint64(v))
	}

	_, err = loadEntityNodes(tx, grph, ids, config, findNeighbors, selectEntity)
	return err
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

func findNeighbors(tx neo4j.Transaction, ids []uint64) ([]neo4jNode, []neo4jNode, []neo4jEdge, error) {
	sources := make([]neo4jNode, 0)
	targets := make([]neo4jNode, 0)
	edges := make([]neo4jEdge, 0)
	idrec, err := tx.Run("MATCH (n)-[e]->(m) where id(n) in $id RETURN n,m,e", map[string]interface{}{"id": ids})
	if err != nil {
		return sources, targets, edges, err
	}
	for idrec.Next() {
		record := idrec.Record()
		sources = append(sources, newNode(record.Values[0].(neo4j.Node)))
		targets = append(targets, newNode(record.Values[1].(neo4j.Node)))
		e, ok := record.Values[2].(neo4j.Relationship)
		if ok {
			edges = append(edges, newEdge(e))
		} else {
			edge := record.Values[2].([]interface{})
			for _, e := range edge {
				edges = append(edges, newEdge(e.(neo4j.Relationship)))
			}
		}
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

func loadEntityNodes(tx neo4j.Transaction, grph graph.Graph, rootIds []uint64, config Config, loadNeighbors func(neo4j.Transaction, []uint64) ([]neo4jNode, []neo4jNode, []neo4jEdge, error), selectEntity func(graph.Node) bool) ([]int64, error) {
	if len(rootIds) == 0 {
		return nil, fmt.Errorf("Empty entity schema nodes")
	}
	// neo4j IDs
	visitedNode := make(map[int64]graph.Node)
	queue := make([]uint64, 0, len(rootIds))
	for _, id := range rootIds {
		queue = append(queue, uint64(id))
	}

	for len(queue) > 0 {
		srcNodes, adjNodes, adjRelationships, err := loadNeighbors(tx, queue)
		queue = queue[len(queue):]
		if err != nil {
			return nil, err
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
				ss := graph.NewStringSet(labels...)
				if (ss.Has(ls.AttributeTypeValue) || ss.Has(ls.AttributeTypeObject) || ss.Has(ls.AttributeTypeArray)) && !ss.Has(ls.AttributeNodeTerm) {
					ss.Add(ls.DocumentNodeTerm)
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
				ss := graph.NewStringSet(labels...)
				if (ss.Has(ls.AttributeTypeValue) || ss.Has(ls.AttributeTypeObject) || ss.Has(ls.AttributeTypeArray)) && !ss.Has(ls.AttributeNodeTerm) {
					ss.Add(ls.DocumentNodeTerm)
				}
				nd.SetLabels(ss)
				tmp := MakeProperties(node.props)
				for k, v := range tmp {
					nd.SetProperty(config.Expand(k), v)
				}
				visitedNode[node.id] = nd
				if selectEntity != nil && selectEntity(nd) {
					queue = append(queue, uint64(node.id))
				}
			}
			if _, ok := node.props[config.Map(ls.EntitySchemaTerm)]; !ok {
				queue = append(queue, uint64(node.id))
			}
		}
		for _, edge := range adjRelationships {
			src := visitedNode[edge.startId]
			target := visitedNode[edge.endId]
			grph.NewEdge(src, target, config.Expand(edge.types), edge.props)
		}
	}
	dbIds := make([]int64, 0, len(visitedNode))
	for id := range visitedNode {
		dbIds = append(dbIds, id)
	}
	return dbIds, nil
}

func (s *Session) entityDBIds(tx neo4j.Transaction, ids []string, config Config) ([]int64, []string, error) {
	var entityDBIds []int64 = make([]int64, 0, len(ids))
	var entityIds []string = make([]string, 0, len(ids))
	if len(ids) == 0 {
		return entityDBIds, entityIds, nil
	}
	idTerm := config.Map(ls.EntityIDTerm)
	query := fmt.Sprintf("MATCH (n) WHERE n.`%s` IS NOT NULL RETURN ID(n), n.`%s`", idTerm, idTerm)
	idrec, err := tx.Run(query, map[string]interface{}{"ids": ids})
	if err != nil {
		return entityDBIds, entityIds, err
	}
	for idrec.Next() {
		record := idrec.Record()
		val, ok := record.Values[1].(string)
		if !ok {
			sl, e := record.Values[1].([]interface{})
			if !e {
				continue
			}
			temp := make([]string, 0, len(sl))
			for _, s := range sl {
				temp = append(temp, s.(string))
			}
			entityIds = append(entityIds, strings.Join(temp, ","))
			entityDBIds = append(entityDBIds, record.Values[0].(int64))
			continue
		}
		entityIds = append(entityIds, val)
		entityDBIds = append(entityDBIds, record.Values[0].(int64))
	}
	return entityDBIds, entityIds, nil
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

const (
	JSON_BOOLEAN  = "json:boolean"
	JSON_NUMBER   = "json:number"
	JSON_DATE     = "json:date"
	JSON_DATETIME = "json:dateTime"
	LS_BOOLEAN    = "ls:boolean"
	UNIX_TIME     = "unix:time"
	XSD_DATE      = "xsd:date"
	XSD_DATETIME  = "xsd:dateTime"
)

func setNativeProperties(x withProperty, c Config) error {
	node := x.(graph.Node)

	if node.HasLabel(ls.AttributeTypeValue) {
		v, err := ls.GetNodeValue(node)
		if err != nil {
			return err
		}
		vt, ok := node.GetProperty(ls.ValueTypeTerm)
		if !ok {
			node.SetProperty(ls.NodeValueTerm, v)
		} else {
			t := vt.(*ls.PropertyValue).AsString()
			if !ok {
				return err
			}
			var va ls.ValueAccessor
			var iface interface{}
			switch t {
			case JSON_BOOLEAN:
				va = ls.GetValueAccessor(types.JSONBooleanTerm)
				iface, _ = va.GetNativeValue(fmt.Sprintf("%v", v), node)
				node.SetProperty(ls.NodeValueTerm, iface)
			case UNIX_TIME:
				va = ls.GetValueAccessor(types.UnixTimeTerm)
				iface, _ = va.GetNativeValue(fmt.Sprintf("%v", v), node)
				native := iface.(types.UnixTime)
				neo4jNative := neo4j.LocalTimeOf(native.ToTime())
				node.SetProperty(ls.NodeValueTerm, neo4jNative)
			case XSD_DATE:
				va = ls.GetValueAccessor(types.XSDDateTerm)
				iface, _ = va.GetNativeValue(fmt.Sprintf("%v", v), node)
				native := iface.(types.Date)
				neo4jNative := neo4j.DateOf(native.ToTime())
				node.SetProperty(ls.NodeValueTerm, neo4jNative)
			case XSD_DATETIME:
				va = ls.GetValueAccessor(types.XSDDateTimeTerm)
				iface, _ = va.GetNativeValue(fmt.Sprintf("%v", v), node)
				native := iface.(types.DateTime)
				neo4jNative := neo4j.LocalDateTimeOf(native.ToTime())
				node.SetProperty(ls.NodeValueTerm, neo4jNative)
			case JSON_DATE:
				va = ls.GetValueAccessor(types.JSONDateTerm)
				iface, _ = va.GetNativeValue(fmt.Sprintf("%v", v), node)
				native := iface.(types.Date)
				neo4jNative := neo4j.DateOf(native.ToTime())
				node.SetProperty(ls.NodeValueTerm, neo4jNative)
			case JSON_DATETIME:
				va = ls.GetValueAccessor(types.JSONDateTimeTerm)
				iface, _ = va.GetNativeValue(fmt.Sprintf("%v", v), node)
				native := iface.(types.DateTime)
				neo4jNative := neo4j.LocalDateTimeOf(native.ToTime())
				node.SetProperty(ls.NodeValueTerm, neo4jNative)
			}
		}
	}
	node.ForEachProperty(func(s string, i interface{}) bool {
		if _, ok := c.PropertyTypeMappings[c.Map(s)]; ok {
			native := c.PropertyMap(s, fmt.Sprintf("%v", i))
			node.SetProperty(s, native)
			return true
		}
		return true
	})
	return nil
}

func makeProperties(c Config, subject withProperty, vars map[string]interface{}, properties map[string]*ls.PropertyValue, idAndValue map[string]*ls.PropertyValue) string {
	out := strings.Builder{}
	first := true

	node := subject.(graph.Node)
	if err := setNativeProperties(node, c); err != nil {
		return fmt.Sprintf("Error setting native node properties %v", node)
	}

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
				switch k {
				case c.Map(ls.AttributeIndexTerm):
					vars[tname] = v.AsInt()
				case c.Map(ls.NodeValueTerm):
					val, _ := node.GetProperty(ls.NodeValueTerm)
					vars[tname] = val
				default:
					if _, exists := c.PropertyTypeMappings[k]; exists {
						val, _ := node.GetProperty(c.Expand(k))
						vars[tname] = val
					} else {
						vars[tname] = v.AsString()
					}
				}
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
