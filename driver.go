// Package neo4j is the graph storage driver using Neo4J database.
//
// The input graph node IDs are stored in `neo4j_id` property. All
// other properties and labels are stored verbatim.
//
//
package neo4j

import (
	"fmt"
	"strconv"
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

	idTerm := config.Shorten(ls.EntityIDTerm)
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

// Called before SetNodeValue, input contains neo4j native values
func SetNodeValueAfterLoad(cfg Config, node graph.Node, input map[string]interface{}) interface{} {
	// check if value type exists in config
	v, ok := input[cfg.Shorten(ls.NodeValueTerm)]
	if !ok {
		return nil
	}
	return neo4jValueToNativeValue(v)
}

// BuildNodePropertiesAfterLoad is during the loading of nodes from database. This function sets all node properties
// to PropertyValues, excluding properties that are assigned to NodeValueTerm
func BuildNodePropertiesAfterLoad(node graph.Node, input map[string]interface{}, cfg Config) {
	var buildNodeProperties func(key string, v interface{})
	buildNodeProperties = func(key string, v interface{}) {
		switch v.(type) {
		case bool:
			node.SetProperty(key, ls.StringPropertyValue(fmt.Sprintf("%v", v.(bool))))
		case float64:
			f := strconv.FormatFloat(v.(float64), 'f', -1, 64)
			node.SetProperty(key, ls.StringPropertyValue(f))
		case int:
			node.SetProperty(key, ls.IntPropertyValue(v.(int)))
		case string:
			node.SetProperty(key, ls.StringPropertyValue(v.(string)))
		case []interface{}:
			isl := v.([]interface{})
			slProps := make([]string, 0, len(isl))
			for _, val := range isl {
				form := fmt.Sprintf("%v", val)
				slProps = append(slProps, form)
			}
			node.SetProperty(key, ls.StringSlicePropertyValue(slProps))
		}
	}

	for k, v := range input {
		expandedKey := cfg.Expand(k)
		// check if there is a type for property in config, otherwise convert to string and store it
		if expandedKey == ls.NodeValueTerm {
			continue
		}
		vt := cfg.Shorten(cfg.PropertyTypes[expandedKey])
		if vt != "" && k != expandedKey {
			va := ls.GetValueAccessor(vt)
			_, ok := v.([]interface{})
			if ok {
				si := make([]string, 0, len(v.([]interface{})))
				for _, vi := range v.([]interface{}) {
					form, err := va.FormatNativeValue(vi, nil, node)
					if err != nil {
						panic(fmt.Errorf("Cannot format native value for %v, %w", node, err))
					}
					si = append(si, form)
				}
				node.SetProperty(expandedKey, ls.StringSlicePropertyValue(si))
			} else {
				form, err := va.FormatNativeValue(v, nil, node)
				if err != nil {
					panic(fmt.Errorf("Cannot format native value for %v, %w", node, err))
				}
				node.SetProperty(expandedKey, ls.StringPropertyValue(form))
			}
		} else {
			buildNodeProperties(expandedKey, v)
		}
	}
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
				src := grph.NewNode(srcNode.labels, nil)
				labels := make([]string, 0, len(srcNode.labels))
				for _, lbl := range srcNode.labels {
					labels = append(labels, config.Expand(lbl))
				}
				ss := graph.NewStringSet(labels...)
				if (ss.Has(ls.AttributeTypeValue) || ss.Has(ls.AttributeTypeObject) || ss.Has(ls.AttributeTypeArray)) && !ss.Has(ls.AttributeNodeTerm) {
					ss.Add(ls.DocumentNodeTerm)
				}
				src.SetLabels(graph.NewStringSet(labels...))
				// Set properties and node value
				BuildNodePropertiesAfterLoad(src, srcNode.props, config)
				nv := SetNodeValueAfterLoad(config, src, srcNode.props)
				if nv != nil {
					if err := ls.SetNodeValue(src, nv); err != nil {
						panic(fmt.Errorf("Cannot set node value for %w %v", err, src))
					}
				}
				visitedNode[srcNode.id] = src
			}
		}
		for _, node := range adjNodes {
			if _, seen := visitedNode[node.id]; !seen {
				nd := grph.NewNode(node.labels, nil)
				labels := make([]string, 0, len(node.labels))
				for _, lbl := range node.labels {
					labels = append(labels, config.Expand(lbl))
				}
				ss := graph.NewStringSet(labels...)
				if (ss.Has(ls.AttributeTypeValue) || ss.Has(ls.AttributeTypeObject) || ss.Has(ls.AttributeTypeArray)) && !ss.Has(ls.AttributeNodeTerm) {
					ss.Add(ls.DocumentNodeTerm)
				}
				nd.SetLabels(ss)
				// Set properties and node value
				BuildNodePropertiesAfterLoad(nd, node.props, config)
				nv := SetNodeValueAfterLoad(config, nd, node.props)
				if nv != nil {
					if err := ls.SetNodeValue(nd, nv); err != nil {
						panic(fmt.Errorf("Cannot set node value for %w %v", err, nd))
					}
				}
				visitedNode[node.id] = nd
				if selectEntity != nil && selectEntity(nd) {
					queue = append(queue, uint64(node.id))
				}
			}
			if _, ok := node.props[config.Shorten(ls.EntitySchemaTerm)]; !ok {
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
	idTerm := config.Shorten(ls.EntityIDTerm)
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

// neo4jValueToNativeValue converts a neo4j value to a native go value
func neo4jValueToNativeValue(val interface{}) interface{} {
	switch val := val.(type) {
	case neo4j.LocalDateTime:
		x := val.Time()
		tm := types.DateTime{
			Month:        int(x.Month()),
			Year:         x.Year(),
			Day:          x.Day(),
			Seconds:      int64(x.Second()),
			Minute:       int64(x.Minute()),
			Milliseconds: int64(x.Second() / 1000),
			Hour:         int64(x.Hour()),
			Nanoseconds:  int64(x.Nanosecond()),
			Location:     x.Location(),
		}
		return tm
	case neo4j.LocalTime:
		x := val.Time()
		tm := types.TimeOfDay{
			Seconds:      int64(x.Second()),
			Milliseconds: int64(x.Second() / 1000),
			Hour:         int64(x.Hour()),
			Minute:       int64(x.Minute()),
			Nanoseconds:  int64(x.Nanosecond()),
			Location:     x.Location(),
		}
		return tm
	case neo4j.Date:
		x := val.Time()
		tm := types.Date{
			Month:    int(x.Month()),
			Day:      x.Day(),
			Year:     x.Year(),
			Location: x.Location(),
		}
		return tm
	}
	return val
}

// nativeValueToNeo4jValue will get native neo4j type based on given value to be represented in the database
func nativeValueToNeo4jValue(val interface{}) interface{} {
	switch val := val.(type) {
	case bool, float32, float64, int8, int16, int, int64, string:
		return val
	case types.Measure:
		f, err := strconv.ParseFloat(val.Value, 64)
		if err != nil {
			return err
		}
		return f
	case types.TimeOfDay:
		return neo4j.LocalTimeOf(val.ToTime())
	case types.Date:
		return neo4j.DateOf(val.ToTime())
	case types.DateTime:
		return neo4j.LocalDateTimeOf(val.ToTime())
	case types.GDay:
		t := time.Date(0, 0, int(val), 0, 0, 0, 0, nil)
		return neo4j.DateOf(t)
	case types.GYear:
		t := time.Date(int(val), 0, 0, 0, 0, 0, 0, nil)
		return neo4j.DateOf(t)
	case types.GYearMonth:
		return neo4j.DateOf(time.Date(val.Year, time.Month(val.Month), 0, 0, 0, 0, 0, time.UTC))
	case types.GMonthDay:
		return neo4j.DateOf(time.Date(0, time.Month(val.Month), val.Day, 0, 0, 0, 0, time.UTC))
	case types.UnixTime:
		return neo4j.LocalTimeOf(time.Unix(val.Seconds, 0))
	case types.UnixTimeNano:
		return neo4j.LocalTimeOf(time.Unix(0, val.Nanoseconds))

	}
	return nil
}

// buildDBPropertiesForSave writes the properties that will be ran by the query
func buildDBPropertiesForSave(c Config, subject withProperty, vars map[string]interface{}, properties map[string]*ls.PropertyValue, idAndValue map[string]*ls.PropertyValue) string {
	out := strings.Builder{}
	first := true
	node := subject.(graph.Node)

	buildProperties := func(m map[string]*ls.PropertyValue) {
		for k, v := range m {
			expandedKey := c.Expand(k)
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
				case c.Shorten(ls.AttributeIndexTerm):
					vars[tname] = v.AsInt()
				case c.Shorten(ls.NodeValueTerm):
					val, _ := ls.GetNodeValue(node)
					n4jNative := nativeValueToNeo4jValue(val)
					vars[tname] = n4jNative
				default:
					if _, exists := c.PropertyTypes[expandedKey]; exists {
						val, _ := node.GetProperty(expandedKey)
						native := c.GetNativePropertyValue(node, expandedKey, val.(*ls.PropertyValue).AsString())
						vars[tname] = native
					} else {
						vars[tname] = v.AsString()
					}
				}
			} else if v.IsStringSlice() {
				vsl := v.AsInterfaceSlice()
				nsl := make([]interface{}, 0, len(vsl))
				for _, vn := range vsl {
					if _, exists := c.PropertyTypes[expandedKey]; exists {
						native := c.GetNativePropertyValue(node, expandedKey, vn.(string))
						nsl = append(nsl, native)
					} else {
						nsl = append(nsl, vn)
					}
				}
				vars[tname] = nsl
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
