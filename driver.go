// Package neo4j is the graph storage driver using Neo4J database.
//
// The input graph node IDs are stored in `neo4j_id` property. All
// other properties and labels are stored verbatim.
package neo4j

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bserdar/slicemap"

	"github.com/cloudprivacylabs/lpg/v2"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/cloudprivacylabs/lsa/pkg/types"
	"github.com/cloudprivacylabs/opencypher"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type Driver struct {
	drv    neo4j.DriverWithContext
	dbName string

	// ID(objectName)=id.
	IDEqValueFunc func(objectName, id string) string
	// ID(objectName)=varname.
	IDEqVarFunc func(objectName, varname string) string
	// ID(objectName)
	IDFunc func(objectName string) string
	// IDValue returns the actual ID value as a string or int64
	IDValue func(string) interface{}
}

type Session struct {
	neo4j.SessionWithContext
	*Driver
}

const (
	PropNodeID = "neo4j_id"
)

type ErrMultipleFound string

func (e ErrMultipleFound) Error() string { return "Multiple found: " + string(e) }

func NewDriver(driver neo4j.DriverWithContext, databaseName string) *Driver {
	drv := &Driver{
		drv:    driver,
		dbName: databaseName,
	}
	srv, err := driver.GetServerInfo(context.Background())
	if err != nil {
		panic(err)
	}
	if srv.ProtocolVersion().Major >= 5 {
		drv.IDEqValueFunc = func(objectName, id string) string { return fmt.Sprintf("elementId(%s)=\"%s\"", objectName, id) }
		drv.IDEqVarFunc = func(objectName, varname string) string {
			return fmt.Sprintf("elementId(%s)in [%s]", objectName, varname)
		}
		drv.IDFunc = func(objectName string) string { return fmt.Sprintf("elementId(%s)", objectName) }
		drv.IDValue = func(value string) interface{} { return value }
	} else {
		drv.IDEqValueFunc = func(objectName, id string) string { return fmt.Sprintf("id(%s)=%s", objectName, id) }
		drv.IDEqVarFunc = func(objectName, varname string) string { return fmt.Sprintf("id(%s)=%s", objectName, varname) }
		drv.IDFunc = func(objectName string) string { return fmt.Sprintf("id(%s)", objectName) }
		drv.IDValue = func(value string) interface{} { v, _ := strconv.Atoi(value); return int64(v) }
	}
	return drv
}

func (d *Driver) Close(ctx context.Context) {
	d.drv.Close(context.Background())
}

func (d *Driver) NewSession(ctx context.Context) *Session {
	s := d.drv.NewSession(ctx, neo4j.SessionConfig{DatabaseName: d.dbName})
	return &Session{SessionWithContext: s,
		Driver: d,
	}
}

func (s *Session) Close(ctx context.Context) {
	s.SessionWithContext.Close(ctx)
}

func (s *Session) Logf(format string, a ...interface{}) {
	fmt.Println(fmt.Sprintf(format+":%v", a))
}

// Insert creates or adds to a graph on a database; does not check existing nodes
func Insert(ctx *ls.Context, session *Session, tx neo4j.ExplicitTransaction, grph *lpg.Graph, selectEntity func(*lpg.Node) bool, config Config, batch int) ([]string, error) {
	eids := make([]string, 0)
	mappedEntities := make(map[*lpg.Node]string) // holds all neo4j id's of entity schema and nonempty entity id
	nonemptyEntityNodeIds := make([]string, 0)
	entities := make([]*lpg.Node, 0)
	allNodes := make(map[*lpg.Node]struct{})

	for nodeItr := grph.GetNodesWithProperty(ls.EntityIDTerm); nodeItr.Next(); {
		node := nodeItr.Node()
		allNodes[node] = struct{}{}
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

	creates := make(map[string]struct{})
	for _, id := range nonemptyEntityNodeIds {
		creates[id] = struct{}{}
	}

	jobs := &JobQueue{
		createNodes: make([]*lpg.Node, 0),
		createEdges: make([]*lpg.Edge, 0),
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
		if _, exists := creates[id]; exists {
			c := &CreateEntity{Config: config, Graph: grph, Node: entity}
			if err := c.Queue(ctx, tx, jobs); err != nil {
				return nil, err
			}

		}
	}
	if err := jobs.Run(ctx, tx, session, config, mappedEntities, batch); err != nil {
		return nil, err
	}

	// Link nodes
	for node := range allNodes {
		if _, exists := node.GetProperty(ls.EntitySchemaTerm); exists {
			if err := LinkNodesForNewEntity(ctx, tx, session, config, node, mappedEntities); err != nil {
				return nil, err
			}
		}
	}
	return eids, nil
}
func (s *Session) LoadEntityNodes(ctx *ls.Context, tx neo4j.ExplicitTransaction, grph *lpg.Graph, rootIds []string, config Config, selectEntity func(*lpg.Node) bool) error {
	_, err := loadEntityNodes(ctx, tx, s, grph, rootIds, config, findNeighbors, selectEntity)
	return err
}

func (s *Session) LoadEntityNodesByEntityId(ctx *ls.Context, tx neo4j.ExplicitTransaction, grph *lpg.Graph, rootIds []string, config Config, selectEntity func(*lpg.Node) bool) error {
	idTerm := config.Shorten(ls.EntityIDTerm)
	res, err := tx.Run(ctx, fmt.Sprintf("match (root) where root.`%s` in $ids return %s", idTerm, s.IDFunc("root")), map[string]interface{}{"ids": rootIds})
	if err != nil {
		return err
	}
	ids := make([]string, 0)
	for res.Next(ctx) {
		record := res.Record()
		v := record.Values[0].(string)
		ids = append(ids, v)
	}

	_, err = loadEntityNodes(ctx, tx, s, grph, ids, config, findNeighbors, selectEntity)
	return err
}

type neo4jNode struct {
	id     string
	labels []string
	props  map[string]interface{}
}

type neo4jEdge struct {
	id      string
	startId string
	endId   string
	types   string
	props   map[string]interface{}
}

func newNode(ob1 neo4j.Node) neo4jNode {
	ob2 := neo4jNode{
		id:     ob1.ElementId,
		labels: ob1.Labels,
		props:  ob1.Props,
	}
	return ob2
}

func newEdge(ob1 neo4j.Relationship) neo4jEdge {
	ob2 := neo4jEdge{
		id:      ob1.ElementId,
		startId: ob1.StartElementId,
		endId:   ob1.EndElementId,
		types:   ob1.Type,
		props:   ob1.Props,
	}
	return ob2
}

func findNeighbors(ctx *ls.Context, tx neo4j.ExplicitTransaction, session *Session, ids []string) ([]neo4jNode, []neo4jNode, []neo4jEdge, error) {
	sources := make([]neo4jNode, 0)
	targets := make([]neo4jNode, 0)
	edges := make([]neo4jEdge, 0)
	remainingIds := make(map[string]struct{})
	for _, x := range ids {
		remainingIds[x] = struct{}{}
	}
	idValues := make([]interface{}, 0, len(ids))
	for _, x := range ids {
		idValues = append(idValues, session.IDValue(x))
	}
	idrec, err := tx.Run(ctx, fmt.Sprintf("MATCH (n)-[e]->(m) where %s in $id RETURN n,m,e", session.IDFunc("n")), map[string]interface{}{"id": idValues})
	if err != nil {
		return sources, targets, edges, err
	}
	for idrec.Next(ctx) {
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
	for _, source := range sources {
		delete(remainingIds, source.id)
	}
	if len(remainingIds) == 0 {
		return sources, targets, edges, nil
	}
	rem := make([]interface{}, 0, len(remainingIds))
	for x := range remainingIds {
		rem = append(rem, session.IDValue(x))
	}
	idrec, err = tx.Run(ctx, fmt.Sprintf("MATCH (n) where %s in $id RETURN n", session.IDFunc("n")), map[string]interface{}{"id": rem})
	if err != nil {
		return sources, targets, edges, err
	}
	for idrec.Next(ctx) {
		record := idrec.Record()
		sources = append(sources, newNode(record.Values[0].(neo4j.Node)))
	}
	return sources, targets, edges, nil
}

// Called before SetNodeValue, input contains neo4j native values
func SetNodeValueAfterLoad(cfg Config, node *lpg.Node, input map[string]interface{}) interface{} {
	// check if value type exists in config
	v, ok := input[cfg.Shorten(ls.NodeValueTerm)]
	if !ok {
		return nil
	}
	return Neo4jValueToNativeValue(v)
}

// BuildNodePropertiesAfterLoad is during the loading of nodes from database. This function sets all node properties
// to PropertyValues, excluding properties that are assigned to NodeValueTerm
func BuildNodePropertiesAfterLoad(node *lpg.Node, input map[string]interface{}, cfg Config) {
	var buildNodeProperties func(key string, v interface{})
	buildNodeProperties = func(key string, v interface{}) {
		switch val := v.(type) {
		case bool:
			node.SetProperty(key, ls.StringPropertyValue(key, fmt.Sprintf("%v", val)))
		case float64:
			f := strconv.FormatFloat(val, 'f', -1, 64)
			node.SetProperty(key, ls.StringPropertyValue(key, f))
		case int:
			node.SetProperty(key, ls.IntPropertyValue(key, val))
		case int64:
			node.SetProperty(key, ls.IntPropertyValue(key, int(val)))
		case string:
			node.SetProperty(key, ls.StringPropertyValue(key, val))
		case []interface{}:
			isl := v.([]interface{})
			slProps := make([]string, 0, len(isl))
			for _, val := range isl {
				form := fmt.Sprintf("%v", val)
				slProps = append(slProps, form)
			}
			node.SetProperty(key, ls.StringSlicePropertyValue(key, slProps))
		case time.Time:
			node.SetProperty(key, ls.StringPropertyValue(key, v.(time.Time).String()))
		case types.TimeOfDay:
			node.SetProperty(key, ls.StringPropertyValue(key, v.(types.TimeOfDay).ToTime().String()))
		case types.Date:
			node.SetProperty(key, ls.StringPropertyValue(key, v.(types.Date).ToTime().String()))
		case types.DateTime:
			node.SetProperty(key, ls.StringPropertyValue(key, v.(types.DateTime).ToTime().String()))
		}
	}

	for k, v := range input {
		expandedKey := cfg.Expand(k)
		// check if there is a type for property in config, otherwise convert to string and store it
		if expandedKey == ls.NodeValueTerm {
			continue
		}
		buildNodeProperties(expandedKey, Neo4jValueToNativeValue(v))
	}
}

func loadEntityNodes(ctx *ls.Context, tx neo4j.ExplicitTransaction, session *Session, grph *lpg.Graph, rootIds []string, config Config, loadNeighbors func(*ls.Context, neo4j.ExplicitTransaction, *Session, []string) ([]neo4jNode, []neo4jNode, []neo4jEdge, error), selectEntity func(*lpg.Node) bool) ([]string, error) {
	if len(rootIds) == 0 {
		return nil, fmt.Errorf("Empty entity schema nodes")
	}
	// neo4j IDs
	visitedNode := make(map[string]*lpg.Node)
	queue := make([]string, 0, len(rootIds))
	for _, id := range rootIds {
		queue = append(queue, id)
	}

	for len(queue) > 0 {
		srcNodes, adjNodes, adjRelationships, err := loadNeighbors(ctx, tx, session, queue)
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
				ss := lpg.NewStringSet(labels...)
				if (ss.Has(ls.AttributeTypeValue) || ss.Has(ls.AttributeTypeObject) || ss.Has(ls.AttributeTypeArray)) && !ss.Has(ls.AttributeNodeTerm) {
					ss.Add(ls.DocumentNodeTerm)
				}
				src.SetLabels(lpg.NewStringSet(labels...))
				// Set properties and node value
				BuildNodePropertiesAfterLoad(src, srcNode.props, config)
				nv := SetNodeValueAfterLoad(config, src, srcNode.props)
				if nv != nil {
					if err := ls.SetNodeValue(src, nv); err != nil {
						panic(fmt.Errorf("Cannot set node value for %w %v", err, src))
					}
				}
				if selectEntity == nil || selectEntity(src) {
					visitedNode[srcNode.id] = src
				} else {
					src.DetachAndRemove()
				}
			}
		}
		for _, node := range adjNodes {
			if _, seen := visitedNode[node.id]; !seen {
				nd := grph.NewNode(node.labels, nil)
				labels := make([]string, 0, len(node.labels))
				for _, lbl := range node.labels {
					labels = append(labels, config.Expand(lbl))
				}
				ss := lpg.NewStringSet(labels...)
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
				if selectEntity == nil || selectEntity(nd) {
					visitedNode[node.id] = nd
					queue = append(queue, node.id)
				} else {
					nd.DetachAndRemove()
				}
			}
		}
		for _, edge := range adjRelationships {
			src := visitedNode[edge.startId]
			if src == nil {
				continue
			}
			target := visitedNode[edge.endId]
			if target == nil {
				continue
			}
			grph.NewEdge(src, target, config.Expand(edge.types), edge.props)
		}
	}
	dbIds := make([]string, 0, len(visitedNode))
	for id := range visitedNode {
		dbIds = append(dbIds, id)
	}
	return dbIds, nil
}

func (s *Session) CollectEntityDBIds(ctx *ls.Context, tx neo4j.ExplicitTransaction, config Config, grph *lpg.Graph, cache *Neo4jCache) (entityRootNodes []*lpg.Node, entityRootDBIds []string, entityInfo map[*lpg.Node]ls.EntityInfo, err error) {

	entityInfo = ls.GetEntityInfo(grph)
	// Remove any empty IDs
	for k, v := range entityInfo {
		if len(v.GetID()) == 0 {
			delete(entityInfo, k)
		}
	}

	// Collect unique labels
	labels := make(map[string][]*lpg.Node)
	for node := range entityInfo {
		l := config.MakeLabels(node.GetLabels().Slice())
		labels[l] = append(labels[l], node)
	}
	// Load entities by their unique labels
	for label, rootNodes := range labels {
		unwind := make([]interface{}, 0, len(rootNodes))
		rootIds := slicemap.SliceMap[string, *lpg.Node]{}
		for _, rootNode := range rootNodes {
			id := entityInfo[rootNode].GetID()
			rootIds.Put(id, rootNode)
			if len(id) == 1 {
				unwind = append(unwind, map[string]interface{}{"id": id[0]})
			} else {
				unwind = append(unwind, map[string]interface{}{"id": id})
			}
		}
		entityIDTerm := config.Shorten(ls.EntityIDTerm)
		query := fmt.Sprintf("unwind $ids as nodeId match (n%s) where n.`%s`=nodeId.id return n", label, entityIDTerm)
		idrec, e := tx.Run(ctx, query, map[string]interface{}{"ids": unwind})

		if e != nil {
			err = e
			return
		}
		nRecords := 0
		for idrec.Next(ctx) {
			nRecords++
			record := idrec.Record()
			dbNode, _ := record.Values[0].(neo4j.Node)
			cache.putNodes(dbNode)
			entityIDAny := dbNode.Props[entityIDTerm]
			var entityRootNode *lpg.Node
			var exists bool
			if s, ok := entityIDAny.(string); ok {
				entityRootNode, exists = rootIds.Get([]string{s})
			} else if arr, ok := entityIDAny.([]interface{}); ok {
				str := make([]string, 0, len(arr))
				for i := range arr {
					str[i] = arr[i].(string)
				}
				entityRootNode, exists = rootIds.Get(str)
			}
			if exists {
				entityRootNodes = append(entityRootNodes, entityRootNode)
				entityRootDBIds = append(entityRootDBIds, dbNode.ElementId)
			} else {
				panic(fmt.Sprintf("Unexpected node loaded: %+v", dbNode))
			}
			// // Find the matching node
			// for _, rootNode := range rootNodes {
			// 	id := entityInfo[rootNode].GetID()
			// 	if len(id) == 1 {
			// 		if s, ok := record.Values[1].(string); ok {
			// 			if s == id[0] {
			// 				entityRootNodes = append(entityRootNodes, rootNode)
			// 				entityRootDBIds = append(entityRootDBIds, dbId)
			// 				break
			// 			}
			// 		}
			// 	} else {
			// 		if arr, ok := record.Values[1].([]interface{}); ok {
			// 			if len(arr) == len(id) {
			// 				found := true
			// 				for i := range arr {
			// 					if arr[i] != id[i] {
			// 						found = false
			// 						break
			// 					}
			// 				}
			// 				if found {
			// 					entityRootNodes = append(entityRootNodes, rootNode)
			// 					entityRootDBIds = append(entityRootDBIds, dbId)
			// 					break
			// 				}
			// 			}
			// 		}
			//	}
			//}
		}
	}
	return
}

func contains(node *lpg.Node, hm map[*lpg.Node]string) bool {
	if _, exists := hm[node]; exists {
		return true
	}
	return false
}

func makeLabels(vars map[string]interface{}, types []string) string {
	sort.Strings(types)
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
func Neo4jValueToNativeValue(val interface{}) interface{} {
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
			Location:     x.Local().Location(),
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
			Location:     x.Local().Location(),
		}
		return tm
	case neo4j.Date:
		x := val.Time()
		tm := types.Date{
			Month:    int(x.Month()),
			Day:      x.Day(),
			Year:     x.Year(),
			Location: x.Local().Location(),
		}
		return tm
	}
	return val
}

// nativeValueToNeo4jValue will get native neo4j type based on given value to be represented in the database
func nativeValueToNeo4jValue(val interface{}) interface{} {
	switch val := val.(type) {
	case neo4j.Date:
		return val
	case neo4j.LocalDateTime:
		return val
	case neo4j.Duration:
		return val
	case neo4j.Time:
		return val
	case neo4j.LocalTime:
		return val
	case bool, float32, float64, int8, int16, int, int64, string:
		return val
	case types.Measure:
		f, err := strconv.ParseFloat(val.Value, 64)
		if err != nil {
			return err
		}
		return f
	case opencypher.Date:
		return neo4j.DateOf(val.Time())
	case opencypher.LocalDateTime:
		return neo4j.LocalDateTimeOf(val.Time())
	case opencypher.Duration:
		return neo4j.DurationOf(val.Months, val.Days, val.Seconds, val.Nanos)
	case opencypher.Time:
		return neo4j.LocalTimeOf(val.Time())
	case opencypher.LocalTime:
		return neo4j.LocalTimeOf(val.Time())
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
func buildDBPropertiesForSave(c Config, itemToSave withProperty, vars map[string]any, properties map[string]any) string {
	out := strings.Builder{}
	first := true

	for k, v := range properties {
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

		if pv, ok := v.(*ls.PropertyValue); ok {
			if pv.IsString() {
				switch k {
				case c.Shorten(ls.AttributeIndexTerm):
					vars[tname] = pv.AsInt()
				case c.Shorten(ls.NodeValueTerm):
					node, ok := itemToSave.(*lpg.Node)
					if ok {
						val, _ := ls.GetNodeValue(node)
						n4jNative := nativeValueToNeo4jValue(val)
						vars[tname] = n4jNative
					}
				default:
					if _, exists := c.PropertyTypes[expandedKey]; exists {
						switch itemToSave.(type) {
						case *lpg.Node, *lpg.Edge:
							val, _ := itemToSave.GetProperty(expandedKey)
							n4jNative, err := c.GetNeo4jPropertyValue(expandedKey, val.(*ls.PropertyValue).AsString())
							if err != nil {
								panic(err)
							}
							vars[tname] = n4jNative
						default:
							for _, v := range itemToSave.(mapWithProperty) {
								n4jNative, err := c.GetNeo4jPropertyValue(expandedKey, v.(*ls.PropertyValue).AsString())
								if err != nil {
									panic(err)
								}
								vars[tname] = n4jNative
							}
						}
					} else {
						vars[tname] = pv.AsString()
					}
				}
			} else if pv.IsStringSlice() {
				vsl := pv.AsInterfaceSlice()
				nsl := make([]interface{}, 0, len(vsl))
				for _, vn := range vsl {
					if _, exists := c.PropertyTypes[expandedKey]; exists {
						n4jNative, err := c.GetNeo4jPropertyValue(expandedKey, vn.(*ls.PropertyValue).AsString())
						if err != nil {
							panic(err)
						}
						nsl = append(nsl, n4jNative)
					} else {
						nsl = append(nsl, vn)
					}
				}
				vars[tname] = nsl
			}
		} else if WriteableType(v) {
			vars[tname] = v
		}

	}

	if !first {
		out.WriteRune('}')
	}

	return out.String()
}

func buildDBPropertiesForSaveObj(c Config, itemToSave withProperty, properties map[string]any) map[string]any {
	ret := make(map[string]interface{})
	for k, v := range properties {
		expandedKey := c.Expand(k)
		if v == nil {
			continue
		}
		if pv, ok := v.(*ls.PropertyValue); ok {
			if pv.IsString() {
				switch k {
				case c.Shorten(ls.AttributeIndexTerm):
					ret[k] = pv.AsInt()
				case c.Shorten(ls.NodeValueTerm):
					node, ok := itemToSave.(*lpg.Node)
					if ok {
						val, _ := ls.GetNodeValue(node)
						n4jNative := nativeValueToNeo4jValue(val)
						ret[k] = n4jNative
					}
				default:
					if _, exists := c.PropertyTypes[expandedKey]; exists {
						switch itemToSave.(type) {
						case *lpg.Node, *lpg.Edge:
							val, _ := itemToSave.GetProperty(expandedKey)
							n4jNative, err := c.GetNeo4jPropertyValue(expandedKey, val.(*ls.PropertyValue).AsString())
							if err != nil {
								panic(err)
							}
							ret[k] = n4jNative
						default:
							for _, v := range itemToSave.(mapWithProperty) {
								n4jNative, err := c.GetNeo4jPropertyValue(expandedKey, v.(*ls.PropertyValue).AsString())
								if err != nil {
									panic(err)
								}
								ret[k] = n4jNative
							}
						}
					} else {
						ret[k] = pv.AsString()
					}
				}
			} else if pv.IsStringSlice() {
				vsl := pv.AsInterfaceSlice()
				nsl := make([]interface{}, 0, len(vsl))
				for _, vn := range vsl {
					if _, exists := c.PropertyTypes[expandedKey]; exists {
						n4jNative, err := c.GetNeo4jPropertyValue(expandedKey, vn.(*ls.PropertyValue).AsString())
						if err != nil {
							panic(err)
						}
						nsl = append(nsl, n4jNative)
					} else {
						nsl = append(nsl, vn)
					}
				}
				ret[k] = nsl
			}
		} else if WriteableType(v) {
			ret[k] = v
		}
	}
	return ret
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

// Determine if value is writeable
func WriteableType(value any) bool {
	switch value.(type) {
	case int, int64, int32, int16, int8, uint, uint64, uint32, uint16, uint8, float64, float32, time.Time, bool, string:
		return true
	case neo4j.Date, neo4j.LocalDateTime, neo4j.Duration, neo4j.Time, neo4j.LocalTime:
		return true
	case types.Measure, opencypher.Date, opencypher.LocalDateTime, opencypher.Duration, opencypher.Time, opencypher.LocalTime, types.TimeOfDay, types.Date, types.DateTime, types.GDay, types.GYear, types.GYearMonth, types.GMonthDay, types.UnixTime, types.UnixTimeNano:
		return true
	}
	return false
}
