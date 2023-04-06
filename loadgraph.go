package neo4j

import (
	"fmt"

	"github.com/bserdar/slicemap"

	"github.com/cloudprivacylabs/lpg/v2"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type DBGraph struct {
	G *lpg.Graph

	NodeIds map[*lpg.Node]string
	Nodes   map[string]*lpg.Node
	EdgeIds map[*lpg.Edge]string
	Edges   map[string]*lpg.Edge
}

func NewDBGraph(g *lpg.Graph) *DBGraph {
	return &DBGraph{
		G:       g,
		NodeIds: make(map[*lpg.Node]string),
		Nodes:   make(map[string]*lpg.Node),
		EdgeIds: make(map[*lpg.Edge]string),
		Edges:   make(map[string]*lpg.Edge),
	}
}

func (dbg *DBGraph) NewNode(node *lpg.Node, dbID string) {
	dbg.Nodes[dbID] = node
	dbg.NodeIds[node] = dbID
}

func (dbg *DBGraph) NewEdge(edge *lpg.Edge, dbID string) {
	dbg.Edges[dbID] = edge
	dbg.EdgeIds[edge] = dbID
}

// LoadDBGraph loads a graph from the DB. The entity root nodes are
// matched based on the config entityMergeActions. If there is an
// entityMergeAction defined in the config for an entity, then this
// function will look up for a node that contains that entity name. If
// not, it will look for exact label match.
//
// For instance, if there is entityMergeAction defined for X, then a
// memory node :X:Y will be looked up as :X. If X does not have
// entityMergeAction, then a memory node :X:Y will look for :X:Y
func (s *Session) LoadDBGraph(ctx *ls.Context, tx neo4j.ExplicitTransaction, memGraph *lpg.Graph, config Config, cache *Neo4jCache) (*DBGraph, error) {

	_, rootIds, _, err := s.CollectEntityDBIds(ctx, tx, config, memGraph, cache)
	if err != nil {
		return nil, err
	}
	g := ls.NewDocumentGraph()
	dbg := NewDBGraph(g)
	if len(rootIds) == 0 {
		return dbg, nil
	}

	err = loadGraphByEntities(ctx, tx, s, dbg, rootIds, config, findNeighbors, func(n *lpg.Node) bool { return true })
	if err != nil {
		return nil, err
	}
	return dbg, nil
}

func loadGraphByEntities(ctx *ls.Context, tx neo4j.ExplicitTransaction, session *Session, grph *DBGraph, rootIds []string, config Config, loadNeighbors func(*ls.Context, neo4j.ExplicitTransaction, *Session, []string) ([]neo4jNode, []neo4jNode, []neo4jEdge, error), selectEntity func(*lpg.Node) bool) error {
	if len(rootIds) == 0 {
		return fmt.Errorf("Empty entity schema nodes")
	}
	// neo4j IDs
	queue := make(map[string]struct{}, len(rootIds))
	for _, id := range rootIds {
		queue[id] = struct{}{}
	}

	for len(queue) > 0 {
		q := make([]string, 0, len(queue))
		for k := range queue {
			q = append(q, k)
		}
		srcNodes, adjNodes, adjRelationships, err := loadNeighbors(ctx, tx, session, q)
		// fmt.Printf("Find neighbors %v\n", queue)
		// for _, x := range srcNodes {
		// 	fmt.Printf("source: %+v\n", x)
		// }
		// for _, x := range adjNodes {
		// 	fmt.Printf("adj: %+v\n", x)
		// }
		if err != nil {
			return err
		}
		if len(srcNodes) == 0 {
			break
		}
		queue = make(map[string]struct{})
		makeNode := func(srcNode neo4jNode) *lpg.Node {
			src := grph.G.NewNode(srcNode.labels, nil)
			labels := make([]string, 0, len(srcNode.labels))
			for _, lbl := range srcNode.labels {
				labels = append(labels, config.Expand(lbl))
			}
			ss := lpg.NewStringSet(labels...)
			if (ss.Has(ls.AttributeTypeValue) || ss.Has(ls.AttributeTypeObject) || ss.Has(ls.AttributeTypeArray)) && !ss.Has(ls.AttributeNodeTerm) {
				ss.Add(ls.DocumentNodeTerm)
			}
			src.SetLabels(ss)
			// Set properties and node value
			BuildNodePropertiesAfterLoad(src, srcNode.props, config)
			nv := SetNodeValueAfterLoad(config, src, srcNode.props)
			if nv != nil {
				if err := ls.SetNodeValue(src, nv); err != nil {
					panic(fmt.Errorf("Cannot set node value for %w %v", err, src))
				}
			}
			grph.NewNode(src, srcNode.id)
			return src
		}
		for _, srcNode := range srcNodes {
			if _, seen := grph.Nodes[srcNode.id]; !seen {
				makeNode(srcNode)
			}
		}
		for _, node := range adjNodes {
			if _, seen := grph.Nodes[node.id]; !seen {
				nd := makeNode(node)
				if selectEntity != nil && selectEntity(nd) {
					queue[node.id] = struct{}{}
				}
				if _, ok := node.props[config.Shorten(ls.EntitySchemaTerm)]; !ok {
					queue[node.id] = struct{}{}
				}
			}
		}
		for _, edge := range adjRelationships {
			if _, seen := grph.Edges[edge.id]; !seen {
				src := grph.Nodes[edge.startId]
				target := grph.Nodes[edge.endId]
				e := grph.G.NewEdge(src, target, config.Expand(edge.types), edge.props)
				grph.NewEdge(e, edge.id)
			}
		}
	}
	return nil
}

func (s *Session) CollectEntityDBIds(ctx *ls.Context, tx neo4j.ExplicitTransaction, config Config, grph *lpg.Graph, cache *Neo4jCache) (entityRootNodes []*lpg.Node, entityRootDBIds []string, entityInfo map[*lpg.Node]ls.EntityInfo, err error) {

	entityInfo = ls.GetEntityRootsByID(grph)
	// Remove any empty IDs
	for k, v := range entityInfo {
		if len(v.GetID()) == 0 {
			delete(entityInfo, k)
		}
	}

	// Collect unique labels
	labels := make(map[string][]*lpg.Node)
	for node := range entityInfo {
		// If one of the labels has entityMergeConfig, then use that label
		// to load the node
		labelSlice := node.GetLabels().Slice()
		entityLabel := ""
		for _, l := range labelSlice {
			if config.IsMergeEntity(l) {
				if entityLabel != "" {
					return nil, nil, nil, fmt.Errorf("Node has both %s and %s, both of which are entity labels", entityLabel, l)
				}
				entityLabel = l
			}
		}
		if entityLabel == "" {
			l := config.MakeLabels(labelSlice)
			labels[l] = append(labels[l], node)
		} else {
			l := config.MakeLabels([]string{entityLabel})
			labels[l] = append(labels[l], node)
		}
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
