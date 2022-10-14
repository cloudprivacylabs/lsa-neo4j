package neo4j

import (
	"fmt"
	"log"
	"strings"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

func compareNativeNode(n1, n2 *lpg.Node) bool {
	if !n1.GetLabels().IsEqual(n2.GetLabels()) {
		return false
	}
	// Expected properties must be a subset
	propertiesOK := true
	n2.ForEachProperty(func(k string, v interface{}) bool {
		pv, ok := v.(*ls.PropertyValue)
		if !ok {
			return true
		}
		v2, ok := n1.GetProperty(k)
		if !ok {
			log.Printf("Error at %s: %v: Property does not exist", k, v)
			propertiesOK = false
			return false
		}
		pv2, ok := v2.(*ls.PropertyValue)
		if !ok {
			log.Printf("Error at %s: %v: Not property value", k, v)
			propertiesOK = false
			return false
		}
		if !pv2.IsEqual(pv) {
			if strings.ToLower(pv2.AsString()) != strings.ToLower(pv.AsString()) {
				log.Printf("Error at %s: Got %v, Expected %v: Values are not equal", k, pv, pv2)
				propertiesOK = false
				return false
			}
		}
		return true
	})
	if !propertiesOK {
		log.Printf("Properties not same")
		return false
	}
	log.Printf("True\n")
	return true
}

func compareNativeEdge(e1, e2 *lpg.Edge) bool {
	return e1.GetLabel() == e2.GetLabel() && ls.IsPropertiesEqual(ls.PropertiesAsMap(e1), ls.PropertiesAsMap(e2))
}

type OperationQueue struct {
	ops []operation
	// updateNodes []updateNode
	// createNodes []createNode
	// updateEdges []createEdge
}

type operation interface {
	writeQuery() string
}

type updateNode struct {
	id     int64
	labels []string
	props  map[string]interface{}
}

type createNode struct {
	id     int64
	labels []string
	props  map[string]interface{}
}
type createEdge struct {
	id    int64
	types []string
	props map[string]interface{}
}

func merge(memGraph, dbGraph *lpg.Graph, dbGraphIds map[*lpg.Node]int64, dbEdges map[*lpg.Edge]int64) (*lpg.Graph, []operation, error) {
	ops := make([]operation, 0)
	memEntitiesMap := ls.GetEntityInfo(memGraph)
	dbEntitiesMap := ls.GetEntityInfo(dbGraph)
	findDBCounterpart := func(dbMap map[*lpg.Node]ls.EntityInfo, term, match string) *lpg.Node {
		for n := range dbMap {
			if ls.AsPropertyValue(n.GetProperty(term)).AsString() == match {
				return n
			}
		}
		return nil
	}
	for memEntity := range memEntitiesMap {
		matchingDBEntity := findDBCounterpart(dbEntitiesMap, ls.EntitySchemaTerm, ls.AsPropertyValue(memEntity.GetProperty(ls.EntityIDTerm)).AsString())
		if matchingDBEntity == nil {
			ops = append(ops, nodeValuesToOperation(memEntity, createNodeOp))
		} else {
			ls.IterateDescendants(memEntity, func(n *lpg.Node) bool {
				found := ls.IterateDescendants(matchingDBEntity, func(n2 *lpg.Node) bool {
					cmp := compareNativeNode(n, n2)
					if !cmp {
						ops = append(ops, nodeValuesToOperation(n2, updateNodeOp))
					}
					return cmp
				}, ls.FollowEdgesInEntity, false)
				return found
			}, ls.FollowEdgesInEntity, false)
		}
	}
	return dbGraph, ops, nil
}

func getNodeProps(n *lpg.Node) map[string]interface{} {
	res := make(map[string]interface{})
	n.ForEachProperty(func(s string, i interface{}) bool {
		res[s] = i
		return true
	})
	return res
}
func getEdgeProps(e *lpg.Edge) map[string]interface{} {
	res := make(map[string]interface{})
	e.ForEachProperty(func(s string, i interface{}) bool {
		res[s] = i
		return true
	})
	return res
}

const (
	createNodeOp int = iota
	updateNodeOp
	createEdgeOp
)

func nodeValuesToOperation(node *lpg.Node, op int) operation {
	switch op {
	case createNodeOp:
		n := createNode{}
		n.labels = node.GetLabels().Slice()
		n.props = getNodeProps(node)
		return n
	case updateNodeOp:
		n := updateNode{}
		n.labels = node.GetLabels().Slice()
		n.props = getNodeProps(node)
		return n
	}
	return nil
}

func edgeValuesToOperation(edge *lpg.Edge, op int) operation {
	e := createEdge{}
	e.types = []string{edge.GetLabel()}
	e.props = getEdgeProps(edge)
	return e
}

func (un updateNode) writeQuery() string {
	return fmt.Sprintf("MERGE (n)")
}
func (cn createNode) writeQuery() string {
	return fmt.Sprintf("MERGE (n)")
}
func (ue createEdge) writeQuery() string {
	return fmt.Sprintf("MERGE (n)")
}

func loadGraphByEntities(tx neo4j.Transaction, grph *lpg.Graph, rootIds []uint64, config Config, loadNeighbors func(neo4j.Transaction, []uint64) ([]neo4jNode, []neo4jNode, []neo4jEdge, error), selectEntity func(*lpg.Node) bool) (map[*lpg.Node]int64, error) {
	if len(rootIds) == 0 {
		return nil, fmt.Errorf("Empty entity schema nodes")
	}
	// neo4j IDs
	visitedNode := make(map[int64]*lpg.Node)
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
	mappedIds := make(map[*lpg.Node]int64)
	dbIds := make([]int64, 0, len(visitedNode))
	for id, n := range visitedNode {
		mappedIds[n] = id
		dbIds = append(dbIds, id)
	}
	return mappedIds, nil
}
