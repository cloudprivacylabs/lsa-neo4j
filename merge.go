package neo4j

import (
	"errors"
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

func merge(memGraph, dbGraph *lpg.Graph, memGraphIds, dbGraphIds map[*lpg.Node]int64, memEdges, dbEdges map[*lpg.Edge]int64) (*lpg.Graph, []string, error) {
	grph := lpg.NewGraph()
	ops := make([]string, 0)

	for memNode, _ := range memGraphIds {
		memEdgeItr := memNode.GetEdges(lpg.AnyEdge)
		memEdge := memEdgeItr.Edge()
		memParent := memEdgeItr.Edge().GetFrom()
		// memChild := memEdgeItr.Edge().GetTo()
		for dbNode, _ := range dbGraphIds {
			dbEdgeItr := dbNode.GetEdges(lpg.AnyEdge)
			dbEdge := dbEdgeItr.Edge()
			dbParent := dbEdgeItr.Edge().GetFrom()
			// dbChild := dbEdgeItr.Edge().GetTo()
			p1, ok := memNode.GetProperty(ls.SchemaNodeIDTerm)
			if !ok {
				return nil, nil, errors.New("must have schema node id term")
			}
			p2, ok := dbNode.GetProperty(ls.SchemaNodeIDTerm)
			if !ok {
				return nil, nil, errors.New("must have schema node id term")
			}
			if ls.AsPropertyValue(p1, true).AsString() == ls.AsPropertyValue(p2, true).AsString() {
				if memParent.GetLabels().IsEqual(dbParent.GetLabels()) && compareNativeEdge(memEdge, dbEdge) {
					cmp := compareNativeNode(memNode, dbNode)
					if !cmp {
						ops = append(ops, writeQuery(memNode, dbGraphIds[dbNode]))
					}
				}
			}
		}
	}

	return grph, ops, nil
}

func writeQuery(n *lpg.Node, id int64) string {
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
