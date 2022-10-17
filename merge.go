package neo4j

import (
	"fmt"
	"log"
	"strings"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

var (
	mergeOp     bool
	overwriteOp bool
)

type delta struct {
	id          int64
	addLabel    []string
	setProp     map[string]interface{}
	removeLabel []string
	removeProp  []string
}

func (d delta) isUpdateEmpty() bool {
	return len(d.addLabel) == 0 && len(d.setProp) == 0
}

func (d delta) isRemoveEmpty() bool {
	return len(d.removeLabel) == 0 && len(d.removeProp) == 0
}

func findNodeLabelDiff(memLabels, dbLabels lpg.StringSet) []string {
	ret := make([]string, 0)
	for _, x := range memLabels.Slice() {
		if !dbLabels.Has(x) {
			ret = append(ret, x)
		}
	}
	return ret
}

// compares two lpg nodes and returns a diff
func compareGraphNode(mem, db *lpg.Node, operationType bool) delta {
	delta := delta{}
	if db == nil {
		delta.addLabel = mem.GetLabels().Slice()
		delta.setProp = ls.CloneProperties(mem)
		return delta
	}
	delta.addLabel = findNodeLabelDiff(mem.GetLabels(), db.GetLabels())
	// Expected properties must be a subset
	mem.ForEachProperty(func(k string, v interface{}) bool {
		pv, ok := v.(*ls.PropertyValue)
		if !ok {
			return true
		}
		v2, ok := mem.GetProperty(k)
		if !ok {
			log.Printf("Error at %s: %v: Property does not exist", k, v)
			delta.setProp[k] = pv
			return false
		}
		pv2, ok := v2.(*ls.PropertyValue)
		if !ok {
			log.Printf("Error at %s: %v: Not property value", k, v)
			return false
		}
		if !pv2.IsEqual(pv) {
			log.Printf("Error at %s: Got %v, Expected %v: Values are not equal", k, pv, pv2)
			delta.setProp[k] = pv2
			return false
		}
		return true
	})
	if operationType == overwriteOp {
		for _, l := range db.GetLabels().Slice() {
			if !mem.HasLabel(l) {
				delta.removeLabel = append(delta.removeLabel, l)
			}
		}
		db.ForEachProperty(func(s string, i interface{}) bool {
			_, ok := mem.GetProperty(s)
			if !ok {
				delta.removeProp = append(delta.removeProp, s)
			}
			return true
		})
	}
	return delta
}

// compares two native lpg nodes and returns a diff
func compareGraphEdge(mem, db *lpg.Edge, operationType bool) delta {
	delta := delta{}
	if db == nil {
		delta.addLabel = []string{mem.GetLabel()}
		delta.setProp = ls.CloneProperties(mem)
	}
	if mem.GetLabel() != db.GetLabel() {
		delta.addLabel = []string{mem.GetLabel()}
	}
	mem.ForEachProperty(func(k string, v interface{}) bool {
		pv, ok := v.(*ls.PropertyValue)
		if !ok {
			return true
		}
		v2, ok := mem.GetProperty(k)
		if !ok {
			log.Printf("Error at %s: %v: Property does not exist", k, v)
			delta.setProp[k] = pv
			return false
		}
		pv2, ok := v2.(*ls.PropertyValue)
		if !ok {
			log.Printf("Error at %s: %v: Not property value", k, v)
			return false
		}
		if !pv2.IsEqual(pv) {
			log.Printf("Error at %s: Got %v, Expected %v: Values are not equal", k, pv, pv2)
			delta.setProp[k] = pv2
			return false
		}
		return true
	})
	if operationType == overwriteOp {
		if mem.GetLabel() != db.GetLabel() {
			delta.removeLabel = append(delta.removeLabel, db.GetLabel())
		}
		db.ForEachProperty(func(s string, i interface{}) bool {
			_, ok := mem.GetProperty(s)
			if !ok {
				delta.removeProp = append(delta.removeProp, s)
			}
			return true
		})
	}
	return delta
}

type OperationQueue struct {
	ops []fmt.Stringer
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

type updateEdge struct {
	id    int64
	types []string
	props map[string]interface{}
}

func buildQueriesFromOperations(ops []fmt.Stringer) []string {
	res := make([]string, 0)
	for _, op := range ops {
		res = append(res, op.String())
	}
	return res
}

func Merge(memGraph, dbGraph *lpg.Graph, dbGraphIds map[*lpg.Node]int64, dbEdges map[*lpg.Edge]int64) (*lpg.Graph, []fmt.Stringer, error) {
	ops := make([]fmt.Stringer, 0)
	memEntitiesMap := ls.GetEntityInfo(memGraph)
	dbEntitiesMap := ls.GetEntityInfo(dbGraph)
	findDBCounterpart := func(n *lpg.Node, schemaPV, idPV *ls.PropertyValue) *lpg.Node {
		if ls.AsPropertyValue(n.GetProperty(ls.EntitySchemaTerm)).IsEqual(schemaPV) {
			if ls.AsPropertyValue(n.GetProperty(ls.EntityIDTerm)).IsEqual(idPV) {
				return n
			}
		}
		return nil
	}
	memEntities := make([]*lpg.Node, 0, len(memEntitiesMap))
	dbEntities := make([]*lpg.Node, 0, len(memEntitiesMap))
	// align by index matching entity roots i.e, first entity in mem slice should match first entity in db slice
	for n := range memEntitiesMap {
		schemaPV := ls.AsPropertyValue(n.GetProperty(ls.EntitySchemaTerm))
		eidPV := ls.AsPropertyValue(n.GetProperty(ls.EntityIDTerm))
		memEntities = append(memEntities, n)
		for db := range dbEntitiesMap {
			if findDBCounterpart(db, schemaPV, eidPV) != nil {
				dbEntities = append(dbEntities, db)
				delete(dbEntitiesMap, db)
			}
		}
	}
	for ix := 0; ix < len(memEntities); ix++ {
		if ix >= len(dbEntities) {
			mergeEntity(memEntities[ix], nil)
		} else {
			mergeEntity(memEntities[ix], dbEntities[ix])
		}
	}
	return dbGraph, ops, nil
}

func (dx delta) setDeltaToNode(dbNode *lpg.Node) {
	for _, v := range dx.removeProp {
		dbNode.RemoveProperty(v)
	}
	for k, v := range dx.setProp {
		dbNode.SetProperty(k, v)
	}
	dbNode.SetLabels(lpg.NewStringSet(dx.addLabel...))
}

func (dx delta) setDeltaToEdge(dbEdge *lpg.Edge) {
	for _, v := range dx.removeProp {
		dbEdge.RemoveProperty(v)
	}
	for k, v := range dx.setProp {
		dbEdge.SetProperty(k, v)
	}
	dbEdge.SetLabel(strings.Join(dx.addLabel, ","))
}

func mergeEntity(memParent, dbParent *lpg.Node) ([]delta, bool) {
	memOutgoing := memParent.GetEdges(lpg.OutgoingEdge)
	dbOutgoing := dbParent.GetEdges(lpg.OutgoingEdge)
	var edgeFunc func(e *lpg.Edge) ls.EdgeFuncResult
	descendWithEdgeResult := func(follow ls.EdgeFuncResult, memOutgoing, dbOutgoing *lpg.Edge, ef func(e *lpg.Edge) ls.EdgeFuncResult) bool {
		if ef != nil {
			follow = edgeFunc(memOutgoing)
		}
		switch follow {
		case ls.StopEdgeResult:
			return false
		case ls.SkipEdgeResult:
		case ls.FollowEdgeResult:
			memNext := memOutgoing.GetTo()
			dbNext := dbOutgoing.GetTo()
			_, ok := mergeEntity(memNext, dbNext)
			if !ok {
				return false
			}
		}
		return true
	}
	var memOutgoingEdge *lpg.Edge
	var dbOutgoingEdge *lpg.Edge
	var deltasNode []delta
	var deltasEdge []delta
	follow := ls.FollowEdgeResult
	// if both entities exist, descend down in tandem
	for memOutgoing.Next() && dbOutgoing.Next() {
		memOutgoingEdge := memOutgoing.Edge()
		dbOutgoingEdge := dbOutgoing.Edge()
		deltasNode = compareGraphNode(memOutgoingEdge.GetTo(), dbOutgoingEdge.GetTo(), mergeOp)
		deltasEdge = compareGraphEdge(memOutgoingEdge, dbOutgoingEdge)
		setDeltaToNode(dbOutgoingEdge.GetTo(), deltasNode)
		setDeltaToEdge(dbOutgoingEdge, deltasEdge)
		*ops = append(*ops, nodeDeltasToOperation(memOutgoingEdge.GetTo(), deltasNode, updateNodeOp), edgeDeltasToOperation(memOutgoingEdge, deltasEdge, createEdgeOp))
		descendWithEdgeResult(follow, memOutgoingEdge, dbOutgoingEdge, edgeFunc)
	}
	// if both entities do not exist or db entity is nil, descend down only path mem path and create db path with it
	if !dbOutgoing.Next() {
		for memOutgoing.Next() {
			memOutgoingEdge = memOutgoing.Edge()
			deltasNode = compareGraphNode(memOutgoingEdge.GetTo(), nil, overwriteOp)
			deltasEdge = compareGraphEdge(memOutgoingEdge, nil)
			setDeltaToNode(dbOutgoingEdge.GetTo(), deltasNode)
			setDeltaToEdge(dbOutgoingEdge, deltasEdge)
			*ops = append(*ops, nodeDeltasToOperation(memOutgoingEdge.GetTo(), deltasNode, createNodeOp), edgeDeltasToOperation(memOutgoingEdge, deltasEdge, updateEdgeOp))
			descendWithEdgeResult(follow, memOutgoingEdge, dbOutgoingEdge, edgeFunc)
		}
	}
	return true
}

const (
	createNodeOp int = iota
	updateNodeOp
	createEdgeOp
	updateEdgeOp
)

func nodeDeltasToOperation(node *lpg.Node, d []delta, op int) fmt.Stringer {
	switch op {
	case createNodeOp:
		n := createNode{}
		n.labels = node.GetLabels().Slice()
		n.props = ls.CloneProperties(node)
		for _, dx := range d {
			n.labels = append(n.labels, dx.addLabel...)
			for k, v := range dx.setProp {
				n.props[k] = v
			}
		}
		return n
	case updateNodeOp:
		n := updateNode{}
		n.labels = node.GetLabels().Slice()
		n.props = ls.CloneProperties(node)
		for _, dx := range d {
			n.labels = append(n.labels, dx.addLabel...)
			for k, v := range dx.setProp {
				n.props[k] = v
			}
		}
		return n
	}
	return nil
}

func edgeDeltasToOperation(edge *lpg.Edge, d []delta, op int) fmt.Stringer {
	switch op {
	case createEdgeOp:
		e := createEdge{}
		e.types = []string{edge.GetLabel()}
		e.props = ls.CloneProperties(edge)
		for _, dx := range d {
			e.types = append(e.types, dx.addLabel...)
			for k, v := range dx.setProp {
				e.props[k] = v
			}
		}
		return e
	case updateEdgeOp:
		e := updateEdge{}
		e.types = []string{edge.GetLabel()}
		e.props = ls.CloneProperties(edge)
		for _, dx := range d {
			e.types = append(e.types, dx.addLabel...)
			for k, v := range dx.setProp {
				e.props[k] = v
			}
		}
		return e
	}
	return nil
}

func nodeValuesToOperation(node *lpg.Node, op int) fmt.Stringer {
	switch op {
	case createNodeOp:
		n := createNode{}
		n.labels = node.GetLabels().Slice()
		n.props = ls.CloneProperties(node)
		return n
	case updateNodeOp:
		n := updateNode{}
		n.labels = node.GetLabels().Slice()
		n.props = ls.CloneProperties(node)
		return n
	}
	return nil
}

func edgeValuesToOperation(edge *lpg.Edge, op int) fmt.Stringer {
	e := createEdge{}
	e.types = []string{edge.GetLabel()}
	e.props = ls.CloneProperties(edge)
	return e
}

func (un updateNode) String() string {
	return fmt.Sprintf("MERGE (n)")
}
func (cn createNode) String() string {
	return fmt.Sprintf("MERGE (n)")
}
func (ce createEdge) String() string {
	return fmt.Sprintf("MERGE (n)")
}

func (ue updateEdge) String() string {
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
