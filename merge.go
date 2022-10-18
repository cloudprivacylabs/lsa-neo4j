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
	isNode      bool
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

func setPropertiesDelta(delta delta, x withProperty, operationType bool) {
	x.ForEachProperty(func(k string, v interface{}) bool {
		pv, ok := v.(*ls.PropertyValue)
		if !ok {
			return true
		}
		v2, ok := x.GetProperty(k)
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
}

func setNodeOverwrite(mem, db *lpg.Node, delta delta) {
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
func setEdgeOverwrite(mem, db *lpg.Edge, delta delta) {
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

// compares two lpg nodes and returns a diff
func compareGraphNode(mem, db *lpg.Node, operationType bool) delta {
	delta := delta{isNode: true}
	if db == nil {
		delta.addLabel = mem.GetLabels().Slice()
		delta.setProp = ls.CloneProperties(mem)
		return delta
	}
	delta.addLabel = findNodeLabelDiff(mem.GetLabels(), db.GetLabels())
	// Expected properties must be a subset
	setPropertiesDelta(delta, mem, operationType)
	if operationType == overwriteOp {
		setNodeOverwrite(mem, db, delta)
	}
	return delta
}

// compares two native lpg nodes and returns a diff
func compareGraphEdge(mem, db *lpg.Edge, operationType bool) delta {
	delta := delta{isNode: false}
	if db == nil {
		delta.addLabel = []string{mem.GetLabel()}
		delta.setProp = ls.CloneProperties(mem)
	}
	if mem.GetLabel() != db.GetLabel() {
		delta.addLabel = []string{mem.GetLabel()}
	}
	setPropertiesDelta(delta, mem, mergeOp)
	if operationType == overwriteOp {
		setEdgeOverwrite(mem, db, delta)
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
	typ   []string
	props map[string]interface{}
}

type updateEdge struct {
	id    int64
	typ   []string
	props map[string]interface{}
}

func buildQueriesFromOperations(ops []fmt.Stringer) []string {
	res := make([]string, 0)
	for _, op := range ops {
		res = append(res, op.String())
	}
	return res
}

func Merge(memGraph, dbGraph *lpg.Graph, dbGraphIds map[*lpg.Node]int64, dbEdges map[*lpg.Edge]int64) (*lpg.Graph, []string, error) {
	qs := make([]string, 0)
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
	deltas := make([]delta, 0)
	for ix := 0; ix < len(memEntities); ix++ {
		d, _ := mergeEntity(memEntities[ix], dbEntities[ix], dbGraphIds, dbEdges)
		deltas = append(deltas, d...)
	}
	ops := make([]fmt.Stringer, 0)
	qs = append(qs, buildQueriesFromOperations(ops)...)
	return dbGraph, qs, nil
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

type step struct {
	edge *lpg.Edge
	node *lpg.Node
}

type edgeStep struct {
	to   *lpg.Node
	from *lpg.Node
	edge *lpg.Edge
}

func matchDBChildNode(memNode, dbNode *lpg.Node, dbGraphIds map[*lpg.Node]int64, dbEdges map[*lpg.Edge]int64) []delta {
	nodeAssociations := make(map[*lpg.Node][]step)
	edgeAssociations := make(map[*lpg.Edge][]edgeStep)
	unmapped := make([]step, 0)
	deltas := make([]delta, 0)
	memItr := memNode.GetEdges(lpg.OutgoingEdge)
	dbItr := dbNode.GetEdges(lpg.OutgoingEdge)
MEM:
	for memItr.Next() {
		memChildNode := memItr.Edge().GetTo()
		memChildEdge := memItr.Edge()
		if !dbItr.Next() {
			unmapped = append(unmapped, step{node: memChildNode, edge: memChildEdge})
		}
		for dbItr.Next() {
			dbChildNode := dbItr.Edge().GetTo()
			dbChildEdge := dbItr.Edge()
			mpv, ok := memChildNode.GetProperty(ls.SchemaNodeIDTerm)
			if !ok {
				panic("")
			}
			dbpv, ok := memChildNode.GetProperty(ls.SchemaNodeIDTerm)
			if !ok {
				panic("")
			}
			if lpg.ComparePropertyValue(mpv, dbpv) == 0 {
				nodeAssociations[memChildNode] = []step{
					{
						node: dbChildNode,
						edge: dbChildEdge,
					}}
				edgeAssociations[memChildEdge] = []edgeStep{
					{
						to:   dbChildEdge.GetFrom(),
						from: dbChildEdge.GetTo(),
						edge: dbChildEdge,
					}}
				continue MEM
			}
		}
	}
	for mn, steps := range nodeAssociations {
		for _, step := range steps {
			dn := compareGraphNode(mn, step.node, mergeOp)
			dn.id = dbGraphIds[step.node]
			dn.setDeltaToNode(step.node)
			deltas = append(deltas, dn)
		}
	}
	for memEdge, steps := range edgeAssociations {
		for _, step := range steps {
			de := compareGraphEdge(memEdge, step.edge, mergeOp)
			de.id = dbEdges[step.edge]
			de.setDeltaToEdge(step.edge)
			deltas = append(deltas, de)
		}
	}
	for _, step := range unmapped {
		dn := compareGraphNode(step.node, nil, overwriteOp)
		dn.id = dbGraphIds[step.node]
		dn.setDeltaToNode(step.node)
		deltas = append(deltas, dn)
		de := compareGraphEdge(step.edge, nil, overwriteOp)
		de.id = dbEdges[step.edge]
		de.setDeltaToEdge(step.edge)
		deltas = append(deltas, de)
	}
	return deltas
}

func mergeEntity(memParent, dbParent *lpg.Node, dbGraphIds map[*lpg.Node]int64, dbEdges map[*lpg.Edge]int64) ([]delta, bool) {
	memOutgoing := memParent.GetEdges(lpg.OutgoingEdge)
	dbOutgoing := dbParent.GetEdges(lpg.OutgoingEdge)
	deltas := matchDBChildNode(memOutgoing.Edge().GetTo(), dbOutgoing.Edge().GetTo(), dbGraphIds, dbEdges)
	return deltas, true
}

const (
	createNodeOp int = iota
	updateNodeOp
	createEdgeOp
	updateEdgeOp
)

func nodeDeltasToOperation(node *lpg.Node, d delta, op int) fmt.Stringer {
	switch op {
	case createNodeOp:
		n := createNode{}
		n.labels = d.addLabel
		n.props = d.setProp
		return n
	case updateNodeOp:
		n := updateNode{}
		n.labels = d.addLabel
		n.props = d.setProp
		return n
	}
	return nil
}

func edgeDeltasToOperation(edge *lpg.Edge, d delta, op int) fmt.Stringer {
	switch op {
	case createEdgeOp:
		e := createEdge{}
		e.typ = d.addLabel
		e.props = d.setProp
		return e
	case updateEdgeOp:
		e := updateEdge{}
		e.typ = d.addLabel
		e.props = d.setProp
		return e
	}
	return nil
}

// func nodeValuesToOperation(node *lpg.Node, op int) fmt.Stringer {
// 	switch op {
// 	case createNodeOp:
// 		n := createNode{}
// 		n.labels = node.GetLabels().Slice()
// 		n.props = ls.CloneProperties(node)
// 		return n
// 	case updateNodeOp:
// 		n := updateNode{}
// 		n.labels = node.GetLabels().Slice()
// 		n.props = ls.CloneProperties(node)
// 		return n
// 	}
// 	return nil
// }

// func edgeValuesToOperation(edge *lpg.Edge, op int) fmt.Stringer {
// 	e := createEdge{}
// 	e.types = []string{edge.GetLabel()}
// 	e.props = ls.CloneProperties(edge)
// 	return e
// }

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
