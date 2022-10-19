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

const (
	createNodeOp int = iota
	updateNodeOp
	createEdgeOp
	updateEdgeOp
)

type delta struct {
	operation   int
	node        *lpg.Node
	edge        *lpg.Edge
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
	if len(ret) == 0 {
		return memLabels.Slice()
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
			return false
		}
		delta.setProp[k] = pv
		pv2, ok := v2.(*ls.PropertyValue)
		if !ok {
			log.Printf("Error at %s: %v: Not property value", k, v)
			return false
		}
		if !pv2.IsEqual(pv) {
			log.Printf("Error at %s: Got %v, Expected %v: Values are not equal", k, pv, pv2)
			return false
		}
		delta.setProp[k] = pv2
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
	delta := delta{isNode: true, setProp: make(map[string]interface{})}
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
	delta := delta{isNode: false, setProp: make(map[string]interface{})}
	if db == nil {
		delta.addLabel = []string{mem.GetLabel()}
		delta.setProp = ls.CloneProperties(mem)
		return delta
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

type updateNode struct {
	id     int64
	labels []string
	props  map[string]interface{}
	n      *lpg.Node
}

type createNode struct {
	id     int64
	labels []string
	props  map[string]interface{}
	n      *lpg.Node
}

type createEdge struct {
	id               int64
	typ              []string
	props            map[string]interface{}
	e                *lpg.Edge
	fromNode, toNode int64
}

type updateEdge struct {
	id               int64
	typ              []string
	props            map[string]interface{}
	e                *lpg.Edge
	fromNode, toNode int64
}

func buildQueriesFromDeltas(deltas []delta, cfg Config) []string {
	res := make([]string, 0)
	for _, d := range deltas {
		switch d.operation {
		case createNodeOp:
			op := createNode{
				n:      d.node,
				id:     d.id,
				labels: d.addLabel,
				props:  d.setProp,
			}
			res = append(res, op.writeQuery(cfg))
		case updateNodeOp:
			op := updateNode{
				n:      d.node,
				id:     d.id,
				labels: d.addLabel,
				props:  d.setProp,
			}
			res = append(res, op.writeQuery(cfg))
		case updateEdgeOp:
			op := updateEdge{
				e:     d.edge,
				id:    d.id,
				typ:   d.addLabel,
				props: d.setProp,
			}
			res = append(res, op.writeQuery(cfg))
		case createEdgeOp:
			op := createEdge{
				e:     d.edge,
				id:    d.id,
				typ:   d.addLabel,
				props: d.setProp,
			}
			res = append(res, op.writeQuery(cfg))
		}
	}
	return res
}

type OperationQueue struct {
	ops []string
}

func Merge(memGraph, dbGraph *lpg.Graph, dbGraphIds map[*lpg.Node]int64, dbEdges map[*lpg.Edge]int64, config Config) (*lpg.Graph, OperationQueue, error) {
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
	memEntities := make([]*lpg.Node, len(memEntitiesMap))
	dbEntities := make([]*lpg.Node, len(memEntitiesMap))
	// align by index matching entity roots i.e, first entity in mem slice should match first entity in db slice
	ix := 0
	for n := range memEntitiesMap {
		schemaPV := ls.AsPropertyValue(n.GetProperty(ls.EntitySchemaTerm))
		eidPV := ls.AsPropertyValue(n.GetProperty(ls.EntityIDTerm))
		memEntities[ix] = n
		for db := range dbEntitiesMap {
			if findDBCounterpart(db, schemaPV, eidPV) != nil {
				dbEntities[ix] = db
				// delete(dbEntitiesMap, db)
				continue
			}
		}
		ix++
	}
	deltas := make([]delta, 0)
	for ix := 0; ix < len(memEntities); ix++ {
		d, _ := mergeEntity(memEntities[ix], dbEntities[ix], dbGraph, dbGraphIds, dbEdges)
		deltas = append(deltas, d...)
	}
	queries := buildQueriesFromDeltas(deltas, config)
	opsQueue := OperationQueue{ops: queries}
	return dbGraph, opsQueue, nil
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

func matchDBChildNode(memNode, dbNode *lpg.Node, dbGraph *lpg.Graph, dbGraphIds map[*lpg.Node]int64, dbEdges map[*lpg.Edge]int64) []delta {
	nodeAssociations := make(map[*lpg.Node][]step)
	edgeAssociations := make(map[*lpg.Edge][]edgeStep)
	unmapped := make([]step, 0)
	deltas := make([]delta, 0)
	memItr := memNode.GetEdges(lpg.OutgoingEdge)
	var dbParent *lpg.Node

	if dbNode != nil {
		nodeAssociations[memNode] = []step{
			{
				node: dbNode,
			}}
	} else {

		dbParent = dbGraph.NewNode(memNode.GetLabels().Slice(), ls.CloneProperties(memNode))

		// dbGraph.NewEdge(dbNode, n, memChildEdge.GetLabel(), ls.CloneProperties(memChildEdge))
	}

	for memItr.Next() {
		memChildNode := memItr.Edge().GetTo()
		memChildEdge := memItr.Edge()
		if dbNode == nil {
			n := dbGraph.NewNode(memChildNode.GetLabels().Slice(), ls.CloneProperties(memChildNode))
			dbGraph.NewEdge(dbParent, n, memChildEdge.GetLabel(), ls.CloneProperties(memChildEdge))
			nodeAssociations[memChildNode] = []step{
				{
					node: n,
					edge: memChildEdge,
				}}
			edgeAssociations[memChildEdge] = []edgeStep{
				{
					to:   memChildEdge.GetFrom(),
					from: memChildEdge.GetTo(),
					edge: memChildEdge,
				}}
			// dbNode = memChildNode
			continue
		}
		dbItr := dbNode.GetEdges(lpg.OutgoingEdge)
		// creates
		if !dbItr.Next() {
			n := dbGraph.NewNode(memChildNode.GetLabels().Slice(), ls.CloneProperties(memChildNode))
			dbGraph.NewEdge(dbNode, n, memChildEdge.GetLabel(), ls.CloneProperties(memChildEdge))
			unmapped = append(unmapped, step{node: memChildNode, edge: memChildEdge})
		}
		// updates
		for dbItr.Next() {
			dbChildNode := dbItr.Edge().GetTo()
			dbChildEdge := dbItr.Edge()
			mpv, ok := memChildNode.GetProperty(ls.SchemaNodeIDTerm)
			if !ok {
				break
			}
			dbpv, ok := memChildNode.GetProperty(ls.SchemaNodeIDTerm)
			if !ok {
				break
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
				break
			}
		}
	}
	for mn, steps := range nodeAssociations {
		for _, step := range steps {
			dn := compareGraphNode(mn, step.node, mergeOp)
			dn.id = dbGraphIds[step.node]
			dn.operation = updateNodeOp
			dn.node = step.node
			dn.setDeltaToNode(step.node)
			deltas = append(deltas, dn)
		}
	}
	for memEdge, steps := range edgeAssociations {
		for _, step := range steps {
			de := compareGraphEdge(memEdge, step.edge, mergeOp)
			de.id = dbEdges[step.edge]
			de.operation = updateEdgeOp
			de.setDeltaToEdge(step.edge)
			deltas = append(deltas, de)
		}
	}
	for _, step := range unmapped {
		dn := compareGraphNode(step.node, nil, overwriteOp)
		dn.operation = createNodeOp
		dn.id = dbGraphIds[step.node]
		dn.setDeltaToNode(step.node)
		dn.node = step.node
		deltas = append(deltas, dn)
		de := compareGraphEdge(step.edge, nil, overwriteOp)
		de.operation = createEdgeOp
		de.id = dbEdges[step.edge]
		de.edge = step.edge
		de.setDeltaToEdge(step.edge)
		deltas = append(deltas, de)
	}
	return deltas
}

func mergeEntity(memParent, dbParent *lpg.Node, dbGraph *lpg.Graph, dbGraphIds map[*lpg.Node]int64, dbEdges map[*lpg.Edge]int64) ([]delta, bool) {
	var memOutgoing lpg.EdgeIterator
	var dbOutgoing lpg.EdgeIterator
	var deltas []delta
	memOutgoing = memParent.GetEdges(lpg.OutgoingEdge)
	if memOutgoing.Edge() != nil {
		if dbParent != nil {
			dbOutgoing = dbParent.GetEdges(lpg.OutgoingEdge)
			deltas = matchDBChildNode(memOutgoing.Edge().GetTo(), dbOutgoing.Edge().GetTo(), dbGraph, dbGraphIds, dbEdges)
		} else {
			deltas = matchDBChildNode(memOutgoing.Edge().GetTo(), nil, dbGraph, dbGraphIds, dbEdges)
		}
	} else {
		if dbParent != nil {
			dbOutgoing = dbParent.GetEdges(lpg.OutgoingEdge)
			deltas = matchDBChildNode(memParent, dbParent, dbGraph, dbGraphIds, dbEdges)
		} else {
			deltas = matchDBChildNode(memParent, nil, dbGraph, dbGraphIds, dbEdges)
		}
	}
	return deltas, true
}

func (un updateNode) writeQuery(c Config) string {
	vars := make(map[string]interface{})
	sb := strings.Builder{}
	prop := c.MakeProperties(un.n, vars)
	labels := c.MakeLabels(un.labels)
	if len(labels) > 0 && len(prop) > 0 {
		sb.WriteString(fmt.Sprintf("MATCH (n) WHERE ID(n) = %d SET n = %v SET n%s", un.id, prop, labels))
	} else if len(labels) == 0 && len(prop) > 0 {
		sb.WriteString(fmt.Sprintf("MATCH (n) WHERE ID(n) = %d SET n = %v", un.id, prop))
	} else if len(prop) == 0 && len(labels) > 0 {
		sb.WriteString(fmt.Sprintf("MATCH (n) WHERE ID(n) = %d SET n:%s", un.id, labels))
	}
	// for ix, l := range labels {
	// 	sb.WriteString(fmt.Sprintf(":%s", l))
	// 	if ix < len(labels) {
	// 		sb.WriteString(fmt.Sprintf("%s", l))
	// 	}
	// }
	return sb.String()
}
func (cn createNode) writeQuery(c Config) string {
	vars := make(map[string]interface{})
	sb := strings.Builder{}
	prop := c.MakeProperties(cn.n, vars)
	labels := c.MakeLabels(cn.labels)
	sb.WriteString(fmt.Sprintf("(n%d%s %s)", cn.id, labels, prop))

	// builder := strings.Builder{}
	// builder.WriteString(fmt.Sprintf("ID(n%d)", cn.id))
	return fmt.Sprintf("CREATE %s RETURN ID(n%d)", sb.String(), cn.id)
}
func (ce createEdge) writeQuery(c Config) string {
	sb := strings.Builder{}
	label := c.MakeLabels(ce.typ)
	sb.WriteString(fmt.Sprintf("MATCH (n) WHERE ID(n) = %d MATCH (m) WHERE ID(m) = %d CREATE (n)-[%s]->(m)", ce.fromNode, ce.toNode, label))
	return sb.String()
}

func (ue updateEdge) writeQuery(c Config) string {
	sb := strings.Builder{}
	label := c.MakeLabels(ue.typ)
	if len(label) > 0 && ue.e != nil {
		sb.WriteString(fmt.Sprintf("MATCH (n)-[rel:%s]->(m) MATCH (n) WHERE ID(n) = %d MATCH (m) WHERE ID(m) = %d MERGE (n)-[:%s]->(m) DELETE rel",
			ue.e.GetLabel(), ue.fromNode, ue.toNode, label))
	}
	return sb.String()
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
