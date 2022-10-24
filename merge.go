package neo4j

import (
	"fmt"
	"log"
	"strings"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

const (
	mergeOp int = iota
	overwriteOp
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

func (d delta) isEmpty() bool {
	return len(d.removeLabel) == 0 && len(d.removeProp) == 0 && len(d.addLabel) == 0 && len(d.setProp) == 0
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

func setPropertiesDelta(delta delta, x withProperty, operationType int) {
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
func compareGraphNode(mem, db *lpg.Node, operationType int) delta {
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
func compareGraphEdge(mem, db *lpg.Edge, operationType int) delta {
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

func buildQueriesFromDeltas(deltas []delta, cfg Config) ([]string, []map[string]interface{}) {
	res := make([]string, 0)
	vars := make([]map[string]interface{}, 0)
	for _, d := range deltas {
		if d.isEmpty() {
			continue
		}
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
			q, v := op.writeQuery(cfg)
			res = append(res, q)
			vars = append(vars, v)
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
	return res, vars
}

type OperationQueue struct {
	Ops  []string
	vars []map[string]interface{}
}

func RunOperations(ctx *ls.Context, session *Session, tx neo4j.Transaction, ops OperationQueue) error {
	for ix, op := range ops.Ops {
		fmt.Println(op)
		_, err := tx.Run(op, ops.vars[ix])
		if err != nil {
			return err
		}
	}
	return nil
}

func Merge(memGraph, dbGraph *lpg.Graph, dbGraphIds map[*lpg.Node]int64, dbEdges map[*lpg.Edge]int64, config Config) (*lpg.Graph, OperationQueue, error) {
	memEntitiesMap := ls.GetEntityInfo(memGraph)
	dbEntitiesMap := ls.GetEntityInfo(dbGraph)
	nodeAssocations := make(map[*lpg.Node][]step)
	edgeAssocations := make(map[*lpg.Edge][]edgeStep)

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
				delete(dbEntitiesMap, db)
				continue
			}
		}
		ix++
	}
	deltas := make([]delta, 0)
	for ix := 0; ix < len(memEntities); ix++ {
		d, _ := mergeEntity(memEntities[ix], dbEntities[ix], dbGraph, dbGraphIds, dbEdges, nodeAssocations, edgeAssocations)
		deltas = append(deltas, d...)
	}
	// for _, steps := range nodeAssocations {
	// 	for _, step := range steps {
	// 		for edgeItr := memGraph.GetEdges(); edgeItr.Next(); {
	// 			edge := edgeItr.Edge()
	// 			if ls.GetEntityRootNode(edge.GetFrom()) != ls.GetEntityRootNode(edge.GetTo()) {
	// 				if _, ok := dbEdges[edge]; !ok {
	// 					dbFrom := dbGraph.NewNode(edge.GetFrom().GetLabels().Slice(), ls.CloneProperties(edge.GetFrom()))
	// 					dbEdge := dbGraph.NewEdge(dbFrom, step.node, edge.GetLabel(), ls.CloneProperties(edge))
	// 					de := compareGraphEdge(edge, dbEdge, mergeOp)
	// 					de.id = dbEdges[dbEdge]
	// 					de.operation = updateEdgeOp
	// 					de.setDeltaToEdge(dbEdge)
	// 					deltas = append(deltas, de)
	// 				}
	// 			}
	// 		}
	// 	}
	// }
	queries, vars := buildQueriesFromDeltas(deltas, config)
	opsQueue := OperationQueue{Ops: queries, vars: vars}
	return dbGraph, opsQueue, nil
}

func (dx delta) setDeltaToNode(dbNode *lpg.Node) {
	for _, v := range dx.removeProp {
		dbNode.RemoveProperty(v)
	}
	for k, v := range dx.setProp {
		dbNode.SetProperty(k, v)
	}
	if len(dx.addLabel) > 0 {
		dbNode.SetLabels(lpg.NewStringSet(dx.addLabel...))
	}
}

func (dx delta) setDeltaToEdge(dbEdge *lpg.Edge) {
	for _, v := range dx.removeProp {
		dbEdge.RemoveProperty(v)
	}
	for k, v := range dx.setProp {
		dbEdge.SetProperty(k, v)
	}
	if len(dx.addLabel) > 0 {
		dbEdge.SetLabel(strings.Join(dx.addLabel, ","))
	}
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

func mergeSubtree(memNode, dbNode, dbNodeParent *lpg.Node, dbGraph *lpg.Graph, dbGraphIds map[*lpg.Node]int64, dbEdges map[*lpg.Edge]int64, nodeAssociations map[*lpg.Node][]step, edgeAssociations map[*lpg.Edge][]edgeStep, isChild bool) []delta {
	deltas := make([]delta, 0)
	memItr := memNode.GetEdges(lpg.OutgoingEdge)

	if dbNode != nil {
		nodeAssociations[memNode] = []step{
			{
				node: dbNode,
			}}
	} else {
		// db entity root is null; create entity and connect to source root
		if !isChild {
			dbNodeParent = dbGraph.NewNode(memNode.GetLabels().Slice(), ls.CloneProperties(memNode))
			// dbGraph.NewEdge(dbNodeParent, dbNode, ls.HasTerm, nil)
			nodeAssociations[memNode] = []step{
				{
					node: dbNodeParent,
				},
			}
			followAllEdges := func(e *lpg.Edge) ls.EdgeFuncResult { return ls.FollowEdgeResult }
			ls.IterateAncestors(memNode, func(n *lpg.Node) bool {
				for edgeItr := n.GetEdges(lpg.AnyEdge); edgeItr.Next(); {
					edge := edgeItr.Edge()
					if edge.GetTo() == memNode {
						sourceItr := dbGraph.FindNodes(edge.GetFrom().GetLabels(), ls.CloneProperties(edge.GetFrom()))
						if sourceItr.Next() {
							dbGraph.NewEdge(sourceItr.Node(), dbNodeParent, ls.HasTerm, nil)
							return false
						}
						dbSource := dbGraph.NewNode(edge.GetFrom().GetLabels().Slice(), ls.CloneProperties(edge.GetFrom()))
						dbGraph.NewEdge(dbSource, dbNodeParent, ls.HasTerm, nil)
						return false
					}
				}
				return true
			}, followAllEdges)
		}
	}

	for memItr.Next() {
		memChildNode := memItr.Edge().GetTo()
		memChildEdge := memItr.Edge()
		if dbNode == nil {
			n := dbGraph.NewNode(memChildNode.GetLabels().Slice(), ls.CloneProperties(memChildNode))
			e := dbGraph.NewEdge(dbNodeParent, n, memChildEdge.GetLabel(), ls.CloneProperties(memChildEdge))
			nodeAssociations[memChildNode] = []step{
				{
					node: n,
					edge: e,
				}}
			edgeAssociations[memChildEdge] = []edgeStep{
				{
					to:   n,
					from: dbNodeParent,
					edge: e,
				}}
			continue
		}
		_, ok := memChildNode.GetProperty(ls.EntitySchemaTerm)
		if ok {
			if !ls.AsPropertyValue(memChildNode.GetProperty(ls.EntitySchemaTerm)).IsEqual(ls.AsPropertyValue(memNode.GetProperty(ls.EntitySchemaTerm))) {
				continue
			}
		}
		dbItr := dbNode.GetEdges(lpg.OutgoingEdge)
		if isChild {
			n := dbGraph.NewNode(memChildNode.GetLabels().Slice(), ls.CloneProperties(memChildNode))
			e := dbGraph.NewEdge(dbNode, n, memChildEdge.GetLabel(), ls.CloneProperties(memChildEdge))
			nodeAssociations[memChildNode] = []step{
				{
					node: n,
					edge: e,
				}}
			edgeAssociations[memChildEdge] = []edgeStep{
				{
					to:   n,
					from: dbNode,
					edge: e,
				}}
			deltas = append(deltas, mergeSubtree(memChildNode, n, dbNodeParent, dbGraph, dbGraphIds, dbEdges, nodeAssociations, edgeAssociations, true)...)
		}
		// updates
		seenDBPath := make(map[string]struct{})
		for dbItr.Next() {
			dbChildEdge := dbItr.Edge()
			mpv, ok := memChildNode.GetProperty(ls.SchemaNodeIDTerm)
			if !ok {
				break
			}
			dbpv, ok := dbChildEdge.GetTo().GetProperty(ls.SchemaNodeIDTerm)
			if !ok {
				break
			}
			seenDBPath[ls.AsPropertyValue(dbpv, true).AsString()] = struct{}{}
			if lpg.ComparePropertyValue(mpv, dbpv) == 0 {
				nodeAssociations[memChildNode] = []step{
					{
						node: dbChildEdge.GetTo(),
						edge: dbChildEdge,
					}}
				edgeAssociations[memChildEdge] = []edgeStep{
					{
						to:   dbChildEdge.GetTo(),
						from: dbChildEdge.GetFrom(),
						edge: dbChildEdge,
					}}
				break
			}
		}
		if len(seenDBPath) > 0 {
			if _, seen := seenDBPath[ls.AsPropertyValue(memChildNode.GetProperty(ls.SchemaNodeIDTerm)).AsString()]; !seen {
				n := dbGraph.NewNode(memChildNode.GetLabels().Slice(), ls.CloneProperties(memChildNode))
				e := dbGraph.NewEdge(dbNode, n, memChildEdge.GetLabel(), ls.CloneProperties(memChildEdge))
				nodeAssociations[memChildNode] = []step{
					{
						node: n,
						edge: e,
					}}
				edgeAssociations[memChildEdge] = []edgeStep{
					{
						to:   n,
						from: dbNode,
						edge: e,
					}}
				seenDBPath = make(map[string]struct{})
				deltas = append(deltas, mergeSubtree(memChildNode, n, dbNodeParent, dbGraph, dbGraphIds, dbEdges, nodeAssociations, edgeAssociations, true)...)
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
	return deltas
}

func mergeEntity(memEntityRoot, dbEntityRoot *lpg.Node, dbGraph *lpg.Graph, dbGraphIds map[*lpg.Node]int64, dbEdges map[*lpg.Edge]int64, nodeAssocations map[*lpg.Node][]step, edgeAssociations map[*lpg.Edge][]edgeStep) ([]delta, bool) {
	var deltas []delta
	deltas = mergeSubtree(memEntityRoot, dbEntityRoot, nil, dbGraph, dbGraphIds, dbEdges, nodeAssocations, edgeAssociations, false)
	return deltas, true
}

func (un updateNode) writeQuery(c Config) (string, map[string]interface{}) {
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
	return sb.String(), vars
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

func (s *Session) LoadDBGraph(tx neo4j.Transaction, memGraph *lpg.Graph, config Config) (*lpg.Graph, map[*lpg.Node]int64, map[*lpg.Edge]int64, error) {
	_, rootIds, _, _, err := s.CollectEntityDBIds(tx, config, memGraph)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(rootIds) == 0 {
		return lpg.NewGraph(), make(map[*lpg.Node]int64), make(map[*lpg.Edge]int64), nil
	}
	return loadGraphByEntities(tx, memGraph, rootIds, config, findNeighbors, func(n *lpg.Node) bool { return true })
}

func loadGraphByEntities(tx neo4j.Transaction, grph *lpg.Graph, rootIds []int64, config Config, loadNeighbors func(neo4j.Transaction, []uint64) ([]neo4jNode, []neo4jNode, []neo4jEdge, error), selectEntity func(*lpg.Node) bool) (*lpg.Graph, map[*lpg.Node]int64, map[*lpg.Edge]int64, error) {
	if len(rootIds) == 0 {
		return nil, nil, nil, fmt.Errorf("Empty entity schema nodes")
	}
	// neo4j IDs
	visitedNode := make(map[int64]*lpg.Node)
	visitedEdge := make(map[int64]*lpg.Edge)
	queue := make([]uint64, 0, len(rootIds))
	for _, id := range rootIds {
		queue = append(queue, uint64(id))
	}

	for len(queue) > 0 {
		srcNodes, adjNodes, adjRelationships, err := loadNeighbors(tx, queue)
		queue = queue[len(queue):]
		if err != nil {
			return nil, nil, nil, err
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
			e := grph.NewEdge(src, target, config.Expand(edge.types), edge.props)
			visitedEdge[edge.id] = e
		}
	}
	mappedIds := make(map[*lpg.Node]int64)
	mappedEdgeIds := make(map[*lpg.Edge]int64)
	for id, n := range visitedNode {
		mappedIds[n] = id
	}
	for id, e := range visitedEdge {
		mappedEdgeIds[e] = id
	}
	return grph, mappedIds, mappedEdgeIds, nil
}
