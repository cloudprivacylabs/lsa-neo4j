package neo4j

import (
	"fmt"
	"log"
	"strings"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type operation int

const (
	updateOp operation = iota
	createOp
)

const (
	createNodeOp int = 100 >> iota
	updateNodeOp
	createEdgeOp
	updateEdgeOp
)

type Delta struct {
	operation              int
	node, fromNode, toNode *lpg.Node
	edge                   *lpg.Edge
	id                     int64
	isNode                 bool
	addLabel               []string
	setProp                map[string]interface{}
	removeLabel            []string
	removeProp             []string
}

func (d Delta) isEmpty() bool {
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

func setPropertiesDelta(Delta Delta, x withProperty, operationType operation) {
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
		Delta.setProp[k] = pv
		pv2, ok := v2.(*ls.PropertyValue)
		if !ok {
			log.Printf("Error at %s: %v: Not property value", k, v)
			return false
		}
		if !pv2.IsEqual(pv) {
			log.Printf("Error at %s: Got %v, Expected %v: Values are not equal", k, pv, pv2)
			return false
		}
		Delta.setProp[k] = pv2
		return true
	})
}

func setNodeOverwrite(mem, db *lpg.Node, Delta Delta) {
	for _, l := range db.GetLabels().Slice() {
		if !mem.HasLabel(l) {
			Delta.removeLabel = append(Delta.removeLabel, l)
		}
	}
	db.ForEachProperty(func(s string, i interface{}) bool {
		_, ok := mem.GetProperty(s)
		if !ok {
			Delta.removeProp = append(Delta.removeProp, s)
		}
		return true
	})
	// Delta.addLabel = mem.GetLabels().Slice()
	// Delta.setProp = ls.CloneProperties(mem)
}
func setEdgeOverwrite(mem, db *lpg.Edge, Delta Delta) {
	if mem.GetLabel() != db.GetLabel() {
		Delta.removeLabel = append(Delta.removeLabel, db.GetLabel())
	}
	db.ForEachProperty(func(s string, i interface{}) bool {
		_, ok := mem.GetProperty(s)
		if !ok {
			Delta.removeProp = append(Delta.removeProp, s)
		}
		return true
	})
	Delta.addLabel = []string{mem.GetLabel()}
	Delta.setProp = ls.CloneProperties(mem)
}

// compares two lpg nodes and returns a diff
func compareGraphNode(mem, db *lpg.Node, operationType operation) Delta {
	Delta := Delta{isNode: true, setProp: make(map[string]interface{})}
	if db == nil {
		Delta.addLabel = mem.GetLabels().Slice()
		Delta.setProp = ls.CloneProperties(mem)
		return Delta
	}
	Delta.addLabel = findNodeLabelDiff(mem.GetLabels(), db.GetLabels())
	// Expected properties must be a subset
	setPropertiesDelta(Delta, mem, operationType)
	if operationType == createOp {
		setNodeOverwrite(mem, db, Delta)
	}
	return Delta
}

// compares two native lpg nodes and returns a diff
func compareGraphEdge(mem, db *lpg.Edge, operationType operation) Delta {
	Delta := Delta{isNode: false, setProp: make(map[string]interface{})}
	if db == nil {
		Delta.addLabel = []string{mem.GetLabel()}
		Delta.setProp = ls.CloneProperties(mem)
		return Delta
	}
	Delta.addLabel = []string{mem.GetLabel()}
	setPropertiesDelta(Delta, mem, updateOp)
	if operationType == createOp {
		setEdgeOverwrite(mem, db, Delta)
	}
	return Delta
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

// called after CreateNodes
func buildUpdateQueriesFromDeltas(deltas []Delta, cfg Config) ([]string, []map[string]interface{}) {
	res := make([]string, 0)
	vars := make([]map[string]interface{}, 0)
	for _, d := range deltas {
		if d.isEmpty() {
			continue
		}
		switch d.operation {
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
			q, v := op.writeQuery(cfg)
			res = append(res, q)
			vars = append(vars, v)
		}
	}
	return res, vars
}

func buildCreateNodeQueriesFromDeltas(deltas []Delta, cfg Config) ([]string, []*lpg.Node, []map[string]interface{}) {
	nodeCreates := make([]string, 0)
	nodes := make([]*lpg.Node, 0)
	nodeCreateVars := make([]map[string]interface{}, 0)
	for _, d := range deltas {
		if d.isEmpty() {
			continue
		}
		switch d.operation {
		case createNodeOp:
			op := createNode{
				n:      d.node,
				labels: d.addLabel,
				props:  d.setProp,
			}
			q, v := op.writeQuery(cfg)
			nodeCreates = append(nodeCreates, q)
			nodes = append(nodes, op.n)
			nodeCreateVars = append(nodeCreateVars, v)
			if len(nodeCreateVars) != len(nodes) && len(nodes) != len(nodeCreates) {
				panic("uneven number of nodes to be created per query")
			}
		}
	}
	return nodeCreates, nodes, nodeCreateVars
}

// called after DB nodes are created
func buildCreateEdgeQueriesFromDeltas(deltas []Delta, dbGraphIds map[*lpg.Node]int64, cfg Config) ([]string, []map[string]interface{}) {
	edgeCreates := make([]string, 0)
	edgeCreateVars := make([]map[string]interface{}, 0)
	for _, d := range deltas {
		if d.isEmpty() {
			continue
		}
		switch d.operation {
		case createEdgeOp:
			op := createEdge{
				e:        d.edge,
				typ:      d.addLabel,
				props:    d.setProp,
				fromNode: dbGraphIds[d.fromNode],
				toNode:   dbGraphIds[d.toNode],
			}
			q, v := op.writeQuery(cfg)
			edgeCreates = append(edgeCreates, q)
			edgeCreateVars = append(edgeCreateVars, v)
		}
	}
	return edgeCreates, edgeCreateVars
}

type OperationQueue struct {
	Ops  []string
	vars []map[string]interface{}
}

func RunUpdateOperations(tx neo4j.Transaction, deltas []Delta, config Config) error {
	ops, vars := buildUpdateQueriesFromDeltas(deltas, config)
	for ix, op := range ops {
		_, err := tx.Run(op, vars[ix])
		if err != nil {
			return err
		}
	}
	return nil
}

func Merge(memGraph, dbGraph *lpg.Graph, dbGraphIds map[*lpg.Node]int64, dbEdges map[*lpg.Edge]int64, config Config) (*lpg.Graph, []Delta, error) {
	memEntitiesMap := ls.GetEntityInfo(memGraph)
	dbEntitiesMap := ls.GetEntityInfo(dbGraph)
	nodeAssociations := make(map[*lpg.Node]step)
	edgeAssociations := make(map[*lpg.Edge]edgeStep)

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
	deltas := make([]Delta, 0)
	for i := 0; i < len(memEntities); i++ {
		mergeEntity(memEntities[i], dbEntities[i], dbGraph, dbGraphIds, dbEdges, nodeAssociations, edgeAssociations)
	}
	for mn, step := range nodeAssociations {
		dn := compareGraphNode(mn, step.node, step.op)
		if step.op == createOp {
			dn.operation = createNodeOp
		}
		if step.op == updateOp {
			dn.operation = updateNodeOp
		}
		dn.node = step.node
		dn.setDeltaToNode(step.node)
		deltas = append(deltas, dn)

	}
	for memEdge, step := range edgeAssociations {
		de := compareGraphEdge(memEdge, step.edge, step.op)
		if step.op == createOp {
			de.operation = createEdgeOp
			de.fromNode = step.from
			de.toNode = step.to
		}
		if step.op == updateOp {
			de.operation = updateEdgeOp
		}
		de.edge = step.edge
		de.setDeltaToEdge(step.edge)
		deltas = append(deltas, de)

	}

	// for edgeItr := memGraph.GetEdges(); edgeItr.Next(); {
	// 	edge := edgeItr.Edge()
	// 	if ls.GetEntityRootNode(edge.GetFrom()) != ls.GetEntityRootNode(edge.GetTo()) {
	// 		if _, ok := dbEdges[edge]; !ok {
	// 			dbFrom := dbGraph.NewNode(edge.GetFrom().GetLabels().Slice(), ls.CloneProperties(edge.GetFrom()))
	// 			dbTo := dbGraph.NewNode(edge.GetTo().GetLabels().Slice(), ls.CloneProperties(edge.GetTo()))
	// 			dbEdge := dbGraph.NewEdge(dbFrom, dbTo, edge.GetLabel(), ls.CloneProperties(edge))
	// 			de := compareGraphEdge(edge, dbEdge, createOp)
	// 			de.id = dbEdges[dbEdge]
	// 			de.operation = updateEdgeOp
	// 			de.setDeltaToEdge(dbEdge)
	// 			deltas = append(deltas, de)
	// 		}
	// 	}
	// }
	return dbGraph, deltas, nil
}

func CreateNodes(tx neo4j.Transaction, deltas []Delta, config Config, dbGraphIds map[*lpg.Node]int64) error {
	nodeCreates, nodes, nodeCreateVars := buildCreateNodeQueriesFromDeltas(deltas, config)
	ops := OperationQueue{Ops: nodeCreates, vars: nodeCreateVars}
	for ix, op := range ops.Ops {
		idrec, err := tx.Run(op, ops.vars[ix])
		if err != nil {
			return err
		}
		rec, err := idrec.Single()
		if err != nil {
			return err
		}
		dbNodeId := rec.Values[0]
		dbGraphIds[nodes[ix]] = dbNodeId.(int64)
	}
	return nil
}

func CreateEdges(tx neo4j.Transaction, deltas []Delta, config Config, dbGraphIds map[*lpg.Node]int64) error {
	edgeCreates, edgeCreateVars := buildCreateEdgeQueriesFromDeltas(deltas, dbGraphIds, config)
	ops := OperationQueue{Ops: edgeCreates, vars: edgeCreateVars}
	for ix, op := range ops.Ops {
		_, err := tx.Run(op, ops.vars[ix])
		if err != nil {
			return err
		}
	}
	return nil
}

func (dx Delta) setDeltaToNode(dbNode *lpg.Node) {
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

func (dx Delta) setDeltaToEdge(dbEdge *lpg.Edge) {
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
	node *lpg.Node
	op   operation
}

type edgeStep struct {
	to   *lpg.Node
	from *lpg.Node
	edge *lpg.Edge
	op   operation
}

func mergeSubtree(memNode, dbNode, dbNodeParent *lpg.Node, dbGraph *lpg.Graph, dbGraphIds map[*lpg.Node]int64, dbEdges map[*lpg.Edge]int64, nodeAssociations map[*lpg.Node]step, edgeAssociations map[*lpg.Edge]edgeStep, isChild bool) []Delta {
	deltas := make([]Delta, 0)
	memItr := memNode.GetEdges(lpg.OutgoingEdge)
	if dbNode != nil {
		nodeAssociations[memNode] = step{
			node: dbNode,
		}
	} else {
		// db entity root is null; create entity and connect to source root
		if !isChild {
			dbNodeParent = dbGraph.NewNode(memNode.GetLabels().Slice(), ls.CloneProperties(memNode))
			nodeAssociations[memNode] = step{
				node: dbNodeParent,
				op:   createOp,
			}
			followAllEdges := func(e *lpg.Edge) ls.EdgeFuncResult { return ls.FollowEdgeResult }
			ls.IterateAncestors(memNode, func(n *lpg.Node) bool {
				for edgeItr := n.GetEdges(lpg.AnyEdge); edgeItr.Next(); {
					edge := edgeItr.Edge()
					if edge.GetTo() == memNode {
						sourceItr := dbGraph.FindNodes(edge.GetFrom().GetLabels(), ls.CloneProperties(edge.GetFrom()))
						if sourceItr.Next() {
							nodeAssociations[edge.GetFrom()] = step{
								node: sourceItr.Node(),
								op:   createOp,
							}
							e := dbGraph.NewEdge(sourceItr.Node(), dbNodeParent, ls.HasTerm, nil)
							edgeAssociations[edge] = edgeStep{
								from: e.GetFrom(),
								to:   e.GetTo(),
								edge: e,
								op:   createOp,
							}
							return false
						}
						dbSource := dbGraph.NewNode(edge.GetFrom().GetLabels().Slice(), ls.CloneProperties(edge.GetFrom()))
						nodeAssociations[edge.GetFrom()] = step{
							node: dbSource,
							op:   createOp,
						}
						ed := dbGraph.NewEdge(dbSource, dbNodeParent, ls.HasTerm, nil)
						edgeAssociations[edge] = edgeStep{
							from: ed.GetFrom(),
							to:   ed.GetTo(),
							edge: ed,
							op:   createOp,
						}
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
			nodeAssociations[memChildNode] = step{
				node: n,
				op:   createOp,
			}
			edgeAssociations[memChildEdge] = edgeStep{
				to:   e.GetTo(),
				from: e.GetFrom(),
				edge: e,
				op:   createOp,
			}
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
			nodeAssociations[memChildNode] = step{
				node: n,
				op:   updateOp,
			}
			edgeAssociations[memChildEdge] = edgeStep{
				to:   e.GetTo(),
				from: e.GetFrom(),
				edge: e,
				op:   updateOp,
			}
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
				nodeAssociations[memChildNode] = step{
					node: dbChildEdge.GetTo(),
					op:   updateOp,
				}
				edgeAssociations[memChildEdge] = edgeStep{
					to:   dbChildEdge.GetTo(),
					from: dbChildEdge.GetFrom(),
					edge: dbChildEdge,
					op:   updateOp,
				}
				break
			}
		}
		if len(seenDBPath) > 0 {
			if _, seen := seenDBPath[ls.AsPropertyValue(memChildNode.GetProperty(ls.SchemaNodeIDTerm)).AsString()]; !seen {
				n := dbGraph.NewNode(memChildNode.GetLabels().Slice(), ls.CloneProperties(memChildNode))
				e := dbGraph.NewEdge(dbNode, n, memChildEdge.GetLabel(), ls.CloneProperties(memChildEdge))
				nodeAssociations[memChildNode] = step{
					node: n,
				}
				edgeAssociations[memChildEdge] = edgeStep{
					to:   e.GetTo(),
					from: e.GetFrom(),
					edge: e,
				}
				seenDBPath = make(map[string]struct{})
				deltas = append(deltas, mergeSubtree(memChildNode, n, dbNodeParent, dbGraph, dbGraphIds, dbEdges, nodeAssociations, edgeAssociations, true)...)
			}
		}
	}
	return nil
}

func mergeEntity(memEntityRoot, dbEntityRoot *lpg.Node, dbGraph *lpg.Graph, dbGraphIds map[*lpg.Node]int64, dbEdges map[*lpg.Edge]int64, nodeAssociations map[*lpg.Node]step, edgeAssociations map[*lpg.Edge]edgeStep) {
	mergeSubtree(memEntityRoot, dbEntityRoot, nil, dbGraph, dbGraphIds, dbEdges, nodeAssociations, edgeAssociations, false)
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
func (cn createNode) writeQuery(c Config) (string, map[string]interface{}) {
	vars := make(map[string]interface{})
	sb := strings.Builder{}
	prop := c.MakeProperties(cn.n, vars)
	labels := c.MakeLabels(cn.labels)
	sb.WriteString(fmt.Sprintf("(n%d%s %s)", cn.id, labels, prop))

	// builder := strings.Builder{}
	// builder.WriteString(fmt.Sprintf("ID(n%d)", cn.id))
	return fmt.Sprintf("CREATE %s RETURN ID(n%d)", sb.String(), cn.id), vars
}

// match node labels and properties
func (ce createEdge) writeQuery(c Config) (string, map[string]interface{}) {
	vars := make(map[string]interface{})
	sb := strings.Builder{}
	label := c.MakeLabels(ce.typ)
	sb.WriteString(fmt.Sprintf("MATCH (n) WHERE ID(n) = %d MATCH (m) WHERE ID(m) = %d CREATE (n)-[%s]->(m)", ce.fromNode, ce.toNode, label))
	return sb.String(), vars
}

func (ue updateEdge) writeQuery(c Config) (string, map[string]interface{}) {
	vars := make(map[string]interface{})
	sb := strings.Builder{}
	label := c.MakeLabels(ue.typ)
	if len(label) > 0 && ue.e != nil {
		sb.WriteString(fmt.Sprintf("MATCH (n)-[rel:%s]->(m) MATCH (n) WHERE ID(n) = %d MATCH (m) WHERE ID(m) = %d MERGE (n)-[:%s]->(m) DELETE rel",
			ue.e.GetLabel(), ue.fromNode, ue.toNode, label))
	}
	return sb.String(), vars
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
