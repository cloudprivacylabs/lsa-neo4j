package neo4j

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cloudprivacylabs/lpg/v2"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type Delta interface {
	WriteQuery(session *Session, dbNodeIds map[*lpg.Node]string, dbEdgeIds map[*lpg.Edge]string, c Config) DeltaQuery
	Run(ctx *ls.Context, tx neo4j.ExplicitTransaction, session *Session, dbNodeIds map[*lpg.Node]string, dbEdgeIds map[*lpg.Edge]string, c Config) error
}

type DeltaQuery struct {
	Query string
	Vars  map[string]interface{}
}

func (d DeltaQuery) String() string {
	return fmt.Sprintf("Statement: %s params: %s", d.Query, d.Vars)
}

func SelectDelta(in []Delta, flt func(Delta) bool) []Delta {
	out := make([]Delta, 0, len(in))
	for _, x := range in {
		if flt(x) {
			out = append(out, x)
		}
	}
	return out
}

type CreateNodeDelta struct {
	DBNode  *lpg.Node
	MemNode *lpg.Node
}

type UpdateNodeDelta struct {
	dbNode      *lpg.Node
	oldLabels   []string
	deltaLabels []string
}

type CreateEdgeDelta struct {
	DBEdge *lpg.Edge

	fromDbNode, toDbNode *lpg.Node
	label                string
	properties           map[string]interface{}
}

type UpdateEdgeDelta struct {
	dbEdge     *lpg.Edge
	properties map[string]interface{}
}

func Merge(memGraph *lpg.Graph, dbGraph *DBGraph, config Config) ([]Delta, error) {
	memEntitiesMap := ls.GetEntityRootsByID(memGraph)
	dbEntitiesMap := ls.GetEntityRootsByID(dbGraph.G)
	nodeAssociations := make(map[*lpg.Node]*lpg.Node)
	edgeAssociations := make(map[*lpg.Edge]*lpg.Edge)
	deltas := make([]Delta, 0)
	unassociatedDbNodes := make(map[*lpg.Node]struct{})
	// Put all dbNodes into a set, We will remove those that are associated later
	for dbNodes := dbGraph.G.GetNodes(); dbNodes.Next(); {
		dbNode := dbNodes.Node()
		unassociatedDbNodes[dbNode] = struct{}{}
	}
	for n := range memEntitiesMap {
		labels := n.GetLabels()
		eidPV := ls.AsPropertyValue(n.GetProperty(ls.EntityIDTerm))
		var foundDBEntity *lpg.Node
		if eidPV != nil {
			for db := range dbEntitiesMap {
				if ls.AsPropertyValue(db.GetProperty(ls.EntityIDTerm)).IsEqual(eidPV) {
					for l := range labels.M {
						if config.IsMergeEntity(l) && db.HasLabel(l) {
							foundDBEntity = db
							delete(dbEntitiesMap, db)
							break
						}
					}
					if foundDBEntity == nil {
						if ls.AsPropertyValue(n.GetProperty(ls.SchemaNodeIDTerm)).AsString() ==
							ls.AsPropertyValue(db.GetProperty(ls.SchemaNodeIDTerm)).AsString() {
							foundDBEntity = db
							delete(dbEntitiesMap, db)
							break
						}
					}
				}
			}
		}
		nlayers := ls.FilterNonLayerTypes(n.GetLabels().Slice())
		mergeActions := EntityMergeAction{}
		for _, label := range nlayers {
			if op, ok := config.EntityMergeActions[label]; ok {
				mergeActions = op
				break
			}
		}
		merge, create := mergeActions.GetMerge(), mergeActions.GetCreate()
		deltas = mergeSubtree(n, foundDBEntity, dbGraph, nodeAssociations, edgeAssociations, deltas, merge, create)
	}

	// Remove all db nodes that are associated with a memnode
	for memNodes := memGraph.GetNodes(); memNodes.Next(); {
		delete(unassociatedDbNodes, nodeAssociations[memNodes.Node()])
	}
	toCreate := make([]*lpg.Node, 0)
	// Deal with standalone clusters
	for memNodes := memGraph.GetNodes(); memNodes.Next(); {
		memNode := memNodes.Node()
		if _, associated := nodeAssociations[memNode]; associated {
			continue
		}
		// This memnode is not associated with a db node
		// Is there an identical unassociated db node
		found := false
		for dbNode := range unassociatedDbNodes {
			if isNodeIdentical(memNode, dbNode) {
				nodeAssociations[memNode] = dbNode
				delete(unassociatedDbNodes, dbNode)
				found = true
				break
			}
		}
		if found {
			continue
		}
		// Create this node
		toCreate = append(toCreate, memNode)
	}
	for _, memNode := range toCreate {
		n, d := mergeNewNode(memNode, dbGraph.G)
		if d != nil {
			deltas = append(deltas, d)
		}
		nodeAssociations[memNode] = n
	}

	for nodes := memGraph.GetNodes(); nodes.Next(); {
		node := nodes.Node()
		if _, exists := nodeAssociations[node]; !exists {
			panic(fmt.Sprintf("Unassociated node: %d %s", nodes.MaxSize(), node))
		}
	}
	// Each entity is merged. Now we have to work on the edges that link
	// entities, or edges between non-entity nodes Any unassociated memEdge needs to be
	// dealt with
	for edges := memGraph.GetEdges(); edges.Next(); {
		edge := edges.Edge()
		if _, associated := edgeAssociations[edge]; associated {
			continue
		}
		// From and to nodes must already be in the dbGraph
		fromDbNode := nodeAssociations[edge.GetFrom()]
		if fromDbNode == nil {
			panic(fmt.Sprintf("Unassociated from node: %s -> %s\n", edge.GetFrom(), edge.GetTo()))
		}
		toDbNode := nodeAssociations[edge.GetTo()]
		if toDbNode == nil {
			panic(fmt.Sprintf("Unassociated to node: %s -> %s\n", edge.GetFrom(), edge.GetTo()))
		}
		// The edge might already be in the dbgraph
		found := false
		nSimilarEdges := 0
		var similarDbEdge *lpg.Edge
		for dbEdges := fromDbNode.GetEdgesWithLabel(lpg.OutgoingEdge, edge.GetLabel()); dbEdges.Next(); {
			dbEdge := dbEdges.Edge()
			if dbEdge.GetTo() != toDbNode {
				continue
			}
			if !propertiesChanged(edge, dbEdge) {
				edgeAssociations[edge] = dbEdge
				found = true
				break
			}
			// If there is only one similar edge, this will remember it. If
			// more than one, we don't use it
			similarDbEdge = dbEdge
			nSimilarEdges++
		}
		if found {
			continue
		}
		if nSimilarEdges == 1 {
			// Update the single edge
			d := mergeEdges(edge, similarDbEdge)
			if d != nil {
				deltas = append(deltas, d)
			}
			edgeAssociations[edge] = similarDbEdge
		} else { // Multiple edges
			// Add another edge
			e, d := mergeNewEdge(edge, nodeAssociations)
			if d != nil {
				deltas = append(deltas, d)
			}
			edgeAssociations[edge] = e
		}
	}

	// Remove duplicate node creations from deltas
	return deltas, nil
}

// memRoot is the root node of the subtree in the memgraph.  dbRoot is
// the root node of the subtree in the db graph. dbRoot can be nil. If
// non-nil, then the dbRoot is the memroot's counterpart in the db.
// dbNodeIds and dbEdgeIds are the DB ids of the db nodes and
// edges. nodeAssociations and edgeAssociations map the memNodes to
// their dbNode counterparts, and memEdges to their dbEdge
// counterparts. When merge is done, the subtree rooted at dbRoot
// contains everything in the subtree under memRoot, and deltas
// contain everything that changed.
func mergeSubtree(memRoot, dbRoot *lpg.Node, dbGraph *DBGraph, nodeAssociations map[*lpg.Node]*lpg.Node, edgeAssociations map[*lpg.Edge]*lpg.Edge, deltas []Delta, doMerge, doCreate bool) []Delta {
	seenMemNodes := make(map[*lpg.Node]struct{})
	queuedMemNodes := make(map[*lpg.Node]struct{})
	// Add the memRoot to the queue
	queuedMemNodes[memRoot] = struct{}{}
	if dbRoot != nil {
		nodeAssociations[memRoot] = dbRoot
	}
	// Process the queue until nothing left
	for len(queuedMemNodes) > 0 {

		// Select a queue mem node that has not been processed yet
		var currentMemNode *lpg.Node
		for currentMemNode = range queuedMemNodes {
			break
		}
		delete(queuedMemNodes, currentMemNode)
		if _, seen := seenMemNodes[currentMemNode]; seen {
			continue
		}
		seenMemNodes[currentMemNode] = struct{}{}

		// Here, currentMemNode is the node we will process.

		// If there is not a matching dbNode, create one
		matchingDbNode := nodeAssociations[currentMemNode]
		creating := false
		merging := false
		if matchingDbNode == nil {
			if doCreate {
				newNode, delta := mergeNewNode(currentMemNode, dbGraph.G)
				matchingDbNode = newNode
				if delta != nil {
					deltas = append(deltas, delta)
				}
				nodeAssociations[currentMemNode] = matchingDbNode
				creating = true
			} else {
				continue
			}
		} else {
			// There is matching node, merge it
			if doMerge {
				d := mergeNodes(currentMemNode, matchingDbNode)
				if d != nil {
					deltas = append(deltas, d)
				}
				merging = true
			}
		}

		if !creating || merging {
			continue
		}

		parentArray := currentMemNode.GetLabels().Has(ls.AttributeTypeArray)
		// Find nodes that are one step away
		for edges := currentMemNode.GetEdges(lpg.OutgoingEdge); edges.Next(); {
			edge := edges.Edge()
			toMemNode := edge.GetTo()
			if edge.GetFrom() == toMemNode {
				// This is a self-loop

				// Find a self-loop in dbnode with the same label
				found := false
				for dbEdges := matchingDbNode.GetEdgesWithLabel(lpg.OutgoingEdge, edge.GetLabel()); dbEdges.Next(); {
					dbEdge := dbEdges.Edge()
					if dbEdge.GetTo() == matchingDbNode {
						found = true
						edgeAssociations[edge] = dbEdge
						d := mergeEdges(edge, dbEdge)
						if d != nil {
							deltas = append(deltas, d)
						}
					}
				}
				if !found {
					// Create the edge
					e, d := mergeNewEdge(edge, nodeAssociations)
					deltas = append(deltas, d)
					edgeAssociations[edge] = e
				}
				continue
			}
			// Do not cross into a new entity
			if ls.IsNodeEntityRoot(toMemNode) {
				continue
			}
			// Here, we found a new node that belongs to this entity
			// Queue the new node, so it can be processed next time around
			queuedMemNodes[toMemNode] = struct{}{}

			// Now we have to find the db counterpart of toNode
			// If it is already known, we're done
			if _, exists := nodeAssociations[toMemNode]; exists {
				continue
			}

			// If there is an identical node in dbGraph, we're done
			// If there is a single node with the same schemaNodeId and if the parent memNode is not an array,
			// we found the matching node
			// Otherwise, we create the new node
			var matchingAttribute *lpg.Node
			found := false
			for dbEdges := matchingDbNode.GetEdgesWithLabel(lpg.OutgoingEdge, edge.GetLabel()); dbEdges.Next(); {
				dbEdge := dbEdges.Edge()
				dbNode := dbEdge.GetTo()
				if isNodeIdentical(toMemNode, dbNode) {
					nodeAssociations[toMemNode] = dbNode
					edgeAssociations[edge] = dbEdge
					found = true
					break
				}
				if !parentArray && ls.AsPropertyValue(toMemNode.GetProperty(ls.SchemaNodeIDTerm)).IsEqual(ls.AsPropertyValue(dbNode.GetProperty(ls.SchemaNodeIDTerm))) {
					if matchingAttribute == nil {
						matchingAttribute = dbNode
					} else {
						// For create new node
						matchingAttribute = nil
						break
					}
				}
			}
			if !found {
				if matchingAttribute != nil {
					// We found the matching attribute node
					d := mergeNodes(toMemNode, matchingAttribute)
					if d != nil {
						deltas = append(deltas, d)
					}
					nodeAssociations[toMemNode] = matchingAttribute
				} else {
					// No matching attribute node, create a new one or use associated
					associatedDbNode := nodeAssociations[toMemNode]
					if associatedDbNode == nil {
						// Create node
						newNode, d := mergeNewNode(toMemNode, dbGraph.G)
						if d != nil {
							deltas = append(deltas, d)
						}
						nodeAssociations[toMemNode] = newNode
						associatedDbNode = newNode
					}
					// And create a new edge
					e, d := mergeNewEdge(edge, nodeAssociations)
					edgeAssociations[edge] = e
					deltas = append(deltas, d)
				}
			}
		}
	}
	return deltas
}

// Return true if n1 contained in n2
func isNodeContained(n1, n2 *lpg.Node) bool {
	if !n2.GetLabels().HasAllSet(n1.GetLabels()) {
		return false
	}
	eq := true
	n1.ForEachProperty(func(k string, v interface{}) bool {
		pv, ok := v.(*ls.PropertyValue)
		if !ok {
			eq = false
			return false
		}
		v2, ok := n2.GetProperty(k)
		if !ok {
			eq = false
			return false
		}
		pv2, ok := v2.(*ls.PropertyValue)
		if !ok {
			eq = false
			return false
		}
		if !pv2.IsEqual(pv) {
			eq = false
			return false
		}
		return true
	})
	return eq
}

// Return true if n1 is identical to n2
func isNodeIdentical(n1, n2 *lpg.Node) bool {
	if !n1.GetLabels().IsEqual(n2.GetLabels()) {
		return false
	}
	eq := true
	n1.ForEachProperty(func(k string, v interface{}) bool {
		pv, ok := v.(*ls.PropertyValue)
		if !ok {
			return true
		}
		v2, ok := n2.GetProperty(k)
		if !ok {
			eq = false
			return false
		}
		pv2, ok := v2.(*ls.PropertyValue)
		if !ok {
			eq = false
			return false
		}
		if !pv2.IsEqual(pv) {
			eq = false
			return false
		}
		return true
	})
	if !eq {
		return false
	}
	n2.ForEachProperty(func(k string, v interface{}) bool {
		_, ok := v.(*ls.PropertyValue)
		if !ok {
			return true
		}
		v2, ok := n2.GetProperty(k)
		if !ok {
			eq = false
			return false
		}
		_, ok = v2.(*ls.PropertyValue)
		if !ok {
			eq = false
			return false
		}
		return true
	})
	return eq
}

func buildCreateNodeQueries(session *Session, in []Delta, dbNodeIds map[*lpg.Node]string, dbEdgeIds map[*lpg.Edge]string, c Config) []DeltaQuery {
	deltas := SelectDelta(in, func(d Delta) bool {
		_, ok := d.(CreateNodeDelta)
		return ok
	})
	ret := make([]DeltaQuery, 0, len(deltas))
	for _, delta := range deltas {
		ret = append(ret, delta.WriteQuery(session, dbNodeIds, dbEdgeIds, c))
	}
	return ret
}

// merge the memnode with the given dbnode
func mergeNodes(memNode, dbNode *lpg.Node) Delta {
	labels := findLabelDiff(memNode.GetLabels(), dbNode.GetLabels())
	if len(labels) == 0 && !propertiesChanged(memNode, dbNode) {
		return nil
	}
	oldLabels := dbNode.GetLabels().Slice()
	// Apply changes to the dbNode
	memNode.ForEachProperty(func(k string, v any) bool {
		dbNode.SetProperty(k, v)
		return true
	})
	l := dbNode.GetLabels()
	l.AddSet(memNode.GetLabels())
	dbNode.SetLabels(l)
	return UpdateNodeDelta{
		dbNode:      dbNode,
		oldLabels:   oldLabels,
		deltaLabels: labels,
	}
}

func mergeEdges(memEdge, dbEdge *lpg.Edge) Delta {
	if !propertiesChanged(memEdge, dbEdge) {
		return nil
	}
	// Apply changes to the dbEdge
	memEdge.ForEachProperty(func(k string, v any) bool {
		dbEdge.SetProperty(k, v)
		return true
	})
	return UpdateEdgeDelta{
		dbEdge:     dbEdge,
		properties: ls.CloneProperties(dbEdge),
	}
}

func mergeNewEdge(memEdge *lpg.Edge, nodeAssociations map[*lpg.Node]*lpg.Node) (*lpg.Edge, Delta) {
	// Find corresponding from/to nodes
	from := nodeAssociations[memEdge.GetFrom()]
	to := nodeAssociations[memEdge.GetTo()]
	props := ls.CloneProperties(memEdge)
	edge := from.GetGraph().NewEdge(from, to, memEdge.GetLabel(), props)
	return edge, CreateEdgeDelta{
		fromDbNode: from,
		toDbNode:   to,
		label:      memEdge.GetLabel(),
		properties: props,
		DBEdge:     edge,
	}
}

func mergeNewNode(memNode *lpg.Node, dbGraph *lpg.Graph) (*lpg.Node, Delta) {
	labels := memNode.GetLabels().Slice()
	newNode := dbGraph.NewNode(labels, ls.CloneProperties(memNode))
	return newNode, CreateNodeDelta{
		DBNode:  newNode,
		MemNode: memNode,
	}
}

// Return labels that are in memLabels but not in dbLabels
func findLabelDiff(memLabels, dbLabels lpg.StringSet) []string {
	ret := make([]string, 0)
	for _, x := range memLabels.Slice() {
		if !dbLabels.Has(x) {
			ret = append(ret, x)
		}
	}
	return ret
}

func propertiesChanged(memObj, dbObj withProperty) bool {
	if dbObj == nil {
		return true
	}
	ret := false
	memObj.ForEachProperty(func(k string, v interface{}) bool {
		v2, ok2 := dbObj.GetProperty(k)
		if !ok2 {
			ret = true
			return false
		}

		pv1, ok1 := v.(*ls.PropertyValue)
		pv2, ok2 := v2.(*ls.PropertyValue)
		if ok1 && ok2 {
			if !pv1.IsEqual(pv2) {
				ret = true
				return false
			}
			return true
		}
		var native1, native2 any
		if ok1 {
			native1 = pv1.GetNativeValue()
		} else {
			native1 = v
		}
		if ok2 {
			native2 = pv2.GetNativeValue()
		} else {
			native2 = v2
		}
		if !reflect.DeepEqual(native1, native2) {
			ret = true
			return false
		}
		return true
	})
	return ret
}

func (un UpdateNodeDelta) WriteQuery(session *Session, dbNodeIds map[*lpg.Node]string, dbEdgeIds map[*lpg.Edge]string, c Config) DeltaQuery {
	hasProperties := false
	un.dbNode.ForEachProperty(func(k string, _ any) bool {
		if len(c.Shorten(k)) > 0 {
			hasProperties = true
		}
		return true
	})
	// If the properties to be updated has no intersection with the delta properties, then they are empty
	if !hasProperties && len(un.deltaLabels) == 0 {
		return DeltaQuery{}
	}
	vars := make(map[string]interface{})
	sb := strings.Builder{}
	prop := c.MakeProperties(un.dbNode, vars)
	labels := c.MakeLabels(un.dbNode.GetLabels().Slice())
	oldLabels := c.MakeLabels(un.oldLabels)
	id, ok := dbNodeIds[un.dbNode]
	if !ok {
		panic(fmt.Sprintf("Node ID not known: %s", un.dbNode))
	}
	if len(un.deltaLabels) > 0 && len(prop) > 0 {
		sb.WriteString(fmt.Sprintf("MATCH (n%s) WHERE %s SET n = %v SET n%s", oldLabels, session.IDEqValueFunc("n", id), prop, labels))
	} else if len(un.deltaLabels) == 0 && len(prop) > 0 {
		sb.WriteString(fmt.Sprintf("MATCH (n%s) WHERE %s SET n = %v", oldLabels, session.IDEqValueFunc("n", id), prop))
	} else if len(prop) == 0 && len(un.deltaLabels) > 0 {
		sb.WriteString(fmt.Sprintf("MATCH (n%s) WHERE  %s SET n:%s", oldLabels, session.IDEqValueFunc("n", id), labels))
	}
	return DeltaQuery{
		Query: sb.String(),
		Vars:  vars,
	}
}

func (un UpdateNodeDelta) Run(ctx *ls.Context, tx neo4j.ExplicitTransaction, session *Session, dbNodeIds map[*lpg.Node]string, dbEdgeIds map[*lpg.Edge]string, c Config) error {
	q := un.WriteQuery(session, dbNodeIds, dbEdgeIds, c)
	if len(q.Query) == 0 {
		return nil
	}
	_, err := tx.Run(ctx, q.Query, q.Vars)
	if err != nil {
		return fmt.Errorf("While running %s: %w", q, err)
	}
	return nil
}

func (cn CreateNodeDelta) WriteQuery(session *Session, dbNodeIds map[*lpg.Node]string, dbEdgeIds map[*lpg.Edge]string, c Config) DeltaQuery {
	vars := make(map[string]interface{})
	prop := c.MakeProperties(cn.DBNode, vars)
	labels := c.MakeLabels(cn.DBNode.GetLabels().Slice())
	return DeltaQuery{
		Query: fmt.Sprintf("CREATE (n%s %s) RETURN %s", labels, prop, session.IDFunc("n")),
		Vars:  vars,
	}
}

func (cn CreateNodeDelta) Run(ctx *ls.Context, tx neo4j.ExplicitTransaction, session *Session, dbNodeIds map[*lpg.Node]string, dbEdgeIds map[*lpg.Edge]string, c Config) error {
	q := cn.WriteQuery(session, dbNodeIds, dbEdgeIds, c)
	rec, err := tx.Run(ctx, q.Query, q.Vars)
	if err != nil {
		return fmt.Errorf("While running %s: %w", q, err)
	}
	records, err := rec.Single(ctx)
	if err != nil {
		return err
	}
	id := records.Values[0].(string)
	dbNodeIds[cn.DBNode] = id
	return nil
}

func (ce CreateEdgeDelta) WriteQuery(session *Session, dbNodeIds map[*lpg.Node]string, dbEdgeIds map[*lpg.Edge]string, c Config) DeltaQuery {
	vars := make(map[string]interface{})
	label := c.MakeLabels([]string{ce.label})
	props := c.MakeProperties(mapWithProperty(ce.properties), vars)
	return DeltaQuery{
		Query: fmt.Sprintf("MATCH (n) WHERE  %s MATCH (m) WHERE  %s CREATE (n)-[%s %s]->(m)", session.IDEqValueFunc("n", dbNodeIds[ce.fromDbNode]), session.IDEqValueFunc("m", dbNodeIds[ce.toDbNode]), label, props),
		Vars:  vars,
	}
}

func (ce CreateEdgeDelta) Run(ctx *ls.Context, tx neo4j.ExplicitTransaction, session *Session, dbNodeIds map[*lpg.Node]string, dbEdgeIds map[*lpg.Edge]string, c Config) error {
	q := ce.WriteQuery(session, dbNodeIds, dbEdgeIds, c)
	_, err := tx.Run(ctx, q.Query, q.Vars)
	return err
}

func (ue UpdateEdgeDelta) WriteQuery(session *Session, dbNodeIds map[*lpg.Node]string, dbEdgeIds map[*lpg.Edge]string, c Config) DeltaQuery {
	vars := make(map[string]interface{})
	props := c.MakeProperties(mapWithProperty(ue.properties), vars)
	return DeltaQuery{
		Query: fmt.Sprintf("MATCH ()-[e]->() WHERE %s set e=%s", session.IDEqValueFunc("e", dbEdgeIds[ue.dbEdge]), props),
		Vars:  vars,
	}
}

// LinkMergedEntities will find the new entities from the delta and link them
func LinkMergedEntities(ctx *ls.Context, tx neo4j.ExplicitTransaction, cfg Config, delta []Delta, nodeMap map[*lpg.Node]string) error {
	rootNodes := make([]*lpg.Node, 0)
	for _, d := range delta {
		if n, ok := d.(CreateNodeDelta); ok {
			if ls.IsNodeEntityRoot(n.DBNode) {
				rootNodes = append(rootNodes, n.DBNode)
			}
		}
	}
	// for _, node := range rootNodes {
	// 	if err := LinkNodesForNewEntity(ctx, tx, cfg, node, nodeMap); err != nil {
	// 		return err
	// 	}
	// }
	return nil
}

func (ue UpdateEdgeDelta) Run(ctx *ls.Context, tx neo4j.ExplicitTransaction, session *Session, dbNodeIds map[*lpg.Node]string, dbEdgeIds map[*lpg.Edge]string, c Config) error {
	q := ue.WriteQuery(session, dbNodeIds, dbEdgeIds, c)
	_, err := tx.Run(ctx, q.Query, q.Vars)
	return err
}
