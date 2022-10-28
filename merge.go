package neo4j

import (
	"fmt"
	"log"
	"strings"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type Delta interface {
	WriteQuery(dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) DeltaQuery
}

type DeltaQuery struct {
	Query string
	Vars  map[string]interface{}
}

func selectDelta(in []Delta, flt func(Delta) bool) []Delta {
	out := make([]Delta, 0, len(in))
	for _, x := range in {
		if flt(x) {
			out = append(out, x)
		}
	}
	return out
}

type createNodeDelta struct {
	labels     []string
	properties map[string]interface{}
}

type updateNodeDelta struct {
	dbNode     *lpg.Node
	labels     []string
	properties map[string]interface{}
}

type createEdgeDelta struct {
	fromDbNode, toDbNode *lpg.Node
	label                string
	properties           map[string]interface{}
}

type updateEdgeDelta struct {
	dbEdge     *lpg.Edge
	properties map[string]interface{}
}

func Merge(memGraph, dbGraph *lpg.Graph, dbGraphIds map[*lpg.Node]int64, dbEdges map[*lpg.Edge]int64, config Config) ([]Delta, error) {
	memEntitiesMap := ls.GetEntityInfo(memGraph)
	dbEntitiesMap := ls.GetEntityInfo(dbGraph)
	nodeAssociations := make(map[*lpg.Node]*lpg.Node)
	edgeAssociations := make(map[*lpg.Edge]*lpg.Edge)
	deltas := make([]Delta, 0)
	unassociatedDbNodes := make(map[*lpg.Node]struct{})
	// Put all dbNodes into a set, We will remove those that are associated later
	for dbNodes := dbGraph.GetNodes(); dbNodes.Next(); {
		dbNode := dbNodes.Node()
		unassociatedDbNodes[dbNode] = struct{}{}
	}

	for n := range memEntitiesMap {
		schemaPV := ls.AsPropertyValue(n.GetProperty(ls.EntitySchemaTerm))
		eidPV := ls.AsPropertyValue(n.GetProperty(ls.EntityIDTerm))
		var foundDBEntity *lpg.Node
		if eidPV != nil {
			for db := range dbEntitiesMap {
				if ls.AsPropertyValue(db.GetProperty(ls.EntitySchemaTerm)).IsEqual(schemaPV) &&
					ls.AsPropertyValue(db.GetProperty(ls.EntityIDTerm)).IsEqual(eidPV) {
					foundDBEntity = db
					delete(dbEntitiesMap, db)
					break
				}
			}
		}
		deltas = append(deltas, mergeSubtree(n, foundDBEntity, dbGraph, dbGraphIds, dbEdges, nodeAssociations, edgeAssociations)...)
	}

	// Remove all db nodes that are associated with a memnode
	for memNodes := memGraph.GetNodes(); memNodes.Next(); {
		delete(unassociatedDbNodes, nodeAssociations[memNodes.Node()])
	}

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
		n, d := mergeNewNode(memNode, dbGraph)
		if d != nil {
			deltas = append(deltas, d)
		}
		nodeAssociations[memNode] = n
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
		toDbNode := nodeAssociations[edge.GetTo()]
		if fromDbNode == nil || toDbNode == nil {
			panic(fmt.Sprintf("Unassociated edge: %s -> %s\n", edge.GetFrom(), edge.GetTo()))
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
			propDiff := findPropDiff(edge, dbEdge)
			if len(propDiff) == 0 {
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
func mergeSubtree(memRoot, dbRoot *lpg.Node, dbGraph *lpg.Graph, dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, nodeAssociations map[*lpg.Node]*lpg.Node, edgeAssociations map[*lpg.Edge]*lpg.Edge) []Delta {
	seenMemNodes := make(map[*lpg.Node]struct{})
	queuedMemNodes := make(map[*lpg.Node]struct{})
	// Add the memRoot to the queue
	queuedMemNodes[memRoot] = struct{}{}
	if dbRoot != nil {
		nodeAssociations[memRoot] = dbRoot
	}
	ret := make([]Delta, 0)
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
		if matchingDbNode == nil {
			newNode, delta := mergeNewNode(currentMemNode, dbGraph)
			matchingDbNode = newNode
			ret = append(ret, delta)
			nodeAssociations[currentMemNode] = matchingDbNode
		} else {
			// There is matching node, merge it
			d := mergeNodes(currentMemNode, matchingDbNode)
			if d != nil {
				ret = append(ret, d)
			}
			nodeAssociations[currentMemNode] = matchingDbNode
		}

		parentArray := currentMemNode.GetLabels().Has(ls.AttributeTypeArray)
		// Find nodes that are one step away
		for edges := currentMemNode.GetEdges(lpg.OutgoingEdge); edges.Next(); {
			edge := edges.Edge()
			toNode := edge.GetTo()
			if edge.GetFrom() == toNode {
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
							ret = append(ret, d)
						}
					}
				}
				if !found {
					// Create the edge
					e, d := mergeNewEdge(edge, nodeAssociations)
					ret = append(ret, d)
					edgeAssociations[edge] = e
				}
				continue
			}
			// Do not cross into a new entity
			if ls.IsNodeEntityRoot(toNode) {
				continue
			}
			// Here, we found a new node that belongs to this entity
			// Queue the new node, so it can be processed next time around
			queuedMemNodes[toNode] = struct{}{}

			// Now we have to find the db counterpart of toNode
			// If it is already known, we're done
			if _, exists := nodeAssociations[toNode]; exists {
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
				if isNodeIdentical(toNode, dbNode) {
					nodeAssociations[toNode] = dbNode
					edgeAssociations[edge] = dbEdge
					found = true
					break
				}
				if !parentArray && ls.AsPropertyValue(toNode.GetProperty(ls.SchemaNodeIDTerm)).IsEqual(ls.AsPropertyValue(dbNode.GetProperty(ls.SchemaNodeIDTerm))) {
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
					d := mergeNodes(toNode, matchingAttribute)
					if d != nil {
						ret = append(ret, d)
					}
					nodeAssociations[toNode] = matchingAttribute
				} else {
					// No matching attribute node, create a new one
					newNode, d := mergeNewNode(toNode, dbGraph)
					ret = append(ret, d)
					nodeAssociations[toNode] = newNode
					// And create a new edge
					e, d := mergeNewEdge(edge, nodeAssociations)
					edgeAssociations[edge] = e
					ret = append(ret, d)
				}
			}
		}
	}
	return ret
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

func buildCreateNodeQueries(in []Delta, dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) []DeltaQuery {
	deltas := selectDelta(in, func(d Delta) bool {
		_, ok := d.(createNodeDelta)
		return ok
	})
	ret := make([]DeltaQuery, 0, len(deltas))
	for _, delta := range deltas {
		ret = append(ret, delta.WriteQuery(dbNodeIds, dbEdgeIds, c))
	}
	return ret
}

// merge the memnode with the given dbnode
func mergeNodes(memNode, dbNode *lpg.Node) Delta {
	labels := findLabelDiff(memNode.GetLabels(), dbNode.GetLabels())
	props := findPropDiff(memNode, dbNode)
	if len(labels) == 0 && len(props) == 0 {
		return nil
	}
	// Apply changes to the dbNode
	for k, v := range props {
		dbNode.SetProperty(k, v)
	}
	l := dbNode.GetLabels()
	l.AddSet(memNode.GetLabels())
	dbNode.SetLabels(l)
	return updateNodeDelta{
		dbNode:     dbNode,
		labels:     labels,
		properties: props,
	}
}

func mergeEdges(memEdge, dbEdge *lpg.Edge) Delta {
	props := findPropDiff(memEdge, dbEdge)
	if len(props) == 0 {
		return nil
	}
	// Apply changes to the dbEdge
	for k, v := range props {
		dbEdge.SetProperty(k, v)
	}
	return updateEdgeDelta{
		dbEdge:     dbEdge,
		properties: props,
	}
}

func mergeNewEdge(memEdge *lpg.Edge, nodeAssociations map[*lpg.Node]*lpg.Node) (*lpg.Edge, Delta) {
	// Find corresponding from/to nodes
	from := nodeAssociations[memEdge.GetFrom()]
	to := nodeAssociations[memEdge.GetTo()]
	props := findPropDiff(memEdge, nil)
	edge := from.GetGraph().NewEdge(from, to, memEdge.GetLabel(), props)
	return edge, createEdgeDelta{
		fromDbNode: from,
		toDbNode:   to,
		label:      memEdge.GetLabel(),
		properties: props,
	}
}

func mergeNewNode(memNode *lpg.Node, dbGraph *lpg.Graph) (*lpg.Node, Delta) {
	props := findPropDiff(memNode, nil)
	labels := memNode.GetLabels().Slice()
	newNode := dbGraph.NewNode(labels, props)
	return newNode, createNodeDelta{
		labels:     labels,
		properties: props,
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

// finds properties in memObj that are not in dbObj
func findPropDiff(memObj, dbObj withProperty) map[string]interface{} {
	ret := make(map[string]interface{})
	memObj.ForEachProperty(func(k string, v interface{}) bool {
		pv, ok := v.(*ls.PropertyValue)
		if !ok {
			return true
		}
		if dbObj == nil {
			ret[k] = v
			return true
		}
		v2, ok := dbObj.GetProperty(k)
		if !ok {
			ret[k] = v
			return true
		}
		pv2, ok := v2.(*ls.PropertyValue)
		if !ok {
			log.Printf("Error at %s: %v: Not property value", k, v)
			return false
		}
		if !pv2.IsEqual(pv) {
			ret[k] = pv
		}
		return true
	})
	return ret
}

func (un updateNodeDelta) WriteQuery(dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) DeltaQuery {
	vars := make(map[string]interface{})
	sb := strings.Builder{}
	prop := c.MakeProperties(mapWithProperty(un.properties), vars)
	labels := c.MakeLabels(un.labels)
	id, ok := dbNodeIds[un.dbNode]
	if !ok {
		panic(fmt.Sprintf("Node ID not known: %s", un.dbNode))
	}
	if len(labels) > 0 && len(prop) > 0 {
		sb.WriteString(fmt.Sprintf("MATCH (n) WHERE ID(n) = %d SET n = %v SET n%s", id, prop, labels))
	} else if len(labels) == 0 && len(prop) > 0 {
		sb.WriteString(fmt.Sprintf("MATCH (n) WHERE ID(n) = %d SET n = %v", id, prop))
	} else if len(prop) == 0 && len(labels) > 0 {
		sb.WriteString(fmt.Sprintf("MATCH (n) WHERE ID(n) = %d SET n:%s", id, labels))
	}
	return DeltaQuery{
		Query: sb.String(),
		Vars:  vars,
	}
}

func (cn createNodeDelta) WriteQuery(dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) DeltaQuery {
	vars := make(map[string]interface{})
	prop := c.MakeProperties(mapWithProperty(cn.properties), vars)
	labels := c.MakeLabels(cn.labels)
	return DeltaQuery{
		Query: fmt.Sprintf("CREATE (n%s %s) RETURN ID(n)", labels, prop),
		Vars:  vars,
	}
}

func (ce createEdgeDelta) WriteQuery(dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) DeltaQuery {
	vars := make(map[string]interface{})
	label := c.MakeLabels([]string{ce.label})
	props := c.MakeProperties(mapWithProperty(ce.properties), vars)
	return DeltaQuery{
		Query: fmt.Sprintf("MATCH (n) WHERE ID(n) = %d MATCH (m) WHERE ID(m) = %d CREATE (n)-[%s %s]->(m)", dbNodeIds[ce.fromDbNode], dbNodeIds[ce.toDbNode], label, props),
		Vars:  vars,
	}
}

func (ue updateEdgeDelta) WriteQuery(dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) DeltaQuery {
	vars := make(map[string]interface{})
	props := c.MakeProperties(mapWithProperty(ue.properties), vars)
	return DeltaQuery{
		Query: fmt.Sprintf("MATCH ()-[e]->() WHERE ID(e) = %d set e=%s", dbEdgeIds[ue.dbEdge], props),
		Vars:  vars,
	}
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
