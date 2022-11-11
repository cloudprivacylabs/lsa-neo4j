package neo4j

import (
	"fmt"
	"log"
	"strings"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type DBGraph struct {
	G *lpg.Graph

	NodeIds map[*lpg.Node]int64
	Nodes   map[int64]*lpg.Node
	EdgeIds map[*lpg.Edge]int64
	Edges   map[int64]*lpg.Edge
}

func NewDBGraph(g *lpg.Graph) *DBGraph {
	return &DBGraph{
		G:       g,
		NodeIds: make(map[*lpg.Node]int64),
		Nodes:   make(map[int64]*lpg.Node),
		EdgeIds: make(map[*lpg.Edge]int64),
		Edges:   make(map[int64]*lpg.Edge),
	}
}

func (dbg *DBGraph) NewNode(node *lpg.Node, dbID int64) {
	dbg.Nodes[dbID] = node
	dbg.NodeIds[node] = dbID
}

func (dbg *DBGraph) NewEdge(edge *lpg.Edge, dbID int64) {
	dbg.Edges[dbID] = edge
	dbg.EdgeIds[edge] = dbID
}

type Delta interface {
	WriteQuery(dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) DeltaQuery
	Run(tx neo4j.Transaction, dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) error
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
	dbNode     *lpg.Node
	labels     []string
	properties map[string]interface{}
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

var lineMap = map[*lpg.Node]int{}

func duplicateCreateNode(delta []Delta, msg string) bool {
	for _, d := range delta {
		n1, ok := d.(CreateNodeDelta)
		if !ok {
			continue
		}
		for _, x := range delta {
			if x == d {
				continue
			}
			n2, ok := x.(CreateNodeDelta)
			if !ok {
				continue
			}
			if n1.MemNode == n2.MemNode {
				fmt.Println(msg, lineMap[n1.MemNode])
				return true
			}
		}
	}
	return false
}

func Merge(memGraph *lpg.Graph, dbGraph *DBGraph, config Config) ([]Delta, error) {
	memEntitiesMap := ls.GetEntityInfo(memGraph)
	dbEntitiesMap := ls.GetEntityInfo(dbGraph.G)
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
		deltas = mergeSubtree(n, foundDBEntity, dbGraph, nodeAssociations, edgeAssociations, deltas)
	}
	duplicateCreateNode(deltas, "end")

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
			duplicateCreateNode(deltas, "178")
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
func mergeSubtree(memRoot, dbRoot *lpg.Node, dbGraph *DBGraph, nodeAssociations map[*lpg.Node]*lpg.Node, edgeAssociations map[*lpg.Edge]*lpg.Edge, deltas []Delta) []Delta {
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
		if matchingDbNode == nil {
			newNode, delta := mergeNewNode(currentMemNode, dbGraph.G)
			matchingDbNode = newNode
			deltas = append(deltas, delta)
			nodeAssociations[currentMemNode] = matchingDbNode
			lineMap[currentMemNode] = 290
			duplicateCreateNode(deltas, "290")
		} else {
			// There is matching node, merge it
			d := mergeNodes(currentMemNode, matchingDbNode)
			if d != nil {
				deltas = append(deltas, d)
			}
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
						deltas = append(deltas, d)
						nodeAssociations[toMemNode] = newNode
						associatedDbNode = newNode
						duplicateCreateNode(deltas, "383")
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

func buildCreateNodeQueries(in []Delta, dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) []DeltaQuery {
	deltas := SelectDelta(in, func(d Delta) bool {
		_, ok := d.(CreateNodeDelta)
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
	return UpdateNodeDelta{
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
	return UpdateEdgeDelta{
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
	return edge, CreateEdgeDelta{
		fromDbNode: from,
		toDbNode:   to,
		label:      memEdge.GetLabel(),
		properties: props,
		DBEdge:     edge,
	}
}

func mergeNewNode(memNode *lpg.Node, dbGraph *lpg.Graph) (*lpg.Node, Delta) {
	props := findPropDiff(memNode, nil)
	labels := memNode.GetLabels().Slice()
	newNode := dbGraph.NewNode(labels, props)
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

func (un UpdateNodeDelta) WriteQuery(dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) DeltaQuery {
	hasProperties := false
	for k := range un.properties {
		if len(c.Shorten(k)) > 0 {
			hasProperties = true
		}
	}
	// If the properties to be updated has no intersection with the delta properties, then they are empty
	if !hasProperties && len(un.labels) == 0 {
		return DeltaQuery{}
	}
	vars := make(map[string]interface{})
	sb := strings.Builder{}
	prop := c.MakeProperties(un.dbNode, vars)
	labels := c.MakeLabels(un.dbNode.GetLabels().Slice())
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

func (un UpdateNodeDelta) Run(tx neo4j.Transaction, dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) error {
	q := un.WriteQuery(dbNodeIds, dbEdgeIds, c)
	if len(q.Query) == 0 {
		return nil
	}
	_, err := tx.Run(q.Query, q.Vars)
	if err != nil {
		return fmt.Errorf("While running %s: %w", q, err)
	}
	return nil
}

func (cn CreateNodeDelta) WriteQuery(dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) DeltaQuery {
	vars := make(map[string]interface{})
	prop := c.MakeProperties(cn.DBNode, vars)
	labels := c.MakeLabels(cn.DBNode.GetLabels().Slice())
	return DeltaQuery{
		Query: fmt.Sprintf("CREATE (n%s %s) RETURN ID(n)", labels, prop),
		Vars:  vars,
	}
}

func (cn CreateNodeDelta) Run(tx neo4j.Transaction, dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) error {
	q := cn.WriteQuery(dbNodeIds, dbEdgeIds, c)
	rec, err := tx.Run(q.Query, q.Vars)
	if err != nil {
		return fmt.Errorf("While running %s: %w", q, err)
	}
	records, err := rec.Single()
	if err != nil {
		return err
	}
	id := records.Values[0].(int64)
	dbNodeIds[cn.DBNode] = id
	return nil
}

func (ce CreateEdgeDelta) WriteQuery(dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) DeltaQuery {
	vars := make(map[string]interface{})
	label := c.MakeLabels([]string{ce.label})
	props := c.MakeProperties(mapWithProperty(ce.properties), vars)
	return DeltaQuery{
		Query: fmt.Sprintf("MATCH (n) WHERE ID(n) = %d MATCH (m) WHERE ID(m) = %d CREATE (n)-[%s %s]->(m)", dbNodeIds[ce.fromDbNode], dbNodeIds[ce.toDbNode], label, props),
		Vars:  vars,
	}
}

func (ce CreateEdgeDelta) Run(tx neo4j.Transaction, dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) error {
	q := ce.WriteQuery(dbNodeIds, dbEdgeIds, c)
	_, err := tx.Run(q.Query, q.Vars)
	return err
}

func (ue UpdateEdgeDelta) WriteQuery(dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) DeltaQuery {
	vars := make(map[string]interface{})
	props := c.MakeProperties(mapWithProperty(ue.properties), vars)
	return DeltaQuery{
		Query: fmt.Sprintf("MATCH ()-[e]->() WHERE ID(e) = %d set e=%s", dbEdgeIds[ue.dbEdge], props),
		Vars:  vars,
	}
}

func (ue UpdateEdgeDelta) Run(tx neo4j.Transaction, dbNodeIds map[*lpg.Node]int64, dbEdgeIds map[*lpg.Edge]int64, c Config) error {
	q := ue.WriteQuery(dbNodeIds, dbEdgeIds, c)
	_, err := tx.Run(q.Query, q.Vars)
	return err
}

func (s *Session) LoadDBGraph(tx neo4j.Transaction, memGraph *lpg.Graph, config Config) (*DBGraph, error) {
	_, rootIds, _, _, err := s.CollectEntityDBIds(tx, config, memGraph)
	if err != nil {
		return nil, err
	}
	g := ls.NewDocumentGraph()
	dbg := NewDBGraph(g)
	if len(rootIds) == 0 {
		return dbg, nil
	}
	err = loadGraphByEntities(tx, dbg, rootIds, config, findNeighbors, func(n *lpg.Node) bool { return true })
	if err != nil {
		return nil, err
	}
	return dbg, nil
}

func loadGraphByEntities(tx neo4j.Transaction, grph *DBGraph, rootIds []int64, config Config, loadNeighbors func(neo4j.Transaction, []int64) ([]neo4jNode, []neo4jNode, []neo4jEdge, error), selectEntity func(*lpg.Node) bool) error {
	if len(rootIds) == 0 {
		return fmt.Errorf("Empty entity schema nodes")
	}
	// neo4j IDs
	queue := make(map[int64]struct{}, len(rootIds))
	for _, id := range rootIds {
		queue[int64(id)] = struct{}{}
	}

	for len(queue) > 0 {
		q := make([]int64, 0, len(queue))
		for k := range queue {
			q = append(q, k)
		}
		srcNodes, adjNodes, adjRelationships, err := loadNeighbors(tx, q)
		if err != nil {
			return err
		}
		if len(srcNodes) == 0 || (len(adjNodes) == 0 && len(adjRelationships) == 0) {
			break
		}
		queue = make(map[int64]struct{})
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
					queue[int64(node.id)] = struct{}{}
				}
			}
			if _, ok := node.props[config.Shorten(ls.EntitySchemaTerm)]; !ok {
				queue[int64(node.id)] = struct{}{}
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
