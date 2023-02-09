package neo4j

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

var DEFAULT_BATCH_SIZE = 1000

type JobQueue struct {
	createNodes []*lpg.Node
	createEdges []*lpg.Edge
	deleteNodes []string
	deleteEdges []string
}

type DeleteEntity struct {
	Config
	*lpg.Graph
	entityId string
}

type CreateEntity struct {
	Config
	*lpg.Graph
	*lpg.Node
}

func (q *JobQueue) Run(ctx *ls.Context, tx neo4j.ExplicitTransaction, session *Session, cfg Config, nodeMap map[*lpg.Node]string, batchSize int) error {
	if batchSize == 0 {
		batchSize = DEFAULT_BATCH_SIZE
	}
	for len(q.deleteNodes) > 0 {
		batch := len(q.deleteNodes)
		if batch > batchSize {
			batch = batchSize
		}
		// delete nodes in batches
		ctx.GetLogger().Debug(map[string]interface{}{"delete nodes": q.deleteNodes[:batch]})
		_, err := tx.Run(ctx, fmt.Sprintf("MATCH (m) WHERE %s in $ids DETACH DELETE m", session.IDFunc("m")), map[string]interface{}{"ids": q.deleteNodes[:batch]})
		if err != nil {
			return err
		}
		q.deleteNodes = q.deleteNodes[batch:]
	}
	// TODO: Delete Edges
	for len(q.deleteEdges) > 0 {

	}
	if err := CreateNodesBatch(ctx, tx, q.createNodes, nodeMap, cfg, batchSize); err != nil {
		return err
	}
	if err := CreateEdgesBatch(ctx, tx, session, q.createEdges, nodeMap, cfg, batchSize); err != nil {
		return err
	}
	return nil
}

func CreateNodesBatch(ctx *ls.Context, tx neo4j.ExplicitTransaction, createNodes []*lpg.Node, nodeMap map[*lpg.Node]string, cfg Config, batchSize int) error {
	for len(createNodes) > 0 {
		vars := make(map[string]interface{})
		batch := len(createNodes)
		if batch > batchSize {
			batch = batchSize
		}
		// create nodes in batches
		query := buildCreateQuery(createNodes[:batch], cfg, vars)
		ctx.GetLogger().Debug(map[string]interface{}{"createNodes": query, "vars": vars})
		idrec, err := tx.Run(ctx, query, vars)
		if err != nil {
			return err
		}
		records, err := idrec.Single(ctx)
		if err != nil {
			return err
		}
		ctx.GetLogger().Debug(map[string]interface{}{"createNodes": "done", "records": records.Values})
		// track database IDs into nodeMap
		for i, rec := range records.Values {
			nodeMap[createNodes[i]] = rec.(neo4j.Node).ElementId
		}
		// dequeue nodes based on batch size
		createNodes = createNodes[batch:]
	}
	return nil
}

func CreateEdgesBatch(ctx *ls.Context, tx neo4j.ExplicitTransaction, session *Session, createEdges []*lpg.Edge, nodeMap map[*lpg.Node]string, cfg Config, batchSize int) error {
	for len(createEdges) > 0 {
		vars := make(map[string]interface{})
		batch := len(createEdges)
		if batch > batchSize {
			batch = batchSize
		}
		// create edges in batches
		query := buildConnectQuery(session, createEdges[:batch], cfg, nodeMap, vars)
		createEdges = createEdges[batch:]
		_, err := tx.Run(ctx, query, vars)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteEntity.Queue will find all connected nodes to the given entity in the database and delete them
func (d *DeleteEntity) Queue(ctx *ls.Context, tx neo4j.ExplicitTransaction, session *Session, q *JobQueue, selectEntity func(*lpg.Node) bool) error {
	ids, err := loadEntityNodes(ctx, tx, session, d.Graph, []string{d.entityId}, d.Config, findNeighbors, selectEntity)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if len(id) != 0 {
			q.deleteNodes = append(q.deleteNodes, id)
		}
	}
	return nil
}

// CreateEntity.Queue will find all connected nodes to the given entity and create, stopping at different entity boundaries
func (c *CreateEntity) Queue(ctx *ls.Context, tx neo4j.ExplicitTransaction, q *JobQueue) error {
	ls.IterateDescendants(c.Node, func(n *lpg.Node) bool {
		if !n.GetLabels().Has(ls.DocumentNodeTerm) {
			return true
		}
		q.createNodes = append(q.createNodes, n)
		return true
	}, func(e *lpg.Edge) ls.EdgeFuncResult {
		to := e.GetTo()
		// Edge must go to a document node
		if !to.GetLabels().Has(ls.DocumentNodeTerm) {
			return ls.SkipEdgeResult
		}
		// If edge goes to a different entity with ID, we should stop here
		if _, ok := to.GetProperty(ls.EntitySchemaTerm); ok {
			if _, ok := to.GetProperty(ls.EntityIDTerm); ok {
				q.createEdges = append(q.createEdges, e)
				return ls.SkipEdgeResult
			}
		}
		q.createEdges = append(q.createEdges, e)
		return ls.FollowEdgeResult
	}, false)
	return nil
}

// query to create nodes
func buildCreateQuery(nodes []*lpg.Node, c Config, vars map[string]interface{}) string {
	if len(nodes) == 0 {
		return ""
	}

	sb := strings.Builder{}
	for ix, node := range nodes {
		prop := c.MakeProperties(node, vars)
		labels := c.MakeLabels(node.GetLabels().Slice())
		sb.WriteString(fmt.Sprintf("(n%d%s %s)", ix, labels, prop))
		// Add a comma until the end of query
		if ix < len(nodes)-1 {
			sb.WriteString(",")
		}
	}
	builder := strings.Builder{}
	for ix := range nodes {
		builder.WriteString(fmt.Sprintf("n%d", ix))
		// Add a comma until the end of query
		if ix < len(nodes)-1 {
			builder.WriteString(",")
		}
	}
	return fmt.Sprintf("CREATE %s RETURN %s", sb.String(), builder.String())
}

// query to create edges and connect existing nodes in the database
func buildConnectQuery(session *Session, edges []*lpg.Edge, c Config, hm map[*lpg.Node]string, vars map[string]interface{}) string {
	if len(edges) == 0 {
		return ""
	}
	first := true
	sb := strings.Builder{}
	scopeNodes := make(map[string]struct{})
	nodeIndexes := make(map[string]int) // This is required to name nodes by id
	for ix, edge := range edges {
		if first {
			first = false
		} else {
			sb.WriteString(" with * ")
		}
		from := hm[edge.GetFrom()]
		to := hm[edge.GetTo()]
		fromIx, ok := nodeIndexes[from]
		if !ok {
			fromIx = len(nodeIndexes)
			nodeIndexes[from] = fromIx
		}
		toIx, ok := nodeIndexes[to]
		if !ok {
			toIx = len(nodeIndexes)
			nodeIndexes[to] = toIx
		}
		label := c.MakeLabels([]string{edge.GetLabel()})
		prop := c.MakeProperties(edge, vars)

		_, fromInScope := scopeNodes[from]
		_, toInScope := scopeNodes[to]
		if !fromInScope && !toInScope {
			fmt.Fprintf(&sb, "match (n%d),(n%d) where %s and %s ", fromIx, toIx, session.IDEqValueFunc(fmt.Sprintf("n%d", fromIx), from), session.IDEqValueFunc(fmt.Sprintf("n%d", toIx), to))
		} else if fromInScope && !toInScope {
			fmt.Fprintf(&sb, "match (n%d) where %s ", toIx, session.IDEqValueFunc(fmt.Sprintf("n%d", toIx), to))
		} else if !fromInScope && toInScope {
			fmt.Fprintf(&sb, "match (n%d) where %s ", fromIx, session.IDEqValueFunc(fmt.Sprintf("n%d", fromIx), from))
		}
		scopeNodes[from] = struct{}{}
		scopeNodes[to] = struct{}{}
		fmt.Fprintf(&sb, "create (n%d)-[e%d %s %s]->(n%d) ", fromIx, ix, label, prop, toIx)
	}
	sb.WriteString(" return ")
	first = true
	for ix := range edges {
		if first {
			first = false
		} else {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "e%d", ix)
	}
	return sb.String()
}

func CreateEdgesUnwind(ctx *ls.Context, session *Session, edges []*lpg.Edge, nodeMap map[*lpg.Node]string, cfg Config) func(neo4j.ExplicitTransaction) error {

	labels := make(map[string][]*lpg.Edge)
	for _, edge := range edges {
		labels[edge.GetLabel()] = append(labels[edge.GetLabel()], edge)
	}
	unwindData := make(map[string][]map[string]interface{})
	for label, insEdges := range labels {
		unwind := make([]map[string]interface{}, 0)
		for _, edge := range insEdges {
			props := cfg.MakePropertiesObj(edge)
			item := map[string]interface{}{
				"from":  session.IDValue(nodeMap[edge.GetFrom()]),
				"to":    session.IDValue(nodeMap[edge.GetTo()]),
				"props": props,
			}
			unwind = append(unwind, item)
		}
		labels := cfg.MakeLabels([]string{label})
		unwindData[labels] = unwind
	}
	return func(tx neo4j.ExplicitTransaction) error {
		for labels, unwind := range unwindData {
			query := fmt.Sprintf(`unwind $edges as edge 
match(a),(b) where %s and %s 
create (a)-[e%s]->(b) set e=edge.props`, session.IDEqVarFunc("a", "edge.from"), session.IDEqVarFunc("b", "edge.to"), labels)
			_, err := tx.Run(ctx, query, map[string]interface{}{"edges": unwind})
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func CreateNodesUnwind(ctx *ls.Context, nodes []*lpg.Node, nodeMap map[*lpg.Node]string, cfg Config) func(neo4j.ExplicitTransaction) error {
	labels := make(map[string][]*lpg.Node)
	for _, node := range nodes {
		slice := node.GetLabels().Slice()
		sort.Strings(slice)
		l := cfg.MakeLabels(slice)
		labels[l] = append(labels[l], node)
	}

	type udata struct {
		insNodes []*lpg.Node
		udata    []map[string]interface{}
	}
	unwindData := make(map[string]udata)
	for label, insNodes := range labels {
		unwind := make([]map[string]interface{}, 0)
		for _, insNode := range insNodes {
			props := cfg.MakePropertiesObj(insNode)
			item := map[string]interface{}{
				"props": props,
			}
			unwind = append(unwind, item)
		}
		unwindData[label] = udata{insNodes: insNodes, udata: unwind}
	}
	return func(tx neo4j.ExplicitTransaction) error {
		for label, unwind := range unwindData {
			query := fmt.Sprintf(`unwind $nodes as node 
create (a%s) set a=node.props return a`, label)
			result, err := tx.Run(ctx, query, map[string]interface{}{"nodes": unwind.udata})
			if err != nil {
				return err
			}
			records, err := result.Collect(ctx)
			if err != nil {
				return err
			}
			for i := range records {
				nodeMap[unwind.insNodes[i]] = records[i].Values[0].(neo4j.Node).ElementId
			}
		}
		return nil
	}
}
