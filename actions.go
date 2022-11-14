package neo4j

import (
	"fmt"
	"strings"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

var DEFAULT_BATCH_SIZE = 1000

type JobQueue struct {
	createNodes []*lpg.Node
	createEdges []*lpg.Edge
	deleteNodes []int64
	deleteEdges []int64
}

type DeleteEntity struct {
	Config
	*lpg.Graph
	entityId int64
}

type CreateEntity struct {
	Config
	*lpg.Graph
	*lpg.Node
}

func (q *JobQueue) Run(ctx *ls.Context, tx neo4j.Transaction, cfg Config, nodeMap map[*lpg.Node]int64, batchSize int) error {
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
		_, err := tx.Run("MATCH (m) WHERE ID(m) in $ids DETACH DELETE m", map[string]interface{}{"ids": q.deleteNodes[:batch]})
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
	if err := CreateEdgesBatch(ctx, tx, q.createEdges, nodeMap, cfg, batchSize); err != nil {
		return err
	}
	return nil
}

func CreateNodesBatch(ctx *ls.Context, tx neo4j.Transaction, createNodes []*lpg.Node, nodeMap map[*lpg.Node]int64, cfg Config, batchSize int) error {
	for len(createNodes) > 0 {
		vars := make(map[string]interface{})
		batch := len(createNodes)
		if batch > batchSize {
			batch = batchSize
		}
		// create nodes in batches
		query := buildCreateQuery(createNodes[:batch], cfg, vars)
		ctx.GetLogger().Debug(map[string]interface{}{"createNodes": query, "vars": vars})
		idrec, err := tx.Run(query, vars)
		if err != nil {
			return err
		}
		records, err := idrec.Single()
		if err != nil {
			return err
		}
		ctx.GetLogger().Debug(map[string]interface{}{"createNodes": "done", "records": records.Values})
		// track database IDs into nodeMap
		for i, rec := range records.Values {
			nodeMap[createNodes[i]] = int64(rec.(int64))
		}
		// dequeue nodes based on batch size
		createNodes = createNodes[batch:]
	}
	return nil
}

func CreateEdgesBatch(ctx *ls.Context, tx neo4j.Transaction, createEdges []*lpg.Edge, nodeMap map[*lpg.Node]int64, cfg Config, batchSize int) error {
	for len(createEdges) > 0 {
		vars := make(map[string]interface{})
		batch := len(createEdges)
		if batch > batchSize {
			batch = batchSize
		}
		// create edges in batches
		query := buildConnectQuery(createEdges[:batch], cfg, nodeMap, vars)
		createEdges = createEdges[batch:]
		_, err := tx.Run(query, vars)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteEntity.Queue will find all connected nodes to the given entity in the database and delete them
func (d *DeleteEntity) Queue(tx neo4j.Transaction, q *JobQueue, selectEntity func(*lpg.Node) bool) error {
	ids, err := loadEntityNodes(tx, d.Graph, []int64{d.entityId}, d.Config, findNeighbors, selectEntity)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if id != 0 {
			q.deleteNodes = append(q.deleteNodes, int64(id))
		}
	}
	return nil
}

// CreateEntity.Queue will find all connected nodes to the given entity and create, stopping at different entity boundaries
func (c *CreateEntity) Queue(tx neo4j.Transaction, q *JobQueue) error {
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
		builder.WriteString(fmt.Sprintf("ID(n%d)", ix))
		// Add a comma until the end of query
		if ix < len(nodes)-1 {
			builder.WriteString(",")
		}
	}
	return fmt.Sprintf("CREATE %s RETURN %s", sb.String(), builder.String())
}

// query to create edges and connect existing nodes in the database
func buildConnectQuery(edges []*lpg.Edge, c Config, hm map[*lpg.Node]int64, vars map[string]interface{}) string {
	if len(edges) == 0 {
		return ""
	}
	first := true
	sb := strings.Builder{}
	scopeNodes := make(map[int64]struct{})
	for ix, edge := range edges {
		if first {
			first = false
		} else {
			sb.WriteString(" with * ")
		}
		from := hm[edge.GetFrom()]
		to := hm[edge.GetTo()]
		label := c.MakeLabels([]string{edge.GetLabel()})
		prop := c.MakeProperties(edge, vars)

		_, fromInScope := scopeNodes[from]
		_, toInScope := scopeNodes[to]
		if !fromInScope && !toInScope {
			fmt.Fprintf(&sb, "match (n%d),(n%d) where ID(n%d)=%d and ID(n%d)=%d ", from, to, from, from, to, to)
		} else if fromInScope && !toInScope {
			fmt.Fprintf(&sb, "match (n%d) where ID(n%d)=%d ", to, to, to)
		} else if !fromInScope && toInScope {
			fmt.Fprintf(&sb, "match (n%d) where ID(n%d)=%d ", from, from, from)
		}
		scopeNodes[from] = struct{}{}
		scopeNodes[to] = struct{}{}
		fmt.Fprintf(&sb, "create (n%d)-[e%d %s %s]->(n%d) ", from, ix, label, prop, to)
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

func CreateEdgesUnwind(ctx *ls.Context, edges []*lpg.Edge, nodeMap map[*lpg.Node]int64, cfg Config) func(neo4j.Transaction) error {

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
				"from":  nodeMap[edge.GetFrom()],
				"to":    nodeMap[edge.GetTo()],
				"props": props,
			}
			unwind = append(unwind, item)
		}
		labels := cfg.MakeLabels([]string{label})
		unwindData[labels] = unwind
	}
	return func(tx neo4j.Transaction) error {
		for labels, unwind := range unwindData {
			query := fmt.Sprintf(`unwind $edges as edge 
match(a) where ID(a)=edge.from with a,edge
match(b) where ID(b)=edge.to 
create (a)-[e%s]->(b) set e=edge.props`, labels)
			_, err := tx.Run(query, map[string]interface{}{"edges": unwind})
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func CreateNodesUnwind(ctx *ls.Context, nodes []*lpg.Node, nodeMap map[*lpg.Node]int64, cfg Config) func(neo4j.Transaction) error {
	labels := make(map[string][]*lpg.Node)
	for _, node := range nodes {
		l := cfg.MakeLabels(node.GetLabels().Slice())
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
	return func(tx neo4j.Transaction) error {
		for label, unwind := range unwindData {
			query := fmt.Sprintf(`unwind $nodes as node 
create (a%s) set a=node.props return ID(a)`, label)
			result, err := tx.Run(query, map[string]interface{}{"nodes": unwind.udata})
			if err != nil {
				return err
			}
			records, err := result.Collect()
			if err != nil {
				return err
			}
			for i := range records {
				id := records[i].Values[0].(int64)
				nodeMap[unwind.insNodes[i]] = id
			}
		}
		return nil
	}
}
