package neo4j

import (
	"fmt"
	"strings"

	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/cloudprivacylabs/opencypher/graph"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

var DEFAULT_BATCH_SIZE = 1000

type JobQueue struct {
	createNodes []graph.Node
	createEdges []graph.Edge
	deleteNodes []uint64
	deleteEdges []uint64
}

type DeleteEntity struct {
	Config
	graph.Graph
	entityId uint64
}

type CreateEntity struct {
	Config
	graph.Graph
	graph.Node
}

func (q *JobQueue) Run(tx neo4j.Transaction, cfg Config, nodeMap map[graph.Node]uint64, batchSize int) error {
	if batchSize == 0 {
		batchSize = DEFAULT_BATCH_SIZE
	}
	vars := make(map[string]interface{})
	for len(q.deleteNodes) > 0 {
		batch := len(q.deleteNodes)
		if batch > batchSize {
			batch = batchSize
		}
		// delete nodes in batches
		_, err := tx.Run("MATCH (m) WHERE ID(m) in $ids DETACH DELETE m", map[string]interface{}{"ids": q.deleteNodes[:batch]})
		if err != nil {
			return err
		}
		q.deleteNodes = q.deleteNodes[batch:]
	}
	// TODO: Delete Edges
	for len(q.deleteEdges) > 0 {

	}
	for len(q.createNodes) > 0 {
		batch := len(q.createNodes)
		if batch > batchSize {
			batch = batchSize
		}
		// create nodes in batches
		query := buildCreateQuery(q.createNodes[:batch], cfg, vars)
		idrec, err := tx.Run(query, vars)
		if err != nil {
			return err
		}
		records, err := idrec.Single()
		if err != nil {
			return err
		}
		// track database IDs into nodeMap
		for i, rec := range records.Values {
			nodeMap[q.createNodes[i]] = uint64(rec.(int64))
		}
		// dequeue nodes based on batch size
		q.createNodes = q.createNodes[batch:]
	}
	for len(q.createEdges) > 0 {
		batch := len(q.createEdges)
		if batch > batchSize {
			batch = batchSize
		}
		// create edges in batches
		query := buildConnectQuery(q.createEdges[:batch], cfg, nodeMap)
		q.createEdges = q.createEdges[batch:]
		_, err := tx.Run(query, vars)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteEntity.Queue will find all connected nodes to the given entity in the database and delete them
func (d *DeleteEntity) Queue(tx neo4j.Transaction, q *JobQueue, selectEntity func(graph.Node) bool) error {
	ids, err := loadEntityNodes(tx, d.Graph, []uint64{d.entityId}, d.Config, findNeighbors, selectEntity)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if id != 0 {
			q.deleteNodes = append(q.deleteNodes, uint64(id))
		}
	}
	return nil
}

// CreateEntity.Queue will find all connected nodes to the given entity and create, stopping at different entity boundaries
func (c *CreateEntity) Queue(tx neo4j.Transaction, q *JobQueue) error {
	ls.IterateDescendants(c.Node, func(n graph.Node) bool {
		if !n.GetLabels().Has(ls.DocumentNodeTerm) {
			return true
		}
		q.createNodes = append(q.createNodes, n)
		return true
	}, func(e graph.Edge) ls.EdgeFuncResult {
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
func buildCreateQuery(nodes []graph.Node, c Config, vars map[string]interface{}) string {
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
func buildConnectQuery(edges []graph.Edge, c Config, hm map[graph.Node]uint64) string {
	sb := strings.Builder{}
	for ix, edge := range edges {
		from := hm[edge.GetFrom()]
		to := hm[edge.GetTo()]
		label := c.MakeLabels([]string{edge.GetLabel()})
		sb.WriteString(fmt.Sprintf("MATCH (n%d) MATCH (m%d) WHERE ID(n%d)=%d AND ID(m%d)=%d CREATE (n%d)-[%s]->(m%d) ", ix, ix, ix, from, ix, to, ix, label, ix))
		// Add UNION until the end of query
		if ix < len(edges)-1 {
			sb.WriteString("UNION ")
		}
	}
	return sb.String()
}
