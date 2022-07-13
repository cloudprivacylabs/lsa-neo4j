package neo4j

import (
	"fmt"
	"strings"

	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/cloudprivacylabs/opencypher/graph"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type neo4jAction interface {
	Queue(neo4j.Transaction, *JobQueue) error
}

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
	vars map[string]interface{}
}

func (q *JobQueue) Run(tx neo4j.Transaction, cfg Config, nodeMap map[graph.Node]uint64, batchSize int) error {
	const DEFAULT_BATCH_SIZE = 1000
	if batchSize == 0 {
		batchSize = DEFAULT_BATCH_SIZE
	}
	vars := make(map[string]interface{})
	for ix := 0; ix < len(q.deleteNodes); ix += batchSize {
		var err error
		if ix+batchSize >= len(q.deleteNodes) {
			_, err = tx.Run("MATCH (m) WHERE ID(m) in $ids DETACH DELETE m", map[string]interface{}{"ids": q.deleteNodes[ix:]})
		} else {
			_, err = tx.Run("MATCH (m) WHERE ID(m) in $ids DETACH DELETE m", map[string]interface{}{"ids": q.deleteNodes[ix : ix+batchSize]})
		}
		if err != nil {
			return err
		}
	}
	// TODO: Delete Edges
	for ix := 0; ix < len(q.deleteEdges); ix += batchSize {

	}
	for ix := 0; ix < len(q.createNodes); ix += batchSize {
		var createQuery string
		if ix+batchSize >= len(q.createNodes) {
			createQuery = buildCreateQuery(q.createNodes[ix:], cfg, vars)
		} else {
			createQuery = buildCreateQuery(q.createNodes[ix:ix+batchSize], cfg, vars)
		}
		idrec, err := tx.Run(createQuery, vars)
		if err != nil {
			return err
		}
		records, err := idrec.Single()
		if err != nil {
			return err
		}
		for i, rec := range records.Values {
			nodeMap[q.createNodes[i+ix]] = uint64(rec.(int64))
		}

	}
	for ix := 0; ix < len(q.createEdges); ix += batchSize {
		var connectQuery string
		if ix+batchSize >= len(q.createEdges) {
			connectQuery = buildConnectQuery(q.createEdges[ix:], cfg, nodeMap)
		} else {
			connectQuery = buildConnectQuery(q.createEdges[ix:ix+batchSize], cfg, nodeMap)
		}
		_, err := tx.Run(connectQuery, vars)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *DeleteEntity) Queue(tx neo4j.Transaction, q *JobQueue) error {
	ids, err := loadEntityNodes(tx, d.Graph, []uint64{d.entityId}, d.Config, findNeighbors, func(n graph.Node) bool {
		return true
	})
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

func (c *CreateEntity) Queue(tx neo4j.Transaction, q *JobQueue) error {
	ls.IterateDescendants(c.Node, func(n graph.Node) bool {
		if !n.GetLabels().Has(ls.DocumentNodeTerm) {
			return true
		}
		if _, exists := n.GetProperty(ls.EntitySchemaTerm); exists {
			id := ls.AsPropertyValue(n.GetProperty(ls.EntityIDTerm)).AsString()
			if id != "" {
				if c.Node != n {
					return false
				}
			}
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

func buildCreateQuery(nodes []graph.Node, c Config, vars map[string]interface{}) string {
	sb := strings.Builder{}
	for ix, node := range nodes {
		prop := c.MakeProperties(node, vars)
		labels := c.MakeLabels(node.GetLabels().Slice())
		if ix < len(nodes)-1 {
			sb.WriteString(fmt.Sprintf("(n%d%s %s),", ix, labels, prop))
		} else {
			sb.WriteString(fmt.Sprintf("(n%d%s %s) ", ix, labels, prop))
		}
	}
	builder := strings.Builder{}
	for ix := range nodes {
		if ix < len(nodes)-1 {
			builder.WriteString(fmt.Sprintf("ID(n%d),", ix))
		} else {
			builder.WriteString(fmt.Sprintf("ID(n%d)", ix))
		}
	}
	return fmt.Sprintf("CREATE %s RETURN %s", sb.String(), builder.String())
}

func buildConnectQuery(edges []graph.Edge, c Config, hm map[graph.Node]uint64) string {
	sb := strings.Builder{}
	for ix, edge := range edges {
		from := hm[edge.GetFrom()]
		to := hm[edge.GetTo()]
		label := c.MakeLabels([]string{edge.GetLabel()})
		if ix < len(edges)-1 {
			sb.WriteString(fmt.Sprintf("MATCH (n%d) MATCH (m%d) WHERE ID(n%d)=%d AND ID(m%d)=%d CREATE (n%d)-[%s]->(m%d) UNION ", ix, ix, ix, from, ix, to, ix, label, ix))
		} else {
			sb.WriteString(fmt.Sprintf("MATCH (n%d) MATCH (m%d) WHERE ID(n%d)=%d AND ID(m%d)=%d CREATE (n%d)-[%s]->(m%d) ", ix, ix, ix, from, ix, to, ix, label, ix))
		}
	}
	return sb.String()
}
