package neo4j

import (
	"errors"
	"fmt"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type linkSpec struct {
	targetEntity string
	// The foreign key fields
	fkFields []string
	// The label of the link
	label string
	// If true, the link is from this entity to the target. If false,
	// the link is from the target to this.
	forward bool
	// If the schema node is not a reference node, then this is the node
	// that should receive the link
	linkNode string
	// If true, the reference can have more than one links
	multi bool
	node  *lpg.Node
}

func getLinkSpec(docNode *lpg.Node) *linkSpec {
	if docNode == nil {
		return nil
	}
	ref := ls.AsPropertyValue(docNode.GetProperty(ls.ReferenceFKFor)).AsString()
	if len(ref) == 0 {
		return nil
	}
	ret := linkSpec{
		targetEntity: ref,
		label:        ls.AsPropertyValue(docNode.GetProperty(ls.ReferenceLabelTerm)).AsString(),
		multi:        ls.AsPropertyValue(docNode.GetProperty(ls.ReferenceMultiTerm)).AsString() != "false",
		node:         docNode,
		fkFields:     ls.AsPropertyValue(docNode.GetProperty(ls.ReferenceFKTerm)).MustStringSlice(),
	}
	if len(ret.label) == 0 {
		ret.label = ls.HasTerm
	}
	ret.linkNode = ls.AsPropertyValue(docNode.GetProperty(ls.ReferenceLinkNodeTerm)).AsString()
	switch ls.AsPropertyValue(docNode.GetProperty(ls.ReferenceDirectionTerm)).AsString() {
	case "to", "toTarget", "":
		ret.forward = true
	case "from", "fromTarget":
		ret.forward = false
	}

	return &ret
}

func (spec linkSpec) getForeignKeys(entityRoot *lpg.Node) ([][]string, error) {
	if len(spec.fkFields) == 0 {
		v, _ := ls.GetRawNodeValue(spec.node)
		return [][]string{{v}}, nil
	}
	// There can be multiple instances of a foreign key in an
	// entity. ForeignKeyNdoes[i] keeps all the nodes for spec.FK[i]
	foreignKeyNodes := make([][]*lpg.Node, len(spec.fkFields))
	ls.IterateDescendants(entityRoot, func(n *lpg.Node) bool {
		attrId := ls.AsPropertyValue(n.GetProperty(ls.SchemaNodeIDTerm)).AsString()
		if len(attrId) == 0 {
			return true
		}
		for i := range spec.fkFields {
			if spec.fkFields[i] == attrId {
				foreignKeyNodes[i] = append(foreignKeyNodes[i], n)
			}
		}
		return true
	}, ls.OnlyDocumentNodes, false)
	// All foreign key elements must have the same number of elements, and no index must be skipped
	var numKeys int
	for index := 0; index < len(foreignKeyNodes); index++ {
		if index == 0 {
			numKeys = len(foreignKeyNodes[index])
		} else {
			if len(foreignKeyNodes[index]) != numKeys {
				return nil, ls.ErrInvalidForeignKeys{Msg: "Inconsistent foreign keys"}
			}
		}
	}
	// foreignKeyNodes is organized as:
	//
	//   0          1         2
	// fk0_key0  fk0_key1  fk0_key2  --> foreign key 1
	// fk1_key0  fk1_key1  fk1_key2  --> foreign key 2
	fks := make([][]string, numKeys)
	for i := 0; i < numKeys; i++ {
		for key := 0; key < len(spec.fkFields); key++ {
			v, _ := ls.GetRawNodeValue(foreignKeyNodes[i][key])
			fks[i] = append(fks[i], v)
		}
	}
	return fks, nil
}

func LinkNodesForNewEntity(ctx *ls.Context, tx neo4j.Transaction, config Config, entityRoot *lpg.Node, nodeMap map[*lpg.Node]uint64) error {
	links := make([]linkSpec, 0)
	// Does the entity have any outstanding links we need to work on?
	var itrErr error
	ls.IterateDescendants(entityRoot, func(node *lpg.Node) bool {
		if !node.GetLabels().Has(ls.DocumentNodeTerm) {
			return true
		}
		spec := getLinkSpec(node)
		if spec != nil {
			links = append(links, *spec)
		}
		return true
	}, func(edge *lpg.Edge) ls.EdgeFuncResult {
		to := edge.GetTo()
		// Edge must go to a document node
		if !to.GetLabels().Has(ls.DocumentNodeTerm) {
			return ls.SkipEdgeResult
		}
		// If edge goes to a different entity with ID, we should stop here
		if _, ok := to.GetProperty(ls.EntitySchemaTerm); ok {
			if _, ok := to.GetProperty(ls.EntityIDTerm); ok {
				return ls.SkipEdgeResult
			}
		}
		return ls.FollowEdgeResult
	}, false)
	if itrErr != nil {
		return itrErr
	}
	for _, link := range links {
		if err := linkEntities(ctx, tx, config, entityRoot, link, nodeMap); err != nil {
			return err
		}
	}

	// Are there any links pointing to this entity in the DB?
	idTerm, ok := entityRoot.GetProperty(ls.EntityIDTerm)
	if !ok {
		return nil
	}
	id := ls.AsPropertyValue(idTerm, ok).MustStringSlice()
	return linkToThisEntity(ctx, tx, config, entityRoot, id, nodeMap)
}

func linkEntities(ctx *ls.Context, tx neo4j.Transaction, config Config, entityRoot *lpg.Node, spec linkSpec, nodeMap map[*lpg.Node]uint64) error {
	foreignKeys, err := spec.getForeignKeys(entityRoot)
	if err != nil {
		return err
	}

	var linkToNode *lpg.Node
	if len(spec.linkNode) > 0 {
		ls.WalkNodesInEntity(entityRoot, func(n *lpg.Node) bool {
			if ls.IsInstanceOf(n, spec.linkNode) {
				linkToNode = n
				return false
			}
			return true
		})
	}
	if linkToNode == nil {
		linkToNode = entityRoot
	}

	for _, fk := range foreignKeys {
		var query string

		params := make(map[string]interface{})
		nodeLabelsClause := config.MakeLabels([]string{spec.targetEntity})
		var fkVal *ls.PropertyValue
		if len(fk) == 1 {
			fkVal = ls.StringPropertyValue(ls.EntityIDTerm, fk[0])
		} else {
			fkVal = ls.StringSlicePropertyValue(ls.EntityIDTerm, fk)
		}
		nodePropertiesClause := config.MakeProperties(mapWithProperty(map[string]interface{}{
			ls.EntityIDTerm: fkVal,
		}), params)

		if spec.forward {
			query = fmt.Sprintf(`match (target %s %s) with target match (source) where ID(source)=%d create (source)-[%s]->(target)`, nodeLabelsClause, nodePropertiesClause, nodeMap[linkToNode], config.MakeLabels([]string{spec.label}))
		} else {
			query = fmt.Sprintf(`match (source %s %s) with source match (target) where ID(target)=%d create (source)-[%s]->(target)`, nodeLabelsClause, nodePropertiesClause, nodeMap[linkToNode], config.MakeLabels([]string{spec.label}))
		}
		ctx.GetLogger().Debug(map[string]interface{}{"linkEntity": query, "params": params})
		_, err := tx.Run(query, params)
		if err != nil {
			return err
		}
	}
	return nil
}

// ID is entity id of entity root
func linkToThisEntity(ctx *ls.Context, tx neo4j.Transaction, config Config, entityRoot *lpg.Node, ID []string, nodeMap map[*lpg.Node]uint64) error {
	// TODO: Search for all nodes with Ref to this entity, and link them
	vars := make(map[string]interface{})
	_, ok := entityRoot.GetProperty(ls.EntitySchemaTerm)
	if !ok {
		return errors.New("invalid schema node, given entity root must contain schema node id property")
	}
	var fkNodesRec neo4j.Result
	var err error
	entityLabels := ls.FilterNonLayerTypes(entityRoot.GetLabels().Slice())
	if len(ID) > 1 {
		fkNodesRec, err = tx.Run(fmt.Sprintf(
			"MATCH (n) WHERE n.`%s` IN $labels MATCH (n) WHERE ALL(cmp IN $ids WHERE cmp IN SPLIT(n.`%s`, %s)) RETURN n",
			config.Shorten(ls.ReferenceFKFor), config.Shorten(ls.ReferenceFK), quoteStringLiteral(",")),
			map[string]interface{}{"ids": ID[0], "labels": entityLabels},
		)
	} else {
		fkNodesRec, err = tx.Run(fmt.Sprintf(
			"MATCH (n) WHERE n.`%s` IN $labels AND n.`%s` = $ids RETURN n",
			config.Shorten(ls.ReferenceFKFor), config.Shorten(ls.ReferenceFK)),
			map[string]interface{}{"ids": ID[0], "labels": entityLabels},
		)
	}
	if err != nil {
		return err
	}
	for fkNodesRec.Next() {
		rec := fkNodesRec.Record()
		fkNode := rec.Values[0].(neo4j.Node)
		dirTo := false
		if fkNode.Props[ls.ReferenceDirectionTerm] == "to" || fkNode.Props[ls.ReferenceDirectionTerm] == "toTarget" {
			dirTo = true
		}
		// if fkNode is entity root, connect directly to new node
		if _, ok := fkNode.Props[ls.EntitySchemaTerm]; ok {
			if dirTo {
				// MATCH (n), (m) WHERE ID(n) = %d AND m.`%s` = ID CREATE (n)-[%s]->(m)
				_, err := tx.Run(fmt.Sprintf("MATCH (n) MATCH (m) WHERE ID(n) = %d AND ID(m) = %d CREATE (n)-[%s]->(m)", fkNode.Id, int64(nodeMap[entityRoot]), config.MakeLabels([]string{ls.HasTerm})), vars)
				if err != nil {
					return err
				}
			} else {
				_, err := tx.Run(fmt.Sprintf("MATCH (n) MATCH (m) WHERE ID(n) = %d AND ID(m) = %d CREATE (n)<-[%s]-(m)", fkNode.Id, int64(nodeMap[entityRoot]), config.MakeLabels([]string{ls.HasTerm})), vars)
				if err != nil {
					return err
				}
			}
		} else {
			// otherwise find nearest entity node
			const MAX_DEPTH = 10
			var depth int = 1
			for {
				if depth >= MAX_DEPTH {
					return errors.New("cannot find entity node")
				}
				eNodesRec, err := tx.Run(fmt.Sprintf("MATCH (n)<-[*%d]-(m) WHERE ID(n) = %d AND m.`%s` IS NOT NULL RETURN m", depth, fkNode.Id, ls.EntitySchemaTerm), vars)
				if err != nil {
					return err
				}
				singleRec, err := eNodesRec.Collect()
				if err != nil {
					return err
				}
				if singleRec == nil {
					depth++
					continue
				}
				if len(singleRec) > 1 {
					return errors.New("mulitple entity nodes found")
				}
				eNode := singleRec[0].Values[0].(neo4j.Node)
				// connect found entity root to new node
				if dirTo {
					_, err := tx.Run(fmt.Sprintf("MATCH (n) MATCH (m) WHERE ID(n) = %d AND ID(m) = %d CREATE (n)-[%s]->(m)", eNode.Id, int64(nodeMap[entityRoot]), config.MakeLabels([]string{ls.HasTerm})), vars)
					if err != nil {
						return err
					}
				} else {
					_, err := tx.Run(fmt.Sprintf("MATCH (n) MATCH (m) WHERE ID(n) = %d AND ID(m) = %d CREATE (n)<-[%s]-(m)", eNode.Id, int64(nodeMap[entityRoot]), config.MakeLabels([]string{ls.HasTerm})), vars)
					if err != nil {
						return err
					}
				}
				break
			}
		}
	}
	return nil
}
