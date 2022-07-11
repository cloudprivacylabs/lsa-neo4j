package neo4j

import (
	"fmt"

	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/cloudprivacylabs/opencypher/graph"
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
	node  graph.Node
}

func getLinkSpec(docNode graph.Node) *linkSpec {
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

func (spec linkSpec) getForeignKeys(entityRoot graph.Node) ([][]string, error) {
	if len(spec.fkFields) == 0 {
		v, _ := ls.GetRawNodeValue(spec.node)
		return [][]string{{v}}, nil
	}
	// There can be multiple instances of a foreign key in an
	// entity. ForeignKeyNdoes[i] keeps all the nodes for spec.FK[i]
	foreignKeyNodes := make([][]graph.Node, len(spec.fkFields))
	ls.IterateDescendants(entityRoot, func(n graph.Node) bool {
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

func LinkNodesForNewEntity(tx neo4j.Transaction, config Config, entityRoot graph.Node, nodeMap map[graph.Node]int64) error {
	links := make([]linkSpec, 0)
	// Does the entity have any outstanding links we need to work on?
	var itrErr error
	ls.IterateDescendants(entityRoot, func(node graph.Node) bool {
		if !node.GetLabels().Has(ls.DocumentNodeTerm) {
			return true
		}
		spec := getLinkSpec(node)
		if spec != nil {
			links = append(links, *spec)
		}
		return true
	}, func(edge graph.Edge) ls.EdgeFuncResult {
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
		if err := linkEntities(tx, config, entityRoot, link, nodeMap); err != nil {
			return err
		}
	}

	// Are there any links pointing to this entity in the DB?
	idTerm, ok := entityRoot.GetProperty(ls.EntityIDTerm)
	if !ok {
		return nil
	}
	id := ls.AsPropertyValue(idTerm, ok).MustStringSlice()
	return linkToThisEntity(id)
}

func linkEntities(tx neo4j.Transaction, config Config, entityRoot graph.Node, spec linkSpec, nodeMap map[graph.Node]int64) error {
	foreignKeys, err := spec.getForeignKeys(entityRoot)
	if err != nil {
		return err
	}

	var linkToNode graph.Node
	if len(spec.linkNode) > 0 {
		ls.WalkNodesInEntity(entityRoot, func(n graph.Node) bool {
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
			fkVal = ls.StringPropertyValue(fk[0])
		} else {
			fkVal = ls.StringSlicePropertyValue(fk)
		}
		nodePropertiesClause := config.MakeProperties(mapWithProperty(map[string]interface{}{
			ls.EntityIDTerm: fkVal,
		}), params)

		if spec.forward {
			query = fmt.Sprintf(`match (target %s %s) with target match (source) where ID(source)=%d create (source)-[%s]->(target)`, nodeLabelsClause, nodePropertiesClause, nodeMap[linkToNode], config.MakeLabels([]string{spec.label}))
		} else {
			query = fmt.Sprintf(`match (source %s %s) with source match (target) where ID(target)=%d create (source)-[%s]->(target)`, nodeLabelsClause, nodePropertiesClause, nodeMap[linkToNode], config.MakeLabels([]string{spec.label}))
		}
		_, err := tx.Run(query, params)
		if err != nil {
			return err
		}
	}
	return nil
}

func linkToThisEntity(ID []string) error {
	// TODO: Search for all nodes with Ref to this entity, and link them
	return nil
}
