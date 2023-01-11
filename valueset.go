package neo4j

import (
	"io"
	"reflect"
	"strings"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type NodesetInput interface {
	ColumnNames() []string
	Next() ([]string, error)
	Reset() error
}

type Nodeset struct {
	ID     string
	Labels []string
	// Data[x] gives NodesetData with Nodeset.ID = x
	Data map[string]NodesetData
}

type NodesetData struct {
	ID                 string
	Labels             []string
	Properties         map[string]interface{}
	LinkedToOtherNodes bool
}

func ParseNodesetData(input NodesetInput) (map[string]Nodeset, error) {
	processed := false
	ret := make(map[string]Nodeset)
	getColumn := func(row []string, header string) string {
		for cIdx, c := range input.ColumnNames() {
			if c == header && cIdx < len(row) {
				return row[cIdx]
			}
		}
		return ""
	}
	for {
		if err := input.Reset(); err != nil {
			return nil, err
		}
		var currNodesetID string
		var nodeset Nodeset
		for {
			row, err := input.Next()
			if err == io.EOF {
				if currNodesetID != "" {
					ret[currNodesetID] = nodeset
				} else {
					processed = false
				}
				currNodesetID = ""
				break
			}
			nodesetId := getColumn(row, "nodeset_id")
			if _, seen := ret[nodesetId]; seen {
				continue
			}
			currNodesetID = nodesetId
			nodeset = Nodeset{
				ID:     currNodesetID,
				Labels: make([]string, 0),
				Data:   make(map[string]NodesetData),
			}
			// Process a new nodeset, parse row and add properties nodeset
			parseRow := func(row []string) error {
				var nsDataID string
				for idx := 0; idx < len(input.ColumnNames()); idx++ {
					val := row[idx]
					header := input.ColumnNames()[idx]
					if header == "nodeset_id" {
						continue
					} else if header == "concept_id" {
						nsDataID = val
						if _, seen := nodeset.Data[nsDataID]; !seen {
							nodeset.Data[nsDataID] = NodesetData{
								ID:         nsDataID,
								Labels:     make([]string, 0),
								Properties: make(map[string]interface{}),
							}
						}
					} else if header == "node_labels" {
						if nsl, ok := nodeset.Data[nsDataID]; ok {
							nsl.Labels = append(nsl.Labels, strings.Fields(row[idx])...)
							nodeset.Data[nsDataID] = nsl
						}
					} else {
						props, _ := nodeset.Data[nsDataID].Properties[input.ColumnNames()[idx]].([]string)
						props = append(props, val)
						nodeset.Data[nsDataID].Properties[input.ColumnNames()[idx]] = props
					}
				}
				return nil
			}
			if err := parseRow(row); err != nil {
				return nil, err
			}
			processed = true
			for {
				r, err := input.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					return nil, err
				}
				// continue scanning with only with the same nodesetID
				if currNodesetID != "" && nodesetId != currNodesetID {
					continue
				}
				if err := parseRow(r); err != nil {
					return nil, err
				}
			}
		}
		if !processed {
			break
		}
	}
	return ret, nil
}

// func buildNodesetNodes(nodeset Nodeset) []*lpg.Node {
// 	grph := ls.NewDocumentGraph()
// 	nodes := make([]*lpg.Node, 0)
// 	for _, nsD := range nodeset.Data {
// 		nodes = append(nodes, grph.NewNode(nsD.Labels, nsD.Properties))
// 	}
// 	return nodes
// }

// oldNodeset is nodeset pulled from DB
func diff(oldNodeset, newNodeset Nodeset) (insertions NodesetData, deletions []string, updates NodesetData) {
	// compare roots
	if oldNodeset.ID != newNodeset.ID {
		// update
		updates.ID = newNodeset.ID
		if !reflect.DeepEqual(oldNodeset.Labels, newNodeset.Labels) {
			// update
			updates.Labels = newNodeset.Labels
		}
	}
	seenOldNodesetData := make(map[*NodesetData]struct{})
	for _, oldNsData := range oldNodeset.Data {
		for _, newNsData := range newNodeset.Data {
			// if newNodeset is not in oldNodeset (newNodeset must === oldNodeset)
			if _, has := seenOldNodesetData[&newNsData]; !has {
				// delete
				deletions = append(deletions, oldNsData.ID)
			}
			for k, v := range newNsData.Properties {
				// if oldNodeset does not have property
				if _, has := oldNsData.Properties[k]; !has {
					// insert
					insertions.Properties[k] = v
				} else {
					// compare, nop or update
					if !reflect.DeepEqual(oldNsData.Properties, newNsData.Properties) {
						// update
						updates.Properties[k] = v
					}
				}
			}
			if !lpg.NewStringSet(oldNsData.Labels...).IsEqual(lpg.NewStringSet(newNsData.Labels...)) {
				// update - setting labels to those in newNodeset but not in oldNodeset
				updates.Labels = findLabelDiff(lpg.NewStringSet(newNsData.Labels...), (lpg.NewStringSet(oldNsData.Labels...)))
			}
			seenOldNodesetData[&oldNsData] = struct{}{}
		}
	}
	return insertions, deletions, updates
}

/*
where insertions and deletions are node ids. (NodesetData.ID)
Find NodesetData that are in oldNodeset but not in NewNodeset -> deletions
Find NodesetData that are in newNodeset but not in oldNodeset: -> insertions
Find NodesetData that are in both oldNodeset and NewNodeset: make sure labels and props are same, if not -> updates

Also write:
*/
func LoadNodeset(tx neo4j.Transaction, nodesetId string) (Nodeset, error) {
	query := "MATCH (root:`NODESET` {nodeset_id: $id})-[:$id]->(m) return root,m"
	idrec, err := tx.Run(query, map[string]interface{}{"id": nodesetId})
	if err != nil {
		return Nodeset{}, err
	}
	ns := Nodeset{
		ID:     nodesetId,
		Labels: make([]string, 0),
		Data:   make(map[string]NodesetData),
	}
	first := false
	for idrec.Next() {
		rec := idrec.Record()
		if !first {
			nsCenter := rec.Values[0].(neo4j.Node)
			ns.Labels = nsCenter.Labels
			first = true
		}
		nsLeaf := rec.Values[1].(neo4j.Node)
		var nsLeafEntityId string
		for k := range nsLeaf.Props {
			if k == ls.EntityIDTerm {
				nsLeafEntityId = k
			}
		}
		ns.Data[nsLeafEntityId] = NodesetData{
			ID:         nsLeafEntityId,
			Labels:     nsLeaf.Labels,
			Properties: nsLeaf.Props,
		}
	}
	return ns, nil
}

func (ns Nodeset) Execute(tx neo4j.Transaction, op string) error {
	db_ns, err := LoadNodeset(tx, ns.ID)
	inserts, deletes, updates := diff(ns, db_ns)
	if err != nil {
		return err
	}
	switch op {
	case "apply":
		// update properties
		if _, err := tx.Run("MATCH (n) WHERE n.entityId = $eid SET n = $props", map[string]interface{}{"eid": updates.ID, "props": updates.Properties}); err != nil {
			return err
		}
		// update labels
		// WITH ['SPX', 'QQQ'] as x MATCH (n) WHERE n.entityId='lol' FOREACH(u in x | set n:u) return n
		formatLabels := strings.Join(updates.Labels, ":")
		if _, err := tx.Run("MATCH (n) WHERE n.entityId = $eid FOREACH(x in $labels | SET n:x)", map[string]interface{}{"eid": updates.ID}); err != nil {
			return err
		}
	case "delete":
		if _, err := tx.Run("MATCH (n) WHERE n.entityId in $deletes DETACH DELETE n", map[string]interface{}{"deletes": deletes}); err != nil {
			return err
		}
	}
	return nil
}
