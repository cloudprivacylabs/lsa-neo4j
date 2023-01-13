package neo4j

import (
	"fmt"
	"io"
	"reflect"
	"sort"
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
	ID         string
	Labels     []string
	Properties map[string]interface{}
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

// oldNodeset is nodeset pulled from DB
func diff(oldNodeset, newNodeset *Nodeset) (rootOp string, insertions []NodesetData, deletions []string, updates []NodesetData) {
	// if DB nodeset does not exists, insert new nodeset
	if oldNodeset.ID == "" {
		root := NodesetData{
			ID:         newNodeset.ID,
			Properties: newNodeset.Properties,
			Labels:     newNodeset.Labels,
		}
		insertions = append(insertions, root)
		rootOp = "insertRoot"
	} else {
		// delete DB nodeset if newNodeset is empty
		if newNodeset.ID == "" {
			deletions = append(deletions, oldNodeset.ID)
			rootOp = "deleteRoot"
		} else if oldNodeset != newNodeset {
			// update to use newNodeset if oldNodeset is not equal
			root := NodesetData{
				ID:         newNodeset.ID,
				Properties: newNodeset.Properties,
				Labels:     newNodeset.Labels,
			}
			updates = append(updates, root)
			rootOp = "updateRoot"
		}
	}
	for oid, oldNsData := range oldNodeset.Data {
		newNsData, exists := newNodeset.Data[oid]
		if exists {
			update := NodesetData{
				ID:         oid,
				Labels:     make([]string, 0),
				Properties: make(map[string]interface{}),
			}
			changed := false
			if !lpg.NewStringSet(oldNsData.Labels...).IsEqual(lpg.NewStringSet(newNsData.Labels...)) {
				// update - setting labels to those in newNodeset
				update.Labels = newNsData.Labels
				changed = true
			}
			// if props are not equal, update to use newNodeset properties
			if !reflect.DeepEqual(oldNsData.Properties, newNsData.Properties) {
				for k, v := range newNsData.Properties {
					update.Properties[k] = v
				}
				changed = true
			}
			if changed {
				updates = append(updates, update)
			}
		} else {
			deletions = append(deletions, oid)
		}
	}
	for nid, newNsData := range newNodeset.Data {
		_, exists := oldNodeset.Data[nid]
		if !exists {
			insertions = append(insertions, newNsData)
		}
	}
	return rootOp, insertions, deletions, updates
}

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

func Execute(tx neo4j.Transaction, oldNodeset, newNodeset *Nodeset, rootOp string, inserts, updates []NodesetData, deletions []string) error {
	type groupedNodesetData struct {
		labelExpr string
		data      []NodesetData
	}
	groupByLabel := func(nodesetData []NodesetData) []groupedNodesetData {
		ret := make([]groupedNodesetData, 0)
		hm := make(map[string][]NodesetData)
		for _, nsD := range nodesetData {
			nsD.Labels = sort.StringSlice(nsD.Labels)
			labelExpr := ""
			hm[labelExpr] = append(hm[labelExpr], nsD)
		}
		for expr, nsData := range hm {
			ret = append(ret, groupedNodesetData{labelExpr: expr, data: nsData})
		}
		return ret
	}
	type udata struct {
		udata []map[string]interface{}
	}
	if len(updates) > 0 {
		unwindData := make(map[string]udata)
		for ix, update := range groupByLabel(updates) {
			unwind := make([]map[string]interface{}, 0)
			item := map[string]interface{}{
				"props": update.data[ix].Properties,
			}
			unwind = append(unwind, item)
			unwindData[update.labelExpr] = udata{udata: unwind}
		}
		for label, unwind := range unwindData {
			query := fmt.Sprintf(`unwind $nodes as node MATCH (n) WHERE n.entityId = node.ID SET n=node.props, n%s`, label)
			if _, err := tx.Run(query, map[string]interface{}{"nodes": unwind.udata}); err != nil {
				return err
			}
		}
	}
	if len(inserts) > 0 {
		unwindData := make(map[string]udata)
		for ix, insert := range groupByLabel(inserts) {
			unwind := make([]map[string]interface{}, 0)
			item := map[string]interface{}{
				"props": insert.data[ix].Properties,
			}
			unwind = append(unwind, item)
			unwindData[insert.labelExpr] = udata{udata: unwind}
		}
		for label, unwind := range unwindData {
			query := fmt.Sprintf(`unwind $nodes as node create (a%s) set a=node.props`, label)
			if _, err := tx.Run(query, map[string]interface{}{"nodes": unwind.udata}); err != nil {
				return err
			}
		}
	}
	if len(deletions) > 0 {
		if _, err := tx.Run("MATCH (n) WHERE n.entityId in $deletes DETACH DELETE n", map[string]interface{}{"deletes": deletions}); err != nil {
			return err
		}
	}
	return nil
}

func CommitNodesetsOperation(tx neo4j.Transaction, nodesets map[string]Nodeset, operation string) error {
	for _, ns := range nodesets {
		db_ns, err := LoadNodeset(tx, ns.ID)
		if err != nil {
			return err
		}
		switch operation {
		case "apply":
			// insert, update
			rootOp, inserts, deletes, updates := diff(&db_ns, &ns)
			if err := Execute(tx, &db_ns, &ns, rootOp, inserts, updates, deletes); err != nil {
				return err
			}
		case "delete":
			rootOp, inserts, deletes, updates := diff(&db_ns, &Nodeset{})
			if err := Execute(tx, &db_ns, &ns, rootOp, inserts, updates, deletes); err != nil {
				return err
			}
		}
	}
	return nil
}
