package neo4j

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/fatih/structs"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
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

func ParseNodesetData(cfg Config, input NodesetInput) (map[string]Nodeset, error) {
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
				ID:         currNodesetID,
				Labels:     []string{"NODESET"},
				Properties: cfg.ShortenProperties(map[string]interface{}{"nodeset_id": currNodesetID}),
				Data:       make(map[string]NodesetData),
			}
			// Process a new nodeset, parse row and add properties nodeset
			parseRow := func(row []string) error {
				var nsDataID string
				for idx := 0; idx < len(input.ColumnNames()); idx++ {
					val := row[idx]
					header := input.ColumnNames()[idx]
					if header == "nodeset_id" {
						currNodesetID = val
						continue
					} else if header == "concept_id" {
						nsDataID = val
						if _, seen := nodeset.Data[nsDataID]; !seen {
							nodeset.Data[nsDataID] = NodesetData{
								ID:         nsDataID,
								Labels:     make([]string, 0),
								Properties: map[string]interface{}{cfg.Shorten(ls.EntityIDTerm): val},
							}
						}
					} else if header == "node_labels" {
						if nsl, ok := nodeset.Data[nsDataID]; ok {
							nsl.Labels = strings.Fields(row[idx])
							nodeset.Data[cfg.Shorten(nsDataID)] = nsl
						}
					} else {
						props, _ := nodeset.Data[nsDataID].Properties[input.ColumnNames()[idx]].([]string)
						props = append(props, val)
						nodeset.Data[nsDataID].Properties[cfg.Shorten(input.ColumnNames()[idx])] = props
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
				nodesetId = getColumn(r, "nodeset_id")
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
func NodesetDiff(oldNodeset, newNodeset Nodeset) (rootOp string, insertions []NodesetData, deletions []NodesetData, updates []NodesetData) {
	// if DB nodeset does not exists, insert new nodeset
	if oldNodeset.ID == "" {
		rootOp = "insertRoot"
	} else {
		// delete DB nodeset if newNodeset is empty
		if newNodeset.ID == "" {
			rootOp = "deleteRoot"
		} else {
			// update to use newNodeset if oldNodeset is not equal
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
			deletions = append(deletions, oldNsData)
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

func LoadNodeset(ctx context.Context, cfg Config, tx neo4j.ExplicitTransaction, nodesetId string) (Nodeset, error) {
	query := fmt.Sprintf("MATCH (root:`NODESET` {nodeset_id: %s})-[:%s]->(m) return root,m", quoteStringLiteral(nodesetId), nodesetId)
	idrec, err := tx.Run(ctx, query, map[string]interface{}{})
	if err != nil {
		return Nodeset{}, err
	}
	ns := Nodeset{
		Labels: make([]string, 0),
		Data:   make(map[string]NodesetData),
	}
	first := false
	for idrec.Next(ctx) {
		rec := idrec.Record()
		if !first {
			nsCenter := rec.Values[0].(neo4j.Node)
			for k, v := range nsCenter.Props {
				if k == cfg.Expand("nodeset_id") {
					ns.ID = v.(string)
				}
			}
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

func buildProps(props map[string]interface{}, vars map[string]interface{}) string {
	out := strings.Builder{}
	first := true

	for k, v := range props {
		if v == nil {
			continue
		}
		if first {
			out.WriteRune('{')
			first = false
		} else {
			out.WriteRune(',')
		}
		out.WriteString(quoteBacktick(k))
		out.WriteRune(':')
		out.WriteRune('$')
		tname := fmt.Sprintf("p%d", len(vars))
		out.WriteString(tname)
		_, ok := v.(string)
		if !ok {
			vars[tname] = v.([]string)
		} else {
			vars[tname] = v.(string)
		}
	}
	if !first {
		out.WriteRune('}')
	}

	return out.String()
}

func Execute(ctx context.Context, tx neo4j.ExplicitTransaction, cfg Config, oldNodeset, newNodeset Nodeset, rootOp string, inserts, updates, deletes []NodesetData) error {
	switch rootOp {
	case "insertRoot":
		vars := make(map[string]interface{})
		if _, err := tx.Run(ctx, fmt.Sprintf("CREATE (n %s %s)", cfg.MakeLabels(newNodeset.Labels), buildProps(newNodeset.Properties, vars)), vars); err != nil {
			return err
		}
	case "updateRoot":
		if _, err := tx.Run(ctx, fmt.Sprintf("MATCH (n) WHERE n.entityId = $eid SET n.props=$props, n%s", cfg.MakeLabels(oldNodeset.Labels)), map[string]interface{}{
			"eid": oldNodeset.ID, "props": oldNodeset.Properties}); err != nil {
			return err
		}
	case "deleteRoot":
		if _, err := tx.Run(ctx, fmt.Sprintf("MATCH (n {nodeset_id: $eid})-[r:%s]->() DELETE r", oldNodeset.ID), map[string]interface{}{"eid": oldNodeset.ID}); err != nil {
			return err
		}
	}
	type groupedNodesetData struct {
		labelExpr string
		data      []NodesetData
	}
	groupByLabel := func(nodesetData []NodesetData) []groupedNodesetData {
		ret := make([]groupedNodesetData, 0)
		hm := make(map[string][]NodesetData)
		for _, nsD := range nodesetData {
			nsD.Labels = sort.StringSlice(nsD.Labels)
			labelExpr := cfg.MakeLabels(nsD.Labels)
			hm[labelExpr] = append(hm[labelExpr], nsD)
		}
		for expr, nsData := range hm {
			ret = append(ret, groupedNodesetData{labelExpr: expr, data: nsData})
		}
		return ret
	}
	mapNodesetData := func(nodesetData []NodesetData) []map[string]interface{} {
		ans := make([]map[string]interface{}, len(nodesetData))
		for ix, item := range nodesetData {
			ans[ix] = structs.Map(item)
		}
		return ans
	}
	if len(updates) > 0 {
		for ix, update := range groupByLabel(updates) {
			unwindData := map[string]interface{}{"nodes": mapNodesetData(update.data), "props": update.data[ix].Properties}
			query := fmt.Sprintf(`unwind $nodes as node MATCH (n) WHERE n.entityId = node.ID SET n=node.Properties, n%s`, update.labelExpr)
			if _, err := tx.Run(ctx, query, unwindData); err != nil {
				return err
			}
		}
	}
	if len(inserts) > 0 {
		for _, insert := range groupByLabel(inserts) {
			unwindData := map[string]interface{}{"nodes": mapNodesetData(insert.data)}
			query := fmt.Sprintf("UNWIND $nodes AS node MERGE (n%s {`https://lschema.org/entityId`: node.ID}) SET n=node.Properties WITH n MATCH(root:`NODESET` {nodeset_id: %s}) MERGE(root)-[:%s]->(n)",
				insert.labelExpr, quoteStringLiteral(newNodeset.ID), newNodeset.ID)

			if _, err := tx.Run(ctx, query, unwindData); err != nil {
				return err
			}
		}
	}
	if len(deletes) > 0 {
		if _, err := tx.Run(ctx, "MATCH (n) WHERE n.`https://lschema.org/entityId` IN $deletes DETACH DELETE n", map[string]interface{}{"deletes": mapNodesetData(deletes)}); err != nil {
			return err
		}
	}
	return nil
}
