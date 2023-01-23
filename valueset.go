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
	ID         string
	Labels     []string
	Properties map[string]interface{}
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
			if nodesetId == "" {
				continue
			}
			if _, seen := ret[nodesetId]; seen {
				continue
			}
			currNodesetID = nodesetId
			nodeset = Nodeset{
				ID:         currNodesetID,
				Labels:     []string{"NODESET"},
				Properties: cfg.ShortenProperties(map[string]interface{}{cfg.Shorten(ls.EntityIDTerm): currNodesetID}),
				Data:       make(map[string]NodesetData),
			}
			// Process a new nodeset, parse row and add properties nodeset
			parseRow := func(row []string) error {
				cid := getColumn(row, "concept_id")
				if len(cid) == 0 {
					return nil
				}
				data := NodesetData{
					ID:         cid,
					Properties: map[string]interface{}{cfg.Shorten(ls.EntityIDTerm): cid},
				}
				labels := lpg.NewStringSet(strings.Fields(getColumn(row, "node_labels"))...)
				if labels.Len() == 0 {
					labels.Add("CONCEPT")
				}
				data.Labels = labels.Slice()

				for idx := 0; idx < len(input.ColumnNames()); idx++ {
					header := input.ColumnNames()[idx]
					if header != "node_labels" && header != "concept_id" && header != "nodeset_id" {
						data.Properties[cfg.Shorten(input.ColumnNames()[idx])] = row[idx]
					}
				}
				nodeset.Data[cid] = data
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
	fmt.Printf("%+v\n", ret)
	return ret, nil
}

type rootOpType int

const (
	rootOpInsert rootOpType = iota
	rootOpUpdate
	rootOpDelete
)

// oldNodeset is nodeset pulled from DB
func NodesetDiff(oldNodeset, newNodeset Nodeset) (rootOp rootOpType, insertions []NodesetData, deletions []NodesetData, updates []NodesetData) {
	// if DB nodeset does not exists, insert new nodeset
	if oldNodeset.ID == "" {
		rootOp = rootOpInsert
	} else {
		// delete DB nodeset if newNodeset is empty
		if newNodeset.ID == "" {
			rootOp = rootOpDelete
		} else {
			// update to use newNodeset if oldNodeset is not equal
			rootOp = rootOpUpdate
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
				// Keep the old, add the new
				for k, v := range oldNsData.Properties {
					update.Properties[k] = v
				}
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
	query := fmt.Sprintf("MATCH (root:`NODESET` {`%s`: %s})-[:%s]->(m) return root,m", cfg.Shorten(ls.EntityIDTerm), quoteStringLiteral(nodesetId), nodesetId)
	idrec, err := tx.Run(ctx, query, map[string]interface{}{})
	if err != nil {
		return Nodeset{}, err
	}
	ns := Nodeset{
		Labels:     make([]string, 0),
		Data:       make(map[string]NodesetData),
		Properties: make(map[string]any),
	}
	first := true
	for idrec.Next(ctx) {
		rec := idrec.Record()
		if first {
			first = false
			nsCenter := rec.Values[0].(neo4j.Node)
			for k, v := range nsCenter.Props {
				if k == cfg.Shorten(ls.EntityIDTerm) {
					ns.ID = v.(string)
					ns.Properties[k] = ns.ID
				}
			}
			ns.Labels = nsCenter.Labels
		}
		nsLeaf := rec.Values[1].(neo4j.Node)
		var nsLeafEntityId string
		for k, v := range nsLeaf.Props {
			if k == cfg.Shorten(ls.EntityIDTerm) {
				nsLeafEntityId = v.(string)
			}
		}
		nsData := NodesetData{
			ID:         nsLeafEntityId,
			Labels:     nsLeaf.Labels,
			Properties: make(map[string]any),
		}
		for k, v := range nsLeaf.Props {
			nsData.Properties[k] = v
		}
		ns.Data[nsLeafEntityId] = nsData
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

func Execute(ctx context.Context, tx neo4j.ExplicitTransaction, cfg Config, oldNodeset, newNodeset Nodeset, rootOp rootOpType, inserts, updates, deletes []NodesetData) error {
	switch rootOp {
	case rootOpInsert:
		vars := make(map[string]interface{})
		if _, err := tx.Run(ctx, fmt.Sprintf("CREATE (n %s %s)", cfg.MakeLabels(newNodeset.Labels), buildProps(newNodeset.Properties, vars)), vars); err != nil {
			return err
		}
	case rootOpUpdate:
		stmt := fmt.Sprintf("MATCH (n %s) WHERE n.`%s` = $eid SET n=$props, n%s", cfg.MakeLabels(oldNodeset.Labels), cfg.Shorten(ls.EntityIDTerm), cfg.MakeLabels(newNodeset.Labels))
		if _, err := tx.Run(ctx, stmt, map[string]interface{}{
			"eid": oldNodeset.ID, "props": newNodeset.Properties}); err != nil {
			return err
		}
	case rootOpDelete:
		if _, err := tx.Run(ctx, fmt.Sprintf("MATCH (n %s {`%s`: $eid})-[r:%s]->() DELETE r", cfg.MakeLabels(oldNodeset.Labels), cfg.Shorten(ls.EntityIDTerm), oldNodeset.ID), map[string]interface{}{"eid": oldNodeset.ID}); err != nil {
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
		for _, update := range groupByLabel(updates) {
			unwindData := map[string]interface{}{"nodes": mapNodesetData(update.data)}
			query := fmt.Sprintf("unwind $nodes as node MATCH (n) WHERE n.`%s` = node.ID SET n=node.Properties, n%s", cfg.Shorten(ls.EntityIDTerm), update.labelExpr)
			if _, err := tx.Run(ctx, query, unwindData); err != nil {
				return err
			}
		}
	}
	if len(inserts) > 0 {
		for _, insert := range groupByLabel(inserts) {
			unwindData := map[string]interface{}{"nodes": mapNodesetData(insert.data)}
			query := fmt.Sprintf("UNWIND $nodes AS node MERGE (n%s {`%s`: node.ID}) SET n=node.Properties WITH n MATCH(root:`NODESET` {`%s`: %s}) MERGE(root)-[:%s]->(n)",
				insert.labelExpr, cfg.Shorten(ls.EntityIDTerm), cfg.Shorten(ls.EntityIDTerm), quoteStringLiteral(newNodeset.ID), newNodeset.ID)

			if _, err := tx.Run(ctx, query, unwindData); err != nil {
				return err
			}
		}
	}
	if len(deletes) > 0 {
		if _, err := tx.Run(ctx, fmt.Sprintf("MATCH (n %s) WHERE n.`%s` IN $deletes DETACH DELETE n", cfg.MakeLabels(oldNodeset.Labels), cfg.Shorten(ls.EntityIDTerm)), map[string]interface{}{"deletes": mapNodesetData(deletes)}); err != nil {
			return err
		}
	}
	return nil
}
