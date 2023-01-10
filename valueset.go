package neo4j

import (
	"io"
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
	Data   map[string]NodesetData
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
		// needs termination condition -- endless loop
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
				Data: map[string]NodesetData{
					currNodesetID: NodesetData{
						ID:         currNodesetID,
						Labels:     make([]string, 0),
						Properties: make(map[string]interface{}),
					},
				},
			}
			// // Process a new nodeset, parse row and add properties nodeset
			parseRow := func(row []string) error {
				for idx := 0; idx < len(input.ColumnNames()); idx++ {
					val := row[idx]
					header := input.ColumnNames()[idx]
					if header == "node_labels" {
						if nsl, ok := nodeset.Data[currNodesetID]; ok {
							removeEnclosures := func(s string, rem ...string) string {
								for _, r := range rem {
									s = strings.ReplaceAll(s, r, "")
								}
								return s
							}
							fields := strings.Fields(row[idx])
							labels := make([]string, 0)
							for _, f := range fields {
								labels = append(labels, removeEnclosures(f, "{", "}", "(", ")", "[", "]", ","))
							}
							nsl.Labels = append(nsl.Labels, labels...)
							nodeset.Data[currNodesetID] = nsl
						}
					} else {
						props, _ := nodeset.Data[currNodesetID].Properties[input.ColumnNames()[idx]].([]string)
						props = append(props, val)
						nodeset.Data[currNodesetID].Properties[input.ColumnNames()[idx]] = props
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

func buildNodesetNodes(nodeset Nodeset) []*lpg.Node {
	grph := ls.NewDocumentGraph()
	nodes := make([]*lpg.Node, 0)
	for _, nsD := range nodeset.Data {
		nodes = append(nodes, grph.NewNode(nsD.Labels, nsD.Properties))
	}
	return nodes
}

func diff(oldNodeset, newNodeset Nodeset) (insertions Nodeset, deletions Nodeset, updates Nodeset) {
	oldLabels := oldNodeset.Data[oldNodeset.ID].Labels
	newLabels := newNodeset.Data[newNodeset.ID].Labels
	oldProps := oldNodeset.Data[oldNodeset.ID].Properties
	newProps := newNodeset.Data[newNodeset.ID].Properties
	seenOldLabels := make(map[string]struct{})
	seenNewLabels := make(map[string]struct{})
	for _, l := range oldLabels {
		seenOldLabels[l] = struct{}{}
	}
	for _, l := range newLabels {
		seenNewLabels[l] = struct{}{}
	}
	for k, v := range oldProps {
		// in old, not in new
		if w, ok := newProps[k]; !ok || v != w {
			deletions.Data[oldNodeset.ID].Properties[k] = v
		} else {
			// in old, in new
			updates.Data[oldNodeset.ID].Properties[k] = v
		}
	}
	for k, v := range newProps {
		// in new, not in old
		if w, ok := oldProps[k]; !ok || v != w {
			insertions.Data[oldNodeset.ID].Properties[k] = v
		} else {
			// in new, in old
			updates.Data[oldNodeset.ID].Properties[k] = v
		}
	}
	uSeen := make(map[string]struct{})
	for _, l := range oldLabels {
		if _, seen := seenNewLabels[l]; !seen {
			// in old, not in new
			deletions.Labels = append(deletions.Labels, l)
		} else {
			// in old, in new
			uSeen[l] = struct{}{}
		}
	}
	for _, l := range newLabels {
		if _, seen := seenOldLabels[l]; !seen {
			// in new, not in old
			insertions.Labels = append(deletions.Labels, l)
		} else {
			// in new, in old
			uSeen[l] = struct{}{}
		}
	}
	for l := range uSeen {
		updates.Labels = append(updates.Labels, l)
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
	query := "MATCH (root:`NODESET` {nodeset_id: $id})-[:$id*]->(m) return root,m"
	idrec, err := tx.Run(query, map[string]interface{}{"id": nodesetId})
	if err != nil {
		return Nodeset{}, err
	}
	ns := Nodeset{ID: nodesetId}
	for idrec.Next() {
		rec := idrec.Record()
		nsCenter := rec.Values[0].(neo4j.Node)
		nsDegree := rec.Values[1].(neo4j.Node)
		ns.Labels = nsCenter.Labels
		if nsl, ok := ns.Data[nodesetId]; ok {
			for _, label := range nsDegree.Labels {
				nsl.Labels = append(nsl.Labels, label)
				ns.Data[nodesetId] = nsl
			}
		}
		if nsp, ok := ns.Data[nodesetId]; ok {
			nsp.Properties = nsDegree.Props
			ns.Data[nodesetId] = nsp
		}
	}
	return ns, nil
}

func NodesetApply(tx neo4j.Transaction, nodesets map[string]Nodeset) error {
	for _, ns := range nodesets {
		db_ns, err := LoadNodeset(tx, ns.ID)
		if err != nil {
			return err
		}
		// get diff
		inserts, _, updates := diff(ns, db_ns)
		insertDBNodes := buildNodesetNodes(inserts)
		updateDBNodes := buildNodesetNodes(updates)
	}
	return nil
}

func NodesetDelete(tx neo4j.Transaction, nodesets map[string]Nodeset) error {
	for _, ns := range nodesets {
		db_ns, err := LoadNodeset(tx, ns.ID)
		if err != nil {
			return err
		}
		// get diff
		_, deletes, updates := diff(ns, db_ns)
		for cId, nsD := range deletes.Data {

			query := `MATCH (n {id: $id}) WITH [key in keys(n) WHERE not(n[key] is null) | n[key]] AS values UNWIND values AS value WITH DISTINCT value 
			MATCH(n) WHERE values = $props MATCH (n) WHERE labels(n)=$labels 
			CALL { WITH n DETACH DELETE n }`
			_, err = tx.Run(query, map[string]interface{}{"id": cId, "labels": nsD.Labels, "$props": nsD.Properties})
			if err != nil {
				return err
			}
		}
		updateDBNodes := buildNodesetNodes(updates)
	}
	return nil
}
