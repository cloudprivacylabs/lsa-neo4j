package neo4j

import (
	"fmt"
	"io"
	"strings"
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
	Properties         map[string][]string
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
		var cerr error
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
			if cerr != nil {
				return nil, err
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
						Properties: make(map[string][]string),
					},
				},
			}
			// // Process a new nodeset, parse row and add properties nodeset
			parseRow := func(row []string, ns *Nodeset) error {
				for idx, val := range row {
					if idx >= len(input.ColumnNames()) {
						fmt.Println(input.ColumnNames()[idx])
						return fmt.Errorf("index out of bounds: retrieving from %v at index %d", row, idx)
					}
					header := input.ColumnNames()[idx]
					if header == "node_labels" {
						// filter labels, may be bracket enclosed or space separated [A, A] or A A
						rl := strings.TrimSpace(row[idx])
						if strings.HasPrefix(rl, "(") || strings.HasPrefix(rl, "[") || strings.HasPrefix(rl, "{") {
							removeEnclosures := func(s string, rem ...string) string {
								for _, r := range rem {
									s = strings.ReplaceAll(s, r, "")
								}
								return s
							}
							// [A, A, A] -> A A A
							rl = removeEnclosures(rl, "{", "}", "(", ")", "[", "]", ",")
							// space delimiter
						}
						if nsl, ok := ns.Data[currNodesetID]; ok {
							nsl.Labels = append(nsl.Labels, strings.Split(rl, " ")...)
							ns.Data[currNodesetID] = nsl
						}
						// ns.Data[currNodesetID].Labels = append(ns.Data[currNodesetID].Labels, strings.Split(rl, " ")...)
					} else {
						ns.Data[currNodesetID].Properties[input.ColumnNames()[idx]] = append(ns.Data[currNodesetID].Properties[input.ColumnNames()[idx]], val)
					}
				}
				return nil
			}
			if err := parseRow(row, &nodeset); err != nil {
				return nil, err
			}

			// nodesetRows := nodeset
			// nodesetRows = append(nodesetRows, row)
			// process a row
			processed = true
			for {
				r, err := input.Next()
				if err == io.EOF {
					// err = err
					break
				}
				if err != nil {
					return nil, err
				}
				// continue scanning with only with the same nodesetID
				if currNodesetID != "" && nodesetId != currNodesetID {
					continue
				}
				// nID := getColumn(row, "nodeset_id")
				// // ns := Nodeset{
				// // 	ID: nID,
				// // 	Rows: map[string][][]string{nodesetId: r},
				// // }
				// currNodesetID = nID
				if err := parseRow(r, &nodeset); err != nil {
					return nil, err
				}
				// nodesetRows = append(nodesetRows, r)
				// process row

			}
		}
		if !processed {
			break
		}
	}
	return ret, nil
}

// func buildNodesetNodes(nodesets map[string]Nodeset) []*lpg.Node {
// 	grph := ls.NewDocumentGraph()
// 	nodes := make([]*lpg.Node, 0)
// 	for _, ns := range nodesets {
// 		nodes = append(nodes, grph.NewNode(ns.Labels, ns.Data[ns.ID].Properties))
// 	}
// 	return nodes
// }

func diff(oldNodeset, newNodeset Nodeset) (insertions []string, deletions []string, updates []string) {
	return nil, nil, nil
}

/*
where insertions and deletions are node ids. (NodesetData.ID)
Find NodesetData that are in newNodeset but not in oldNodeset: -> insertions
Find NodesetData that are in oldNodeset but not in NewNodeset -> deletions
Find NodesetData that are in both oldNodeset and NewNodeset: make sure labels and props are same, if not -> updates

Also write:
*/
func LoadNodeset(nodesetId string) Nodeset {
	return Nodeset{}
}

func NodesetApply(nodesets map[string]Nodeset) error {
	return nil
}

func NodesetDelete(nodesets map[string]Nodeset) error {
	return nil
}
