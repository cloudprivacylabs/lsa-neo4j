package neo4j

import (
	"fmt"
	"io"

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
	Rows   map[string]itrRows
}

type itrRows struct {
	r    int
	rows [][]string
}

func (ir *itrRows) next() ([]string, error) {
	ir.r++
	if ir.r >= len(ir.rows) {
		return nil, io.EOF
	}
	return ir.rows[ir.r], nil
}

type NodesetData struct {
	ID                 string
	Labels             []string
	Properties         map[string]string
	LinkedToOtherNodes bool
}

func (ns Nodeset) ColumnNames() []string {
	cols := make([]string, 0)
	for _, d := range ns.Data {
		cols = append(cols, d.ID)
	}
	return cols
}

func (ns Nodeset) Next() ([]string, error) {
	for _, k := range ns.Rows {
		return k.next()
	}
	return nil, nil
}

func (ns Nodeset) Reset() error {

}

func ParseNodesetData(input NodesetInput) (map[string]Nodeset, error) {
	processed := false
	ret := make(map[string]Nodeset)
	getColumn := func(row []string, header string) string {
		for cIdx, c := range input.ColumnNames() {
			if c == header {
				return row[cIdx]
			}
		}
		return ""
	}
	/*
		A
		A
		B
		A
		C
	*/
	seenIDs := make(map[string]struct{})
	for {
		if err := input.Reset(); err != nil {
			return nil, err
		}
		var prevNodesetID string
		for row, err := input.Next(); ; {
			if err == io.EOF {
				seenIDs[prevNodesetID] = struct{}{}
				break
			}
			if err != nil {
				return nil, err
			}
			nodesetId := getColumn(row, "nodeset_id")
			// Process a new nodeset
			nodeset := Nodeset{
				ID:   nodesetId,
				Rows: make(map[string]itrRows),
			}
			nodesetRows := nodeset.Rows[nodesetId].rows
			nodesetRows = append(nodesetRows, row)
			// process a row
			processed = true
			for r, err := input.Next(); ; {
				if err != nil {
					return nil, err
				}
				// continue scanning with only with the same nodesetID
				if prevNodesetID != "" && nodesetId != prevNodesetID {
					continue
				}
				nID := getColumn(row, "nodeset_id")
				// ns := Nodeset{
				// 	ID: nID,
				// 	Rows: map[string][][]string{nodesetId: r},
				// }
				prevNodesetID = nID
				nodesetRows = append(nodesetRows, r)
				// process row
			}
			ret[nodesetId] = nodeset
		}
		if !processed {
			break
		}
	}
	return ret, nil
}

func BuildNodesets(ctx *ls.Context, nodesets map[string]Nodeset, cfg Config) func(neo4j.Transaction) error {
	labels := make(map[string][]*lpg.Node)
	for _, nodeset := range nodesets {
		l := cfg.MakeLabels(nodeset.Labels)
		labels[l] = append(labels[l], node)
	}

	type udata struct {
		insNodes []*lpg.Node
		udata    []map[string]interface{}
	}
	unwindData := make(map[string]udata)
	for label, insNodes := range labels {
		unwind := make([]map[string]interface{}, 0)
		for _, insNode := range insNodes {
			props := cfg.MakePropertiesObj(insNode)
			item := map[string]interface{}{
				"props": props,
			}
			unwind = append(unwind, item)
		}
		unwindData[label] = udata{insNodes: insNodes, udata: unwind}
	}
	return func(tx neo4j.Transaction) error {
		for label, unwind := range unwindData {
			query := fmt.Sprintf(`unwind $nodes as node 
create (a%s) set a=node.props return ID(a)`, label)
			result, err := tx.Run(query, map[string]interface{}{"nodes": unwind.udata})
			if err != nil {
				return err
			}
			records, err := result.Collect()
			if err != nil {
				return err
			}
			for i := range records {
				id := records[i].Values[0].(int64)
				nodeMap[unwind.insNodes[i]] = id
			}
		}
		return nil
	}
}

func diff(oldNodeset, newNodeset Nodeset) (insertions []string, deletions []string, updates []string) {

}

/*
where insertions and deletions are node ids. (NodesetData.ID)
Find NodesetData that are in newNodeset but not in oldNodeset: -> insertions
Find NodesetData that are in oldNodeset but not in NewNodeset -> deletions
Find NodesetData that are in both oldNodeset and NewNodeset: make sure labels and props are same, if not -> updates

Also write:
*/
func LoadNodeset(nodesetId string) Nodeset

// func ParseNodesetData(data map[string][][]string, headerColStart int) []NodesetInput {
// 	nsi := make([]NodesetInput, 0)
// 	var prevNodesetID string
// 	for _, sheet := range data {
// 	ROW:
// 		for r := 0; r < len(sheet); r++ {
// 			for c := 0; c < len(sheet[r]); c++ {
// 				if c == headerColStart {
// 					if sheet[r][c] == "nodeset_id" {
// 						nodeset := Nodeset{data: make(map[string][][]string)}
// 						for j := r + 1; j < len(sheet); j++ {
// 							if sheet[j][c] == "" {
// 								continue
// 							}
// 							if j != r+1 && sheet[j][c] != prevNodesetID {
// 								continue
// 							}
// 							prevNodesetID = sheet[j][c]
// 							// scan down
// 							nodeset.data[sheet[j][c]] = append(nodeset.data[sheet[j][c]], sheet[j])
// 							nsi = append(nsi, nodeset)
// 							clearRow(sheet[j])
// 							continue ROW
// 						}
// 					}
// 				}
// 			}
// 		}
// 	}
// 	return nsi
// }

// func clearRow(row []string) {
// 	for ix := range row {
// 		row[ix] = ""
// 	}
// }
