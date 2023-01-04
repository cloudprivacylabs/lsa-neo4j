package neo4j

type NodesetInput interface {
	ColumnNames() []string
	Next() ([]string, error)
	Reset() error
}

type Nodeset struct {
	data      map[string][][]string
	Operation string
	HeaderRow int
	StartRow  int
}

func (ns Nodeset) ColumnNames() []string {
	for _, d := range ns.data {
		return d[ns.HeaderRow]
	}
	return nil
}

func (ns Nodeset) Next() ([]string, error) {

}

func (ns Nodeset) Reset() error {

}

func ParseNodesetData(data map[string][][]string, headerColStart int) []NodesetInput {
	nsi := make([]NodesetInput, 0)
	var prevNodesetID string
	for _, sheet := range data {
	ROW:
		for r := 0; r < len(sheet); r++ {
			for c := 0; c < len(sheet[r]); c++ {
				if c == headerColStart {
					if sheet[r][c] == "nodeset_id" {
						nodeset := Nodeset{data: make(map[string][][]string)}
						for j := r + 1; j < len(sheet); j++ {
							if sheet[j][c] == "" {
								continue
							}
							if j != r+1 && sheet[j][c] != prevNodesetID {
								continue
							}
							prevNodesetID = sheet[j][c]
							// scan down
							nodeset.data[sheet[j][c]] = append(nodeset.data[sheet[j][c]], sheet[j])
							nsi = append(nsi, nodeset)
							clearRow(sheet[j])
							continue ROW
						}
					}
				}
			}
		}
	}
	return nsi
}

func clearRow(row []string) {
	for ix := range row {
		row[ix] = ""
	}
}
