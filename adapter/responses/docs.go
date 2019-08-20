package responses

import (
	"encoding/json"

	"kibouse/data/wrappers"
)

type docs struct {
	ResponseInputs
}

// CreateElasticJSON converts result into JSON string compatible with kibana.
func (di *docs) CreateElasticJSON() (string, error) {
	if di.rows == nil {
		return `{"docs":[]}`, nil
	}
	di.rows.Reset()
	response := struct {
		Docs []*hit `json:"docs"`
	}{
		Docs: make([]*hit, 0, di.rows.Items()),
	}

	for item := di.rows.NextItem(); item != nil; item = di.rows.NextItem() {
		response.Docs = append(response.Docs, newHit(di.index, item))
	}
	bytes, err := json.Marshal(response)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func NewDocItemsResponseBuilder() Builder {
	return &docs{}
}

func newHit(index string, source wrappers.DataItem) *hit {
	return &hit{
		Index:   index,
		Type:    source.ChTableName(),
		Version: 1,
		ID:      source.ID(),
		Score:   1,
		Found:   true,
		Source:  source.Data(),
		Sort:    nil,
		Fields:  nil,
	}
}