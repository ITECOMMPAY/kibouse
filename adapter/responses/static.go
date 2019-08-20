package responses

import "fmt"

const NotFoundResponseTemplate = `{
  "error": {
    "root_cause": [
      {
        "type": "index_not_found_exception",
        "reason": "no such index",
        "resource.type": "index_or_alias",
        "resource.id": "%s",
        "index_uuid": "_na_",
        "index": "%s"
      }
    ],
    "type": "index_not_found_exception",
    "reason": "no such index",
    "resource.type": "index_or_alias",
    "resource.id": "%s",
    "index_uuid": "_na_",
    "index": "%s"
  },
  "status": 404
}`

const ResponseTemplate = `{"_index":".kibana","_type":"%s","_id":"%s","_version":6,"result":"updated","_shards":{"total":1,"successful":1,"failed":0},"_seq_no":361,"_primary_term":38}`

func CreateDataNotFoundResponse() string {
	return "{\"hits\":{\"total\":0}}"
}

func CreateIndexNotFoundResponse(id string) string {
	return fmt.Sprintf(NotFoundResponseTemplate, id, id, id, id)
}

func CreateUpdatingResponse(typename string, id string) string {
	return fmt.Sprintf(ResponseTemplate, typename, id)
}
