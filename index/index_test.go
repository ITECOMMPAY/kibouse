package index

import (
	"fmt"
	"strings"
	"testing"
)

func TestCreateInvertedIndexRequest(t *testing.T) {

	testData := []struct {
		text    string
		column  string
		table   string
		tsRange string
		result  string
	}{
		{
			text:    "Worker callback_0 ends handling",
			column:  "message",
			table:   "logs.inverted_index_logs_2p_gate",
			tsRange: "(0 < ts) AND (ts <= 1542894389184806000)",
			result:  "SELECT ts FROM logs.logs.inverted_index_logs_2p_gate WHERE (word_hash IN (cityHash64('worker'),cityHash64('callback_0'),cityHash64('ends'),cityHash64('handling')) AND column_hash = cityHash64('message')) AND ((0 < ts) AND (ts <= 1542894389184806000)) GROUP BY ts HAVING uniq(word_hash) = 4 ORDER BY ts DESC",
		},
		{
			text:    "SQL : UPDATE Shard02.uniques00 SET status = 'update'",
			column:  "message",
			table:   "logs.inverted_index_logs_2p_gate",
			tsRange: "(0 < ts) AND (ts <= 1542894389184806000)",
			result:  "SELECT ts FROM logs.logs.inverted_index_logs_2p_gate WHERE (word_hash IN (cityHash64('sql'),cityHash64('update'),cityHash64('shard02'),cityHash64('uniques00'),cityHash64('set'),cityHash64('status')) AND column_hash = cityHash64('message')) AND ((0 < ts) AND (ts <= 1542894389184806000)) GROUP BY ts HAVING uniq(word_hash) = 6 ORDER BY ts DESC",
		},
		{
			text:    "#1197-62-1542894388|2117811957304647739:7567544320748365149:4913069837673205509",
			column:  "message",
			table:   "logs.inverted_index_logs_2p_gate",
			tsRange: "(0 < ts) AND (ts <= 1542894389183470000)",
			result:  "SELECT ts FROM logs.logs.inverted_index_logs_2p_gate WHERE (word_hash IN (cityHash64('1197'),cityHash64('62'),cityHash64('1542894388'),cityHash64('2117811957304647739'),cityHash64('7567544320748365149'),cityHash64('4913069837673205509')) AND column_hash = cityHash64('message')) AND ((0 < ts) AND (ts <= 1542894389183470000)) GROUP BY ts HAVING uniq(word_hash) = 6 ORDER BY ts DESC",
		},
		{
			text:    "/data/pmx/vendor/eco/connection-manager/src/DB.php",
			column:  "file",
			table:   "logs.inverted_index_logs_2p",
			tsRange: "(0 < ts) AND (ts <= 1542894389181529000)",
			result:  "SELECT ts FROM logs.logs.inverted_index_logs_2p WHERE (word_hash IN (cityHash64('connection'),cityHash64('manager'),cityHash64('db')) AND column_hash = cityHash64('file')) AND ((0 < ts) AND (ts <= 1542894389181529000)) GROUP BY ts HAVING uniq(word_hash) = 3 ORDER BY ts DESC",
		},
	}

	for _, test := range testData {
		request := createInvertedIndexRequest(GetTokens(test.text), test.column, test.table).WhereAnd(test.tsRange).Build()
		if strings.TrimSpace(request) != test.result {
			t.Error("\n error: ",
				"\n expected: ", test.result,
				"\n get: ", request)
		}
	}
}

func TestPrepareEnvironment(t *testing.T) {
	inserts := []struct {
		uuid string
		text string
		ts   uint64
	}{
		{
			uuid: "b276316b-9c2a-4cde-afe5-11da972ffe07",
			text: "Worker callback_0 ends handling of message #1197-62-1542894388|2117811957304647739:7567544320748365149:4913069837673205509:1",
			ts:   1542894389184806000,
		},
		{
			uuid: "4ef64f23-5c91-4179-a14d-f11e1ef9aef3",
			text: "[localhost / writer] COMMIT TRANSACTION",
			ts:   1542894389183470000,
		},
		{
			uuid: "0f4b6b53-e8b5-45e7-8fd8-65991409e3b8",
			text: "[localhost / writer] SQL : UPDATE Shard00.uniques00 SET status = 'committed', updated_at = '2018-11-22 13:46:29' WHERE request_id = 612 and unique_id = '582764f803e38939bad81591b8a2225245ca69bc3cfec14bbc99dbf46255e7fe32f9b537d4b7288dc4b4433928208008fa3109baaca24dfe8ef9c6fc05b7fcdf-1542894388.2593' LIMIT 1",
			ts:   1542894389181529000,
		},
	}

	for i := range inserts {
		tokens := GetTokens(inserts[i].text)
		for _, token := range tokens {
			insertion := fmt.Sprintf(
				"INSERT into logs.inverted_index_logs_2p_gate (word_hash, uuid, ts, column_hash) VALUES (cityHash64('%s'), '%s', %d, cityHash64('%s'))",
				token,
				inserts[i].uuid,
				inserts[i].ts,
				"message",
			)
			println(insertion)
		}
	}
}
