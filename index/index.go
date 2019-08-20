package index

import (
	"fmt"
	"regexp"
	"strings"

	"kibouse/data/models"
	"kibouse/db"
)

const (
	tokenDelimiter = `\W+`
)

// gateLogsWordsToOmits contains most frequent words,
// these words must be omitted in search request as well!
var gateLogsWordsToOmits = map[string]struct{}{
	"":       {},
	" ":      {},
	"data":   {},
	"php":    {},
	"src":    {},
	"logs":   {},
	"pmx":    {},
	"Eco":    {},
	"eco":    {},
	"vendor": {},
}

// GetTokens is used for getting search units from analyzed text.
func GetTokens(text string) []string {
	tokens := regexp.MustCompile(tokenDelimiter).Split(strings.ToLower(text), -1)

	result := make([]string, 0, len(tokens))
	uniqWords := make(map[string]int)
	for i := range tokens {
		if _, toOmit := gateLogsWordsToOmits[tokens[i]]; toOmit {
			continue
		}
		if uniqWords[tokens[i]]++; uniqWords[tokens[i]] > 1 {
			continue
		}
		result = append(result, tokens[i])
	}

	return result
}

func generateWhere(tokens []string, column string) string {
	if len(tokens) == 0 {
		return ""
	}
	hashedTokens := make([]string, len(tokens))
	for i := range tokens {
		hashedTokens[i] = fmt.Sprintf("cityHash64('%s')", tokens[i])
	}
	cond := strings.Builder{}
	cond.WriteString(
		fmt.Sprintf(
			"word_hash IN (%s) AND column_hash = cityHash64('%s')",
			strings.Join(hashedTokens, ","),
			column,
		),
	)

	return fmt.Sprintf("(%s)", cond.String())
}

// GetInvertedIndexTableName generates inverted index table name for data table by its name.
func GetInvertedIndexTableName(dataTable string) string {
	return models.InvertedIndexTablePrefix + dataTable
}

// createInvertedIndexRequest creates request for fetching log timestamps from inverted index.
func createInvertedIndexRequest(tokens []string, column string, invertedIndexTable string) *db.Request {
	request := db.NewRequest(db.DataBaseName+"."+invertedIndexTable, "ts")
	request.Where(generateWhere(tokens, column))
	request.GroupBy("ts")
	request.Having(fmt.Sprintf("uniq(word_hash) = %d", len(tokens)))
	request.OrderBy("ts", db.DESC)
	return request
}

// createAdditionalFilters generate conditions for filtering log entries by its content.
func createAdditionalFilters(tokens []string, column string) string {
	conds := make([]string, len(tokens))
	for i := 0; i < len(tokens); i++ {
		conds[i] = fmt.Sprintf("(positionCaseInsensitive(%s, '%s') != 0)", column, tokens[i])
	}
	return strings.Join(conds, " AND ")
}

// CreateFullTextSearchConditions returns conditions required for full text searching.
func CreateFullTextSearchConditions(searchedText string, column string, invertedIndexTable string) (*db.Request, string) {
	tokens := GetTokens(searchedText)
	return createInvertedIndexRequest(tokens, column, invertedIndexTable),
		createAdditionalFilters(tokens, column)
}
