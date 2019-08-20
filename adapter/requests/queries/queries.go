package queries

import (
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	"kibouse/data/models"
	"kibouse/index"
)

type Order string

const (
	Asc  Order = "ASC"
	Desc Order = "DESC"
)

// Clause represents condition in elasticsearch terms
// used for generating equal SQL code.
type Clause interface {
	String() string
}

// Section represents multiple similar conditions in elasticsearch terms.
// used for generating equal SQL code
type Section interface {
	Clause
	AppendChild(Clause) Section
	Children() []Clause
}

type composite struct {
	children []Clause
}

// String added for implementing Section interface.
func (c *composite) String() string {
	return ""
}

// AppendChild adds new child clause to the composite clause.
func (c *composite) AppendChild(child Clause) Section {
	if child == nil {
		return nil
	}
	c.children = append(c.children, child)
	return c
}

// Children returns child clauses.
func (c *composite) Children() []Clause {
	return c.children
}

func (c *composite) buildChildQueries() []string {
	queries := make([]string, 0, len(c.children))
	for _, val := range c.children {
		if childQuery := val.String(); childQuery != "" {
			queries = append(queries, childQuery)
		}
	}

	return queries
}

func (c *composite) buildChildQueriesStr(glue string, format string) string {
	if queries := strings.Join(c.buildChildQueries(), glue); queries != "" {
		return fmt.Sprintf(format, queries)
	}
	return ""
}

func NewSortSection(fields map[string]Order) *SortSection {
	sorting := SortSection{}
	for name, order := range fields {
		sorting.AppendChild(&SortClause{Field: name, Sorting: string(order)})
	}
	return &sorting
}

// SortSection represents whole sort object from elasticsearch JSON.
type SortSection struct {
	composite
}

func (sc *SortSection) String() string {
	return sc.composite.buildChildQueriesStr(",", "%s")
}

// SortClause represents sort settings for single field.
type SortClause struct {
	Field   string
	Sorting string
}

func (sc *SortClause) String() string {
	return fmt.Sprintf("%s %s", sc.Field, strings.ToUpper(sc.Sorting))
}

// BoolSection represents elastic bool query.
type BoolSection struct {
	Must    Section
	MustNot Section
	Should  Section
	Filter  Section
}

func (qbs *BoolSection) String() string {
	query := make([]string, 0, 3)
	if must := qbs.Must.String(); must != "" {
		query = append(query, must)
	}
	if mustNot := qbs.MustNot.String(); mustNot != "" {
		query = append(query, mustNot)
	}
	if filter := qbs.Filter.String(); filter != "" {
		query = append(query, filter)
	}
	if len(query) == 0 {
		query = append(query, qbs.Should.String())
	}
	return strings.Join(query, " AND ")
}

// NewEmptyMustSection created for testing purposes.
func NewEmptyMustSection() *MustSection {
	return &MustSection{}
}

// MustSection represents list of must occurrences from bool clause.
type MustSection struct {
	composite
}

func (qms *MustSection) String() string {
	return qms.composite.buildChildQueriesStr(" AND ", "(%s)")
}

// NewEmptyMustNotSection created for testing purposes.
func NewEmptyMustNotSection() *MustNotSection {
	return &MustNotSection{}
}

// MustNotSection represents list of must not occurrences from bool clause.
type MustNotSection struct {
	composite
}

func (qmns *MustNotSection) String() string {
	return qmns.composite.buildChildQueriesStr(" AND NOT", "( NOT %s)")
}

// NewEmptyShouldSection created for testing purposes.
func NewEmptyShouldSectionn() *ShouldSection {
	return &ShouldSection{}
}

// ShouldSection represents list of should occurrences from bool clause.
type ShouldSection struct {
	composite
}

func (qhs *ShouldSection) String() string {
	return qhs.composite.buildChildQueriesStr(" OR ", "(%s)")
}

// FilterSection represents list of filter occurrences from bool clause.
type FilterSection struct {
	composite
}

func (qfs *FilterSection) String() string {
	return qfs.composite.buildChildQueriesStr(" AND ", "(%s)")
}

// NewMatchClause creates new representation of elastic match_phrase clause.
func NewMatchClause(fieldInfo models.CHField, value interface{}) *MatchClause {
	return &MatchClause{
		Field: fieldInfo,
		Value: value,
	}
}

// NewStringMatch creates new representation of elastic match_phrase clause for string field.
func NewStringMatch(field string, value string) *MatchClause {
	return &MatchClause{
		Field: models.CHField{
			CHName: field,
			CHType: "String",
		}, Value: value,
	}
}

// MatchClause represents elastic match_phrase clause.
type MatchClause struct {
	Field models.CHField
	Value interface{}
}

func (mc *MatchClause) String() string {
	arrayField := mc.Field.IsArray()
	if mc.Field.IsString() {
		if arrayField {
			return fmt.Sprintf("(has(%s, '%v'))", mc.Field, mc.Value)
		}
		return fmt.Sprintf("(%s = '%v')", mc.Field.CHName, mc.Value)
	}

	if mc.Field.IsNumeric() {
		if arrayField {
			return fmt.Sprintf("(has(%s, %v))", mc.Field.CHName, mc.Value)
		}
		return fmt.Sprintf("(%s = %v)", mc.Field.CHName, mc.Value)
	}

	log.Warn("required value in 'match_phrase' clause has incorrect type")
	return ""
}

type threshold struct {
	value  float64
	strict bool
}

// RangeClause represents elastic range clause.
type RangeClause struct {
	field    string
	low      threshold
	high     threshold
	format   string
	ArrayVal bool
}

// NewRange creates new representation of elastic range clause.
func NewRange(name string, isArrayVal bool) *RangeClause {
	return &RangeClause{
		field:    name,
		low:      threshold{value: 0, strict: false},
		high:     threshold{value: 0, strict: false},
		format:   "",
		ArrayVal: isArrayVal,
	}
}

// AddLower sets lower boundary of data range.
func (rc *RangeClause) AddLower(value float64, strict bool) *RangeClause {
	rc.low = threshold{value: value, strict: strict}
	return rc
}

// GetLower returns lower boundary of data range.
func (rc *RangeClause) GetLower() (float64, bool) {
	return rc.low.value, rc.low.strict
}

// AddUpper sets upper boundary of data range.
func (rc *RangeClause) AddUpper(value float64, strict bool) *RangeClause {
	rc.high = threshold{value: value, strict: strict}
	return rc
}

// GetUpper returns upper boundary of data range.
func (rc *RangeClause) GetUpper() (float64, bool) {
	return rc.high.value, rc.high.strict
}

// AddFormat sets elastic data format, like epoch_millis, etc.
func (rc *RangeClause) AddFormat(format string) *RangeClause {
	rc.format = format
	return rc
}

// GetFormat returns elastic data format.
func (rc *RangeClause) GetFormat() string {
	return rc.format
}

// GetField returns field name.
func (rc *RangeClause) GetField() string {
	return rc.field
}

func (rc *RangeClause) buildLow() string {
	if rc.low.strict {
		return fmt.Sprintf("%v < %s", rc.low.value, rc.field)
	}
	return fmt.Sprintf("%v <= %s", rc.low.value, rc.field)
}

func (rc *RangeClause) buildHigh() string {
	if rc.high.strict {
		return fmt.Sprintf("%s < %v", rc.field, rc.high.value)
	}
	return fmt.Sprintf("%s <= %v", rc.field, rc.high.value)
}

func (rc *RangeClause) String() string {
	if !rc.ArrayVal && rc.field != "" {
		return fmt.Sprintf("(%s AND %s)", rc.buildLow(), rc.buildHigh())
	}
	return ""
}

// ExistsClause represents elastic exists query.
type ExistsClause struct {
	Field string
}

func (rc *ExistsClause) String() string {
	return fmt.Sprintf("(isNotNull(%s))", rc.Field)
}

// EmptySection represents empty multiple clauses JSON object.
type EmptySection struct {
	composite
}

func (uc *EmptySection) String() string {
	return ""
}

// UnknownClause stubs processing for all unknown elastic clauses.
type UnknownClause struct {
}

func (uc *UnknownClause) String() string {
	return ""
}

// NewTermsClause creates new elastic terms query.
func NewTermsClause() *TermsClause {
	clause := TermsClause{}
	clause.terms = make(map[string][]string)
	return &clause
}

// TermsClause represents elastic terms query.
type TermsClause struct {
	terms map[string][]string
}

// AddTerm appends new term to query.
func (tc *TermsClause) AddTerm(field string, value string) {
	if _, ok := tc.terms[field]; !ok {
		tc.terms[field] = make([]string, 0)
	}
	tc.terms[field] = append(tc.terms[field], value)
}

func (tc *TermsClause) String() string {
	conds := make([]string, 0, len(tc.terms))
	for field := range tc.terms {
		termConds := make([]string, len(tc.terms[field]))
		for j := range tc.terms[field] {
			termConds[j] = fmt.Sprintf("(%s = '%s')", field, tc.terms[field][j])
		}
		conds = append(conds, "("+strings.Join(termConds, " OR ")+")")
	}
	return strings.Join(conds, " AND ")
}

type queryItem interface {
	toString(bool, RangeClause) string
}

type simpleItem struct {
	content string
}

func (si *simpleItem) toString(wildCards bool, r RangeClause) string {
	return si.content
}

type fieldMatch struct {
	logicalOp  string
	field      models.CHField
	expr       string
	indexTable string
	tsColumn   string
}

func (fm *fieldMatch) toString(wildCards bool, r RangeClause) string {
	format := "%s (position(%s, '%s') != 0)"
	match := strings.Trim(fm.expr, "\"")

	// column has been indexed
	if fm.indexTable != "" && fm.tsColumn != "" {
		// Request to inverted index returns timestamps of required log entries,
		// after that we should remove all inappropriate logs with the same time
		// using additional filtering conditions
		invIndexRequest, filters := index.CreateFullTextSearchConditions(fm.expr, fm.field.CHName, fm.indexTable)
		// add time range for search optimization
		invIndexRequest.WhereAnd(r.String())
		return fmt.Sprintf("%s (%s IN (%s) AND %s)", fm.logicalOp, fm.tsColumn, invIndexRequest.Build(), filters)
	}

	if _, err := strconv.ParseFloat(fm.expr, 64); err == nil && fm.field.IsNumeric() {
		format = "%s (%s = %s)"
	} else if wildCards && strings.ContainsAny(match, "? & *") {
		match = strings.Replace(match, "*", "%", -1)
		match = strings.Replace(match, "?", "_", -1)
		match = "%" + match + "%"
		format = "%s like(%s, '%s')"
	}

	return fmt.Sprintf(format, fm.logicalOp, fm.field.CHName, match)
}

// MatchQueryClause represents elastic expr query.
type MatchQueryClause struct {
	analyzeWildCard bool
	timeRange       RangeClause
	items           []queryItem
}

func (uc *MatchQueryClause) String() string {
	query := ""
	for _, item := range uc.items {
		query += item.toString(uc.analyzeWildCard, uc.timeRange)
	}
	return query
}

// SetTimeRange uses for adding time range for requests to inverted index.
func (uc *MatchQueryClause) SetTimeRange(r RangeClause) {
	uc.timeRange = r
}

// NewMatchQueryClause returns new representation of elastic expr query.
func NewMatchQueryClause(query string, analyzeWildCards bool, tableInfo *models.ModelInfo) *MatchQueryClause {
	items := parseMatchQuery(query, tableInfo)
	if items == nil {
		return nil
	}
	return &MatchQueryClause{
		items:           items,
		analyzeWildCard: analyzeWildCards,
	}
}

func parseMatchQuery(query string, tableInfo *models.ModelInfo) []queryItem {
	parts := splitQuery(query)
	if len(parts) == 1 && tableInfo != nil {
		return parseShortNotation(parts, tableInfo)
	}
	return parseWideNotation(parts, tableInfo)
}

func createAllIndexedColsSearchQuery(tableInfo *models.ModelInfo, expr string, op string) []queryItem {
	logicalOp := op
	items := make([]queryItem, 0)
	tsColumn, tsOk := tableInfo.GetTimestampField()
	for name := range tableInfo.DataFields {

		if tableInfo.DataFields[name].FullTextSearch && tsOk {
			items = append(items, &fieldMatch{
				field:      tableInfo.DataFields[name].CHField,
				expr:       expr,
				logicalOp:  logicalOp,
				indexTable: index.GetInvertedIndexTableName(tableInfo.DBName),
				tsColumn:   tsColumn.CHName,
			})
			logicalOp = "OR"
		}
	}

	return items
}

func parseShortNotation(parts []string, tableInfo *models.ModelInfo) []queryItem {
	value := strings.Trim(parts[0], `"`)
	if value == "*" {
		return []queryItem{&simpleItem{}}
	}
	items := createAllIndexedColsSearchQuery(tableInfo, value, "")
	return items
}

func parseWideNotation(parts []string, tableInfo *models.ModelInfo) []queryItem {
	items := make([]queryItem, 0)
	prev := ""
	currField := ""
	openedBraces := 0
	currLogicalOp := ""
	currMatch := &fieldMatch{}
	tsField, tsOk := tableInfo.GetTimestampField()
	for i := range parts {
		if kind := getBraceKind(parts[i]); kind != notBrace {
			if openedBraces > 0 || prev == ":" {
				openedBraces += int(kind)
			}
			if currLogicalOp != "" {
				items = append(items, &simpleItem{content: currLogicalOp})
				currLogicalOp = ""
			}
			items = append(items, &simpleItem{content: parts[i]})
		} else if isLogicalOp(parts[i]) {
			currLogicalOp += " " + parts[i]
		} else if parts[i] == ":" {
			currField = prev
		} else if prev == ":" || (currField != "" && openedBraces > 0) {
			currMatch.field.CHName = currField
			currMatch.expr = parts[i]
			currMatch.logicalOp = currLogicalOp
			// incorrect field name.
			if _, ok := tableInfo.DataFields[currField]; !ok {
				return nil
			} else if tableInfo.DataFields[currField].FullTextSearch && tsOk {
				currMatch.indexTable = index.GetInvertedIndexTableName(tableInfo.DBName)
				currMatch.tsColumn = tsField.CHName
			}

			currMatch.field.CHType = tableInfo.DataFields[currField].CHType
			items = append(items, currMatch)

			currMatch = &fieldMatch{}
			currLogicalOp = ""
			if openedBraces == 0 {
				currField = ""
			}
		}
		prev = parts[i]
	}

	return items
}

func splitQuery(query string) []string {
	query = strings.TrimSpace(query)
	// query doesn't contain field names, full text search in all indexed fields should be used.
	if !strings.Contains(query, ":") {
		return []string{query}
	}
	parts := make([]string, 0, len(query))
	// beginning of unprocessed part of adapter
	rest := 0
	quoted := false
	for i := 0; i < len(query); i++ {
		if query[i] == '"' {
			quoted = !quoted
		}
		// do not process data between ""
		if quoted {
			continue
		}
		if query[i] == ' ' || query[i] == '(' || query[i] == ')' || query[i] == ':' {
			if rest < i {
				parts = append(parts, query[rest:i])
			}
			rest = i + 1
			if query[i] != ' ' {
				parts = append(parts, string(query[i]))
			}
		}
	}
	if rest < len(query) {
		parts = append(parts, string(query[rest:]))
	}
	return parts
}

// GetSimpleClausesList returns all simple clauses from complex section.
func GetSimpleClausesList(cond Clause) []Clause {
	if boolSection, ok := cond.(*BoolSection); ok {
		children := make([]Clause, 0)
		children = append(children, boolSection.Must.Children()...)
		children = append(children, boolSection.MustNot.Children()...)
		children = append(children, boolSection.Filter.Children()...)
		children = append(children, boolSection.Should.Children()...)
		return children
	}
	if section, ok := cond.(Section); ok {
		return section.Children()
	}
	return []Clause{cond}
}

type braceKind int

const notBrace = 0
const openingBrace = 1
const closingBrace = -1

func getBraceKind(s string) braceKind {
	switch s {
	case "(":
		return openingBrace
	case ")":
		return closingBrace
	default:
		return notBrace
	}
}

func isLogicalOp(s string) bool {
	s = strings.ToLower(s)
	if s == "and" || s == "or" || s == "xor" || s == "not" {
		return true
	}
	return false
}

// IsEqual compares two Clause instances for testing purposes.
func IsEqual(first Clause, second Clause) bool {
	firstSection, firstOk := first.(Section)
	secondSection, secondOk := second.(Section)
	if firstOk && secondOk {
		return compareSection(firstSection, secondSection)
	}
	if !(firstOk || secondOk) {
		firstBoolSection, firstBoolOk := first.(*BoolSection)
		secondBoolSection, secondBoolOk := second.(*BoolSection)

		if firstBoolOk && secondBoolOk {
			return compareBoolSection(firstBoolSection, secondBoolSection)
		}
		if !(firstBoolOk || secondBoolOk) {
			return compareClause(first, second)
		}
	}

	return false
}

func compareBoolSection(first *BoolSection, second *BoolSection) bool {
	return compareSection(first.Must, second.Must) &&
		compareSection(first.Should, second.Should) &&
		compareSection(first.MustNot, second.MustNot) &&
		compareSection(first.Filter, second.Filter)
}

func compareClause(first Clause, second Clause) bool {
	if first == nil && second == nil {
		return true
	}

	return first != nil &&
		second != nil &&
		strings.ToLower(first.String()) == strings.ToLower(second.String())
}

func compareSection(first Section, second Section) bool {
	if (first == nil || len(first.Children()) == 0) && (second == nil || len(second.Children()) == 0) {
		return true
	}

	if len(first.Children()) != len(second.Children()) {
		return false
	}

	var equal bool
	for _, fChild := range first.Children() {
		for _, sChild := range second.Children() {
			if equal = IsEqual(fChild, sChild); equal {
				break
			}
		}
		if !equal {
			return false
		}
	}

	return true
}