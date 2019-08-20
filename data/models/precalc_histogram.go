package models

import "time"

const PreparedHistogramDataTablePrefix = "histogram_"

// HistogramPreCalcTable defines table for storing prepared results for count histogram aggregation.
type HistogramPreCalcTable struct {
	Day   time.Time `db:"day" type:"Date" partitioning:"true"`
	Count int64     `db:"count" type:"UInt64"`
	Key   int64     `db:"key" type:"Int64" ch_index_pos:"1"`
}
