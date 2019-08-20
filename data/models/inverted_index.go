package models

import "time"

const InvertedIndexTablePrefix = "inverted_index_"

// InvertedIndex declares inverted index data item structure.
type InvertedIndex struct {
	Day    time.Time `db:"day" type:"Date" partitioning:"true"`
	TS     uint64    `db:"ts" type:"UInt64" timestamp:"true" ch_index_pos:"3"`
	Hash   uint64    `db:"word_hash" type:"UInt64" ch_index_pos:"1"`
	Column uint64    `db:"column_hash" type:"UInt64" ch_index_pos:"2"`
}

// IndexingQueueItem declares structure of inverted index queue item.
type IndexingQueueItem struct {
	Word   string `db:"word" type:"String"`
	TS     uint64 `db:"ts" type:"UInt64"`
	Column string `db:"column" type:"String"`
}
