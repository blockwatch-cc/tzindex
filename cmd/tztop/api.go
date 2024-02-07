// Copyright (c) 2024 Blockwatch Data Inc.
// Authors: abdul@blockwatch.cc, alex@blockwatch.cc
package main

import (
	"fmt"
	"strconv"
	"time"
)

func (c *Client) GetTip() (Tip, error) {
	var tip Tip
	err := c.get("/explorer/tip", &tip)
	if err != nil {
		return tip, err
	}
	return tip, nil
}

type Tip struct {
	Name      string    `json:"name"`
	Network   string    `json:"network"`
	ChainId   string    `json:"chain_id"`
	Timestamp time.Time `json:"timestamp"`
	Height    int64     `json:"height"`
	Health    int       `json:"health"`
	Status    Status    `json:"status"`
}

type Status struct {
	Mode     string  `json:"mode"`
	Status   string  `json:"status"`
	Blocks   int64   `json:"blocks"`
	Indexed  int64   `json:"indexed"`
	Progress float64 `json:"progress"`
}

func (c *Client) GetTableStats() (TableResponse, error) {
	table := make(TableResponse, 0)
	err := c.get("/system/tables", &table)
	if err != nil {
		return nil, err
	}
	return table, nil
}

type TableResponse []TableStats

func (r TableResponse) Find(n string) TableStats {
	for _, v := range r {
		if v.GetName() == n {
			return v
		}
	}
	return TableStats{}
}

func (r TableResponse) NumTables() int {
	var n int
	for _, v := range r {
		if !v.IsIndex() {
			n++
		}
	}
	return n
}

func (r TableResponse) NumIndexes() int {
	var n int
	for _, v := range r {
		if v.IsIndex() {
			n++
		}
	}
	return n
}

type TableStats struct {
	// global statistics
	TableName         string        `json:"table_name,omitempty"`
	IndexName         string        `json:"index_name,omitempty"`
	LastFlushTime     time.Time     `json:"last_flush_time"`
	LastFlushDuration time.Duration `json:"last_flush_duration"`

	// tuple statistics
	TupleCount     int64 `json:"tuples_count"`
	InsertedTuples int64 `json:"tuples_inserted"`
	UpdatedTuples  int64 `json:"tuples_updated"`
	DeletedTuples  int64 `json:"tuples_deleted"`
	FlushedTuples  int64 `json:"tuples_flushed"`
	QueriedTuples  int64 `json:"tuples_queried"`
	StreamedTuples int64 `json:"tuples_streamed"`

	// call statistics
	InsertCalls int64 `json:"calls_insert"`
	UpdateCalls int64 `json:"calls_update"`
	DeleteCalls int64 `json:"calls_delete"`
	FlushCalls  int64 `json:"calls_flush"`
	QueryCalls  int64 `json:"calls_query"`
	StreamCalls int64 `json:"calls_stream"`

	// metadata statistics
	MetaBytesRead    int64 `json:"meta_bytes_read"`
	MetaBytesWritten int64 `json:"meta_bytes_written"`
	MetaSize         int64 `json:"meta_size"`

	// journal statistics
	JournalSize            int64 `json:"journal_size"`
	JournalDiskSize        int64 `json:"journal_disk_size"`
	JournalTuplesCount     int64 `json:"journal_tuples_count"`
	JournalTuplesThreshold int64 `json:"journal_tuples_threshold"`
	JournalTuplesCapacity  int64 `json:"journal_tuples_capacity"`
	JournalPacksStored     int64 `json:"journal_packs_stored"`
	JournalTuplesFlushed   int64 `json:"journal_tuples_flushed"`
	JournalBytesWritten    int64 `json:"journal_bytes_written"`

	// tombstone statistics
	TombstoneSize            int64 `json:"tombstone_size"`
	TombstoneDiskSize        int64 `json:"tombstone_disk_size"`
	TombstoneTuplesCount     int64 `json:"tomb_tuples_count"`
	TombstoneTuplesThreshold int64 `json:"tomb_tuples_threshold"`
	TombstoneTuplesCapacity  int64 `json:"tomb_tuples_capacity"`
	TombstonePacksStored     int64 `json:"tomb_packs_stored"`
	TombstoneTuplesFlushed   int64 `json:"tomb_tuples_flushed"`
	TombstoneBytesWritten    int64 `json:"tomb_bytes_written"`

	// pack statistics
	PacksCount        int64 `json:"packs_count"`
	PacksAlloc        int64 `json:"packs_alloc"`
	PacksRecycled     int64 `json:"packs_recycled"`
	PacksLoaded       int64 `json:"packs_loaded"`
	PacksStored       int64 `json:"packs_stored"`
	PacksBytesRead    int64 `json:"packs_bytes_read"`
	PacksBytesWritten int64 `json:"packs_bytes_written"`
	PacksSize         int64 `json:"packs_size"`

	// pack cache statistics
	PackCacheSize      int64 `json:"pack_cache_size"`
	PackCacheCount     int64 `json:"pack_cache_count"`
	PackCacheCapacity  int64 `json:"pack_cache_capacity"`
	PackCacheHits      int64 `json:"pack_cache_hits"`
	PackCacheMisses    int64 `json:"pack_cache_misses"`
	PackCacheInserts   int64 `json:"pack_cache_inserts"`
	PackCacheUpdates   int64 `json:"pack_cache_updates"`
	PackCacheEvictions int64 `json:"pack_cache_evictions"`
}

func (t TableStats) GetCached() string {
	return fmt.Sprintf("%s / %s",
		FormatBytes(int(t.PackCacheSize)),
		FormatBytes(int(t.PackCacheCapacity)),
	)
}

func (t TableStats) GetType() string {
	if t.IsIndex() {
		return "index"
	}
	return "table"
}

func (t TableStats) GetName() string {
	n := t.TableName
	if n == "" {
		n = t.IndexName
	}
	return n
}

func (t TableStats) IsIndex() bool {
	return t.IndexName != ""
}

func (t TableStats) GetJournal() string {
	return fmt.Sprintf("%s / %s",
		FormatPretty(t.JournalTuplesCount),
		FormatPretty(t.JournalTuplesCapacity),
	)
}

func (t TableStats) GetDiskSize() string {
	return FormatBytes(int(t.JournalDiskSize + t.TombstoneDiskSize + t.PacksSize))
}

func (t TableStats) GetCacheHitRate(p TableStats, tm time.Time) string {
	num := t.PackCacheHits
	denom := t.PackCacheHits + t.PackCacheMisses
	if denom == 0 {
		denom = 1
	}
	total := strconv.FormatFloat(float64(num*100)/float64(denom), 'f', 2, 64) + "%"
	now := "-- %"
	if !tm.IsZero() {
		num = t.PackCacheHits - p.PackCacheHits
		denom = t.PackCacheHits + t.PackCacheMisses - p.PackCacheHits - p.PackCacheMisses
		if denom == 0 {
			denom = 1
		}
		now = strconv.FormatFloat(float64(num*100)/float64(denom), 'f', 2, 64) + "%"
	}
	return now + " / " + total
}

func (t TableStats) GetIO(p TableStats, tm time.Time) string {
	if tm.IsZero() {
		return "-- / --"
	}
	readDiff := t.PacksBytesRead - p.PacksBytesRead
	writeDiff := t.PacksBytesWritten + t.JournalBytesWritten + t.TombstoneBytesWritten
	writeDiff -= p.PacksBytesWritten + p.JournalBytesWritten + p.TombstoneBytesWritten
	tmDiff := float64(time.Since(tm)) / float64(time.Second)
	readRate := float64(readDiff) / tmDiff
	writeRate := float64(writeDiff) / tmDiff
	return FormatBytes(int(readRate)) + " / " + FormatBytes(int(writeRate))
}

func (t TableStats) GetThroughput(p TableStats, tm time.Time) string {
	if tm.IsZero() {
		return "-- qps / -- tps"
	}

	queryDiff := t.QueryCalls + t.StreamCalls + t.InsertCalls + t.UpdateCalls + t.DeleteCalls
	queryDiff -= p.QueryCalls + p.StreamCalls + p.InsertCalls + p.UpdateCalls + p.DeleteCalls
	tupleDiff := t.QueriedTuples + t.StreamedTuples + t.InsertedTuples + t.UpdatedTuples + t.DeletedTuples
	tupleDiff -= p.QueriedTuples + p.StreamedTuples + p.InsertedTuples + p.UpdatedTuples + p.DeletedTuples
	tmDiff := float64(time.Since(tm)) / float64(time.Second)
	queryRate := float64(queryDiff) / tmDiff
	tupleRate := float64(tupleDiff) / tmDiff
	return fmt.Sprintf("%3.1f qps / %3.1f tps", queryRate, tupleRate)
}

func (c *Client) GetSysStats() (SysStat, error) {
	var sys SysStat
	err := c.get("/system/sysstat", &sys)
	return sys, err
}

type SysStat struct {
	Hostname      string    `json:"hostname"`
	ContainerName string    `json:"container_name"`
	Timestamp     time.Time `json:"timestamp"`

	NumCpu       int    `json:"num_cpu"`
	NumGoroutine int    `json:"num_goroutine"`
	NumThreads   uint64 `json:"num_threads"`
	TotalMem     uint64 `json:"total_mem"`
	TotalSwap    uint64 `json:"total_swap"`

	VmPageFaults uint64 `json:"vm_page_faults"`
	VmPeak       uint64 `json:"vm_peak"` // Peak virtual memory size.
	VmSize       uint64 `json:"vm_size"` // Virtual memory size.
	VmSwap       uint64 `json:"vm_swap"` // Swapped-out virtual memory size
	VmRss        uint64 `json:"vm_rss"`  // Resident set size. (mapped + anon + shm)
	VmMapped     uint64 `json:"vm_map"`  // Size of resident file mappings.
	VmAnon       uint64 `json:"vm_anon"` // Size of resident anonymous memory.
	VmShm        uint64 `json:"vm_shm"`  // Size of resident shared memory.

	MemMallocs    uint64 `json:"mem_mallocs"`
	MemFrees      uint64 `json:"mem_frees"`
	MemHeapAlloc  uint64 `json:"mem_heap"`
	MemStackInuse uint64 `json:"mem_stack"`

	DiskSize uint64 `json:"disk_size"`
	DiskUsed uint64 `json:"disk_used"`
	DiskFree uint64 `json:"disk_free"`

	CpuUser  float64 `json:"cpu_user"`
	CpuSys   float64 `json:"cpu_system"`
	CpuTotal float64 `json:"cpu_total"`
}
