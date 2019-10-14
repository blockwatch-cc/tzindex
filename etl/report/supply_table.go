// Copyright (c) 2018 KIDTSUNAMI
// Author: alex@kidtsunami.com

package report

import (
	"context"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/etl"
)

var (
	supplyReportTableKey = "supply_stats"
)

func (r *SupplyReport) InitTable(db *pack.DB) error {
	opts := pack.Options{
		PackSizeLog2:    etl.ReportTablePackSizeLog2,
		JournalSizeLog2: etl.ReportTableJournalSizeLog2,
		CacheSize:       etl.ReportTableCacheSize,
		FillLevel:       etl.ReportTableFillLevel,
	}
	fields, err := pack.Fields(SupplyStats{})
	if err != nil {
		return err
	}
	r.table, err = db.CreateTableIfNotExists(supplyReportTableKey, fields, opts)
	if err != nil {
		return err
	}
	return nil
}

func (r *SupplyReport) Tables() []*pack.Table {
	return []*pack.Table{r.table}
}

func (r *SupplyReport) CloseTable() error {
	if r.table != nil {
		if err := r.table.Close(); err != nil {
			log.Errorf("Closing %s table: %v", supplyReportTableKey, err)
		}
	}
	r.table = nil
	return nil
}

func (r *SupplyReport) Flush(ctx context.Context) error {
	if r.table != nil {
		if err := r.table.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", supplyReportTableKey, err)
		}
	}
	return nil
}

func (r *SupplyReport) FlushJournal(ctx context.Context) error {
	if r.table != nil {
		if err := r.table.FlushJournal(ctx); err != nil {
			log.Errorf("Flushing %s journal: %v", supplyReportTableKey, err)
		}
	}
	return nil
}

func (r *SupplyReport) LoadStats(ctx context.Context, at time.Time, height int64) (*SupplyStats, error) {
	s := &SupplyStats{}
	q := pack.Query{
		Name: "supply_stats.lookup",
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: r.table.Fields().Find("h"), // height
				Mode:  pack.FilterModeEqual,
				Value: height,
			},
		},
	}
	found := false
	err := r.table.Stream(ctx, q, func(row pack.Row) error {
		if err := row.Decode(s); err != nil {
			return err
		}
		found = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrReportNotFound
	}
	return s, nil
}

func (r *SupplyReport) StoreStats(ctx context.Context, stats *SupplyStats) error {
	stats.RowId = 0
	return r.table.Insert(ctx, stats)
}

func (r *SupplyReport) UpdateStats(ctx context.Context, stats *SupplyStats) error {
	if err := r.DeleteStats(ctx, stats.Height, stats.Timestamp); err != nil {
		return err
	}
	if err := r.table.Flush(ctx); err != nil {
		return err
	}
	return r.StoreStats(ctx, stats)
}

func (r *SupplyReport) DeleteStats(ctx context.Context, height int64, at time.Time) error {
	q := pack.Query{
		Name: "supply_stats.lookup",
	}
	if !at.IsZero() {
		q.Conditions = pack.ConditionList{
			pack.Condition{
				Field: r.table.Fields().Find("T"), // time
				Mode:  pack.FilterModeEqual,
				Value: at,
			},
		}
	} else {
		q.Conditions = pack.ConditionList{
			pack.Condition{
				Field: r.table.Fields().Find("h"), // height
				Mode:  pack.FilterModeEqual,
				Value: height,
			},
		}
	}
	_, err := r.table.Delete(ctx, q)
	return err
}
