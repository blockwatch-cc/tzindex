// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package report

import (
	"context"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl"
)

var (
	opReportTableKey = "op_stats"
)

type OpStatsRow struct {
	RowId     uint64    `pack:"I,pk,snappy"`
	Timestamp time.Time `pack:"T,snappy"`
	Height    int64     `pack:"h,snappy"`
	Name      string    `pack:"n,snappy"` // fee, fee_rate, size, n_vin, n_vout, vol, cdd, add
	Type      int       `pack:"t,snappy"` // operation type
	N         int       `pack:"N,snappy"`
	Sum       float64   `pack:"s,snappy"`
	Min       float64   `pack:"m,snappy"`
	Max       float64   `pack:"M,snappy"`
	Mean      float64   `pack:"e,snappy"`
	Median    float64   `pack:"d,snappy"`
}

var _ pack.Item = (*OpStatsRow)(nil)

func (r OpStatsRow) ID() uint64 {
	return r.RowId
}

func (r *OpStatsRow) SetID(id uint64) {
	r.RowId = id
}

func (r *OpReport) InitTable(db *pack.DB) error {
	opts := pack.Options{
		PackSizeLog2:    etl.ReportTablePackSizeLog2,
		JournalSizeLog2: etl.ReportTableJournalSizeLog2,
		CacheSize:       etl.ReportTableCacheSize,
		FillLevel:       etl.ReportTableFillLevel,
	}
	fields, err := pack.Fields(OpStatsRow{})
	if err != nil {
		return err
	}
	r.table, err = db.CreateTableIfNotExists(opReportTableKey, fields, opts)
	if err != nil {
		return err
	}
	return nil
}

func (r *OpReport) Tables() []*pack.Table {
	return []*pack.Table{r.table}
}

func (r *OpReport) CloseTable() error {
	if r.table != nil {
		if err := r.table.Close(); err != nil {
			log.Errorf("Closing %s table: %v", opReportTableKey, err)
		}
	}
	r.table = nil
	return nil
}

func (r *OpReport) Flush(ctx context.Context) error {
	if r.table != nil {
		if err := r.table.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", opReportTableKey, err)
		}
	}
	return nil
}

func (r *OpReport) FlushJournal(ctx context.Context) error {
	if r.table != nil {
		if err := r.table.FlushJournal(ctx); err != nil {
			log.Errorf("Flushing %s journal: %v", opReportTableKey, err)
		}
	}
	return nil
}

func (r *OpReport) LoadStats(ctx context.Context, at time.Time, height int64) (*OpStats, error) {
	s := &OpStats{
		Time:    at,
		Height:  height,
		Buckets: make(map[chain.OpType][]OpStatsBucket),
	}
	q := pack.Query{
		Name: "op_stats.lookup",
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
		rep := OpStatsRow{}
		if err := row.Decode(&rep); err != nil {
			return err
		}
		typ := chain.OpType(rep.Type)
		if t, ok := s.Buckets[typ]; ok {
			s.Buckets[typ] = append(t, OpStatsBucket{
				Name:   rep.Name,
				Type:   typ.String(),
				N:      rep.N,
				Sum:    rep.Sum,
				Min:    rep.Min,
				Max:    rep.Max,
				Mean:   rep.Mean,
				Median: rep.Median,
			})
		} else {
			s.Buckets[typ] = []OpStatsBucket{
				OpStatsBucket{
					Name:   rep.Name,
					Type:   typ.String(),
					N:      rep.N,
					Sum:    rep.Sum,
					Min:    rep.Min,
					Max:    rep.Max,
					Mean:   rep.Mean,
					Median: rep.Median,
				},
			}
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

func (r *OpReport) StoreStats(ctx context.Context, stats *OpStats) error {
	items := make([]pack.Item, 0)
	for typ, list := range stats.Buckets {
		for _, v := range list {
			rep := &OpStatsRow{
				Timestamp: stats.Time,
				Height:    stats.Height,
				Name:      v.Name,
				Type:      int(typ),
				N:         v.N,
				Sum:       v.Sum,
				Min:       v.Min,
				Max:       v.Max,
				Mean:      v.Mean,
				Median:    v.Median,
			}
			items = append(items, rep)
		}
	}
	return r.table.Insert(ctx, items)
}

func (r *OpReport) UpdateStats(ctx context.Context, stats *OpStats) error {
	if err := r.DeleteStats(ctx, stats.Height, stats.Time); err != nil {
		return err
	}
	if err := r.table.Flush(ctx); err != nil {
		return err
	}
	return r.StoreStats(ctx, stats)
}

func (r *OpReport) DeleteStats(ctx context.Context, height int64, at time.Time) error {
	q := pack.Query{
		Name: "op_stats.lookup",
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
