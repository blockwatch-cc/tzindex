// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package report

import (
	"context"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/etl/model"
)

const (
	opReportKey = "op"
)

type OpReport struct {
	crawler model.BlockCrawler
	table   *pack.Table
}

// Ensure the OpReport type implements the BlockReporter interface.
var _ model.BlockReporter = (*OpReport)(nil)

func NewOpReport() *OpReport {
	return &OpReport{}
}

func (r *OpReport) Init(_ context.Context, crawler model.BlockCrawler, db *pack.DB) error {
	r.crawler = crawler
	return r.InitTable(db)
}

func (r *OpReport) IsEOD() bool {
	return true
}

func (r *OpReport) Name() string {
	return supplyReportKey + " report"
}

func (r *OpReport) Key() string {
	return supplyReportKey
}

func (r *OpReport) Close() error {
	r.crawler = nil
	return r.CloseTable()
}

func (r *OpReport) AddBlock(ctx context.Context, block *model.Block) error {
	_, eod := Yesterday(block.Timestamp)

	// first try loading any existing stats from report tables
	stats, err := r.LoadStats(ctx, eod, block.Height-1)
	if err != nil {
		if err != ErrReportNotFound {
			return err
		}
		// run expensive queries
		stats, err = r.BuildStats(ctx, eod, block.Height-1)
		if err != nil {
			return err
		}
		// store result
		if err := r.StoreStats(ctx, stats); err != nil {
			return err
		}
	}

	return nil
}

func (r *OpReport) RemoveBlock(ctx context.Context, block *model.Block, ts time.Time) error {
	// roll back report table
	if err := r.DeleteStats(ctx, block.Height-1, ts); err != nil {
		log.Errorf("Rollback op report tables: %v", err)
	}
	return nil
}

func (r *OpReport) FixReport(ctx context.Context, date time.Time, height int64) error {
	// common timestamp is midnight UTC
	today, eod := Today(date)

	log.Debugf("Fixing op metrics for time=%s eod=%s height=%d", today, eod, height)

	// run expensive queries
	stats, err := r.BuildStats(ctx, eod, height)
	if err != nil {
		return err
	}

	// update table row
	if err := r.UpdateStats(ctx, stats); err != nil {
		return err
	}
	return nil
}
