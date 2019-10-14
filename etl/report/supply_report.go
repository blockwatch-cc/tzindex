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
	supplyReportKey = "supply"
)

type SupplyReport struct {
	crawler model.BlockCrawler
	table   *pack.Table
}

// Ensure the SupplyReport type implements the BlockReporter interface.
var _ model.BlockReporter = (*SupplyReport)(nil)

func NewSupplyReport() *SupplyReport {
	return &SupplyReport{}
}

func (r *SupplyReport) Init(_ context.Context, crawler model.BlockCrawler, db *pack.DB) error {
	r.crawler = crawler
	return r.InitTable(db)
}

func (r *SupplyReport) IsEOD() bool {
	return true
}

func (r *SupplyReport) Name() string {
	return supplyReportKey + " report"
}

func (r *SupplyReport) Key() string {
	return supplyReportKey
}

func (r *SupplyReport) Close() error {
	r.crawler = nil
	return r.CloseTable()
}

func (r *SupplyReport) AddBlock(ctx context.Context, block *model.Block) error {
	_, eod := Yesterday(block.Timestamp)

	// first try loading any existing stats from report tables
	stats, err := r.LoadStats(ctx, eod, block.Height-1)
	if err != nil {
		if err != ErrReportNotFound {
			return err
		}
		// need supply at last block of the day
		supply, err := r.crawler.SupplyByHeight(ctx, block.Height-1)
		if err != nil {
			return err
		}
		// run expensive queries
		stats, err = r.BuildStats(ctx, eod, supply)
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

func (r *SupplyReport) RemoveBlock(ctx context.Context, block *model.Block, ts time.Time) error {
	// roll back report table
	if err := r.DeleteStats(ctx, block.Height-1, ts); err != nil {
		log.Errorf("Rollback supply report tables: %v", err)
	}
	return nil
}

// assuming date is EOD and height is correct last block
func (r *SupplyReport) FixReport(ctx context.Context, date time.Time, height int64) error {
	// common timestamp is midnight UTC
	today, eod := Today(date)

	log.Debugf("Fixing supply metrics for time=%s eod=%s height=%d", today, eod, height)

	// run expensive queries
	supply, err := r.crawler.SupplyByHeight(ctx, height)
	if err != nil {
		return err
	}
	stats, err := r.BuildStats(ctx, eod, supply)
	if err != nil {
		return err
	}

	// update table row
	if err := r.UpdateStats(ctx, stats); err != nil {
		return err
	}
	return nil
}
