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
	accountReportKey = "account"
)

var units = []string{
	"1",
	"10",
	"100",
	"1k",
	"10k",
	"100k",
	"1M",
	"10M",
	"100M",
}

type AccountReport struct {
	crawler       model.BlockCrawler
	activityTable *pack.Table
	ageTable      *pack.Table
	balanceTable  *pack.Table
}

// Ensure the AccountReport type implements the BlockReporter interface.
var _ model.BlockReporter = (*AccountReport)(nil)

func NewAccountReport() *AccountReport {
	return &AccountReport{}
}

func (r *AccountReport) Init(_ context.Context, c model.BlockCrawler, db *pack.DB) error {
	r.crawler = c
	return r.InitTable(db)
}

func (r *AccountReport) IsEOD() bool {
	return true
}

func (r *AccountReport) Name() string {
	return accountReportKey + " report"
}

func (r *AccountReport) Key() string {
	return accountReportKey
}

func (r *AccountReport) Close() error {
	r.crawler = nil
	return r.CloseTable()
}

func (r *AccountReport) AddBlock(ctx context.Context, block *model.Block) error {
	sod, eod := Yesterday(block.Timestamp)

	// first try loading any existing stats from report tables
	stats, err := r.LoadStats(ctx, eod, block.Height-1)
	if err != nil {
		if err != ErrReportNotFound {
			return err
		}
		// run expensive queries
		stats, err = r.BuildStats(ctx, sod, eod, block.Height-1)
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

func (r *AccountReport) RemoveBlock(ctx context.Context, block *model.Block, ts time.Time) error {
	// roll back report table
	if err := r.DeleteStats(ctx, block.Height-1, ts); err != nil {
		log.Errorf("Rollback account report tables: %v", err)
	}
	return nil
}

// assuming date is EOD and height is correct last block
func (r *AccountReport) FixReport(ctx context.Context, date time.Time, height int64) error {
	// common timestamp is midnight UTC
	today, eod := Today(date)

	log.Debugf("Fixing account metrics for time=%s eod=%s height=%d", today, eod, height)

	// run expensive queries
	stats, err := r.BuildStats(ctx, today, eod, height)
	if err != nil {
		return err
	}

	// update table row
	if err := r.UpdateStats(ctx, stats); err != nil {
		return err
	}

	return nil
}
