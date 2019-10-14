// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package etl

import (
	"context"
	"fmt"
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/store"
	"blockwatch.cc/packdb/util"

	. "blockwatch.cc/tzindex/etl/model"
)

// we store expensive daily reports in pack tables
const (
	// these are daily reports
	ReportTablePackSizeLog2    = 12 // =4k packs ~10 years
	ReportTableJournalSizeLog2 = 10 // =1k entries
	ReportTableCacheSize       = 2  //
	ReportTableFillLevel       = 100

	// report database
	reportDbName = "reports"

	// main bucket that houses all serialized report stats objects
	reportBucketKey = "reports"

	// height for which all reports have been buffered successfully
	reportHeightKey = "best_height"
)

var (
	OneDay = 24 * time.Hour
)

type ReporterConfig struct {
	StateDB store.DB
	DBPath  string
	DBOpts  interface{}
	Reports []BlockReporter
}

type Reporter struct {
	db         *pack.DB
	dbpath     string
	dbopts     interface{}
	statedb    store.DB
	reports    []BlockReporter
	lastHeight int64
	crawler    *Crawler
	tips       map[string]ReportTip
}

// NewReporter returns a new report manager with the provided reports enabled.
func NewReporter(cfg ReporterConfig) *Reporter {
	return &Reporter{
		statedb: cfg.StateDB,
		dbopts:  cfg.DBOpts,
		dbpath:  cfg.DBPath,
		reports: cfg.Reports,
		tips:    make(map[string]ReportTip),
	}
}

func (m *Reporter) Init(ctx context.Context, c *Crawler) error {
	log.Info("Initializing report manager.")
	m.crawler = c

	// create reports bucket
	err := m.statedb.Update(func(dbTx store.Tx) error {
		_, err := dbTx.Root().CreateBucketIfNotExists([]byte(reportBucketKey))
		return err
	})
	if err != nil {
		return err
	}

	// open or create reports database
	m.db, err = pack.CreateDatabaseIfNotExists(
		m.dbpath,
		reportDbName,
		c.Tip().Symbol,
		m.dbopts,
	)
	if err != nil {
		return fmt.Errorf("creating database: %v", err)
	}

	// load last height
	m.lastHeight, err = m.LoadHeight()
	if err != nil {
		return err
	}

	// Initialize each of the enabled reports.
	return m.statedb.Update(func(dbTx store.Tx) error {
		for _, rep := range m.reports {
			tip := ReportTip{}
			if err := m.LoadTip(rep.Key(), &tip); err != nil {
				return err
			}
			m.tips[rep.Key()] = tip
			if err := rep.Init(ctx, c, m.db); err != nil {
				return err
			}
		}
		return nil
	})
}

func (m *Reporter) GC(ctx context.Context, ratio float64) error {
	if err := m.Flush(ctx); err != nil {
		return err
	}
	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}
	log.Infof("Garbage collecting %s (%s).", reportDbName, m.db.Path())
	return m.db.GC(ctx, ratio)
}

func (m *Reporter) Flush(ctx context.Context) error {
	for _, r := range m.reports {
		for _, t := range r.Tables() {
			if t == nil {
				continue
			}
			log.Debugf("Flushing %s table.", t.Name())
			if err := t.Flush(ctx); err != nil {
				log.Errorf("Flushing %s table: %v", t.Name(), err)
			}
		}
	}
	return m.StoreHeight()
}

func (m *Reporter) FlushJournals(ctx context.Context) error {
	for _, r := range m.reports {
		for _, t := range r.Tables() {
			if t == nil {
				continue
			}
			log.Debugf("Flushing %s journal.", t.Name())
			if err := t.FlushJournal(ctx); err != nil {
				log.Errorf("Flushing %s journal: %v", t.Name(), err)
			}
		}
	}
	return m.StoreHeight()
}

func (m *Reporter) Close() error {
	if m.db == nil {
		return nil
	}

	log.Info("Stopping report manager.")

	if err := m.StoreHeight(); err != nil {
		return err
	}

	// close reports
	for _, r := range m.reports {
		if err := r.Close(); err != nil {
			log.Errorf("Closing %s: %v", r.Name(), err)
		}
	}
	m.reports = nil

	// close report database
	if m.db != nil {
		if err := m.db.Close(); err != nil {
			log.Errorf("Closing report database: %v", err)
		}
		m.db = nil
	}

	m.crawler = nil
	log.Info("Stopped report manager.")
	return nil
}

func (m *Reporter) DB() *pack.DB {
	return m.db
}

func (m *Reporter) Height() int64 {
	return m.lastHeight
}

func (m *Reporter) LoadHeight() (int64, error) {
	var h int64
	err := m.statedb.View(func(dbTx store.Tx) error {
		return dbLoadReportTip(dbTx, reportHeightKey, &h)
	})
	return h, err
}

func (m *Reporter) StoreHeight() error {
	return m.statedb.Update(func(dbTx store.Tx) error {
		return dbStoreReportTip(dbTx, reportHeightKey, &m.lastHeight)
	})
}

func (m *Reporter) LoadTip(key string, val interface{}) error {
	return m.statedb.View(func(dbTx store.Tx) error {
		return dbLoadReportTip(dbTx, key, val)
	})
}

func (m *Reporter) StoreTip(key string, tip ReportTip) error {
	return m.statedb.Update(func(dbTx store.Tx) error {
		if err := dbStoreReportTip(dbTx, key, &tip); err != nil {
			return err
		}
		// concurrency-safe because statedb update locks access already
		m.tips[key] = tip
		return nil
	})
}

func (m *Reporter) AddBlock(ctx context.Context, block *Block) error {
	if m.lastHeight >= block.Height {
		log.Debugf("Block %d already reported.", block.Height)
		return nil
	}

	// parallel reporting
	var wg sync.WaitGroup

	// capture multiple errors to avoid blocking on error channel
	today := block.Timestamp.Truncate(OneDay)
	errC := make(chan error, len(m.reports))
	for _, rep := range m.reports {
		// keep eod report status
		if rep.IsEOD() {
			// prepare
			key := rep.Key()
			state, _ := m.tips[key]

			// init from first seen block
			if state.LastReportTime.IsZero() {
				state.LastReportTime = today
				if err := m.StoreTip(key, state); err != nil {
					return err
				}
			}

			// run EOD reports at most once every day, starting after day 0
			if state.LastReportTime.Add(OneDay).After(block.Timestamp) {
				continue
			}
		}

		// run report in go-routine
		wg.Add(1)
		go func(r BlockReporter) {
			defer wg.Done()
			err := r.AddBlock(ctx, block)
			if err != nil {
				errC <- err
			}
			if r.IsEOD() {
				// store status on success
				newtip := ReportTip{
					LastReportTime: today,
				}
				if err := m.StoreTip(r.Key(), newtip); err != nil {
					errC <- err
				}
			}
		}(rep)
	}
	wg.Wait()

	// fail with first error (e.g. context canceled on shutdown)
	if len(errC) > 0 {
		return <-errC
	}

	m.lastHeight = block.Height
	return nil
}

func (m *Reporter) RemoveBlock(ctx context.Context, block *Block) error {
	if m.lastHeight < block.Height {
		log.Warnf("Block %d already removed.", block.Height)
		return nil
	}

	for _, rep := range m.reports {
		// prepare
		key := rep.Key()
		tip, _ := m.tips[key]

		// keep eod report status
		if rep.IsEOD() {
			if tip.LastReportTime.Before(block.Timestamp) {
				continue
			}
		}

		if err := rep.RemoveBlock(ctx, block, tip.LastReportTime); err != nil {
			return err
		}

		// store EOD status on success
		if rep.IsEOD() {
			newtip := ReportTip{
				LastReportTime: block.Timestamp.Truncate(OneDay),
			}
			if err := m.StoreTip(key, newtip); err != nil {
				return err
			}
		}
	}

	m.lastHeight = block.Height - 1
	return m.StoreHeight()
}

func (m *Reporter) FixReport(ctx context.Context, date time.Time, height int64, report string) error {
	for _, r := range m.reports {
		if report != r.Key() {
			continue
		}
		if err := r.FixReport(ctx, date, height); err != nil {
			return err
		}
	}
	return nil
}
