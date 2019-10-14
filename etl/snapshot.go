// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package etl

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
)

func (c *Crawler) MaybeSnapshot(ctx context.Context) error {
	if c.snap == nil {
		return nil
	}

	// use current block
	tip := c.Tip()

	// check pre-condition (all conditions are logical OR)
	var match bool
	if len(c.snap.Blocks) > 0 {
		for _, v := range c.snap.Blocks {
			if v == tip.BestHeight {
				match = true
				break
			}
		}
	}
	if !match && c.snap.BlockInterval > 0 {
		if tip.BestHeight > 0 && tip.BestHeight%c.snap.BlockInterval == 0 {
			match = true
		}
	}
	if !match {
		return nil
	}

	return c.Snapshot(ctx)
}

func (c *Crawler) Snapshot(ctx context.Context) error {
	tip := c.Tip()

	// perform snapshot of all databases
	start := time.Now()
	log.Infof("Starting database snapshots at block %d.", tip.BestHeight)

	// dump state db ()
	snapName := "block-" + strconv.FormatInt(tip.BestHeight, 10)
	dbName := filepath.Base(c.db.Path())
	snapPath := filepath.Join(c.snap.Path, snapName, dbName)
	if err := os.MkdirAll(filepath.Dir(snapPath), 0700); err != nil {
		return err
	}
	log.Infof("Creating snapshot for %s", dbName)
	f, err := os.OpenFile(snapPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	err = c.db.Dump(f)
	_ = f.Close()
	if err != nil {
		return err
	}

	// dump index and report db's
	dbs := []*pack.DB{c.reporter.db}
	for _, v := range c.indexer.indexes {
		dbs = append(dbs, v.DB())
	}
	for _, db := range dbs {
		if util.InterruptRequested(ctx) {
			return ctx.Err()
		}
		if db == nil {
			continue
		}
		// determine target filename
		dbName := filepath.Base(db.Path())
		snapPath := filepath.Join(c.snap.Path, snapName, dbName)
		if err := os.MkdirAll(filepath.Dir(snapPath), 0700); err != nil {
			return err
		}
		log.Infof("Creating snapshot for %s", dbName)
		f, err := os.OpenFile(snapPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			return err
		}
		err = db.Dump(f)
		_ = f.Close()
		if err != nil {
			return err
		}
	}
	log.Infof("Successfully finished database snapshots in %s.", time.Since(start))
	return nil
}
