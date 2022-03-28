// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"blockwatch.cc/packdb/pack"
)

func (c *Crawler) SnapshotRequest(ctx context.Context) error {
	if c.snap == nil {
		return fmt.Errorf("snapshots disabled")
	}
	c.Lock()
	if c.snapch != nil {
		c.Unlock()
		return fmt.Errorf("snapshot in progress")
	}
	// snapshot under lock held
	switch c.state {
	case STATE_LOADING, STATE_STOPPING:
		return fmt.Errorf("snapshot disabled in %s state", c.state)
	case STATE_SYNCHRONIZED, STATE_STOPPED, STATE_WAITING, STATE_FAILED, STATE_CONNECTING:
		defer c.Unlock()
		return c.snapshot_locked(ctx)
	case STATE_SYNCHRONIZING:
		// continue below
	}

	log.Infof("Scheduling snapshot.")
	c.snapch = make(chan error)

	// no more lock required
	c.Unlock()

	// prevent shutdown
	c.wg.Add(1)
	defer c.wg.Done()

	// wait for snapshot to finish (or connection to close)
	select {
	case <-ctx.Done():
		c.snapch = nil
		return ctx.Err()
	case err := <-c.snapch:
		c.snapch = nil
		return err
	}
}

func (c *Crawler) MaybeSnapshot(ctx context.Context) error {
	if c.snap == nil {
		return nil
	}

	var (
		err   error
		match bool
	)
	snapch := c.snapch
	if snapch != nil {
		match = true
		defer func() {
			snapch <- err
			close(snapch)
		}()
	}

	// run under lock
	c.Lock()
	defer c.Unlock()
	tip := c.Tip()

	// check pre-condition (all conditions are logical OR)
	if !match && len(c.snap.Blocks) > 0 {
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

	// prevent shutdown
	c.wg.Add(1)
	defer c.wg.Done()

	err = c.snapshot_locked(ctx)
	return err
}

// run under lock
func (c *Crawler) snapshot_locked(ctx context.Context) error {
	// perform snapshot of all databases
	tip := c.Tip()
	start := time.Now()
	log.Infof("Starting database snapshots at block %d.", tip.BestHeight)

	// dump state db ()
	snapName := "block-" + strconv.FormatInt(tip.BestHeight, 10)
	dbName := filepath.Base(c.db.Path())
	snapPath := filepath.Join(c.snap.Path, snapName, dbName)
	if err := os.MkdirAll(filepath.Dir(snapPath), 0700); err != nil {
		return err
	}
	log.Infof("Creating snapshot for %s -> %s", dbName, snapPath)
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
	dbs := []*pack.DB{}
	for _, v := range c.indexer.indexes {
		dbs = append(dbs, v.DB())
	}
	for _, db := range dbs {
		if interruptRequested(ctx) {
			return errInterruptRequested
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
		log.Infof("Creating snapshot for %s -> %s", dbName, snapPath)
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
