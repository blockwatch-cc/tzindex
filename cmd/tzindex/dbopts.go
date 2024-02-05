// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"time"

	_ "blockwatch.cc/packdb/store/bolt"
	"github.com/echa/config"

	bolt "go.etcd.io/bbolt"
)

const engine = "bolt"

var (
	boltDefaultOpts = bolt.Options{
		// open timeout when file is locked
		Timeout: time.Second,
		// faster for large databases
		FreelistType: bolt.FreelistMapType,

		// User-controlled options
		//
		// // skip fsync (DANGEROUS on crashes, but better performance for bulk load)
		// NoSync: false,
		//
		// // skip fsync+alloc on grow; don't use with ext3/4, good in Docker + XFS
		// NoGrowSync: false,
		//
		// // don't fsync freelist (improves write performance at the cost of full
		// // database scan on start-up)
		// NoFreelistSync: false,
		//
		// // PageSize overrides the default OS page size.
		// PageSize: 4096
	}
)

func DBOpts(readOnly bool) any {
	opts := boltDefaultOpts
	opts.ReadOnly = readOnly
	opts.NoSync = config.GetBool("db.nosync")
	opts.NoGrowSync = config.GetBool("db.no_grow_sync")
	opts.NoFreelistSync = config.GetBool("db.no_free_sync")
	opts.PageSize = config.GetInt("db.page_size")
	dataLog.Debug("Bolt DB config")
	dataLog.Debugf("  Readonly         %t", opts.ReadOnly)
	dataLog.Debugf("  No-Sync          %t", opts.NoSync)
	dataLog.Debugf("  No-Grow-Sync     %t", opts.NoGrowSync)
	dataLog.Debugf("  No-Freelist-Sync %t", opts.NoFreelistSync)
	dataLog.Debugf("  Pagesize         %d", opts.PageSize)
	return &opts
}
