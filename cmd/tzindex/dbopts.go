// Copyright (c) 2020-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	_ "blockwatch.cc/packdb/store/bolt"
	"github.com/echa/config"

	"time"

	bolt "go.etcd.io/bbolt"
)

const engine = "bolt"

var (
	boltDefaultOpts = bolt.Options{
		Timeout:      time.Second,
		FreelistType: bolt.FreelistMapType,
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
