// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
// +build !linux,!windows

package cmd

import (
	_ "blockwatch.cc/packdb/store/bolt"

	bolt "go.etcd.io/bbolt"
	"time"
)

var (
	boltOpts = &bolt.Options{
		// open timeout when file is locked
		Timeout: time.Second,
		// skip fsync+alloc on grow; don't use with ext3/4, good in Docker + XFS
		NoGrowSync:   true,
		FreelistType: bolt.FreelistMapType,
		// don't fsync freelist (improves write performance at the cost of full
		// database scan on start-up)
		// NoFreelistSync: true,
	}
	boltReadOnlyOpts = &bolt.Options{
		Timeout:      time.Second,
		NoGrowSync:   true,
		ReadOnly:     true,
		FreelistType: bolt.FreelistMapType,
	}
	boltNoSyncOpts = &bolt.Options{
		Timeout:    time.Second,
		NoGrowSync: true,
		// skip fsync (DANGEROUS on crashes, but better performance for bulk load)
		NoSync:       true,
		FreelistType: bolt.FreelistMapType,
	}
	dbOpts = map[string]interface{}{
		"bolt": boltOpts,
	}
	dbReadOnlyOpts = map[string]interface{}{
		"bolt": boltReadOnlyOpts,
	}
	dbNoSyncOpts = map[string]interface{}{
		"bolt": boltNoSyncOpts,
	}
)

func DBOpts(engine string, readOnly, noSync bool) interface{} {
	if readOnly {
		o, _ := dbReadOnlyOpts[engine]
		return o
	}
	if noSync {
		o, _ := dbNoSyncOpts[engine]
		return o
	}
	o, _ := dbOpts[engine]
	return o
}
