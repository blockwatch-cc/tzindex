// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"errors"
)

var (
	// ErrInvalidHash is an error that indicates a requested hash is zero.
	ErrInvalidHash = errors.New("invalid hash")

	// errNoChainTip is an error that indicates a requested entry does
	// not exist in the database.
	ErrNoChainTip = errors.New("chain tip not found")

	// ErrNoTable is an error that indicates a requested table does
	// not exist in the database.
	ErrNoTable = errors.New("no such table")

	// ErrNoIndex is an error that indicates a requested indexer does
	// not exist.
	ErrNoIndex = errors.New("no such index")

	// ErrNoDb is an error that indicates a requested database does
	// not exist in the database.
	ErrNoDb = errors.New("no such database")

	// ErrNoData is an error that indicates a requested map or cache does
	// not exist.
	ErrNoData = errors.New("no data")

	// errInterruptRequested indicates that an operation was cancelled due
	// to a user-requested interrupt.
	errInterruptRequested = errors.New("interrupt requested")
)

// interruptRequested returns true when the provided channel has been closed.
// This simplifies early shutdown slightly since the caller can just use an if
// statement instead of a select.
func interruptRequested(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}
	return false
}
