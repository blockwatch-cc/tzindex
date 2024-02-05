// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"errors"
)

var (
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
)
