// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
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

	// ErrNoDb is an error that indicates a requested database does
	// not exist in the database.
	ErrNoDb = errors.New("no such database")

	// ErrNoData is an error that indicates a requested map or cache does
	// not exist.
	ErrNoData = errors.New("no data")
)
