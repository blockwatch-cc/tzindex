// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"

	"blockwatch.cc/packdb/pack"
)

const StorageTableKey = "storage"

var ErrNoStorage = errors.New("storage not indexed")

type StorageID uint64

func (id StorageID) U64() uint64 {
	return uint64(id)
}

// Storage holds a snapshot of smart contract storage content.
type Storage struct {
	RowId     StorageID `pack:"I,pk"             json:"row_id"`
	AccountId AccountID `pack:"A,bloom=3,snappy" json:"account_id"`
	Hash      uint64    `pack:"H,bloom=3,snappy" json:"hash"`
	Height    int64     `pack:"h"                json:"height"`
	Storage   []byte    `pack:"S,snappy"         json:"storage"`
}

// Ensure Storage implements the pack.Item interface.
var _ pack.Item = (*Storage)(nil)

func (s Storage) ID() uint64 {
	return uint64(s.RowId)
}

func (s *Storage) SetID(id uint64) {
	s.RowId = StorageID(id)
}

func (s *Storage) Reset() {
	*s = Storage{}
}

func (m Storage) TableKey() string {
	return StorageTableKey
}

func (m Storage) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    10,
		JournalSizeLog2: 10,
		CacheSize:       32,
		FillLevel:       100,
	}
}

func (m Storage) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}
