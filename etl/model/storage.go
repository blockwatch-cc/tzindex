// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
    "blockwatch.cc/packdb/pack"
)

type StorageID uint64

func (id StorageID) Value() uint64 {
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
    s.RowId = 0
    s.AccountId = 0
    s.Hash = 0
    s.Height = 0
    s.Storage = nil
}
