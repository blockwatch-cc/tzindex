// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
    "blockwatch.cc/packdb/pack"
    "errors"
)

const BalanceTableKey = "balance"

var ErrNoBalance = errors.New("balance not indexed")

type Balance struct {
    RowId     uint64    `pack:"I,pk"     json:"row_id"`
    AccountId AccountID `pack:"A,bloom"  json:"account_id"`
    Balance   int64     `pack:"B"        json:"balance"`
    ValidFrom int64     `pack:">"        json:"valid_from"`
}

// Ensure Balance implements the pack.Item interface.
var _ pack.Item = (*Balance)(nil)

func (b Balance) ID() uint64 {
    return b.RowId
}

func (b *Balance) SetID(id uint64) {
    b.RowId = id
}

func (b *Balance) Reset() {
    *b = Balance{}
}

func (m Balance) TableKey() string {
    return BalanceTableKey
}

func (m Balance) TableOpts() pack.Options {
    return pack.Options{
        PackSizeLog2:    15,
        JournalSizeLog2: 16,
        CacheSize:       256,
        FillLevel:       100,
    }
}

func (m Balance) IndexOpts(key string) pack.Options {
    return pack.NoOptions
}
