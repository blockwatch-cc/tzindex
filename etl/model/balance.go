// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
    "blockwatch.cc/packdb/pack"
)

type Balance struct {
    RowId      uint64    `pack:"I,pk"     json:"row_id"`
    AccountId  AccountID `pack:"A,bloom"  json:"account_id"`
    Balance    int64     `pack:"B"        json:"balance"`
    ValidFrom  int64     `pack:">"        json:"valid_from"`
    ValidUntil int64     `pack:"<"        json:"valid_until"`
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
    b.RowId = 0
    b.AccountId = 0
    b.Balance = 0
    b.ValidFrom = 0
    b.ValidUntil = 0
}
