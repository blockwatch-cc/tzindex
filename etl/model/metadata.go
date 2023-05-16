// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
)

const MetadataTableKey = "metadata"

var ErrNoMetadata = errors.New("metadata not indexed")

type Metadata struct {
	RowId     uint64        `pack:"I,pk"      json:"row_id"`
	AccountId AccountID     `pack:"A"         json:"account_id"`
	Address   tezos.Address `pack:"H"         json:"address"`
	AssetId   int64         `pack:"D"         json:"asset_id"` // u256 in Eth, nat in Tezos
	IsAsset   bool          `pack:"d"         json:"-"`        // flag indicating whether asset_id is used
	Content   []byte        `pack:"C,snappy"  json:"content"`  // JSON or binary encoded content
}

// Ensure Metadata implements the pack.Item interface.
var _ pack.Item = (*Metadata)(nil)

func (c *Metadata) ID() uint64 {
	return c.RowId
}

func (c *Metadata) SetID(id uint64) {
	c.RowId = id
}

func (m Metadata) TableKey() string {
	return MetadataTableKey
}

func (m Metadata) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    12,
		JournalSizeLog2: 12,
		CacheSize:       16,
		FillLevel:       100,
	}
}

func (m Metadata) IndexOpts(key string) pack.Options {
	return pack.Options{
		PackSizeLog2:    12,
		JournalSizeLog2: 12,
		CacheSize:       16,
		FillLevel:       90,
	}
}
