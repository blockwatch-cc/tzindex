// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
)

const MetadataTableKey = "metadata"

var ErrNoMetadata = errors.New("metadata not indexed")

type MetaID uint64

func (i MetaID) U64() uint64 {
	return uint64(i)
}

type Metadata struct {
	RowId     MetaID        `pack:"I,pk"      json:"row_id"`
	AccountId AccountID     `pack:"A,u32"     json:"account_id"`
	Address   tezos.Address `pack:"H,bloom=3" json:"address"`
	Content   []byte        `pack:"C,snappy"  json:"content"` // JSON or binary encoded content
}

// Ensure Metadata implements the pack.Item interface.
var _ pack.Item = (*Metadata)(nil)

func (c *Metadata) ID() uint64 {
	return uint64(c.RowId)
}

func (c *Metadata) SetID(id uint64) {
	c.RowId = MetaID(id)
}

func (m Metadata) TableKey() string {
	return MetadataTableKey
}

func (m Metadata) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    11, // 2k packs
		JournalSizeLog2: 11, // 2k journal size
		CacheSize:       64, // 64 MB
		FillLevel:       100,
	}
}

func (m Metadata) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}
