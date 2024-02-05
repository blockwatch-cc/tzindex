// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"
	"sync"

	"blockwatch.cc/packdb/pack"
)

const (
	TokenMetaTableKey = "token_metadata"
)

var (
	ErrNoTokenMeta = errors.New("token metadata not indexed")

	tokenMetaPool = &sync.Pool{
		New: func() any { return new(TokenMeta) },
	}
)

type TokenMetaID uint64

// TokenMeta tracks token metadata from off-chain resolution
type TokenMeta struct {
	Id    TokenMetaID `pack:"I,pk"      json:"row_id"`
	Token TokenID     `pack:"T,snappy"  json:"token"`
	Data  []byte      `pack:"D,snappy"  json:"data"`
}

// Ensure TokenMeta items implement the pack.Item interface.
var _ pack.Item = (*TokenMeta)(nil)

func (m *TokenMeta) ID() uint64 {
	return uint64(m.Id)
}

func (m *TokenMeta) SetID(id uint64) {
	m.Id = TokenMetaID(id)
}

func NewTokenMeta() *Token {
	return tokenMetaPool.Get().(*Token)
}

func (m *TokenMeta) Reset() {
	*m = TokenMeta{}
}

func (m *TokenMeta) Free() {
	m.Reset()
	tokenMetaPool.Put(m)
}

func (m TokenMeta) TableKey() string {
	return TokenMetaTableKey
}

func (m TokenMeta) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    11,  // 2k pack size
		JournalSizeLog2: 11,  // 2k journal size
		CacheSize:       64,  // max MB
		FillLevel:       100, // boltdb fill level to limit reallocations
	}
}

func (m TokenMeta) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}
