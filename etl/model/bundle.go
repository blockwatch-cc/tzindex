// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/rpc"
)

type Bundle struct {
	Block  *rpc.Block
	Params *tezos.Params
	Cycle  int64
}

func (b *Bundle) Height() int64 {
	if b.Block == nil {
		return -1
	}
	return b.Block.Header.Level
}

func (b *Bundle) Hash() tezos.BlockHash {
	if b.Block == nil {
		return tezos.BlockHash{}
	}
	return b.Block.Hash
}

func (b *Bundle) Parent() tezos.BlockHash {
	if b.Block == nil {
		return tezos.BlockHash{}
	}
	return b.Block.Header.Predecessor
}
