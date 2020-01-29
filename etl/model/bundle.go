// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/rpc"
)

type Bundle struct {
	Block     *rpc.Block
	Params    *chain.Params
	Cycle     int64
	Baking    []rpc.BakingRight
	Endorsing []rpc.EndorsingRight
	Snapshot  *rpc.SnapshotIndex
}

func (b *Bundle) Height() int64 {
	if b.Block == nil {
		return -1
	}
	return b.Block.Header.Level
}

func (b *Bundle) Hash() chain.BlockHash {
	if b.Block == nil {
		return chain.BlockHash{}
	}
	return b.Block.Hash
}

func (b *Bundle) Parent() chain.BlockHash {
	if b.Block == nil {
		return chain.BlockHash{}
	}
	return b.Block.Header.Predecessor
}
