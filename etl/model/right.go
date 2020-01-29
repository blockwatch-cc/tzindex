// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"sync"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/chain"
)

var rightPool = &sync.Pool{
	New: func() interface{} { return new(Right) },
}

type Right struct {
	RowId          uint64          `pack:"I,pk,snappy"   json:"row_id"`           // unique id
	Type           chain.RightType `pack:"t,snappy"      json:"type"`             // default accounts
	Height         int64           `pack:"h,snappy"      json:"height"`           // bc: block height (also for orphans)
	Cycle          int64           `pack:"c,snappy"      json:"cycle"`            // bc: block cycle (tezos specific)
	Priority       int             `pack:"p,snappy"      json:"priority"`         // baking prio or endorsing slot
	AccountId      AccountID       `pack:"A,snappy"      json:"account_id"`       // original rights holder
	IsLost         bool            `pack:"l,snappy"      json:"is_lost"`          // owner lost this baking right
	IsStolen       bool            `pack:"s,snappy"      json:"is_stolen"`        // owner stole this baking right
	IsMissed       bool            `pack:"m,snappy"      json:"is_missed"`        // owner missed using this endorsement right
	IsSeedRequired bool            `pack:"R,snappy"      json:"is_seed_required"` // seed nonce must be revealed (height%32==0)
	IsSeedRevealed bool            `pack:"r,snappy"      json:"is_seed_revealed"` // seed nonce has been revealed in next cycle
}

// Ensure Right implements the pack.Item interface.
var _ pack.Item = (*Right)(nil)

func (r *Right) ID() uint64 {
	return r.RowId
}

func (r *Right) SetID(id uint64) {
	r.RowId = id
}

func AllocRight() *Right {
	return rightPool.Get().(*Right)
}

func (r *Right) Free() {
	r.Reset()
	rightPool.Put(r)
}

func (r *Right) Reset() {
	r.RowId = 0
	r.Type = 0
	r.Height = 0
	r.Cycle = 0
	r.Priority = 0
	r.AccountId = 0
	r.IsLost = false
	r.IsStolen = false
	r.IsMissed = false
	r.IsSeedRequired = false
	r.IsSeedRevealed = false
}
