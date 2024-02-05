// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"
	"sync"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzgo/tezos"
)

const RightsTableKey = "rights"

var (
	rightPool = &sync.Pool{
		New: func() interface{} { return new(Right) },
	}

	ErrNoRights = errors.New("rights not indexed")
)

type Right struct {
	RowId     uint64     `pack:"I,pk"      json:"row_id"`           // unique id
	Cycle     int64      `pack:"c,i16"     json:"cycle"`            // cycle
	Height    int64      `pack:"h,i32"     json:"height"`           // height
	AccountId AccountID  `pack:"A,u32"     json:"account_id"`       // rights holder
	Bake      vec.BitSet `pack:"B,snappy"  json:"baking_rights"`    // bits for every block
	Endorse   vec.BitSet `pack:"E,snappy"  json:"endorsing_rights"` // bits for every block
	Baked     vec.BitSet `pack:"b,snappy"  json:"blocks_baked"`     // bits for every block
	Endorsed  vec.BitSet `pack:"e,snappy"  json:"blocks_endorsed"`  // bits for every block
	Seed      vec.BitSet `pack:"S,snappy"  json:"seeds_required"`   // only bits for every seed block
	Seeded    vec.BitSet `pack:"s,snappy"  json:"seeds_revealed"`   // only bits for every seed block
}

type BaseRight struct {
	AccountId      AccountID
	Type           tezos.RightType
	IsUsed         bool
	IsLost         bool
	IsStolen       bool
	IsMissed       bool
	IsSeedRequired bool
	IsSeedRevealed bool
}

// Ensure Right implements the pack.Item interface.
var _ pack.Item = (*Right)(nil)

func (r *Right) ID() uint64 {
	return r.RowId
}

func (r *Right) SetID(id uint64) {
	r.RowId = id
}

func (m Right) TableKey() string {
	return RightsTableKey
}

func (m Right) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    12,
		JournalSizeLog2: 16,
		CacheSize:       32,
		FillLevel:       100,
	}
}

func (m Right) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

func AllocRight() *Right {
	return rightPool.Get().(*Right)
}

func NewRight(acc AccountID, height, cycle int64, nBlocks, nSeeds int) *Right {
	r := AllocRight()
	r.Height = height
	r.Cycle = cycle
	r.AccountId = acc
	r.Bake.Resize(nBlocks)
	r.Endorse.Resize(nBlocks)
	r.Baked.Resize(nBlocks)
	r.Endorsed.Resize(nBlocks)
	r.Seed.Resize(nSeeds)
	r.Seeded.Resize(nSeeds)
	return r
}

func (r *Right) Free() {
	r.Reset()
	rightPool.Put(r)
}

func (r *Right) Reset() {
	r.RowId = 0
	r.Cycle = 0
	r.Height = 0
	r.AccountId = 0
	r.Bake.Reset()
	r.Endorse.Reset()
	r.Baked.Reset()
	r.Endorsed.Reset()
	r.Seed.Reset()
	r.Seeded.Reset()
}

func (r Right) ToBase(pos int, typ tezos.RightType) (BaseRight, bool) {
	if typ == tezos.RightTypeBaking && (r.Bake.IsSet(pos) || r.Baked.IsSet(pos)) {
		return BaseRight{
			AccountId:      r.AccountId,
			Type:           typ,
			IsUsed:         r.Bake.IsSet(pos) && r.Baked.IsSet(pos),
			IsLost:         r.Bake.IsSet(pos) && !r.Baked.IsSet(pos),
			IsStolen:       !r.Bake.IsSet(pos) && r.Baked.IsSet(pos),
			IsSeedRequired: r.Seed.IsSet(pos),
			IsSeedRevealed: r.Seeded.IsSet(pos),
		}, true
	}
	if typ == tezos.RightTypeEndorsing && r.Endorse.IsSet(pos) {
		return BaseRight{
			AccountId: r.AccountId,
			Type:      typ,
			IsUsed:    r.Endorse.IsSet(pos) && r.Endorsed.IsSet(pos),
			IsMissed:  r.Endorse.IsSet(pos) && !r.Endorsed.IsSet(pos),
		}, true
	}
	return BaseRight{}, false
}

func (r Right) IsUsed(pos int) bool {
	return r.Bake.IsSet(pos) && r.Baked.IsSet(pos) || r.Endorse.IsSet(pos) && r.Endorsed.IsSet(pos)
}

func (r Right) IsLost(pos int) bool {
	return r.Bake.IsSet(pos) && !r.Baked.IsSet(pos)
}

func (r Right) IsStolen(pos int) bool {
	return !r.Bake.IsSet(pos) && r.Baked.IsSet(pos)
}

func (r Right) IsMissed(pos int) bool {
	return r.Endorse.IsSet(pos) && !r.Endorsed.IsSet(pos)
}

func (r Right) IsSeedRequired(pos int) bool {
	return r.Seed.IsSet(pos)
}

func (r Right) IsSeedRevealed(pos int) bool {
	return r.Seeded.IsSet(pos)
}

func (r Right) Reliability(pos int) int64 {
	// ensure bitsets are all same length before AND
	bits := r.Bake.Clone().Resize(pos + 1)
	must := bits.Count()
	bits.And(r.Baked.Clone().Resize(pos + 1))
	have := bits.Count()
	bits.Close()
	bits = r.Endorse.Clone().Resize(pos + 1)
	must += bits.Count()
	bits.And(r.Endorsed.Clone().Resize(pos + 1))
	have += bits.Count()
	bits.Close()
	if must == 0 {
		return 0
	}
	return int64(have * 10000 / must)
}
