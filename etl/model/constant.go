// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/rpc"
)

type ConstantID uint64

func (id ConstantID) Value() uint64 {
	return uint64(id)
}

// Constant holds code and info about registered global constants
type Constant struct {
	RowId       ConstantID         `pack:"I,pk,snappy"   json:"row_id"`
	Address     tezos.ExprHash     `pack:"H,snappy"      json:"address"`
	CreatorId   AccountID          `pack:"C,snappy"      json:"creator_id"`
	Value       []byte             `pack:"v,snappy"      json:"value"`
	Height      int64              `pack:"h,snappy"      json:"height"`
	StorageSize int64              `pack:"z,snappy"      json:"storage_size"`
	Features    micheline.Features `pack:"F,snappy"      json:"features"`

	prim micheline.Prim `pack:"-" json:"-"` // cached decoded version
}

// Ensure Constant implements the pack.Item interface.
var _ pack.Item = (*Constant)(nil)

// assuming the op was successful!
func NewConstant(rop *rpc.ConstantRegistration, op *Op) *Constant {
	res := rop.Metadata.Result
	g := &Constant{
		Address:     res.GlobalAddress.Clone(),
		CreatorId:   op.SenderId,
		Height:      op.Height,
		StorageSize: res.StorageSize,
		Features:    rop.Value.Features(),
	}
	if rop.Value.IsValid() {
		g.Value, _ = rop.Value.MarshalBinary()
	}
	return g
}

func (g *Constant) ID() uint64 {
	return uint64(g.RowId)
}

func (g *Constant) SetID(id uint64) {
	g.RowId = ConstantID(id)
}
