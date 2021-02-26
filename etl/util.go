// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"fmt"

	"github.com/cespare/xxhash"

	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/micheline"
)

const vestingContractBalancePath = "RLLL"

// Smart Contract Storage Access
func GetVestingBalance(prim *micheline.Prim) (int64, error) {
	if prim == nil {
		return 0, nil
	}
	p, err := prim.GetPathString(vestingContractBalancePath)
	if err != nil {
		return 0, err
	}
	if p.Type != micheline.PrimInt {
		return 0, fmt.Errorf("unexpected prim type %s for vesting contract balance", p.Type)
	}
	return p.Int.Int64(), nil
}

func hashKey(typ chain.AddressType, h []byte) uint64 {
	var buf [21]byte
	buf[0] = byte(typ)
	copy(buf[1:], h)
	return xxhash.Sum64(buf[:])
}

func accountHashKey(a *model.Account) uint64 {
	return hashKey(a.Type, a.Hash)
}

func addressHashKey(a chain.Address) uint64 {
	return hashKey(a.Type, a.Hash)
}
