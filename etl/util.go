// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"fmt"

	"github.com/cespare/xxhash"

	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/micheline"
)

// Smart Contract Storage Access
var vestingContractBalancePath = []int{1, 0, 0, 0}

func GetVestingBalance(prim *micheline.Prim) (int64, error) {
	if prim == nil {
		return 0, nil
	}
	for i, v := range vestingContractBalancePath {
		if len(prim.Args) < v+1 {
			return 0, fmt.Errorf("non existing path at %v in vesting contract storage", vestingContractBalancePath[:i])
		}
		prim = prim.Args[v]
	}
	if prim.Type != micheline.PrimInt {
		return 0, fmt.Errorf("unexpected prim type %s for vesting contract balance", prim.Type)
	}
	return prim.Int.Int64(), nil
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
