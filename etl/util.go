// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"fmt"

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
