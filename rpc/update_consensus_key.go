// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
    "blockwatch.cc/tzgo/tezos"
)

// Ensure UpdateConsensusKey implements the TypedOperation interface.
var _ TypedOperation = (*UpdateConsensusKey)(nil)

// UpdateConsensusKey represents a transaction operation
type UpdateConsensusKey struct {
    Manager
    Pk tezos.Key `json:"pk"`
}

// Costs returns operation cost to implement TypedOperation interface.
func (t UpdateConsensusKey) Costs() tezos.Costs {
    return tezos.Costs{
        Fee:     t.Manager.Fee,
        GasUsed: t.Metadata.Result.Gas(),
    }
}
