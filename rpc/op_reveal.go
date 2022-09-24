// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzgo/tezos"
)

// Ensure Reveal implements the TypedOperation interface.
var _ TypedOperation = (*Reveal)(nil)

// Reveal represents a reveal operation
type Reveal struct {
	Manager
	PublicKey tezos.Key `json:"public_key"`
}
