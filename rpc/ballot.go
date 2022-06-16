// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzgo/tezos"
)

// Ensure Ballot implements the TypedOperation interface.
var _ TypedOperation = (*Ballot)(nil)

// Ballot represents a ballot operation
type Ballot struct {
	Generic
	Source   tezos.Address      `json:"source"`
	Period   int                `json:"period"`
	Ballot   tezos.BallotVote   `json:"ballot"` // yay, nay, pass
	Proposal tezos.ProtocolHash `json:"proposal"`
}
