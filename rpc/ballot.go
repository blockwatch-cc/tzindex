// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package rpc

import (
	"blockwatch.cc/tzindex/chain"
	"encoding/json"
)

// BallotOp represents a ballot operation
type BallotOp struct {
	GenericOp
	Source   chain.Address      `json:"source"`
	Period   int                `json:"period"`
	Ballot   chain.BallotVote   `json:"ballot"` // yay, nay, pass
	Proposal chain.ProtocolHash `json:"proposal"`
	Metadata json.RawMessage    `json:"metadata"` // missing example
}
