// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package rpc

import (
	"blockwatch.cc/tzindex/chain"
)

// ProposalsOp represents a proposal operation
type ProposalsOp struct {
	GenericOp
	Source    chain.Address        `json:"source"`
	Period    int                  `json:"period"`
	Proposals []chain.ProtocolHash `json:"proposals"`
}
