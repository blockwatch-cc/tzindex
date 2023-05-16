// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"bytes"
	"context"
	"fmt"

	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

// this is a generic op only, details are in governance table
func (b *Builder) AppendBallotOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P}, args...)...,
		)
	}

	bop, ok := o.(*rpc.Ballot)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	bkr, ok := b.BakerByAddress(bop.Source)
	if !ok {
		return Errorf("missing baker %s ", bop.Source)
	}

	// build op, ballots have no fees, volume, gas, etc
	op := model.NewOp(b.block, id)
	op.Status = tezos.OpStatusApplied
	op.IsSuccess = true
	op.SenderId = bkr.AccountId

	// store protocol and vote as string: `protocol,vote`
	op.Data = bop.Proposal.String() + "," + bop.Ballot.String()
	b.block.Ops = append(b.block.Ops, op)

	// update account
	if !rollback {
		bkr.NBakerOps++
		bkr.NBallot++
		bkr.IsDirty = true
		bkr.Account.LastSeen = b.block.Height
		bkr.Account.IsDirty = true
	} else {
		bkr.NBakerOps--
		bkr.NBallot--
		bkr.IsDirty = true
		// approximation only
		bkr.Account.LastSeen = util.Max64N(bkr.Account.LastSeen, bkr.Account.LastIn, bkr.Account.LastOut)
		bkr.Account.IsDirty = true
	}
	return nil
}

// this is a generic op only, details are in governance table
func (b *Builder) AppendProposalOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P}, args...)...,
		)
	}

	pop, ok := o.(*rpc.Proposals)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	bkr, ok := b.BakerByAddress(pop.Source)
	if !ok {
		return Errorf("missing account %s ", pop.Source)
	}

	// build op, proposals have no fees, volume, gas, etc
	op := model.NewOp(b.block, id)
	op.Status = tezos.OpStatusApplied
	op.IsSuccess = true
	op.SenderId = bkr.AccountId

	// store proposals as comma separated base58 strings (same format as in JSON RPC)
	buf := bytes.NewBuffer(make([]byte, 0))
	for i, v := range pop.Proposals {
		buf.WriteString(v.String())
		if i < len(pop.Proposals)-1 {
			buf.WriteByte(',')
		}
	}
	op.Data = buf.String()
	b.block.Ops = append(b.block.Ops, op)

	// update account
	if !rollback {
		bkr.NBakerOps++
		bkr.NProposal++
		bkr.IsDirty = true
		bkr.Account.LastSeen = b.block.Height
		bkr.Account.IsDirty = true
	} else {
		bkr.NBakerOps--
		bkr.NProposal--
		bkr.IsDirty = true
		// approximation only
		bkr.Account.LastSeen = util.Max64N(bkr.Account.LastSeen, bkr.Account.LastIn, bkr.Account.LastOut)
		bkr.Account.IsDirty = true
	}
	return nil
}
