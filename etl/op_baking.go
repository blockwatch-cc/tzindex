// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

func (b *Builder) AppendEndorsementOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P}, args...)...,
		)
	}

	eop, ok := o.(*rpc.Endorsement)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	meta := eop.Metadata
	bkr, ok := b.BakerByAddress(meta.Address())
	if !ok {
		return Errorf("missing baker %s ", meta.Address())
	}

	// build op
	op := model.NewOp(b.block, id)
	op.Status = tezos.OpStatusApplied
	op.IsSuccess = true
	op.SenderId = bkr.AccountId

	// store endorsed slots as data
	// pre-Ithaca: use slots
	// post-Ithaca: use EndorsementPower and PreendorsementPower
	power := meta.Power()
	op.Data = strconv.Itoa(power)

	// build flows; post-Ithaca this is empty
	flows := b.NewEndorserFlows(bkr.Account, meta.Balances(), id)

	// fill op amounts from flows
	for _, f := range flows {
		switch f.Kind {
		case model.FlowKindRewards:
			op.Reward += f.AmountIn
		case model.FlowKindDeposits:
			op.Deposit += f.AmountIn
		case model.FlowKindBalance:
			// don't count internal flows against volume
		}
	}
	b.block.Ops = append(b.block.Ops, op)

	// update account
	if !rollback {
		bkr.NBakerOps++
		if id.Kind == model.OpTypePreendorsement {
			bkr.NPreendorsement++
		} else {
			bkr.NEndorsement++
			bkr.BlocksEndorsed++
			bkr.SlotsEndorsed += int64(power)
		}
		// extend grace period
		if bkr.IsActive {
			bkr.UpdateGracePeriod(b.block.Cycle, b.block.Params)
		} else {
			// reset inactivity
			bkr.IsActive = true
			bkr.BakerUntil = 0
			bkr.InitGracePeriod(b.block.Cycle, b.block.Params)
		}
		bkr.IsDirty = true
		bkr.Account.LastSeen = b.block.Height
		bkr.Account.IsDirty = true
	} else {
		// don't update inactivity because we don't know its previous state
		bkr.NBakerOps--
		if id.Kind == model.OpTypePreendorsement {
			bkr.NPreendorsement--
		} else {
			bkr.NEndorsement--
			bkr.BlocksEndorsed--
			bkr.SlotsEndorsed -= int64(power)
		}
		bkr.IsDirty = true
		bkr.Account.LastSeen = b.block.Height - 1 // approximation
		bkr.Account.IsDirty = true
	}
	return nil
}

func (b *Builder) AppendSeedNonceOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P}, args...)...,
		)
	}

	// Seed nonces are special, they are sent by some baker and may be included by another
	sop, ok := o.(*rpc.SeedNonce)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}

	flows := b.NewSeedNonceFlows(sop.Fees(), id)

	// build op
	op := model.NewOp(b.block, id)
	op.Status = tezos.OpStatusApplied
	op.IsSuccess = true
	op.SenderId = b.block.ProposerId

	// lookup baker for original level
	if refBlock, err := b.idx.BlockByHeight(ctx, sop.Level); err != nil {
		return Errorf("missing block %d: %v", sop.Level, err)
	} else {
		op.CreatorId = refBlock.BakerId
	}

	// data is `level,nonce`
	op.Data = strconv.FormatInt(sop.Level, 10) + "," + sop.Nonce.String()

	for _, f := range flows {
		op.Reward += f.AmountIn
	}
	b.block.Ops = append(b.block.Ops, op)

	// update account
	if !rollback {
		b.block.Proposer.NBakerOps++
		b.block.Proposer.NSeedNonce++
		b.block.Proposer.IsDirty = true
		b.block.Proposer.Account.LastSeen = b.block.Height
		b.block.Proposer.Account.IsDirty = true
	} else {
		b.block.Proposer.NBakerOps--
		b.block.Proposer.NSeedNonce--
		b.block.Proposer.IsDirty = true
		b.block.Proposer.Account.IsDirty = true
	}
	return nil
}

func (b *Builder) AppendVdfRevelationOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P}, args...)...,
		)
	}

	// Vdf revelations are special, they are sent by some baker and may be included by another
	vop, ok := o.(*rpc.VdfRevelation)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}

	// same as seed nonce
	flows := b.NewSeedNonceFlows(vop.Fees(), id)

	// build op
	op := model.NewOp(b.block, id)
	op.Status = tezos.OpStatusApplied
	op.IsSuccess = true
	op.SenderId = b.block.ProposerId

	// VDF revelations contain data, but no reference to other blocks or the producer
	for _, v := range vop.Solution {
		op.Parameters = append(op.Parameters, v.Bytes()...)
	}

	for _, f := range flows {
		op.Reward += f.AmountIn
	}
	b.block.Ops = append(b.block.Ops, op)

	// update account
	if !rollback {
		b.block.Proposer.NBakerOps++
		b.block.Proposer.NSeedNonce++
		b.block.Proposer.IsDirty = true
		b.block.Proposer.Account.LastSeen = b.block.Height
		b.block.Proposer.Account.IsDirty = true
	} else {
		b.block.Proposer.NBakerOps--
		b.block.Proposer.NSeedNonce--
		b.block.Proposer.IsDirty = true
		b.block.Proposer.Account.IsDirty = true
	}
	return nil
}

func (b *Builder) AppendDoubleBakingOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P}, args...)...,
		)
	}

	dop, ok := o.(*rpc.DoubleBaking)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}

	// determine who's the offender from balance updates
	var offender *model.Baker
	upd := dop.Fees()
	switch {
	case b.block.Params.Version < 12:
		// - before Ithaca: last & first (dynamic count of updates when one subaccount is empty)
		addr := upd[0].Address()
		if acc, ok := b.BakerByAddress(addr); !ok {
			return Errorf("missing offender account %s", addr)
		} else {
			offender = acc
		}
	case b.block.Params.Version < 18:
		// - after Ithaca: second, third (always 4 entries)
		if len(upd) == 4 {
			addr := upd[2].Address()
			if acc, ok := b.BakerByAddress(addr); !ok {
				return Errorf("missing offender account %s", addr)
			} else {
				offender = acc
			}
		}
	default:
		// no balance updates in oxford+, we can only look at rights
		var head rpc.BlockHeader
		_ = json.Unmarshal(dop.BH1, &head)
		addr, err := b.rpc.GetBakingRightOwner(ctx,
			rpc.BlockLevel(head.Level-1),
			head.PayloadRound,
			b.block.Params,
		)
		if err != nil {
			return err
		}
		if addr.IsValid() {
			if acc, ok := b.BakerByAddress(addr); !ok {
				return Errorf("missing offender account %s", addr)
			} else {
				offender = acc
			}
		}
	}

	// build flows first to determine burn
	flows := b.NewPenaltyFlows(b.block.Proposer, offender, upd, id)

	// build op
	op := model.NewOp(b.block, id)
	op.IsSuccess = true
	op.Status = tezos.OpStatusApplied
	op.SenderId = b.block.ProposerId
	if offender != nil {
		op.ReceiverId = offender.AccountId
	}

	// we store both block headers as json array
	buf, err := json.Marshal(dop.Strip())
	if err != nil {
		log.Error(Errorf("cannot write data: %v", err))
	} else {
		op.Data = string(buf)
	}

	// calc burn from flows
	for _, f := range flows {
		if f.IsBurned {
			// track all burned coins
			op.Burned += f.AmountOut

			// track offender losses by category
			switch f.Kind {
			case model.FlowKindRewards:
				op.Reward -= f.AmountOut
			case model.FlowKindDeposits:
				op.Deposit -= f.AmountOut
			case model.FlowKindFees:
				op.Fee -= f.AmountOut
			case model.FlowKindStake:
				op.Deposit -= f.AmountOut
			}
		} else {
			// track accuser reward as volume
			op.Volume += f.AmountIn
			op.Burned -= f.AmountIn
		}
	}
	b.block.Ops = append(b.block.Ops, op)

	// update accounts
	if !rollback {
		b.block.Proposer.NBakerOps++
		b.block.Proposer.NAccusations++
		b.block.Proposer.IsDirty = true
		acc := b.block.Proposer.Account
		acc.LastSeen = b.block.Height
		acc.IsDirty = true

		if offender != nil {
			offender.NBakerOps++
			offender.N2Baking++
			offender.IsDirty = true
			acc := offender.Account
			acc.LastSeen = b.block.Height
			acc.IsDirty = true

			// apply losses to unstake requests
			// - bakers can have unstake requests too
			// - accounts referenced in unstake may not be pre-loaded
			// - emit a slash event
			// if reqs, err := b.idx.ListFrozenUnstakeRequests(ctx, offender.AccountId, op.Cycle, b.block.Params); err == nil {
			// 	for _, req := range reqs {
			// 		acc, err := b.LoadAccountByAccountId(ctx, req.SenderId)
			// 		if err != nil {
			// 			return Errorf("missing account %d with unstake request %d", req.SenderId, req.RowId)
			// 		}
			// 		// 10% loss
			// 		acc.UnstakedBalance -= req.Volume / 10
			// 		acc.LostStake += req.Volume / 10
			// 		acc.IsDirty = true

			// 		// emit slash event for non-bakers
			// 		if !acc.IsBaker {
			// 			slash := model.NewEventOp(b.block, req.SenderId, id)
			// 			slash.Type = model.OpTypeStakeSlash
			// 			slash.SenderId = req.SenderId
			// 			slash.BakerId = offender.AccountId
			// 			slash.Burned = req.Volume / 10
			// 			b.block.Ops = append(b.block.Ops, slash)
			// 		}
			// 	}
			// }
		}

	} else {
		b.block.Proposer.NBakerOps--
		b.block.Proposer.NAccusations--
		b.block.Proposer.IsDirty = true
		acc := b.block.Proposer.Account
		acc.LastSeen = max(acc.LastSeen, acc.LastIn, acc.LastOut)
		acc.IsDirty = true

		if offender != nil {
			offender.NBakerOps--
			offender.N2Baking--
			offender.IsDirty = true
			acc := offender.Account
			acc.LastSeen = max(acc.LastSeen, acc.LastIn, acc.LastOut)
			acc.IsDirty = true

			// reverse-apply losses to unstake requests
			// - bakers can have unstake requests too
			// - accounts referenced in unstake may not be pre-loaded
			// - emit a slash event
			// if reqs, err := b.idx.ListFrozenUnstakeRequests(ctx, offender.AccountId, op.Cycle, b.block.Params); err == nil {
			// 	for _, req := range reqs {
			// 		acc, err := b.LoadAccountByAccountId(ctx, req.SenderId)
			// 		if err != nil {
			// 			return Errorf("missing account %d with unstake request %d", req.SenderId, req.RowId)
			// 		}
			// 		// 10% loss
			// 		acc.UnstakedBalance -= req.Volume / 10
			// 		acc.LostStake += req.Volume / 10
			// 		acc.IsDirty = true
			// 	}
			// }
		}

	}

	return nil
}

func (b *Builder) AppendDoubleEndorsingOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P}, args...)...,
		)
	}

	dop, ok := o.(*rpc.DoubleEndorsement)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}

	var offender *model.Baker
	upd := dop.Fees()
	switch {
	case b.block.Params.Version < 12:
		// - before Ithaca: last & first (dynamic count of updates when one subaccount is empty)
		addr := upd[0].Address()
		if acc, ok := b.BakerByAddress(addr); !ok {
			return Errorf("missing offender account %s", addr)
		} else {
			offender = acc
		}
	case b.block.Params.Version < 18:
		// - after Ithaca: second, third (always 4 entries)
		if len(upd) == 4 {
			addr := upd[2].Address()
			if acc, ok := b.BakerByAddress(addr); !ok {
				return Errorf("missing offender account %s", addr)
			} else {
				offender = acc
			}
		}
	default:
		// no balance updates in oxford+, we can only look at rights
		var end rpc.InlinedEndorsement
		_ = json.Unmarshal(dop.OP1, &end)
		addr, err := b.rpc.GetEndorsingSlotOwner(ctx,
			rpc.BlockLevel(end.Operations.Level),
			end.Operations.Slot,
			b.block.Params,
		)
		if err != nil {
			return err
		}
		if addr.IsValid() {
			if acc, ok := b.BakerByAddress(addr); !ok {
				return Errorf("missing offender account %s", addr)
			} else {
				offender = acc
			}
		}
	}

	// build flows first to determine burn
	flows := b.NewPenaltyFlows(b.block.Proposer, offender, upd, id)

	// build op
	op := model.NewOp(b.block, id)
	op.IsSuccess = true
	op.Status = tezos.OpStatusApplied
	op.SenderId = b.block.Proposer.AccountId
	if offender != nil {
		op.ReceiverId = offender.AccountId
	}

	// we store double-endorsed evidences as JSON
	buf, err := json.Marshal(dop.Strip())
	if err != nil {
		log.Error(Errorf("cannot write data: %v", err))
	} else {
		op.Data = string(buf)
	}

	// calc burn from flows
	for _, f := range flows {
		if f.IsBurned {
			// track all burned coins
			op.Burned += f.AmountOut

			// track offender losses by category
			switch f.Kind {
			case model.FlowKindRewards:
				op.Reward -= f.AmountOut
			case model.FlowKindDeposits:
				op.Deposit -= f.AmountOut
			case model.FlowKindFees:
				op.Fee -= f.AmountOut
			case model.FlowKindStake:
				op.Deposit -= f.AmountOut
			}
		} else {
			// track accuser reward as volume
			op.Volume += f.AmountIn
			op.Burned -= f.AmountIn
		}
	}
	b.block.Ops = append(b.block.Ops, op)

	// update accounts
	if !rollback {
		b.block.Proposer.NBakerOps++
		b.block.Proposer.NAccusations++
		b.block.Proposer.IsDirty = true
		acc := b.block.Proposer.Account
		acc.LastSeen = b.block.Height
		acc.IsDirty = true

		if offender != nil {
			offender.NBakerOps++
			offender.N2Endorsement++
			offender.IsDirty = true
			acc := offender.Account
			acc.LastSeen = b.block.Height
			acc.IsDirty = true

			// apply losses to unstake requests
			// - bakers can have unstake requests too
			// - accounts referenced in unstake may not be pre-loaded
			// - emit a slash event
			// p := b.block.Params
			// if reqs, err := b.idx.ListFrozenUnstakeRequests(ctx, offender.AccountId, op.Cycle, p); err == nil {
			// 	log.Warnf("%d slashing %d unstake requests from C%d to C%d", op.Height, len(reqs), op.Cycle-p.PreservedCycles-p.MaxSlashingPeriod+1, op.Cycle)
			// 	for _, req := range reqs {
			// 		log.Warnf("Slashing unstake request op=%d src=%d amount=%d slash=%d",
			// 			req.RowId, req.SenderId, req.Volume, req.Volume/2,
			// 		)
			// 		acc, err := b.LoadAccountByAccountId(ctx, req.SenderId)
			// 		if err != nil {
			// 			return Errorf("missing account %d with unstake request %d", req.SenderId, req.RowId)
			// 		}
			// 		// 50% loss
			// 		acc.UnstakedBalance -= req.Volume / 2
			// 		acc.LostStake += req.Volume / 2
			// 		acc.IsDirty = true

			// 		// emit slash event for non-bakers
			// 		if !acc.IsBaker {
			// 			slash := model.NewEventOp(b.block, req.SenderId, id)
			// 			slash.Type = model.OpTypeStakeSlash
			// 			slash.SenderId = req.SenderId
			// 			slash.BakerId = offender.AccountId
			// 			slash.Burned = req.Volume / 2
			// 			b.block.Ops = append(b.block.Ops, slash)
			// 		}
			// 	}
			// }

		}

	} else {
		b.block.Proposer.NBakerOps--
		b.block.Proposer.NAccusations--
		b.block.Proposer.IsDirty = true
		acc := b.block.Proposer.Account
		acc.LastSeen = max(acc.LastSeen, acc.LastIn, acc.LastOut)
		acc.IsDirty = true

		if offender != nil {
			offender.NBakerOps--
			offender.N2Endorsement--
			offender.IsDirty = true
			acc := offender.Account
			acc.LastSeen = max(acc.LastSeen, acc.LastIn, acc.LastOut)
			acc.IsDirty = true

			// reverse-apply losses to unstake requests
			// - bakers can have unstake requests too
			// - accounts referenced in unstake may not be pre-loaded
			// - emit a slash event
			// if reqs, err := b.idx.ListFrozenUnstakeRequests(ctx, offender.AccountId, op.Cycle, b.block.Params); err == nil {
			// 	for _, req := range reqs {
			// 		acc, err := b.LoadAccountByAccountId(ctx, req.SenderId)
			// 		if err != nil {
			// 			return Errorf("missing account %d with unstake request %d", req.SenderId, req.RowId)
			// 		}
			// 		// 50% loss
			// 		acc.UnstakedBalance += req.Volume / 2
			// 		acc.LostStake -= req.Volume / 2
			// 		acc.IsDirty = true
			// 	}
			// }
		}
	}
	return nil
}

func (b *Builder) AppendDepositsLimitOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
		)
	}

	gop, ok := o.(*rpc.SetDepositsLimit)
	if !ok {
		return Errorf("unexpected type %T", o)
	}

	sender, ok := b.AccountByAddress(gop.Source)
	if !ok {
		return Errorf("missing sender account %s", gop.Source)
	}

	res := gop.Result()
	src, ok := b.BakerByAddress(gop.Source)
	if !ok && res.Status.IsSuccess() {
		return Errorf("missing source baker account %s", gop.Source)
	}

	// build op
	op := model.NewOp(b.block, id)
	op.SenderId = sender.RowId
	op.Counter = gop.Counter
	op.Fee = gop.Fee
	op.GasLimit = gop.GasLimit
	op.StorageLimit = gop.StorageLimit

	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.Gas()
	op.StoragePaid = res.PaidStorageSizeDiff
	if gop.Limit != nil {
		op.Data = strconv.FormatInt(*gop.Limit, 10)
	}

	_ = b.NewSetDepositsLimitFlows(sender, gop.Fees(), id)

	if op.IsSuccess {
		if gop.Limit != nil {
			src.DepositsLimit = *gop.Limit
		} else {
			src.DepositsLimit = -1
		}
	} else {
		// handle errors
		if len(res.Errors) > 0 {
			if buf, err := json.Marshal(res.Errors); err == nil {
				op.Errors = buf
			} else {
				// non-fatal, but error data will be missing from index
				log.Error(Errorf("marshal op errors: %s", err))
			}
		}
	}

	b.block.Ops = append(b.block.Ops, op)

	// update sender account
	if !rollback {
		if src != nil {
			src.NBakerOps++
			src.NSetDepositsLimit++
			src.IsDirty = true
		}
		sender.Counter = op.Counter
		sender.LastSeen = b.block.Height
		sender.IsDirty = true
	} else {
		sender.Counter = op.Counter - 1
		sender.IsDirty = true
		if src != nil {
			src.NBakerOps--
			src.NSetDepositsLimit--
			src.IsDirty = true
		}
	}

	return nil
}

func (b *Builder) AppendDrainDelegateOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
		)
	}

	dop, ok := o.(*rpc.DrainDelegate)
	if !ok {
		return Errorf("unexpected type %T", o)
	}

	// baker who is drained
	baker, ok := b.BakerByAddress(dop.Delegate)
	if !ok {
		return Errorf("missing drained baker account %s", dop.Delegate)
	}

	// no sender, but destination is receiver
	dst, ok := b.AccountByAddress(dop.Destination)
	if !ok {
		return Errorf("missing destination account %s", dop.Destination)
	}

	// load destination baker
	var dbkr *model.Baker
	if dst.BakerId != 0 {
		if dbkr, ok = b.BakerById(dst.BakerId); !ok {
			return Errorf("missing baker %d for dest account %d", dst.BakerId, dst.RowId)
		}
	}

	// build op
	op := model.NewOp(b.block, id)
	op.SenderId = baker.Account.RowId
	op.ReceiverId = dst.RowId
	op.BakerId = b.block.ProposerId
	op.Status = tezos.OpStatusApplied
	op.IsSuccess = true
	op.Data = dop.ConsensusKey.String()
	b.block.Ops = append(b.block.Ops, op)

	// extract flows from balance updates
	op.Volume, op.Reward, _ = b.NewDrainDelegateFlows(baker.Account, dst, dbkr, dop.Fees(), id)

	// update accounts
	if !rollback {
		dst.LastSeen = b.block.Height
		dst.IsDirty = true
		baker.NBakerOps++
		baker.NDrainDelegate++
		baker.IsDirty = true
	} else {
		baker.NBakerOps--
		baker.NDrainDelegate--
		baker.IsDirty = true
	}

	return nil
}

func (b *Builder) AppendUpdateConsensusKeyOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
		)
	}

	uop, ok := o.(*rpc.UpdateConsensusKey)
	if !ok {
		return Errorf("unexpected type %T", o)
	}

	// sender must be a baker
	sender, ok := b.AccountByAddress(uop.Source)
	if !ok {
		return Errorf("missing sender account %s", uop.Source)
	}

	res := uop.Result()
	baker, ok := b.BakerByAddress(uop.Source)
	if !ok && res.Status.IsSuccess() {
		return Errorf("missing source baker account %s", uop.Source)
	}

	// build op
	op := model.NewOp(b.block, id)
	op.SenderId = sender.RowId
	op.Counter = uop.Counter
	op.Fee = uop.Fee
	op.GasLimit = uop.GasLimit
	op.StorageLimit = uop.StorageLimit
	op.Status = res.Status
	op.IsSuccess = res.Status.IsSuccess()
	op.GasUsed = res.Gas()
	op.StoragePaid = res.PaidStorageSizeDiff
	op.Data = uop.Pk.String()
	b.block.Ops = append(b.block.Ops, op)

	_ = b.NewUpdateConsensusKeyFlows(sender, uop.Fees(), id)

	if op.IsSuccess {
		baker.ConsensusKey = uop.Pk
	} else {
		op.Errors, _ = json.Marshal(res.Errors)
	}

	// update sender account
	if !rollback {
		sender.Counter = op.Counter
		sender.LastSeen = b.block.Height
		sender.IsDirty = true
		if op.IsSuccess {
			sender.NTxOut++
			sender.NTxSuccess++
		} else {
			sender.NTxFailed++
		}
		if op.IsSuccess && baker != nil {
			baker.NBakerOps++
			baker.NUpdateConsensusKey++
			baker.IsDirty = true
		}
	} else {
		sender.Counter = op.Counter - 1
		sender.IsDirty = true
		if op.IsSuccess {
			sender.NTxOut--
			sender.NTxSuccess--
		} else {
			sender.NTxFailed--
		}
		if op.IsSuccess && baker != nil {
			baker.NBakerOps--
			baker.NUpdateConsensusKey--
			baker.IsDirty = true
		}
	}

	return nil
}
