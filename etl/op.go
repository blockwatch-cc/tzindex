// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/index"
	. "blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

func (b *Builder) NewActivationOp(ctx context.Context, oh *rpc.OperationHeader, op_n, op_c int, rollback bool) error {
	o := oh.Contents[op_c]
	aop, ok := o.(*rpc.AccountActivationOp)
	if !ok {
		return fmt.Errorf("activation op [%d:%d]: unexpected type %T ", op_n, op_c, o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return fmt.Errorf("activation op [%d:%d]: missing branch %s", oh.Branch)
	}

	// need to lookup using blinded key
	bkey, err := chain.BlindAddress(aop.Pkh, aop.Secret)
	if err != nil {
		return fmt.Errorf("activation op [%d:%d]: blinded address creation failed: %v",
			op_n, op_c, err)
	}

	acc, ok := b.AccountByAddress(bkey)
	if !ok {
		return fmt.Errorf("activation op [%d:%d]: missing account %s", op_n, op_c, aop.Pkh)
	}

	// cross-check if account exists under it's implicity address
	origacc, ok := b.AccountByAddress(aop.Pkh)
	if !ok {
		origacc, _ = b.idx.LookupAccount(ctx, aop.Pkh)
	}

	// check activated amount against UnclaimedBalance
	activated := aop.Metadata.BalanceUpdates[0].(*rpc.ContractBalanceUpdate).Change

	// build op
	op := NewOp(b.block, branch, oh, op_n, op_c, 0)
	op.IsSuccess = true
	op.Status = chain.OpStatusApplied
	op.Volume = activated
	op.SenderId = acc.RowId
	op.HasData = true
	op.Data = hex.EncodeToString(aop.Secret) + "," + bkey.String()
	b.block.Ops = append(b.block.Ops, op)

	// update account
	if !rollback {
		// remove blinded hash from builder
		key := accountHashKey(acc)
		delete(b.accHashMap, key)

		// merge with original account, empty blinded account
		if origacc != nil {
			// move funds and deactivate blinded account
			origacc.UnclaimedBalance = acc.UnclaimedBalance
			origacc.NOps++
			origacc.IsActivated = true
			origacc.IsDirty = true
			op.ReceiverId = origacc.RowId // keep reference to activated account
			acc.UnclaimedBalance = 0
			acc.LastSeen = b.block.Height
			acc.IsSpendable = false
			acc.IsActivated = true
			acc.NOps++
			acc.IsDirty = true

			// register original account with builder
			b.accHashMap[accountHashKey(origacc)] = origacc
			b.accMap[origacc.RowId] = origacc

			// use original account from now
			acc = origacc
		} else {
			// update blinded account with new hash
			acc.Hash = aop.Pkh.Hash
			acc.Type = aop.Pkh.Type
			acc.FirstSeen = b.block.Height
			acc.IsActivated = true
			acc.IsSpendable = true
			acc.IsFunded = true
			acc.IsDirty = true
			b.accHashMap[accountHashKey(acc)] = acc
		}
	} else {
		// check if deactivated blinded account exists
		blindedacc, _ := b.idx.LookupAccount(ctx, bkey)
		if blindedacc != nil {
			// reactivate blinded account
			blindedacc.SpendableBalance = activated

			// register blinded account with builder
			b.accHashMap[accountHashKey(blindedacc)] = blindedacc
			b.accMap[blindedacc.RowId] = blindedacc

			// rollback current account (adjust spendable balance here!)
			acc.SpendableBalance -= activated
			acc.NOps--
			acc.IsActivated = false
			acc.IsDirty = true

			// use blinded account for flow updates
			acc = blindedacc
		} else {
			acc.NOps--
			acc.Hash = bkey.Hash
			acc.Type = bkey.Type
			acc.IsActivated = false
			acc.IsDirty = true
			acc.FirstSeen = 1 // reset to genesis
			// replace implicit hash with blinded hash
			key := accountHashKey(acc)
			delete(b.accHashMap, key)
			b.accHashMap[accountHashKey(acc)] = acc
		}
	}

	// build flows
	_, err = b.NewActivationFlow(acc, aop)
	if err != nil {
		return err
	}
	return nil
}

func (b *Builder) NewEndorsementOp(ctx context.Context, oh *rpc.OperationHeader, op_n, op_c int, rollback bool) error {
	o := oh.Contents[op_c]
	eop, ok := o.(*rpc.EndorsementOp)
	if !ok {
		return fmt.Errorf("endorsement op [%d:%d]: unexpected type %T ", op_n, op_c, o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return fmt.Errorf("endorsement op [%d:%d]: missing branch %s", oh.Branch)
	}
	acc, ok := b.AccountByAddress(eop.Metadata.Delegate)
	if !ok {
		return fmt.Errorf("endorsement op [%d:%d]: missing account %s ", op_n, op_c, eop.Metadata.Delegate)
	}

	// build flows
	flows, err := b.NewEndorserFlows(acc, eop.Metadata.BalanceUpdates)
	if err != nil {
		return err
	}

	// build op
	op := NewOp(b.block, branch, oh, op_n, op_c, 0)
	op.Status = chain.OpStatusApplied
	op.IsSuccess = true
	op.SenderId = acc.RowId

	// extend grace period
	if acc.IsActiveDelegate {
		acc.UpdateGracePeriod(b.block.Cycle, b.block.Params)
	} else {
		acc.InitGracePeriod(b.block.Cycle, b.block.Params)
	}

	// store endorsed slots as data
	op.HasData = true
	var slotmask uint32
	for _, v := range eop.Metadata.Slots {
		slotmask |= 1 << uint(v)
	}
	op.Data = strconv.FormatUint(uint64(slotmask), 10)

	// fill op amounts from flows
	for _, f := range flows {
		switch f.Category {
		case FlowCategoryRewards:
			op.Reward += f.AmountIn
		case FlowCategoryDeposits:
			op.Deposit += f.AmountIn
		case FlowCategoryBalance:
			// don't count internal flows against volume
		}
	}
	b.block.Ops = append(b.block.Ops, op)

	// update account
	if !rollback {
		acc.NOps++
		acc.BlocksEndorsed++
		acc.SlotsEndorsed += len(eop.Metadata.Slots)
		acc.IsActiveDelegate = true // reset inactivity unconditionally
		acc.IsDirty = true
	} else {
		acc.NOps--
		acc.BlocksEndorsed--
		acc.SlotsEndorsed -= len(eop.Metadata.Slots)
		// don't update inactivity because we don't know its previous state
		acc.IsDirty = true
	}
	return nil
}

// this is a generic op only, details are in governance table
func (b *Builder) NewBallotOp(ctx context.Context, oh *rpc.OperationHeader, op_n, op_c int, rollback bool) error {
	o := oh.Contents[op_c]
	bop, ok := o.(*rpc.BallotOp)
	if !ok {
		return fmt.Errorf("ballot op [%d:%d]: unexpected type %T ", op_n, op_c, o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return fmt.Errorf("ballot op [%d:%d]: missing branch %s", oh.Branch)
	}
	acc, ok := b.AccountByAddress(bop.Source)
	if !ok {
		return fmt.Errorf("ballot op [%d:%d]: missing account %s ", op_n, op_c, bop.Source)
	}

	// build op, ballots have no fees, volume, gas, etc
	op := NewOp(b.block, branch, oh, op_n, op_c, 0)
	op.Status = chain.OpStatusApplied
	op.IsSuccess = true
	op.SenderId = acc.RowId

	// store protocol and vote as string: `protocol,vote`
	op.Data = bop.Proposal.String() + "," + bop.Ballot.String()
	op.HasData = true

	b.block.Ops = append(b.block.Ops, op)

	// update account
	if !rollback {
		acc.NOps++
		acc.NBallot++
		acc.IsDirty = true
		acc.LastSeen = b.block.Height
	} else {
		acc.NOps--
		acc.NBallot--
		acc.IsDirty = true
		acc.LastSeen = util.Max64N(acc.LastSeen, acc.LastIn, acc.LastOut) // approximation only
	}
	return nil
}

// this is a generic op only, details are in governance table
func (b *Builder) NewProposalsOp(ctx context.Context, oh *rpc.OperationHeader, op_n, op_c int, rollback bool) error {
	o := oh.Contents[op_c]
	pop, ok := o.(*rpc.ProposalsOp)
	if !ok {
		return fmt.Errorf("proposals op [%d:%d]: unexpected type %T ", op_n, op_c, o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return fmt.Errorf("proposals op [%d:%d]: missing branch %s", oh.Branch)
	}
	acc, ok := b.AccountByAddress(pop.Source)
	if !ok {
		return fmt.Errorf("proposals op [%d:%d]: missing account %s ", op_n, op_c, pop.Source)
	}

	// build op, proposals have no fees, volume, gas, etc
	op := NewOp(b.block, branch, oh, op_n, op_c, 0)
	op.Status = chain.OpStatusApplied
	op.IsSuccess = true
	op.SenderId = acc.RowId

	// store proposals as comma separated base58 strings (same format as in JSON RPC)
	op.HasData = true
	buf := bytes.NewBuffer(make([]byte, 0, len(pop.Proposals)*chain.HashTypeProtocol.Base58Len()-1))
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
		acc.NOps++
		acc.NProposal++
		acc.IsDirty = true
		acc.LastSeen = b.block.Height
	} else {
		acc.NOps--
		acc.NProposal--
		acc.IsDirty = true
		acc.LastSeen = util.Max64N(acc.LastSeen, acc.LastIn, acc.LastOut) // approximation only
	}
	return nil
}

// manager operation, extends grace period
func (b *Builder) NewRevealOp(ctx context.Context, oh *rpc.OperationHeader, op_n, op_c int, rollback bool) error {
	o := oh.Contents[op_c]
	rop, ok := o.(*rpc.RevelationOp)
	if !ok {
		return fmt.Errorf("revelation op [%d:%d]: unexpected type %T ", op_n, op_c, o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return fmt.Errorf("revelation op [%d:%d]: missing branch %s", oh.Branch)
	}
	src, ok := b.AccountByAddress(rop.Source)
	if !ok {
		return fmt.Errorf("revelation op [%d:%d]: missing account %s", op_n, op_c, rop.Source)
	}
	var dlg *Account
	if src.DelegateId != 0 {
		if dlg, ok = b.AccountById(src.DelegateId); !ok {
			return fmt.Errorf("revelation op [%d:%d]: missing delegate %d for source account %d",
				op_n, op_c, src.DelegateId, src.RowId)
		}
	}

	// build op
	op := NewOp(b.block, branch, oh, op_n, op_c, 0)
	op.SenderId = src.RowId
	op.Counter = rop.Counter
	op.Fee = rop.Fee
	op.GasLimit = rop.GasLimit
	op.StorageLimit = rop.StorageLimit
	op.HasData = true
	op.Data = rop.PublicKey.String()
	res := rop.Metadata.Result
	op.GasUsed = res.ConsumedGas
	if op.GasUsed > 0 && op.Fee > 0 {
		op.GasPrice = float64(op.Fee) / float64(op.GasUsed)
	}
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	_, err := b.NewRevealFlows(src, dlg, rop.Metadata.BalanceUpdates)
	if err != nil {
		return err
	}
	// extend grace period for delegates
	if src.IsActiveDelegate {
		src.UpdateGracePeriod(b.block.Cycle, b.block.Params)
	}

	// update accounts
	if !rollback {
		if !op.IsSuccess {
			src.NOps++
			src.NOpsFailed++
			if len(res.Errors) > 0 {
				if buf, err := json.Marshal(res.Errors); err == nil {
					op.Errors = string(buf)
				} else {
					log.Errorf("internal revelation op [%d:%d]: marshal op errors: %v", op_n, op_c, err)
				}
			}
		} else {
			src.NOps++
			src.IsRevealed = true
			src.PubkeyHash = rop.PublicKey.Hash
			src.PubkeyType = rop.PublicKey.Type
			src.IsDirty = true
		}
	} else {
		if !op.IsSuccess {
			src.NOps--
			src.NOpsFailed--
		} else {
			src.NOps--
			src.IsRevealed = false
			src.PubkeyHash = nil
			src.PubkeyType = chain.HashTypeInvalid
			src.IsDirty = true
		}
	}
	b.block.Ops = append(b.block.Ops, op)
	return nil
}

func (b *Builder) NewSeedNonceOp(ctx context.Context, oh *rpc.OperationHeader, op_n, op_c int, rollback bool) error {
	o := oh.Contents[op_c]
	sop, ok := o.(*rpc.SeedNonceOp)
	if !ok {
		return fmt.Errorf("seed nonce op [%d:%d]: unexpected type %T ", op_n, op_c, o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return fmt.Errorf("seed nonce op [%d:%d]: missing branch %s", oh.Branch)
	}

	flows, err := b.NewSeedNonceFlows(sop.Metadata.BalanceUpdates)
	if err != nil {
		return err
	}

	// build op
	op := NewOp(b.block, branch, oh, op_n, op_c, 0)
	op.Status = chain.OpStatusApplied
	op.IsSuccess = true
	op.SenderId = b.block.Baker.RowId
	op.HasData = true

	// data is `level,nonce`
	op.Data = strconv.FormatInt(sop.Level, 10) + "," + hex.EncodeToString(sop.Nonce)

	for _, f := range flows {
		op.Reward += f.AmountIn
	}
	b.block.Ops = append(b.block.Ops, op)

	if !rollback {
		b.block.Baker.NOps++
		b.block.Baker.IsDirty = true
	} else {
		b.block.Baker.NOps--
		b.block.Baker.IsDirty = true
	}

	return nil
}

func (b *Builder) NewDoubleBakingOp(ctx context.Context, oh *rpc.OperationHeader, op_n, op_c int, rollback bool) error {
	o := oh.Contents[op_c]
	dop, ok := o.(*rpc.DoubleBakingOp)
	if !ok {
		return fmt.Errorf("double baking op [%d:%d]: unexpected type %T ", op_n, op_c, o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return fmt.Errorf("double baking op [%d:%d]: missing branch %s", oh.Branch)
	}
	upd := dop.Metadata.BalanceUpdates
	accuser, ok := b.AccountByAddress(upd[len(upd)-1].(*rpc.FreezerBalanceUpdate).Delegate)
	if !ok {
		return fmt.Errorf("double baking op [%d:%d]: missing accuser account %s",
			op_n, op_c, upd[len(upd)-1].(*rpc.FreezerBalanceUpdate).Delegate)
	}
	offender, ok := b.AccountByAddress(upd[0].(*rpc.FreezerBalanceUpdate).Delegate)
	if !ok {
		return fmt.Errorf("double baking op [%d:%d]: missing offender account %s",
			op_n, op_c, upd[0].(*rpc.FreezerBalanceUpdate).Delegate)
	}

	// build flows first to determine burn
	flows, err := b.NewDoubleBakingFlows(accuser, offender, upd)
	if err != nil {
		return err
	}

	// build op
	op := NewOp(b.block, branch, oh, op_n, op_c, 0)
	op.IsSuccess = true
	op.Status = chain.OpStatusApplied
	op.SenderId = accuser.RowId
	op.ReceiverId = offender.RowId
	op.HasData = true
	// we store both block headers as json array
	bhs := []rpc.BlockHeader{dop.BH1, dop.BH2}
	buf, err := json.Marshal(bhs)
	if err != nil {
		return fmt.Errorf("double baking op [%d:%d]: cannot write data: %v", op_n, op_c, err)
	}
	op.Data = string(buf)

	// calc burn from flows
	for _, f := range flows {
		op.Burned += f.AmountOut - f.AmountIn
		op.Reward += f.AmountIn
		op.Volume += f.AmountOut
	}
	b.block.Ops = append(b.block.Ops, op)

	// update accounts
	if !rollback {
		accuser.NOps++
		accuser.IsDirty = true
		offender.NOps++
		offender.IsDirty = true
	} else {
		accuser.NOps--
		accuser.IsDirty = true
		offender.NOps--
		offender.IsDirty = true
	}

	return nil
}

func (b *Builder) NewDoubleEndorsingOp(ctx context.Context, oh *rpc.OperationHeader, op_n, op_c int, rollback bool) error {
	o := oh.Contents[op_c]
	dop, ok := o.(*rpc.DoubleEndorsementOp)
	if !ok {
		return fmt.Errorf("double endorse op [%d:%d]: unexpected type %T ", op_n, op_c, o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return fmt.Errorf("double endorse op [%d:%d]: missing branch %s", oh.Branch)
	}
	upd := dop.Metadata.BalanceUpdates
	accuser, ok := b.AccountByAddress(upd[len(upd)-1].(*rpc.FreezerBalanceUpdate).Delegate)
	if !ok {
		return fmt.Errorf("double endorse op [%d:%d]: missing accuser account %s",
			op_n, op_c, upd[len(upd)-1].(*rpc.FreezerBalanceUpdate).Delegate)
	}
	offender, ok := b.AccountByAddress(upd[0].(*rpc.FreezerBalanceUpdate).Delegate)
	if !ok {
		return fmt.Errorf("double endorse op [%d:%d]: missing offender account %s",
			op_n, op_c, upd[0].(*rpc.FreezerBalanceUpdate).Delegate)
	}

	// build flows first to determine burn
	flows, err := b.NewDoubleEndorsingFlows(accuser, offender, upd)
	if err != nil {
		return err
	}

	// build op
	op := NewOp(b.block, branch, oh, op_n, op_c, 0)
	op.IsSuccess = true
	op.Status = chain.OpStatusApplied
	op.SenderId = accuser.RowId
	op.ReceiverId = offender.RowId
	op.HasData = true
	// we store double-endorsed evidences as JSON
	dops := []rpc.DoubleEndorsementEvidence{dop.OP1, dop.OP2}
	buf, err := json.Marshal(dops)
	if err != nil {
		return fmt.Errorf("double endorse op [%d:%d]: cannot write data: %v", op_n, op_c, err)
	}
	op.Data = string(buf)

	// calc burn from flows
	for _, f := range flows {
		op.Burned += f.AmountOut - f.AmountIn
		op.Reward += f.AmountIn
		op.Volume += f.AmountOut
	}
	b.block.Ops = append(b.block.Ops, op)

	// update accounts
	if !rollback {
		accuser.NOps++
		accuser.IsDirty = true
		offender.NOps++
		offender.IsDirty = true
	} else {
		accuser.NOps--
		accuser.IsDirty = true
		offender.NOps--
		offender.IsDirty = true
	}

	return nil
}

// can implicitly burn a fee when new account is created
// manager operation, does not extend grace period
func (b *Builder) NewTransactionOp(ctx context.Context, oh *rpc.OperationHeader, op_n, op_c int, rollback bool) error {
	o := oh.Contents[op_c]
	top, ok := o.(*rpc.TransactionOp)
	if !ok {
		return fmt.Errorf("transaction op [%d:%d]: unexpected type %T ", op_n, op_c, o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return fmt.Errorf("transaction op [%d:%d]: missing branch %s", oh.Branch)
	}
	src, ok := b.AccountByAddress(top.Source)
	if !ok {
		return fmt.Errorf("transaction op [%d:%d]: missing source account %s", op_n, op_c, top.Source)
	}
	dst, ok := b.AccountByAddress(top.Destination)
	if !ok {
		return fmt.Errorf("transaction op [%d:%d]: missing source account %s", op_n, op_c, top.Destination)
	}
	var srcdlg, dstdlg *Account
	if src.DelegateId != 0 {
		if srcdlg, ok = b.AccountById(src.DelegateId); !ok {
			return fmt.Errorf("transaction op [%d:%d]: missing delegate %d for source account %d",
				op_n, op_c, src.DelegateId, src.RowId)
		}
	}
	if dst.DelegateId != 0 {
		if dstdlg, ok = b.AccountById(dst.DelegateId); !ok {
			return fmt.Errorf("transaction op [%d:%d]: missing delegate %d for dest account %d",
				op_n, op_c, dst.DelegateId, dst.RowId)
		}
	}

	// build op
	op := NewOp(b.block, branch, oh, op_n, op_c, 0)
	op.SenderId = src.RowId
	op.ReceiverId = dst.RowId
	op.Counter = top.Counter
	op.Fee = top.Fee
	op.GasLimit = top.GasLimit
	op.StorageLimit = top.StorageLimit
	op.IsContract = dst.IsContract
	res := top.Metadata.Result
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.ConsumedGas
	if op.GasUsed > 0 && op.Fee > 0 {
		op.GasPrice = float64(op.Fee) / float64(op.GasUsed)
	}
	op.HasData = res.Storage != nil || top.Parameters != nil || len(res.BigMapDiff) > 0

	var err error
	if top.Parameters != nil {
		op.Parameters, err = top.Parameters.MarshalBinary()
		if err != nil {
			return fmt.Errorf("transaction op [%d:%d]: marshal params: %v", op_n, op_c, err)
		}
	}
	if res.Storage != nil {
		op.Storage, err = res.Storage.MarshalBinary()
		if err != nil {
			return fmt.Errorf("transaction op [%d:%d]: marshal storage: %v", op_n, op_c, err)
		}
	}
	if len(res.BigMapDiff) > 0 {
		top.Metadata.Result.BigMapDiff, err = b.PatchBigMapDiff(ctx, res.BigMapDiff, op.ReceiverId, nil)
		if err != nil {
			return fmt.Errorf("transaction op [%d:%d]: patch bigmap: %v", op_n, op_c, err)
		}
		op.BigMapDiff, err = top.Metadata.Result.BigMapDiff.MarshalBinary()
		if err != nil {
			return fmt.Errorf("transaction op [%d:%d]: marshal bigmap: %v", op_n, op_c, err)
		}
	}

	var flows []*Flow

	if op.IsSuccess {
		op.Volume = top.Amount
		op.StorageSize = res.StorageSize
		op.StoragePaid = res.PaidStorageSizeDiff // paid for all extra storage

		flows, err = b.NewTransactionFlows(src, dst, srcdlg, dstdlg,
			top.Metadata.BalanceUpdates, // fees
			res.BalanceUpdates,          // move
			b.block,
		)
		if err != nil {
			return err
		}

		// update burn from burn flow (for implicit originated contracts)
		for _, f := range flows {
			if f.IsBurned {
				op.Burned += f.AmountOut
			}
		}

		// when funds were moved into a new account, calc token days destroyed
		if op.Volume > 0 {
			// token age in block offsets times target block duration
			diffsec := b.block.Age(src.LastIn)
			// convert seconds to days and volume from atomic units to coins
			op.TDD += float64(diffsec) / 86400 * b.block.Params.ConvertValue(op.Volume)
		}

	} else {
		// handle errors
		if len(res.Errors) > 0 {
			if buf, err := json.Marshal(res.Errors); err == nil {
				op.Errors = string(buf)
			} else {
				log.Errorf("transaction op [%d:%d]: marshal op errors: %v", op_n, op_c, err)
			}
		}

		// fees only
		flows, err = b.NewTransactionFlows(src, nil, srcdlg, nil,
			top.Metadata.BalanceUpdates,
			nil, // no result balance updates
			b.block,
		)
		if err != nil {
			return err
		}
	}

	// update accounts
	if !rollback {
		if !op.IsSuccess {
			src.NOps++
			src.NTx++
			src.NOpsFailed++
			src.IsDirty = true
			dst.NOps++
			dst.NTx++
			dst.NOpsFailed++
			dst.IsDirty = true
		} else {
			src.NOps++
			src.NTx++
			src.IsDirty = true
			dst.NOps++
			dst.NTx++
			dst.IsDirty = true
			if res.Allocated {
				// init dest account when allocated
				dst.IsSpendable = true
				dst.IsDelegatable = dst.Type == chain.AddressTypeContract
			}
			if dst.IsDelegate && b.block.Params.ReactivateByTx {
				// reactivate inactive delegates (receiver only)
				// - it seems from reverse engineering delegate activation rules
				//   that received transactions will reactivate an inactive delegate
				//   and extend grace period for active delegates
				// - support for this feature ends with proto_004
				if !dst.IsActiveDelegate {
					dst.IsActiveDelegate = true
					dst.InitGracePeriod(b.block.Cycle, b.block.Params)
				} else {
					dst.UpdateGracePeriod(b.block.Cycle, b.block.Params)
				}
			}
			// update last seen for contracts (flow might be missing)
			if op.IsContract {
				dst.LastSeen = b.block.Height
			}
		}
	} else {
		if !op.IsSuccess {
			src.NOps--
			src.NTx--
			src.NOpsFailed--
			src.IsDirty = true
			dst.NOps--
			dst.NTx--
			dst.NOpsFailed--
			dst.IsDirty = true
		} else {
			src.NOps--
			src.NTx--
			src.IsDirty = true
			dst.NOps--
			dst.NTx--
			dst.IsDirty = true
			if res.Allocated {
				dst.MustDelete = true
			}
		}
	}

	// append before potential internal ops
	b.block.Ops = append(b.block.Ops, op)

	// apply internal operation result (may generate new op and flows)
	for i, v := range top.Metadata.InternalResults {
		switch v.OpKind() {
		case chain.OpTypeTransaction:
			if err := b.NewInternalTransactionOp(ctx, src, srcdlg, oh, v, op_n, op_c, i, rollback); err != nil {
				return err
			}
		case chain.OpTypeDelegation:
			if err := b.NewInternalDelegationOp(ctx, src, srcdlg, oh, v, op_n, op_c, i, rollback); err != nil {
				return err
			}
		case chain.OpTypeOrigination:
			if err := b.NewInternalOriginationOp(ctx, src, srcdlg, oh, v, op_n, op_c, i, rollback); err != nil {
				return err
			}
		default:
			return fmt.Errorf("internal op [%d:%d]: unsupported internal operation type %s",
				op_n, op_c, v.OpKind())
		}
	}
	return nil
}

func (b *Builder) NewInternalTransactionOp(ctx context.Context, origsrc, origdlg *Account, oh *rpc.OperationHeader, iop *rpc.InternalResult, op_n, op_c, op_i int, rollback bool) error {
	src, ok := b.AccountByAddress(iop.Source)
	if !ok {
		return fmt.Errorf("internal transaction op [%d:%d]: missing source account %s", op_n, op_c, iop.Source)
	}
	dst, ok := b.AccountByAddress(*iop.Destination)
	if !ok {
		return fmt.Errorf("internal transaction op [%d:%d]: missing source account %s", op_n, op_c, iop.Destination)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return fmt.Errorf("internal transaction op [%d:%d]: missing branch %s", oh.Branch)
	}
	var srcdlg, dstdlg *Account
	if src.DelegateId != 0 {
		if srcdlg, ok = b.AccountById(src.DelegateId); !ok {
			return fmt.Errorf("internal transaction op [%d:%d]: missing delegate %d for source account %d",
				op_n, op_c, src.DelegateId, src.RowId)
		}
	}
	if dst.DelegateId != 0 {
		if dstdlg, ok = b.AccountById(dst.DelegateId); !ok {
			return fmt.Errorf("internal transaction op [%d:%d]: missing delegate %d for dest account %d",
				op_n, op_c, dst.DelegateId, dst.RowId)
		}
	}

	// build op (internal and outer tx share the same hash and block location)
	op := NewOp(b.block, branch, oh, op_n, op_c, op_i)
	op.Type = chain.OpTypeTransaction
	op.IsInternal = true
	op.SenderId = src.RowId
	op.ReceiverId = dst.RowId
	op.Counter = iop.Nonce
	op.Fee = 0          // n.a. for internal ops
	op.GasLimit = 0     // n.a. for internal ops
	op.StorageLimit = 0 // n.a. for internal ops
	op.IsContract = dst.IsContract
	res := iop.Result
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.ConsumedGas
	op.HasData = iop.Parameters != nil || res.Storage != nil || len(res.BigMapDiff) > 0

	var err error
	if iop.Parameters != nil {
		op.Parameters, err = iop.Parameters.MarshalBinary()
		if err != nil {
			return fmt.Errorf("internal transaction op [%d:%d]: marshal params: %v", op_n, op_c, err)
		}
	}
	if res.Storage != nil {
		op.Storage, err = res.Storage.MarshalBinary()
		if err != nil {
			return fmt.Errorf("internal transaction op [%d:%d]: marshal storage: %v", op_n, op_c, err)
		}
	}
	if len(res.BigMapDiff) > 0 {
		iop.Result.BigMapDiff, err = b.PatchBigMapDiff(ctx, res.BigMapDiff, op.ReceiverId, nil)
		if err != nil {
			return fmt.Errorf("internal transaction op [%d:%d]: patch bigmap: %v", op_n, op_c, err)
		}
		op.BigMapDiff, err = iop.Result.BigMapDiff.MarshalBinary()
		if err != nil {
			return fmt.Errorf("internal transaction op [%d:%d]: marshal bigmap: %v", op_n, op_c, err)
		}
	}

	var flows []*Flow

	// on success, create flows and update accounts
	if op.IsSuccess {
		op.Volume = iop.Amount
		op.StorageSize = res.StorageSize
		op.StoragePaid = res.PaidStorageSizeDiff // paid for all extra storage

		// Note: need to use storage from the outer operation result
		flows, err = b.NewInternalTransactionFlows(
			origsrc, src, dst, // outer and inner source, inner dest
			origdlg, srcdlg, dstdlg, // delegates
			res.BalanceUpdates, // moved and bruned amounts
			oh.Contents[op_c].(*rpc.TransactionOp).Metadata.Result.Storage, // updated contract storage
			b.block,
		)
		if err != nil {
			return err
		}

		// update burn from burn flow (for storage paid)
		for _, f := range flows {
			if f.IsBurned {
				op.Burned += f.AmountOut
			}
		}

		// when funds were moved into a new account, calc token days destroyed
		if op.Volume > 0 {
			// token age in block offsets times target block duration
			diffsec := b.block.Age(src.LastIn)
			// convert seconds to days and volume from atomic units to coins
			op.TDD += float64(diffsec) / 86400 * b.block.Params.ConvertValue(op.Volume)
		}
	} else {
		// handle errors
		if len(res.Errors) > 0 {
			if buf, err := json.Marshal(res.Errors); err == nil {
				op.Errors = string(buf)
			} else {
				log.Errorf("internal transaction op [%d:%d]: marshal op errors: %v", op_n, op_c, err)
			}
		}
		// a negative outcome leaves no trace because fees are payed by outer tx
	}

	// update accounts
	if !rollback {
		if !op.IsSuccess {
			src.NOps++
			src.NTx++
			src.NOpsFailed++
			src.IsDirty = true
			dst.NOps++
			dst.NTx++
			dst.NOpsFailed++
			dst.IsDirty = true
		} else {
			src.NOps++
			src.NTx++
			src.IsDirty = true
			dst.NOps++
			dst.NTx++
			dst.IsDirty = true

			if dst.IsDelegate && b.block.Params.ReactivateByTx {
				// reactivate inactive delegates (receiver only)
				// - it seems from reverse engineering delegate activation rules
				//   that received transactions will reactivate an inactive delegate
				if !dst.IsActiveDelegate {
					dst.IsActiveDelegate = true
					dst.InitGracePeriod(b.block.Cycle, b.block.Params)
				} else {
					dst.UpdateGracePeriod(b.block.Cycle, b.block.Params)
				}
			}
			// update last seen for contracts (flow might be missing)
			if op.IsContract {
				dst.LastSeen = b.block.Height
			}
		}
	} else {
		if !op.IsSuccess {
			src.NOps--
			src.NTx--
			src.NOpsFailed--
			src.IsDirty = true
			dst.NOps--
			dst.NTx--
			dst.NOpsFailed--
			dst.IsDirty = true
		} else {
			src.NOps--
			src.NTx--
			src.IsDirty = true
			dst.NOps--
			dst.NTx--
			dst.IsDirty = true
		}
	}
	b.block.Ops = append(b.block.Ops, op)
	return nil
}

// - manager operation, does not extend grace period
// - burns a fee (optional, not used early on)
// - can delegate funds
// - only originated accounts (KT1) can delegate
// - only implicit accounts (tz1) can be delegates
// - by default originated accounts are not delegatable (but initial delegate can be set)
func (b *Builder) NewOriginationOp(ctx context.Context, oh *rpc.OperationHeader, op_n, op_c int, rollback bool) error {
	o := oh.Contents[op_c]
	oop, ok := o.(*rpc.OriginationOp)
	if !ok {
		return fmt.Errorf("origination op [%d:%d]: unexpected type %T ", op_n, op_c, o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return fmt.Errorf("origination op [%d:%d]: missing branch %s", oh.Branch)
	}
	src, ok := b.AccountByAddress(oop.Source)
	if !ok {
		return fmt.Errorf("origination op [%d:%d]: missing source account %s", op_n, op_c, oop.Source)
	}
	var mgr *Account
	if oop.ManagerPubkey.IsValid() {
		mgr, ok = b.AccountByAddress(oop.ManagerPubkey)
		if !ok {
			return fmt.Errorf("origination op [%d:%d]: missing manager account %s", op_n, op_c, oop.ManagerPubkey)
		}
	} else if oop.ManagerPubkey2.IsValid() {
		mgr, ok = b.AccountByAddress(oop.ManagerPubkey2)
		if !ok {
			return fmt.Errorf("origination op [%d:%d]: missing manager account %s", op_n, op_c, oop.ManagerPubkey2)
		}
	}
	var srcdlg, newdlg, dst *Account
	if src.DelegateId != 0 {
		if srcdlg, ok = b.AccountById(src.DelegateId); !ok {
			return fmt.Errorf("origination op [%d:%d]: missing delegate %d for source account %d",
				op_n, op_c, src.DelegateId, src.RowId)
		}
	}
	if oop.Delegate != nil {
		if newdlg, ok = b.AccountByAddress(*oop.Delegate); !ok && oop.Metadata.Result.Status.IsSuccess() {
			return fmt.Errorf("origination op [%d:%d]: missing delegate account %s",
				op_n, op_c, oop.Delegate)
		}
	}

	// build op
	op := NewOp(b.block, branch, oh, op_n, op_c, 0)
	op.SenderId = src.RowId
	op.Counter = oop.Counter
	op.Fee = oop.Fee
	op.GasLimit = oop.GasLimit
	op.StorageLimit = oop.StorageLimit
	op.IsContract = oop.Script != nil
	res := oop.Metadata.Result
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.ConsumedGas
	if op.GasUsed > 0 && op.Fee > 0 {
		op.GasPrice = float64(op.Fee) / float64(op.GasUsed)
	}
	op.HasData = len(res.BigMapDiff) > 0

	// store manager and delegate
	if mgr != nil {
		op.ManagerId = mgr.RowId
	}
	if newdlg != nil {
		op.DelegateId = newdlg.RowId
	}

	var (
		flows []*Flow
		err   error
	)
	if op.IsSuccess {
		op.Volume = oop.Balance
		op.StorageSize = res.StorageSize
		op.StoragePaid = res.PaidStorageSizeDiff

		if l := len(res.OriginatedContracts); l != 1 {
			return fmt.Errorf("origination op [%d:%d]: %d originated accounts", op_n, op_c, l)
		}

		dst, ok = b.AccountByAddress(res.OriginatedContracts[0])
		if !ok {
			return fmt.Errorf("origination op [%d:%d]: missing originated account %s", op_n, op_c, res.OriginatedContracts[0])
		}
		op.ReceiverId = dst.RowId
		flows, err = b.NewOriginationFlows(src, dst, srcdlg, newdlg,
			oop.Metadata.BalanceUpdates,
			res.BalanceUpdates,
		)
		if err != nil {
			return err
		}

		// update burn from burn flow
		for _, f := range flows {
			if f.IsBurned {
				op.Burned += f.AmountOut
			}
		}

		// when funds were moved into a new account, calc token days destroyed
		if op.Volume > 0 {
			// instead of real time we use block offsets and the target time
			// between blocks as time diff
			blocksec := int64(b.block.Params.TimeBetweenBlocks[0] / time.Second)
			diffsec := (b.block.Height - src.LastIn) * blocksec
			// convert seconds to days and volume from atomic units to coins
			op.TDD += float64(diffsec) / 86400 * b.block.Params.ConvertValue(op.Volume)
		}

		// create or extend bigmap diff to inject alloc for proto < v005
		oop.Metadata.Result.BigMapDiff, err = b.PatchBigMapDiff(ctx, res.BigMapDiff, op.ReceiverId, oop.Script)
		if err != nil {
			return fmt.Errorf("origination op [%d:%d]: patch bigmap: %v", op_n, op_c, err)
		}
		if len(oop.Metadata.Result.BigMapDiff) > 0 {
			op.BigMapDiff, err = oop.Metadata.Result.BigMapDiff.MarshalBinary()
			if err != nil {
				return fmt.Errorf("origination op [%d:%d]: marshal bigmap: %v", op_n, op_c, err)
			}
			op.HasData = true
		}

	} else {
		// handle errors
		if len(res.Errors) > 0 {
			if buf, err := json.Marshal(res.Errors); err == nil {
				op.Errors = string(buf)
			} else {
				log.Errorf("origination op [%d:%d]: marshal op errors: %v", op_n, op_c, err)
			}
		}

		// fees flows
		flows, err = b.NewOriginationFlows(src, nil, srcdlg, nil,
			oop.Metadata.BalanceUpdates, nil)
		if err != nil {
			return err
		}
	}

	b.block.Ops = append(b.block.Ops, op)

	// update accounts
	if !rollback {
		if !op.IsSuccess {
			src.NOps++
			src.NOrigination++
			src.NOpsFailed++
			src.IsDirty = true
		} else {
			src.NOps++
			src.NOrigination++
			src.IsDirty = true
			// initialize originated account
			// in babylon, keep the sender as manager regardless
			if mgr != nil {
				dst.ManagerId = mgr.RowId
			} else {
				dst.ManagerId = src.RowId
			}
			dst.IsContract = oop.Script != nil

			if b.block.Params.SilentSpendable {
				if oop.Spendable != nil {
					dst.IsSpendable = *oop.Spendable
				} else {
					dst.IsSpendable = true
				}
				if oop.Delegatable != nil {
					dst.IsDelegatable = *oop.Delegatable
				} else {
					dst.IsDelegatable = true
				}
			}
			dst.LastSeen = b.block.Height
			dst.IsDirty = true
			if newdlg != nil {
				// register self delegate if not registered yet (only before v002)
				if b.block.Params.HasOriginationBug && src.RowId == newdlg.RowId && !newdlg.IsDelegate {
					b.RegisterDelegate(newdlg)
				}

				dst.IsDelegated = true
				dst.DelegateId = newdlg.RowId
				dst.DelegatedSince = b.block.Height

				newdlg.TotalDelegations++
				if op.Volume > 0 {
					// delegation becomes active only when dst KT1 is funded
					newdlg.ActiveDelegations++
				}
				newdlg.IsDirty = true
			}
		}
	} else {
		if !op.IsSuccess {
			src.NOps--
			src.NOrigination--
			src.NOpsFailed--
			src.IsDirty = true
		} else {
			src.NOps--
			src.NOrigination--
			src.IsDirty = true
			// reverse delegation
			dst.MustDelete = true
			dst.IsDirty = true
			// only update when new delegate is a registered delegate
			if newdlg != nil && newdlg.IsDelegate {
				dst.IsDelegated = false
				dst.DelegateId = 0
				newdlg.TotalDelegations--
				if op.Volume > 0 {
					newdlg.ActiveDelegations--
				}
				newdlg.IsDirty = true
			}
			// handle self-delegate deregistration (note: there is no previous delegate)
			if b.block.Params.HasOriginationBug && newdlg != nil && newdlg.TotalDelegations == 0 && src.RowId == newdlg.RowId {
				b.UnregisterDelegate(newdlg)
			}
		}
	}

	return nil
}

// no manager
// no delegate
// no gas
// no more flags (delegatable, spendable)
func (b *Builder) NewInternalOriginationOp(ctx context.Context, origsrc, origdlg *Account, oh *rpc.OperationHeader, iop *rpc.InternalResult, op_n, op_c, op_i int, rollback bool) error {
	src, ok := b.AccountByAddress(iop.Source)
	if !ok {
		return fmt.Errorf("internal origination op [%d:%d:%d]: missing source account %s", op_n, op_c, op_i, iop.Source)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return fmt.Errorf("internal origination op [%d:%d]: missing branch %s", oh.Branch)
	}
	var srcdlg, dst *Account
	if src.DelegateId != 0 {
		if srcdlg, ok = b.AccountById(src.DelegateId); !ok {
			return fmt.Errorf("internal origination op [%d:%d:%d]: missing delegate %d for source account %d",
				op_n, op_c, op_i, src.DelegateId, src.RowId)
		}
	}

	// build op (internal and outer op share the same hash and block location)
	op := NewOp(b.block, branch, oh, op_n, op_c, op_i)
	op.IsInternal = true
	op.Type = chain.OpTypeOrigination
	op.SenderId = src.RowId
	op.Counter = iop.Nonce
	op.Fee = 0          // n.a. for internal ops
	op.GasLimit = 0     // n.a. for internal ops
	op.StorageLimit = 0 // n.a. for internal ops
	op.IsContract = iop.Script != nil
	res := iop.Result // same as transaction result
	op.Status = res.Status
	op.GasUsed = res.ConsumedGas
	op.IsSuccess = op.Status.IsSuccess()
	op.HasData = len(res.BigMapDiff) > 0

	var (
		flows []*Flow
		err   error
	)
	if op.IsSuccess {
		op.Volume = iop.Balance
		op.StorageSize = res.StorageSize
		op.StoragePaid = res.PaidStorageSizeDiff
		if l := len(res.OriginatedContracts); l != 1 {
			return fmt.Errorf("internal origination op [%d:%d:%d]: %d originated accounts", op_n, op_c, op_i, l)
		}

		dst, ok = b.AccountByAddress(res.OriginatedContracts[0])
		if !ok {
			return fmt.Errorf("internal origination op [%d:%d:%d]: missing originated account %s", op_n, op_c, op_i, res.OriginatedContracts[0])
		}
		op.ReceiverId = dst.RowId

		// no target delegate
		flows, err = b.NewInternalOriginationFlows(origsrc, src, dst, origdlg, srcdlg, res.BalanceUpdates)
		if err != nil {
			return err
		}

		// update burn from burn flow
		for _, f := range flows {
			if f.IsBurned {
				op.Burned += f.AmountOut
			}
		}

		// when funds were moved into a new account, calc token days destroyed
		if op.Volume > 0 {
			// instead of real time we use block offsets and the target time
			// between blocks as time diff
			blocksec := int64(b.block.Params.TimeBetweenBlocks[0] / time.Second)
			diffsec := (b.block.Height - src.LastIn) * blocksec
			// convert seconds to days and volume from atomic units to coins
			op.TDD += float64(diffsec) / 86400 * b.block.Params.ConvertValue(op.Volume)
		}

		// create or extend bigmap diff to inject alloc for proto < v005
		iop.Result.BigMapDiff, err = b.PatchBigMapDiff(ctx, res.BigMapDiff, op.ReceiverId, iop.Script)
		if err != nil {
			return fmt.Errorf("internal origination op [%d:%d]: patch bigmap: %v", op_n, op_c, err)
		}
		if len(iop.Result.BigMapDiff) > 0 {
			op.BigMapDiff, err = iop.Result.BigMapDiff.MarshalBinary()
			if err != nil {
				return fmt.Errorf("internal origination op [%d:%d]: marshal bigmap: %v", op_n, op_c, err)
			}
			op.HasData = true
		}

	} else {
		// handle errors
		if len(res.Errors) > 0 {
			if buf, err := json.Marshal(res.Errors); err == nil {
				op.Errors = string(buf)
			} else {
				log.Errorf("internal origination op [%d:%d:%d]: marshal op errors: %v",
					op_n, op_c, op_i, err)
			}
		}

		// no internal fees, no flows on failure
	}

	b.block.Ops = append(b.block.Ops, op)

	// update accounts
	if !rollback {
		if !op.IsSuccess {
			src.NOps++
			src.NOrigination++
			src.NOpsFailed++
			src.IsDirty = true
		} else {
			src.NOps++
			src.NOrigination++
			src.IsDirty = true

			// initialize originated account
			dst.LastSeen = b.block.Height
			dst.IsDirty = true

			// internal originations have no manager, delegate and flags (in protocol v5)
			// but we still keep the original caller as manager to track contract ownership
			dst.ManagerId = origsrc.RowId
		}
	} else {
		if !op.IsSuccess {
			src.NOps--
			src.NOrigination--
			src.NOpsFailed--
			src.IsDirty = true
		} else {
			src.NOps--
			src.NOrigination--
			src.IsDirty = true
			// reverse delegation
			dst.MustDelete = true
			dst.IsDirty = true
		}
	}

	return nil
}

// Notes
// - manager operation, inits, but does not extend grace period
// - delegations may or may not pay a fee, so BalanceUpdates may be empty
// - originations may delegate as well, so consider this there!
// - early delegations did neither pay nor consume gas
func (b *Builder) NewDelegationOp(ctx context.Context, oh *rpc.OperationHeader, op_n, op_c int, rollback bool) error {
	o := oh.Contents[op_c]
	dop, ok := o.(*rpc.DelegationOp)
	if !ok {
		return fmt.Errorf("delegation op [%d:%d]: unexpected type %T ", op_n, op_c, o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return fmt.Errorf("delegation op [%d:%d]: missing branch %s", oh.Branch)
	}
	src, ok := b.AccountByAddress(dop.Source)
	if !ok {
		return fmt.Errorf("delegation op [%d:%d]: missing account %s", op_n, op_c, dop.Source)
	}
	// on delegate withdraw, new delegate is empty!
	var ndlg, odlg *Account
	if dop.Delegate.IsValid() {
		ndlg, ok = b.AccountByAddress(dop.Delegate)
		if !ok && dop.Metadata.Result.Status.IsSuccess() {
			return fmt.Errorf("delegation op [%d:%d]: missing account %s", op_n, op_c, dop.Delegate)
		}
	}
	if src.DelegateId != 0 {
		if odlg, ok = b.AccountById(src.DelegateId); !ok {
			return fmt.Errorf("delegation op [%d:%d]: missing delegate %d for source account %d",
				op_n, op_c, src.DelegateId, src.RowId)
		}
	}

	// build op
	op := NewOp(b.block, branch, oh, op_n, op_c, 0)
	op.SenderId = src.RowId
	if ndlg != nil {
		op.DelegateId = ndlg.RowId
	}
	op.Counter = dop.Counter
	op.Fee = dop.Fee
	op.GasLimit = dop.GasLimit
	op.StorageLimit = dop.StorageLimit
	res := dop.Metadata.Result
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.ConsumedGas
	if op.GasUsed > 0 && op.Fee > 0 {
		op.GasPrice = float64(op.Fee) / float64(op.GasUsed)
	}
	b.block.Ops = append(b.block.Ops, op)

	// build flows
	// - fee payment by source
	// - fee reception by baker
	// - (re)delegate source balance on success
	if op.IsSuccess {
		if _, err := b.NewDelegationFlows(src, ndlg, odlg, dop.Metadata.BalanceUpdates); err != nil {
			return err
		}
	} else {
		// on error fees still deduct from old delegation
		if _, err := b.NewDelegationFlows(src, odlg, odlg, dop.Metadata.BalanceUpdates); err != nil {
			return err
		}

		// handle errors
		if len(res.Errors) > 0 {
			if buf, err := json.Marshal(res.Errors); err == nil {
				op.Errors = string(buf)
			} else {
				log.Errorf("delegation op [%d:%d]: marshal op errors: %v", op_n, op_c, err)
			}
		}
	}

	// update accounts
	if !rollback {
		if !op.IsSuccess {
			src.NOps++
			src.NOpsFailed++
			src.NDelegation++
			src.IsDirty = true
			src.LastSeen = b.block.Height
		} else {
			src.NOps++
			src.NDelegation++
			src.IsDirty = true
			src.LastSeen = b.block.Height

			// handle delegate registration
			if ndlg != nil {
				// this is idempotent, always sets grace to cycle + 11
				if src.RowId == ndlg.RowId {
					src.DelegateId = src.RowId
					b.RegisterDelegate(src)
				} else {
					src.DelegatedSince = b.block.Height
				}
			}

			// handle delegate withdraw
			if ndlg == nil {
				src.IsDelegated = false
				src.DelegateId = 0
				src.DelegatedSince = 0
			}

			// handle new delegate
			if ndlg != nil && src.RowId != ndlg.RowId {
				// delegate must be registered
				if !ndlg.IsDelegate {
					return fmt.Errorf("delegation op [%d:%d]: target delegate %s %d not registered",
						op_n, op_c, ndlg.String(), ndlg.RowId)
				}
				src.IsDelegated = true
				src.DelegateId = ndlg.RowId
				src.DelegatedSince = b.block.Height
				ndlg.TotalDelegations++
				if src.Balance() > 0 {
					// delegation becomes active only when src is funded
					ndlg.ActiveDelegations++
				}
				ndlg.IsDirty = true
			}

			// handle withdraw from old delegate (also ensures we're duplicate safe)
			if odlg != nil && src.RowId != odlg.RowId {
				if src.Balance() > 0 {
					odlg.ActiveDelegations--
					odlg.IsDirty = true
				}
			}
		}
	} else {
		// rollback accounts
		if !op.IsSuccess {
			src.NOps--
			src.NOpsFailed--
			src.NDelegation--
			src.IsDirty = true
			src.LastSeen = util.Max64N(src.LastSeen, src.LastIn, src.LastOut) // approximation only
		} else {
			src.NOps--
			src.NDelegation--
			src.IsDirty = true
			src.LastSeen = util.Max64N(src.LastSeen, src.LastIn, src.LastOut) // approximation only

			// handle delegate un-registration
			if ndlg != nil && src.RowId == ndlg.RowId {
				src.DelegateId = 0
				src.IsDelegate = false
				src.DelegateSince = 0
				src.IsActiveDelegate = false
				src.IsDirty = true
				hashkey := accountHashKey(src)
				b.accMap[src.RowId] = src
				b.accHashMap[hashkey] = src
				delete(b.dlgMap, src.RowId)
				delete(b.dlgHashMap, hashkey)
			}

			// find previous delegate, if any
			var lastsince int64
			if prevop, err := b.idx.FindLatestDelegation(ctx, src.RowId); err != nil {
				if err != index.ErrNoOpEntry {
					return err
				}
				odlg = nil
			} else if prevop.DelegateId > 0 {
				lastsince = prevop.Height
				odlg, ok = b.AccountById(prevop.DelegateId)
				if !ok {
					return fmt.Errorf("delegation rollback [%d:%d]: missing previous delegate id %d", op_n, op_c, prevop.DelegateId)
				}
			}
			if odlg == nil {
				// we must also look for the sources' origination op that may have
				// set the initial delegate
				if prevop, err := b.idx.FindOrigination(ctx, src.RowId); err != nil {
					if err != index.ErrNoOpEntry {
						return err
					}
				} else if prevop.DelegateId != 0 {
					odlg, ok = b.AccountById(prevop.DelegateId)
					if !ok {
						return fmt.Errorf("delegation rollback [%d:%d]: missing origin delegate %s", op_n, op_c, prevop.DelegateId)
					}
				}
			}

			// reverse new delegate
			if ndlg != nil && src.RowId != ndlg.RowId {
				ndlg.TotalDelegations--
				ndlg.ActiveDelegations--
				ndlg.IsDirty = true
			}

			// reverse handle withdraw from old delegate
			if odlg != nil {
				src.IsDelegated = true
				src.DelegateId = odlg.RowId
				src.DelegatedSince = lastsince
				if ndlg != nil && src.RowId != ndlg.RowId {
					odlg.ActiveDelegations++
				}
				odlg.IsDirty = true
			} else {
				src.IsDelegated = false
				src.DelegateId = 0
				src.DelegatedSince = 0
			}
		}
	}

	return nil
}

func (b *Builder) NewInternalDelegationOp(ctx context.Context, origsrc, origdlg *Account, oh *rpc.OperationHeader, iop *rpc.InternalResult, op_n, op_c, op_i int, rollback bool) error {
	src, ok := b.AccountByAddress(iop.Source)
	if !ok {
		return fmt.Errorf("internal delegation op [%d:%d:%d]: missing source account %s", op_n, op_c, op_i, iop.Source)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return fmt.Errorf("internal delegation op [%d:%d]: missing branch %s", oh.Branch)
	}
	var odlg, ndlg *Account
	if src.DelegateId != 0 {
		if odlg, ok = b.AccountById(src.DelegateId); !ok {
			return fmt.Errorf("internal delegation op [%d:%d:%d]: missing delegate %d for source account %d",
				op_n, op_c, op_i, src.DelegateId, src.RowId)
		}
	}
	if iop.Delegate != nil {
		ndlg, ok = b.AccountByAddress(*iop.Delegate)
		if !ok && iop.Result.Status.IsSuccess() {
			return fmt.Errorf("internal delegation op [%d:%d:%d]: missing account %s", op_n, op_c, op_i, iop.Delegate)
		}
	}

	// build op (internal and outer op share the same hash and block location)
	op := NewOp(b.block, branch, oh, op_n, op_c, op_i)
	op.IsInternal = true
	op.Type = chain.OpTypeDelegation
	op.SenderId = src.RowId
	if ndlg != nil {
		op.DelegateId = ndlg.RowId
	}
	op.Counter = iop.Nonce
	op.Fee = 0          // n.a. for internal ops
	op.GasLimit = 0     // n.a. for internal ops
	op.StorageLimit = 0 // n.a. for internal ops
	res := iop.Result
	op.Status = res.Status
	op.GasUsed = res.ConsumedGas
	op.IsSuccess = op.Status.IsSuccess()

	// build op
	b.block.Ops = append(b.block.Ops, op)

	// build flows
	// - no fees (paid by outer op)
	// - (re)delegate source balance on success
	// - no fees paid, no flow on failure
	if op.IsSuccess {
		if _, err := b.NewDelegationFlows(src, ndlg, odlg, nil); err != nil {
			return err
		}
	} else {
		// handle errors
		if len(res.Errors) > 0 {
			if buf, err := json.Marshal(res.Errors); err == nil {
				op.Errors = string(buf)
			} else {
				log.Errorf("internal delegation op [%d:%d:%d]: marshal op errors: %v", op_n, op_c, op_i, err)
			}
		}
	}

	// update accounts
	if !rollback {
		if !op.IsSuccess {
			src.NOps++
			src.NOpsFailed++
			src.NDelegation++
			src.IsDirty = true
			src.LastSeen = b.block.Height
		} else {
			src.NOps++
			src.NDelegation++
			src.IsDirty = true
			src.LastSeen = b.block.Height

			// no delegate registration via internal op

			// handle delegate withdraw
			if ndlg == nil {
				src.IsDelegated = false
				src.DelegateId = 0
				src.DelegatedSince = 0
			}

			// handle new delegate
			if ndlg != nil && src.RowId != ndlg.RowId {
				// delegate must be registered
				if !ndlg.IsDelegate {
					return fmt.Errorf("internal delegation op [%d:%d:%d]: target delegate %s %d not registered",
						op_n, op_c, op_i, ndlg.String(), ndlg.RowId)
				}
				src.IsDelegated = true
				src.DelegateId = ndlg.RowId
				src.DelegatedSince = b.block.Height
				ndlg.TotalDelegations++
				if src.Balance() > 0 {
					// delegation becomes active only when src is funded
					ndlg.ActiveDelegations++
				}
				ndlg.IsDirty = true
			}

			// handle withdraw from old delegate (also ensures we're duplicate safe)
			if odlg != nil && src.RowId != odlg.RowId {
				if src.Balance() > 0 {
					odlg.ActiveDelegations--
					odlg.IsDirty = true
				}
			}
		}
	} else {
		// rollback accounts
		if !op.IsSuccess {
			src.NOps--
			src.NOpsFailed--
			src.NDelegation--
			src.IsDirty = true
			src.LastSeen = util.Max64N(src.LastSeen, src.LastIn, src.LastOut) // approximation only
		} else {
			src.NOps--
			src.NDelegation--
			src.IsDirty = true
			src.LastSeen = util.Max64N(src.LastSeen, src.LastIn, src.LastOut) // approximation only

			// find previous delegate, if any
			var lastsince int64
			if prevop, err := b.idx.FindLatestDelegation(ctx, src.RowId); err != nil {
				if err != index.ErrNoOpEntry {
					return err
				}
				odlg = nil
			} else if prevop.DelegateId > 0 {
				lastsince = prevop.Height
				odlg, ok = b.AccountById(prevop.DelegateId)
				if !ok {
					return fmt.Errorf("delegation rollback [%d:%d]: missing previous delegate id %d", op_n, op_c, prevop.DelegateId)
				}
			}
			if odlg == nil {
				// we must also look for the sources' origination op that may have
				// set the initial delegate
				if prevop, err := b.idx.FindOrigination(ctx, src.RowId); err != nil {
					if err != index.ErrNoOpEntry {
						return err
					}
				} else if prevop.DelegateId != 0 {
					odlg, ok = b.AccountById(prevop.DelegateId)
					if !ok {
						return fmt.Errorf("delegation rollback [%d:%d]: missing origin delegate %s", op_n, op_c, prevop.DelegateId)
					}
				}
			}

			// reverse new delegate
			if ndlg != nil && src.RowId != ndlg.RowId {
				ndlg.TotalDelegations--
				ndlg.ActiveDelegations--
				ndlg.IsDirty = true
			}

			// reverse handle withdraw from old delegate
			if odlg != nil {
				src.IsDelegated = true
				src.DelegateId = odlg.RowId
				src.DelegatedSince = lastsince
				if ndlg != nil && src.RowId != ndlg.RowId {
					odlg.ActiveDelegations++
				}
				odlg.IsDirty = true
			} else {
				src.IsDelegated = false
				src.DelegateId = 0
				src.DelegatedSince = 0
			}
		}
	}

	return nil
}
