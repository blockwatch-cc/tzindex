// Copyright (c) 2020-2021 Blockwatch Data Inc.
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

// implicit operation list ids
const (
	OPL_PROTOCOL_UPGRADE = -2
	OPL_BLOCK_HEADER     = -1
)

// generate synthetic ops from flows for
// OpTypeBake
// OpTypeUnfreeze
// OpTypeSeedSlash
func (b *Builder) AppendImplicitOps(ctx context.Context) error {
	flows, err := b.NewImplicitFlows()
	if err != nil {
		return err
	}
	b.block.Flows = append(b.block.Flows, flows...)

	if len(flows) == 0 {
		return nil
	}

	// prepare ops
	ops := make([]*Op, flows[len(flows)-1].OpN+1)

	// parse all flows and reverse-assign to ops
	for _, f := range flows {
		if f.OpN < 0 || f.OpN >= len(ops) {
			log.Errorf("Implicit ops: out of range %d/%d", f.OpN, len(ops))
			continue
		}
		switch true {
		case f.Operation == FlowTypeBaking:
			// OpTypeBake
			if ops[f.OpN] == nil {
				ops[f.OpN] = NewImplicitOp(b.block, f.AccountId, chain.OpTypeBake, f.OpN, f.OpL, f.OpP)
				ops[f.OpN].SenderId = f.AccountId
			}
			// assuming only one flow per category per baker
			switch f.Category {
			case FlowCategoryDeposits:
				ops[f.OpN].Deposit = f.AmountIn
			case FlowCategoryRewards:
				ops[f.OpN].Reward = f.AmountIn
				// case FlowCategoryFee:
				// Note: fees appear in balance updates for individual operations;
				//       we could parse and sum them here, but for performance reasons
				//       we will update this bake operation in the operation index
				//       after all ops have been parsed and block fees are summed up
			}
		case f.IsUnfrozen && f.Category != FlowCategoryBalance:
			// OpTypeUnfreeze
			if ops[f.OpN] == nil {
				ops[f.OpN] = NewImplicitOp(b.block, f.AccountId, chain.OpTypeUnfreeze, f.OpN, f.OpL, f.OpP)
				ops[f.OpN].SenderId = f.AccountId
			}
			// assuming only one flow per category per baker
			switch f.Category {
			case FlowCategoryDeposits:
				ops[f.OpN].Deposit = f.AmountOut
			case FlowCategoryRewards:
				ops[f.OpN].Reward = f.AmountOut
			case FlowCategoryFees:
				ops[f.OpN].Fee = f.AmountOut
			}
		case f.IsBurned && f.Operation == FlowTypeNonceRevelation:
			// OpTypeSeedSlash
			if ops[f.OpN] == nil {
				ops[f.OpN] = NewImplicitOp(b.block, f.AccountId, chain.OpTypeSeedSlash, f.OpN, f.OpL, f.OpC)
			}
			switch f.Category {
			case FlowCategoryRewards:
				// sum multiple consecuitive seed slashes into one op
				ops[f.OpN].Reward += f.AmountOut
			case FlowCategoryFees:
				// sum multiple consecuitive seed slashes into one op
				ops[f.OpN].Fee += f.AmountOut
			}
		}
	}

	// make sure we don't accidentally add a nil op
	for _, v := range ops {
		if v == nil {
			continue
		}
		b.block.Ops = append(b.block.Ops, v)
	}

	return nil
}

func (b *Builder) AppendInvoiceOp(ctx context.Context, acc *Account, amount int64, p int) error {
	n := len(b.block.Ops)
	b.block.Flows = append(b.block.Flows, b.NewInvoiceFlow(acc, amount, n, p))
	op := NewImplicitOp(b.block, acc.RowId, chain.OpTypeInvoice, n, OPL_PROTOCOL_UPGRADE, p)
	op.SenderId = acc.RowId
	op.Volume = amount
	b.block.Ops = append(b.block.Ops, op)
	return nil
}

func (b *Builder) AppendAirdropOp(ctx context.Context, acc *Account, amount int64, p int) error {
	n := len(b.block.Ops)
	b.block.Flows = append(b.block.Flows, b.NewAirdropFlow(acc, amount, n, p))
	op := NewImplicitOp(b.block, acc.RowId, chain.OpTypeAirdrop, n, OPL_PROTOCOL_UPGRADE, p)
	op.Volume = amount
	b.block.Ops = append(b.block.Ops, op)
	return nil
}

func (b *Builder) AppendMagicBakerRegistrationOp(ctx context.Context, acc *Account, p int) error {
	n := len(b.block.Ops)
	op := NewImplicitOp(b.block, 0, chain.OpTypeDelegation, n, OPL_PROTOCOL_UPGRADE, p)
	op.SenderId = acc.RowId
	op.DelegateId = acc.RowId
	b.block.Ops = append(b.block.Ops, op)
	return nil
}

// func (b *Builder) AppendContractMigrationOp(ctx context.Context, acc *Account, c *Contract, p int) error {
// 	n := len(b.block.Ops)
// 	op := NewImplicitOp(b.block, acc.RowId, chain.OpTypeMigration, n, OPL_PROTOCOL_UPGRADE, p)
// 	op.IsContract = true
// 	if c.Type == chain.ContractTypeDelegator {
// 		var err error
// 		op.Storage, err = c.InitialStorage()
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	b.block.Ops = append(b.block.Ops, op)
// 	return nil
// }

func (b *Builder) AppendActivationOp(ctx context.Context, oh *rpc.OperationHeader, op_l, op_p, op_c int, rollback bool) error {
	o := oh.Contents[op_c]

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.OpKind(), op_l, op_p}, args...)...,
		)
	}

	aop, ok := o.(*rpc.AccountActivationOp)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return Errorf("missing branch %s", oh.Branch)
	}

	// need to lookup using blinded key
	bkey, err := chain.BlindAddress(aop.Pkh, aop.Secret)
	if err != nil {
		return Errorf("blinded address creation failed: %v", err)
	}

	acc, ok := b.AccountByAddress(bkey)
	if !ok {
		return Errorf("missing account %s", aop.Pkh)
	}

	// cross-check if account exists under it's implicity address
	origacc, ok := b.AccountByAddress(aop.Pkh)
	if !ok {
		origacc, _ = b.idx.LookupAccount(ctx, aop.Pkh)
	}

	// check activated amount against UnclaimedBalance
	activated := aop.Metadata.BalanceUpdates[0].(*rpc.ContractBalanceUpdate).Change

	// build op
	op_n := len(b.block.Ops)
	op := NewOp(b.block, branch, oh, op_n, op_l, op_p, op_c, 0)
	op.IsSuccess = true
	op.Status = chain.OpStatusApplied
	op.Volume = activated
	op.SenderId = acc.RowId
	op.ReceiverId = acc.RowId
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
			acc.IsDelegatable = true
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
			// replace implicit hash with blinded hash
			delete(b.accHashMap, accountHashKey(acc))
			acc.NOps--
			acc.Hash = bkey.Hash
			acc.Type = bkey.Type
			acc.IsActivated = false
			acc.IsDirty = true
			acc.FirstSeen = 1 // reset to genesis
			b.accHashMap[accountHashKey(acc)] = acc
		}
	}

	// build flows
	_, err = b.NewActivationFlow(acc, aop, op_n, op_l, op_p, op_c)
	if err != nil {
		return Errorf("building flows: %v", err)
	}
	return nil
}

func (b *Builder) AppendEndorsementOp(
	ctx context.Context,
	oh *rpc.OperationHeader,
	op_l, op_p, op_c int,
	rollback bool) error {

	o := oh.Contents[op_c]

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.OpKind(), op_l, op_p}, args...)...,
		)
	}

	eop, ok := o.(*rpc.EndorsementOp)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return Errorf("missing branch %s", oh.Branch)
	}
	acc, ok := b.AccountByAddress(eop.Metadata.Address())
	if !ok {
		return Errorf("missing account %s ", eop.Metadata.Address())
	}

	// build flows
	op_n := len(b.block.Ops)
	flows, err := b.NewEndorserFlows(acc, eop.Metadata.BalanceUpdates, op_n, op_l, op_p)
	if err != nil {
		return Errorf("building flows: %v", err)
	}

	// build op
	op := NewOp(b.block, branch, oh, op_n, op_l, op_p, op_c, 0)
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
func (b *Builder) AppendBallotOp(ctx context.Context, oh *rpc.OperationHeader, op_l, op_p, op_c int, rollback bool) error {
	o := oh.Contents[op_c]

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.OpKind(), op_l, op_p}, args...)...,
		)
	}

	bop, ok := o.(*rpc.BallotOp)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return Errorf("missing branch %s", oh.Branch)
	}
	acc, ok := b.AccountByAddress(bop.Source)
	if !ok {
		return Errorf("missing account %s ", bop.Source)
	}

	// build op, ballots have no fees, volume, gas, etc
	op_n := len(b.block.Ops)
	op := NewOp(b.block, branch, oh, op_n, op_l, op_p, op_c, 0)
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
func (b *Builder) AppendProposalsOp(ctx context.Context, oh *rpc.OperationHeader, op_l, op_p, op_c int, rollback bool) error {
	o := oh.Contents[op_c]

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.OpKind(), op_l, op_p}, args...)...,
		)
	}

	pop, ok := o.(*rpc.ProposalsOp)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return Errorf("missing branch %s", oh.Branch)
	}
	acc, ok := b.AccountByAddress(pop.Source)
	if !ok {
		return Errorf("missing account %s ", pop.Source)
	}

	// build op, proposals have no fees, volume, gas, etc
	op_n := len(b.block.Ops)
	op := NewOp(b.block, branch, oh, op_n, op_l, op_p, op_c, 0)
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
func (b *Builder) AppendRevealOp(ctx context.Context, oh *rpc.OperationHeader, op_l, op_p, op_c int, rollback bool) error {
	o := oh.Contents[op_c]

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.OpKind(), op_l, op_p}, args...)...,
		)
	}

	rop, ok := o.(*rpc.RevelationOp)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return Errorf("missing branch %s", oh.Branch)
	}
	src, ok := b.AccountByAddress(rop.Source)
	if !ok {
		return Errorf("missing account %s", rop.Source)
	}
	var dlg *Account
	if src.DelegateId != 0 {
		if dlg, ok = b.AccountById(src.DelegateId); !ok {
			return Errorf("missing delegate %d for source account %d", src.DelegateId, src.RowId)
		}
	}

	// build op
	op_n := len(b.block.Ops)
	op := NewOp(b.block, branch, oh, op_n, op_l, op_p, op_c, 0)
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
	_, err := b.NewRevealFlows(src, dlg, rop.Metadata.BalanceUpdates, op_n, op_l, op_p, op_c)
	if err != nil {
		return Errorf("building flows: %v", err)
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
			src.LastSeen = b.block.Height
			src.IsDirty = true
			if len(res.Errors) > 0 {
				if buf, err := json.Marshal(res.Errors); err == nil {
					op.Errors = string(buf)
				} else {
					// non-fatal, but error data will be missing from index
					log.Error(Errorf("marshal op errors: %v", err))
				}
			}
		} else {
			// update account, key may be ed25519 (edpk), secp256k1 (sppk) or p256 (p2pk)
			src.NOps++
			src.IsRevealed = true
			// src.Pubkey = rop.PublicKey.Bytes()
			src.PubkeyHash = rop.PublicKey.Hash
			src.PubkeyType = rop.PublicKey.Type
			src.LastSeen = b.block.Height
			src.IsDirty = true
		}
	} else {
		if !op.IsSuccess {
			src.NOps--
			src.NOpsFailed--
			src.IsDirty = true
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

func (b *Builder) AppendSeedNonceOp(ctx context.Context, oh *rpc.OperationHeader, op_l, op_p, op_c int, rollback bool) error {
	o := oh.Contents[op_c]

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.OpKind(), op_l, op_p}, args...)...,
		)
	}

	sop, ok := o.(*rpc.SeedNonceOp)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return Errorf("missing branch %s", oh.Branch)
	}

	op_n := len(b.block.Ops)
	flows, err := b.NewSeedNonceFlows(sop.Metadata.BalanceUpdates, op_n, op_l, op_p)
	if err != nil {
		return Errorf("building flows: %v", err)
	}

	// build op
	op := NewOp(b.block, branch, oh, op_n, op_l, op_p, op_c, 0)
	op.Status = chain.OpStatusApplied
	op.IsSuccess = true
	op.SenderId = b.block.Baker.RowId
	op.HasData = true

	// lookup baker for original level
	if refBlock, err := b.idx.BlockByHeight(ctx, sop.Level); err != nil {
		return Errorf("missing block %d: %v", sop.Level, err)
	} else {
		op.ReceiverId = refBlock.BakerId
	}

	// data is `level,nonce`
	op.Data = strconv.FormatInt(sop.Level, 10) + "," + hex.EncodeToString(sop.Nonce)

	for _, f := range flows {
		op.Reward += f.AmountIn
	}
	b.block.Ops = append(b.block.Ops, op)

	if !rollback {
		b.block.Baker.NOps++
		b.block.Baker.LastSeen = b.block.Height
		b.block.Baker.IsDirty = true
	} else {
		b.block.Baker.NOps--
		b.block.Baker.IsDirty = true
	}

	return nil
}

func (b *Builder) AppendDoubleBakingOp(ctx context.Context, oh *rpc.OperationHeader, op_l, op_p, op_c int, rollback bool) error {
	o := oh.Contents[op_c]

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.OpKind(), op_l, op_p}, args...)...,
		)
	}

	dop, ok := o.(*rpc.DoubleBakingOp)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return Errorf("missing branch %s", oh.Branch)
	}
	upd := dop.Metadata.BalanceUpdates
	addr := upd[len(upd)-1].(*rpc.FreezerBalanceUpdate).Address()
	accuser, ok := b.AccountByAddress(addr)
	if !ok {
		return Errorf("missing accuser account %s", addr)
	}
	addr = upd[0].(*rpc.FreezerBalanceUpdate).Address()
	offender, ok := b.AccountByAddress(addr)
	if !ok {
		return Errorf("missing offender account %s", addr)
	}

	// build flows first to determine burn
	op_n := len(b.block.Ops)
	flows, err := b.NewDoubleBakingFlows(accuser, offender, upd, op_n, op_l, op_p)
	if err != nil {
		return Errorf("building flows: %v", err)
	}

	// build op
	op := NewOp(b.block, branch, oh, op_n, op_l, op_p, op_c, 0)
	op.IsSuccess = true
	op.Status = chain.OpStatusApplied
	op.SenderId = accuser.RowId
	op.ReceiverId = offender.RowId
	op.HasData = true
	// we store both block headers as json array
	bhs := []rpc.BlockHeader{dop.BH1, dop.BH2}
	buf, err := json.Marshal(bhs)
	if err != nil {
		return Errorf("cannot write data: %v", err)
	}
	op.Data = string(buf)

	// calc burn from flows
	for _, f := range flows {
		// track accuser reward as volume
		op.Volume += f.AmountIn

		// track all burned coins
		op.Burned += f.AmountOut - f.AmountIn

		// track offener losses by category
		switch f.Category {
		case FlowCategoryRewards:
			op.Reward -= f.AmountOut
		case FlowCategoryDeposits:
			op.Deposit -= f.AmountOut
		case FlowCategoryFees:
			op.Fee -= f.AmountOut
		}
	}
	b.block.Ops = append(b.block.Ops, op)

	// update accounts
	if !rollback {
		accuser.NOps++
		accuser.IsDirty = true
		accuser.LastSeen = b.block.Height
		offender.NOps++
		offender.LastSeen = b.block.Height
		offender.IsDirty = true
	} else {
		accuser.NOps--
		accuser.IsDirty = true
		offender.NOps--
		offender.IsDirty = true
	}

	return nil
}

func (b *Builder) AppendDoubleEndorsingOp(ctx context.Context, oh *rpc.OperationHeader, op_l, op_p, op_c int, rollback bool) error {
	o := oh.Contents[op_c]

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.OpKind(), op_l, op_p}, args...)...,
		)
	}

	dop, ok := o.(*rpc.DoubleEndorsementOp)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return Errorf("missing branch %s", oh.Branch)
	}
	upd := dop.Metadata.BalanceUpdates
	addr := upd[len(upd)-1].(*rpc.FreezerBalanceUpdate).Address()
	accuser, ok := b.AccountByAddress(addr)
	if !ok {
		return Errorf("missing accuser account %s", addr)
	}
	addr = upd[0].(*rpc.FreezerBalanceUpdate).Address()
	offender, ok := b.AccountByAddress(addr)
	if !ok {
		return Errorf("missing offender account %s", addr)
	}

	// build flows first to determine burn
	op_n := len(b.block.Ops)
	flows, err := b.NewDoubleEndorsingFlows(accuser, offender, upd, op_n, op_l, op_p)
	if err != nil {
		return Errorf("building flows: %v", err)
	}

	// build op
	op := NewOp(b.block, branch, oh, op_n, op_l, op_p, op_c, 0)
	op.IsSuccess = true
	op.Status = chain.OpStatusApplied
	op.SenderId = accuser.RowId
	op.ReceiverId = offender.RowId
	op.HasData = true
	// we store double-endorsed evidences as JSON
	dops := []rpc.DoubleEndorsementEvidence{dop.OP1, dop.OP2}
	buf, err := json.Marshal(dops)
	if err != nil {
		return Errorf("cannot write data: %v", err)
	}
	op.Data = string(buf)

	// calc burn from flows
	for _, f := range flows {
		// track accuser reward as volume
		op.Volume += f.AmountIn

		// track all burned coins
		op.Burned += f.AmountOut - f.AmountIn

		// track offener losses by category
		switch f.Category {
		case FlowCategoryRewards:
			op.Reward -= f.AmountOut
		case FlowCategoryDeposits:
			op.Deposit -= f.AmountOut
		case FlowCategoryFees:
			op.Fee -= f.AmountOut
		}
	}
	b.block.Ops = append(b.block.Ops, op)

	// update accounts
	if !rollback {
		accuser.NOps++
		accuser.IsDirty = true
		accuser.LastSeen = b.block.Height
		offender.NOps++
		offender.LastSeen = b.block.Height
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
// NOTE: this seems to not extend grace period
func (b *Builder) AppendTransactionOp(ctx context.Context, oh *rpc.OperationHeader, op_l, op_p, op_c int, rollback bool) error {
	o := oh.Contents[op_c]

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d:%d]: "+format,
			append([]interface{}{o.OpKind(), op_l, op_p, op_c}, args...)...,
		)
	}

	top, ok := o.(*rpc.TransactionOp)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return Errorf("missing branch %s", oh.Branch)
	}
	src, ok := b.AccountByAddress(top.Source)
	if !ok {
		return Errorf("missing source account %s", top.Source)
	}
	dst, ok := b.AccountByAddress(top.Destination)
	if !ok {
		return Errorf("missing target account %s", top.Destination)
	}
	var srcdlg, dstdlg *Account
	if src.DelegateId != 0 {
		if srcdlg, ok = b.AccountById(src.DelegateId); !ok {
			return Errorf("missing delegate %d for source account %d", src.DelegateId, src.RowId)
		}
	}
	if dst.DelegateId != 0 {
		if dstdlg, ok = b.AccountById(dst.DelegateId); !ok {
			return Errorf("missing delegate %d for dest account %d", dst.DelegateId, dst.RowId)
		}
	}

	// build op
	op_n := len(b.block.Ops)
	op := NewOp(b.block, branch, oh, op_n, op_l, op_p, op_c, 0)
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
	op.Volume = top.Amount
	op.StorageSize = res.StorageSize
	op.StoragePaid = res.PaidStorageSizeDiff
	if op.GasUsed > 0 && op.Fee > 0 {
		op.GasPrice = float64(op.Fee) / float64(op.GasUsed)
	}
	op.HasData = res.Storage != nil || top.Parameters != nil || len(res.BigMapDiff) > 0

	var err error
	if top.Parameters != nil {
		op.Parameters, err = top.Parameters.MarshalBinary()
		// log.Debugf("Call [%d:%d:%d] %s entrypoint %s bin=%x",
		// 	op.Height, op_l, op_p, op.Hash, top.Parameters.Entrypoint, op.Parameters)
		if err != nil {
			return Errorf("marshal params: %v", err)
		}

		// load contract to identify which real entrypoint was called
		con, err := b.LoadContractByAccountId(ctx, dst.RowId)
		if err != nil {
			return Errorf("loading contract: %v", err)
		}
		tip := &ChainTip{ChainId: b.block.TZ.Block.ChainId}
		script, err := con.LoadScript(tip, b.block.Height, nil)
		if err != nil {
			return Errorf("loading script: %v", err)
		}
		ep, _, err := top.Parameters.MapEntrypoint(script)
		if op.IsSuccess && err != nil {
			return Errorf("searching entrypoint: %v", err)
		}
		op.Entrypoint = ep.Id
	}
	if res.Storage != nil {
		op.Storage, err = res.Storage.MarshalBinary()
		if err != nil {
			return Errorf("marshal storage: %v", err)
		}
	}
	if len(res.BigMapDiff) > 0 {
		top.Metadata.Result.BigMapDiff, err = b.PatchBigMapDiff(ctx, res.BigMapDiff, dst.Address(), nil)
		if err != nil {
			return Errorf("patch bigmap: %v", err)
		}
		op.BigMapDiff, err = top.Metadata.Result.BigMapDiff.MarshalBinary()
		if err != nil {
			return Errorf("marshal bigmap: %v", err)
		}
	}

	var flows []*Flow

	if op.IsSuccess {
		flows, err = b.NewTransactionFlows(src, dst, srcdlg, dstdlg,
			top.Metadata.BalanceUpdates, // fees
			res.BalanceUpdates,          // move
			b.block,
			op_n, op_l, op_p, op_c,
		)
		if err != nil {
			return Errorf("building flows: %v", err)
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
				// non-fatal, but error data will be missing from index
				log.Error(Errorf("marshal op errors: %v", err))
			}
		}

		// fees only
		flows, err = b.NewTransactionFlows(src, nil, srcdlg, nil,
			top.Metadata.BalanceUpdates,
			nil, // no result balance updates
			b.block,
			op_n, op_l, op_p, op_c,
		)
		if err != nil {
			return Errorf("building flows: %v", err)
		}
	}

	// update accounts
	if !rollback {
		if !op.IsSuccess {
			src.NOps++
			src.NTx++
			src.NOpsFailed++
			src.LastSeen = b.block.Height
			src.IsDirty = true
			dst.NOps++
			dst.NTx++
			dst.NOpsFailed++
			dst.LastSeen = b.block.Height
			dst.IsDirty = true
		} else {
			src.NOps++
			src.NTx++
			src.LastSeen = b.block.Height
			src.IsDirty = true
			dst.NOps++
			dst.NTx++
			dst.LastSeen = b.block.Height
			dst.IsDirty = true
			if res.Allocated {
				// init dest account when allocated
				dst.IsSpendable = true
				dst.IsDelegatable = true
			}
			if dst.IsDelegate && b.block.Params.ReactivateByTx {
				// reactivate inactive delegates (receiver only)
				// - it seems from reverse engineering delegate activation rules
				//   that received transactions will reactivate an inactive delegate
				//   and extend grace period for active delegates
				// - support for this feature ends with proto_004
				if !dst.IsActiveDelegate && dst.DelegateId == dst.RowId {
					dst.IsActiveDelegate = true
					dst.InitGracePeriod(b.block.Cycle, b.block.Params)
				} else {
					dst.UpdateGracePeriod(b.block.Cycle, b.block.Params)
				}
			}
			// update last seen for contracts (flow might be missing)
			// update entrypoint call stats
			if op.IsContract {
				dst.IncCallStats(byte(op.Entrypoint), b.block.Height, b.block.Params)
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

			if op.IsContract {
				dst.DecCallStats(byte(op.Entrypoint), b.block.Height, b.block.Params)
			}
		}
	}

	// append before potential internal ops
	b.block.Ops = append(b.block.Ops, op)

	// apply internal operation result (may generate new op and flows)
	for i, v := range top.Metadata.InternalResults {
		switch v.OpKind() {
		case chain.OpTypeTransaction:
			if err := b.AppendInternalTransactionOp(ctx, src, srcdlg, oh, v, op_l, op_p, op_c, i, rollback); err != nil {
				return err
			}
		case chain.OpTypeDelegation:
			if err := b.AppendInternalDelegationOp(ctx, src, srcdlg, oh, v, op_l, op_p, op_c, i, rollback); err != nil {
				return err
			}
		case chain.OpTypeOrigination:
			if err := b.AppendInternalOriginationOp(ctx, src, srcdlg, oh, v, op_l, op_p, op_c, i, rollback); err != nil {
				return err
			}
		default:
			return Errorf("unsupported internal operation type %s", v.OpKind())
		}
	}
	return nil
}

func (b *Builder) AppendInternalTransactionOp(
	ctx context.Context,
	origsrc, origdlg *Account,
	oh *rpc.OperationHeader,
	iop *rpc.InternalResult,
	op_l, op_p, op_c, op_i int,
	rollback bool) error {

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"internal %s op [%d:%d:%d:%d]: "+format,
			append([]interface{}{iop.OpKind(), op_l, op_p, op_c, op_i}, args...)...,
		)
	}

	src, ok := b.AccountByAddress(iop.Source)
	if !ok {
		return Errorf("missing source account %s", iop.Source)
	}
	dst, ok := b.AccountByAddress(*iop.Destination)
	if !ok {
		return Errorf("missing source account %s", iop.Destination)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return Errorf("missing branch %s", oh.Branch)
	}
	var srcdlg, dstdlg *Account
	if src.DelegateId != 0 {
		if srcdlg, ok = b.AccountById(src.DelegateId); !ok {
			return Errorf("missing delegate %d for source account %d", src.DelegateId, src.RowId)
		}
	}
	if dst.DelegateId != 0 {
		if dstdlg, ok = b.AccountById(dst.DelegateId); !ok {
			return Errorf("missing delegate %d for dest account %d", dst.DelegateId, dst.RowId)
		}
	}

	// build op (internal and outer tx share the same hash and block location)
	op_n := len(b.block.Ops)
	op := NewOp(b.block, branch, oh, op_n, op_l, op_p, op_c, op_i)
	op.Type = chain.OpTypeTransaction
	op.IsInternal = true
	op.SenderId = src.RowId
	op.ReceiverId = dst.RowId
	op.ManagerId = origsrc.RowId
	op.Counter = iop.Nonce
	op.Fee = 0          // n.a. for internal ops
	op.GasLimit = 0     // n.a. for internal ops
	op.StorageLimit = 0 // n.a. for internal ops
	op.IsContract = dst.IsContract
	op.Volume = iop.Amount
	res := iop.Result
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.ConsumedGas
	op.StorageSize = res.StorageSize
	op.StoragePaid = res.PaidStorageSizeDiff
	op.HasData = iop.Parameters != nil || res.Storage != nil || len(res.BigMapDiff) > 0

	var err error
	if iop.Parameters != nil {
		op.Parameters, err = iop.Parameters.MarshalBinary()
		if err != nil {
			return Errorf("marshal params: %v", err)
		}
		// load contract to identify which real entrypoint was called
		con, err := b.LoadContractByAccountId(ctx, dst.RowId)
		if err != nil {
			return Errorf("loading contract: %v", err)
		}
		tip := &ChainTip{ChainId: b.block.TZ.Block.ChainId}
		script, err := con.LoadScript(tip, b.block.Height, nil)
		if err != nil {
			return Errorf("loading script: %v", err)
		}
		ep, _, err := iop.Parameters.MapEntrypoint(script)
		if op.IsSuccess && err != nil {
			return Errorf("searching entrypoint: %v", err)
		}
		op.Entrypoint = ep.Id
	}
	if res.Storage != nil {
		op.Storage, err = res.Storage.MarshalBinary()
		if err != nil {
			return Errorf("marshal storage: %v", err)
		}
	}
	if len(res.BigMapDiff) > 0 {
		iop.Result.BigMapDiff, err = b.PatchBigMapDiff(ctx, res.BigMapDiff, dst.Address(), nil)
		if err != nil {
			return Errorf("patch bigmap: %v", err)
		}
		op.BigMapDiff, err = iop.Result.BigMapDiff.MarshalBinary()
		if err != nil {
			return Errorf("marshal bigmap: %v", err)
		}
	}

	var flows []*Flow

	// on success, create flows and update accounts
	if op.IsSuccess {
		// Note: need to use storage from the outer operation result
		flows, err = b.NewInternalTransactionFlows(
			origsrc, src, dst, // outer and inner source, inner dest
			origdlg, srcdlg, dstdlg, // delegates
			res.BalanceUpdates, // moved and bruned amounts
			oh.Contents[op_c].(*rpc.TransactionOp).Metadata.Result.Storage, // updated contract storage
			b.block,
			op_n, op_l, op_p, op_c, op_i,
		)
		if err != nil {
			return Errorf("building flows: %v", err)
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
				// non-fatal, but error data will be missing from index
				log.Error(Errorf("marshal op errors: %v", err))
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
			src.LastSeen = b.block.Height
			src.IsDirty = true
			dst.NOps++
			dst.NTx++
			dst.NOpsFailed++
			dst.LastSeen = b.block.Height
			dst.IsDirty = true
		} else {
			src.NOps++
			src.NTx++
			src.LastSeen = b.block.Height
			src.IsDirty = true
			dst.NOps++
			dst.NTx++
			dst.LastSeen = b.block.Height
			dst.IsDirty = true

			// update last seen for contracts (flow might be missing)
			// update entrypoint call stats
			if op.IsContract {
				dst.IncCallStats(byte(op.Entrypoint), b.block.Height, b.block.Params)
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
			if op.IsContract {
				dst.DecCallStats(byte(op.Entrypoint), b.block.Height, b.block.Params)
			}
		}
	}
	b.block.Ops = append(b.block.Ops, op)
	return nil
}

// NOTE: does not extend grace period although it's a manager operation
// - burns a fee (optional, not used early on)
// - can delegate funds
// - only originated accounts (KT1) can delegate
// - only implicit accounts (tz1) can be delegates
// - by default originated accounts are not delegatable (but initial delegate can be set)
func (b *Builder) AppendOriginationOp(ctx context.Context, oh *rpc.OperationHeader, op_l, op_p, op_c int, rollback bool) error {
	o := oh.Contents[op_c]

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d:%d]: "+format,
			append([]interface{}{o.OpKind(), op_l, op_p, op_c}, args...)...,
		)
	}

	oop, ok := o.(*rpc.OriginationOp)
	if !ok {
		return Errorf("unexpected type %T", o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return Errorf("missing branch %s", oh.Branch)
	}
	src, ok := b.AccountByAddress(oop.Source)
	if !ok {
		return Errorf("missing source account %s", oop.Source)
	}
	var mgr *Account
	if oop.ManagerPubkey.IsValid() {
		mgr, ok = b.AccountByAddress(oop.ManagerPubkey)
		if !ok {
			return Errorf("missing manager account %s", oop.ManagerPubkey)
		}
	} else if oop.ManagerPubkey2.IsValid() {
		mgr, ok = b.AccountByAddress(oop.ManagerPubkey2)
		if !ok {
			return Errorf("missing manager account %s", oop.ManagerPubkey2)
		}
	}
	var srcdlg, newdlg, dst *Account
	if src.DelegateId != 0 {
		if srcdlg, ok = b.AccountById(src.DelegateId); !ok {
			return Errorf("missing delegate %d for source account %d", src.DelegateId, src.RowId)
		}
	}
	if oop.Delegate != nil {
		if newdlg, ok = b.AccountByAddress(*oop.Delegate); !ok && oop.Metadata.Result.Status.IsSuccess() {
			return Errorf("missing delegate account %s", oop.Delegate)
		}
	}

	// build op
	op_n := len(b.block.Ops)
	op := NewOp(b.block, branch, oh, op_n, op_l, op_p, op_c, 0)
	op.SenderId = src.RowId
	op.Counter = oop.Counter
	op.Fee = oop.Fee
	op.GasLimit = oop.GasLimit
	op.StorageLimit = oop.StorageLimit
	op.IsContract = oop.Script != nil
	op.Volume = oop.Balance
	res := oop.Metadata.Result
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.ConsumedGas
	op.StorageSize = res.StorageSize
	op.StoragePaid = res.PaidStorageSizeDiff
	if op.GasUsed > 0 && op.Fee > 0 {
		op.GasPrice = float64(op.Fee) / float64(op.GasUsed)
	}

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
		if l := len(res.OriginatedContracts); l != 1 {
			return Errorf("%d originated accounts", l)
		}

		dst, ok = b.AccountByAddress(res.OriginatedContracts[0])
		if !ok {
			return Errorf("missing originated account %s", res.OriginatedContracts[0])
		}
		op.ReceiverId = dst.RowId
		flows, err = b.NewOriginationFlows(src, dst, srcdlg, newdlg,
			oop.Metadata.BalanceUpdates,
			res.BalanceUpdates,
			op_n, op_l, op_p, op_c,
		)
		if err != nil {
			return Errorf("building flows: %v", err)
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
		oop.Metadata.Result.BigMapDiff, err = b.PatchBigMapDiff(ctx, res.BigMapDiff, dst.Address(), oop.Script)
		if err != nil {
			return Errorf("patch bigmap: %v", err)
		}
		if len(oop.Metadata.Result.BigMapDiff) > 0 {
			op.BigMapDiff, err = oop.Metadata.Result.BigMapDiff.MarshalBinary()
			if err != nil {
				return Errorf("marshal bigmap: %v", err)
			}
			op.HasData = true
		}

	} else {
		// handle errors
		if len(res.Errors) > 0 {
			if buf, err := json.Marshal(res.Errors); err == nil {
				op.Errors = string(buf)
			} else {
				// non-fatal, but error data will be missing from index
				log.Error(Errorf("marshal op errors: %v", err))
			}
		}

		// fees flows
		flows, err = b.NewOriginationFlows(src, nil, srcdlg, nil,
			oop.Metadata.BalanceUpdates, nil, op_n, op_l, op_p, op_c)
		if err != nil {
			return Errorf("building flows: %v", err)
		}
	}

	b.block.Ops = append(b.block.Ops, op)

	// update accounts
	if !rollback {
		if !op.IsSuccess {
			src.NOps++
			src.NOrigination++
			src.NOpsFailed++
			src.LastSeen = b.block.Height
			src.IsDirty = true
		} else {
			src.NOps++
			src.NOrigination++
			src.LastSeen = b.block.Height
			src.IsDirty = true

			// initialize originated account
			// in babylon, keep the sender as manager regardless
			if mgr != nil {
				dst.ManagerId = mgr.RowId
				mgr.LastSeen = b.block.Height
				mgr.IsDirty = true
			} else {
				dst.ManagerId = src.RowId
			}
			dst.IsContract = oop.Script != nil

			// from Babylon, these flags always exist
			if oop.Spendable != nil {
				dst.IsSpendable = *oop.Spendable
			}
			if oop.Delegatable != nil {
				dst.IsDelegatable = *oop.Delegatable
			}
			// pre-babylon, they were true when missing
			if b.block.Params.SilentSpendable {
				dst.IsSpendable = dst.IsSpendable || oop.Spendable == nil || *oop.Spendable
				dst.IsDelegatable = dst.IsDelegatable || oop.Delegatable == nil || *oop.Delegatable
			}
			dst.LastSeen = b.block.Height
			dst.IsDirty = true

			// handle delegation
			if newdlg != nil {
				// register self delegate if not registered yet (only before v002)
				if b.block.Params.HasOriginationBug && src.RowId == newdlg.RowId && !newdlg.IsDelegate {
					b.RegisterDelegate(newdlg, false)
				}

				dst.IsDelegated = true
				dst.DelegateId = newdlg.RowId
				dst.DelegatedSince = b.block.Height

				newdlg.TotalDelegations++
				if op.Volume > 0 {
					// delegation becomes active only when dst KT1 is funded
					newdlg.ActiveDelegations++
				}
				newdlg.LastSeen = b.block.Height
				newdlg.IsDirty = true
			}

			// create and register temporary a contract for potential use by
			// subsequent transactions in the same block (this happened once
			// on Carthagenet block 346116); the actual contract table entry
			// is later created & inserted separately in contract index and
			// this temporary object is discarded
			// b.conMap[dst.RowId] = NewContract(dst, oop, op)
			b.conMap[dst.RowId] = NewContract(dst, oop, op_l, op_p)
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
			// reverse origination
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
func (b *Builder) AppendInternalOriginationOp(
	ctx context.Context,
	origsrc, origdlg *Account,
	oh *rpc.OperationHeader,
	iop *rpc.InternalResult,
	op_l, op_p, op_c, op_i int,
	rollback bool) error {

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"internal %s op [%d:%d:%d:%d]: "+format,
			append([]interface{}{iop.OpKind(), op_l, op_p, op_c, op_i}, args...)...,
		)
	}

	src, ok := b.AccountByAddress(iop.Source)
	if !ok {
		return Errorf("missing source account %s", iop.Source)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return Errorf("missing branch %s", oh.Branch)
	}
	var srcdlg, newdlg, dst *Account
	if src.DelegateId != 0 {
		if srcdlg, ok = b.AccountById(src.DelegateId); !ok {
			return Errorf("missing delegate %d for source account %d", src.DelegateId, src.RowId)
		}
	}
	if iop.Delegate != nil {
		if newdlg, ok = b.AccountByAddress(*iop.Delegate); !ok && iop.Result.Status.IsSuccess() {
			return fmt.Errorf("internal origination op [%d:%d:%d]: missing delegate account %s",
				op_l, op_p, op_i, iop.Delegate)
		}
	}

	// build op (internal and outer op share the same hash and block location)
	op_n := len(b.block.Ops)
	op := NewOp(b.block, branch, oh, op_n, op_l, op_p, op_c, op_i)
	op.IsInternal = true
	op.Type = chain.OpTypeOrigination
	op.SenderId = src.RowId
	op.ManagerId = origsrc.RowId
	op.Counter = iop.Nonce
	op.Fee = 0          // n.a. for internal ops
	op.GasLimit = 0     // n.a. for internal ops
	op.StorageLimit = 0 // n.a. for internal ops
	op.IsContract = iop.Script != nil
	op.Volume = iop.Balance
	res := iop.Result // same as transaction result
	op.Status = res.Status
	op.GasUsed = res.ConsumedGas
	op.IsSuccess = op.Status.IsSuccess()
	op.StorageSize = res.StorageSize
	op.StoragePaid = res.PaidStorageSizeDiff

	// store delegate
	if newdlg != nil {
		op.DelegateId = newdlg.RowId
	}

	var (
		flows []*Flow
		err   error
	)
	if op.IsSuccess {
		if l := len(res.OriginatedContracts); l != 1 {
			return Errorf("%d originated accounts", l)
		}

		dst, ok = b.AccountByAddress(res.OriginatedContracts[0])
		if !ok {
			return Errorf("missing originated account %s", res.OriginatedContracts[0])
		}
		op.ReceiverId = dst.RowId
		flows, err = b.NewInternalOriginationFlows(
			origsrc,
			src,
			dst,
			origdlg,
			srcdlg,
			newdlg,
			res.BalanceUpdates,
			op_n, op_l, op_c, op_c, op_i,
		)
		if err != nil {
			return Errorf("building flows: %v", err)
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
		iop.Result.BigMapDiff, err = b.PatchBigMapDiff(ctx, res.BigMapDiff, dst.Address(), iop.Script)
		if err != nil {
			return Errorf("patch bigmap: %v", err)
		}
		if len(iop.Result.BigMapDiff) > 0 {
			op.BigMapDiff, err = iop.Result.BigMapDiff.MarshalBinary()
			if err != nil {
				return Errorf("marshal bigmap: %v", err)
			}
			op.HasData = true
		}

	} else {
		// handle errors
		if len(res.Errors) > 0 {
			if buf, err := json.Marshal(res.Errors); err == nil {
				op.Errors = string(buf)
			} else {
				// non-fatal, but error data will be missing from index
				log.Error(Errorf("marshal op errors: %v", err))
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
			src.LastSeen = b.block.Height
			src.IsDirty = true
		} else {
			src.NOps++
			src.NOrigination++
			src.LastSeen = b.block.Height
			src.IsDirty = true

			// initialize originated account
			dst.LastSeen = b.block.Height
			dst.IsContract = iop.Script != nil
			dst.IsDirty = true

			// internal originations have no manager, delegate and flags (in protocol v5)
			// but we still keep the original caller as manager to track contract ownership
			dst.ManagerId = origsrc.RowId

			// handle delegation
			if newdlg != nil {
				dst.IsDelegated = true
				dst.DelegateId = newdlg.RowId
				dst.DelegatedSince = b.block.Height

				newdlg.TotalDelegations++
				if op.Volume > 0 {
					// delegation becomes active only when dst KT1 is funded
					newdlg.ActiveDelegations++
				}
				newdlg.LastSeen = b.block.Height
				newdlg.IsDirty = true
			}

			// create and register temporary a contract for potential use by
			// subsequent transactions in the same block (this happened once
			// on Carthagenet block 346116); the actual contract table entry
			// is later created & inserted separately in contract index and
			// this temporary object is discarded
			// b.conMap[dst.RowId] = NewInternalContract(dst, iop, op)
			b.conMap[dst.RowId] = NewInternalContract(dst, iop, op_l, op_p, op_i)
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
			// reverse origination
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
		}
	}

	return nil
}

// Notes
// - manager operation, extends grace period
// - delegations may or may not pay a fee, so BalanceUpdates may be empty
// - originations may delegate as well, so consider this there!
// - early delegations did neither pay nor consume gas
// - status is either `applied` or `failed`
// - failed delegations may still pay fees, but don't not consume gas
// - self-delegation (src == ndlg) represents a new/renewed delegate registration
// - zero-delegation (ndlg == null) represents a delegation withdraw
func (b *Builder) AppendDelegationOp(ctx context.Context, oh *rpc.OperationHeader, op_l, op_p, op_c int, rollback bool) error {
	o := oh.Contents[op_c]

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d:%d]: "+format,
			append([]interface{}{o.OpKind(), op_l, op_p, op_c}, args...)...,
		)
	}

	dop, ok := o.(*rpc.DelegationOp)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return Errorf("missing branch %s", oh.Branch)
	}
	src, ok := b.AccountByAddress(dop.Source)
	if !ok {
		return Errorf("missing account %s", dop.Source)
	}
	// on delegate withdraw, new delegate is empty!
	var ndlg, odlg *Account
	if dop.Delegate.IsValid() {
		ndlg, ok = b.AccountByAddress(dop.Delegate)
		if !ok && dop.Metadata.Result.Status.IsSuccess() {
			return Errorf("missing account %s", dop.Delegate)
		}
	}
	if src.DelegateId != 0 {
		if odlg, ok = b.AccountById(src.DelegateId); !ok {
			return Errorf("missing delegate %d for source account %d", src.DelegateId, src.RowId)
		}
	}

	// build op
	op_n := len(b.block.Ops)
	op := NewOp(b.block, branch, oh, op_n, op_l, op_p, op_c, 0)
	op.SenderId = src.RowId
	if ndlg != nil {
		op.DelegateId = ndlg.RowId
	}
	if odlg != nil {
		op.ReceiverId = odlg.RowId
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
	op.Volume = src.Balance()
	b.block.Ops = append(b.block.Ops, op)

	// build flows
	// - fee payment by source
	// - fee reception by baker
	// - (re)delegate source balance on success
	if op.IsSuccess {
		if _, err := b.NewDelegationFlows(
			src,
			ndlg,
			odlg,
			dop.Metadata.BalanceUpdates,
			op_n, op_l, op_p, op_c, 0,
		); err != nil {
			return Errorf("building flows: %v", err)
		}
	} else {
		// on error fees still deduct from old delegation
		if _, err := b.NewDelegationFlows(
			src,
			odlg,
			odlg,
			dop.Metadata.BalanceUpdates,
			op_n, op_l, op_p, op_c, 0,
		); err != nil {
			return Errorf("building flows: %v", err)
		}

		// handle errors
		if len(res.Errors) > 0 {
			if buf, err := json.Marshal(res.Errors); err == nil {
				op.Errors = string(buf)
			} else {
				// non-fatal, but error data will be missing from index
				log.Error(Errorf("marshal op errors: %v", err))
			}
		}
	}

	// update accounts
	if !rollback {
		if !op.IsSuccess {
			src.NOps++
			src.NOpsFailed++
			src.NDelegation++
			src.LastSeen = b.block.Height
			src.IsDirty = true
			if ndlg != nil {
				ndlg.LastSeen = b.block.Height
				ndlg.IsDirty = true
			}
			if odlg != nil {
				odlg.LastSeen = b.block.Height
				odlg.IsDirty = true
			}
		} else {
			src.NOps++
			src.NDelegation++
			src.LastSeen = b.block.Height
			src.IsDirty = true

			// handle delegate registration
			if ndlg != nil {
				// this is idempotent, always sets grace to cycle + 11
				if src.RowId == ndlg.RowId {
					b.RegisterDelegate(src, true)
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
					return Errorf("target delegate %s %d not registered", ndlg.String(), ndlg.RowId)
				}
				// only update when this delegation changes baker
				if src.DelegateId != ndlg.RowId {
					src.IsDelegated = true
					src.DelegateId = ndlg.RowId
					src.DelegatedSince = b.block.Height
					ndlg.TotalDelegations++
					if src.Balance() > 0 {
						// delegation becomes active only when src is funded
						ndlg.ActiveDelegations++
					}
				}
				ndlg.LastSeen = b.block.Height
				ndlg.IsDirty = true
			}

			// handle withdraw from old delegate (also ensures we're duplicate safe)
			if odlg != nil && src.RowId != odlg.RowId {
				if src.Balance() > 0 {
					odlg.ActiveDelegations--
				}
				odlg.LastSeen = b.block.Height
				odlg.IsDirty = true
			}
		}
	} else {
		// rollback accounts
		if !op.IsSuccess {
			src.NOps--
			src.NOpsFailed--
			src.NDelegation--
			src.IsDirty = true
		} else {
			src.NOps--
			src.NDelegation--
			src.IsDirty = true

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
					return Errorf("rollback: missing previous delegation op for account %d: %v", src.RowId, err)
				}
				odlg = nil
			} else if prevop.DelegateId > 0 {
				lastsince = op.Height
				odlg, ok = b.AccountById(prevop.DelegateId)
				if !ok {
					return Errorf("rollback: missing previous delegate id %d", prevop.DelegateId)
				}
			}
			if odlg == nil {
				// we must also look for the sources' origination op that may have
				// set the initial delegate
				if prevop, err := b.idx.FindOrigination(ctx, src.RowId, src.FirstSeen); err != nil {
					if err != index.ErrNoOpEntry {
						return Errorf("rollback: failed loading previous origination op for account %d: %v", src.RowId, err)
					}
				} else if prevop.DelegateId != 0 {
					odlg, ok = b.AccountById(prevop.DelegateId)
					if !ok {
						return Errorf("rollback: missing origin delegate %s", prevop.DelegateId)
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
				odlg.ActiveDelegations++
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

func (b *Builder) AppendInternalDelegationOp(
	ctx context.Context,
	origsrc, origdlg *Account,
	oh *rpc.OperationHeader,
	iop *rpc.InternalResult,
	op_l, op_p, op_c, op_i int,
	rollback bool) error {

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"internal %s op [%d:%d:%d:%d]: "+format,
			append([]interface{}{iop.OpKind(), op_l, op_p, op_c, op_i}, args...)...,
		)
	}

	src, ok := b.AccountByAddress(iop.Source)
	if !ok {
		return Errorf("missing source account %s", iop.Source)
	}
	branch, ok := b.BranchByHash(oh.Branch)
	if !ok {
		return Errorf("missing branch %s", oh.Branch)
	}
	var odlg, ndlg *Account
	if src.DelegateId != 0 {
		if odlg, ok = b.AccountById(src.DelegateId); !ok {
			return Errorf("missing delegate %d for source account %d", src.DelegateId, src.RowId)
		}
	}
	if iop.Delegate != nil {
		ndlg, ok = b.AccountByAddress(*iop.Delegate)
		if !ok && iop.Result.Status.IsSuccess() {
			return Errorf("missing account %s", iop.Delegate)
		}
	}

	// build op (internal and outer op share the same hash and block location)
	op_n := len(b.block.Ops)
	op := NewOp(b.block, branch, oh, op_n, op_l, op_p, op_c, op_i)
	op.IsInternal = true
	op.Type = chain.OpTypeDelegation
	op.SenderId = src.RowId
	op.ManagerId = origsrc.RowId
	if ndlg != nil {
		op.DelegateId = ndlg.RowId
	}
	if odlg != nil {
		op.ReceiverId = odlg.RowId
	}
	op.Counter = iop.Nonce
	op.Fee = 0          // n.a. for internal ops
	op.GasLimit = 0     // n.a. for internal ops
	op.StorageLimit = 0 // n.a. for internal ops
	res := iop.Result
	op.Status = res.Status
	op.GasUsed = res.ConsumedGas
	op.IsSuccess = op.Status.IsSuccess()
	op.Volume = src.Balance()

	// build op
	b.block.Ops = append(b.block.Ops, op)

	// build flows
	// - no fees (paid by outer op)
	// - (re)delegate source balance on success
	// - no fees paid, no flow on failure
	if op.IsSuccess {
		if _, err := b.NewDelegationFlows(src, ndlg, odlg, nil, op_n, op_l, op_p, op_c, op_i); err != nil {
			return Errorf("building flows: %v", err)
		}
	} else {
		// handle errors
		if len(res.Errors) > 0 {
			if buf, err := json.Marshal(res.Errors); err == nil {
				op.Errors = string(buf)
			} else {
				// non-fatal, but error data will be missing from index
				log.Error(Errorf("marshal op errors: %v", err))
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
			if ndlg != nil {
				ndlg.LastSeen = b.block.Height
				ndlg.IsDirty = true
			}
			if odlg != nil {
				odlg.LastSeen = b.block.Height
				odlg.IsDirty = true
			}
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
					return Errorf("target delegate %s %d not registered", ndlg.String(), ndlg.RowId)
				}
				// only update when this delegation changes baker
				if src.DelegateId != ndlg.RowId {
					src.IsDelegated = true
					src.DelegateId = ndlg.RowId
					src.DelegatedSince = b.block.Height
					ndlg.TotalDelegations++
					if src.Balance() > 0 {
						// delegation becomes active only when src is funded
						ndlg.ActiveDelegations++
					}
				}
				ndlg.LastSeen = b.block.Height
				ndlg.IsDirty = true
			}

			// handle withdraw from old delegate (also ensures we're duplicate safe)
			if odlg != nil && src.RowId != odlg.RowId {
				if src.Balance() > 0 {
					odlg.ActiveDelegations--
				}
				odlg.LastSeen = b.block.Height
				odlg.IsDirty = true
			}
		}
	} else {
		// rollback accounts
		if !op.IsSuccess {
			src.NOps--
			src.NOpsFailed--
			src.NDelegation--
			src.IsDirty = true
		} else {
			src.NOps--
			src.NDelegation--
			src.IsDirty = true

			// find previous delegate, if any
			var lastsince int64
			if prevop, err := b.idx.FindLatestDelegation(ctx, src.RowId); err != nil {
				if err != index.ErrNoOpEntry {
					return Errorf("rollback: failed loading previous delegation op for account %d: %v", src.RowId, err)
				}
				odlg = nil
			} else if prevop.DelegateId > 0 {
				lastsince = prevop.Height
				odlg, ok = b.AccountById(prevop.DelegateId)
				if !ok {
					return Errorf("rollback: missing previous delegate id %d", prevop.DelegateId)
				}
			}
			if odlg == nil {
				// we must also look for the sources' origination op that may have
				// set the initial delegate
				if prevop, err := b.idx.FindOrigination(ctx, src.RowId, src.FirstSeen); err != nil {
					if err != index.ErrNoOpEntry {
						return Errorf("rollback: failed loading previous origination op for account %d: %v", src.RowId, err)
					}
				} else if prevop.DelegateId != 0 {
					odlg, ok = b.AccountById(prevop.DelegateId)
					if !ok {
						return Errorf("rollback: missing origin delegate %s", prevop.DelegateId)
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
