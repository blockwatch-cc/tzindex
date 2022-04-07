// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

// generate synthetic ops from flows for
// OpTypeInvoice
// OpTypeBake
// OpTypeUnfreeze
// OpTypeSeedSlash
// OpTypeBonus - reward to Ithaca proposer when <> baker
// OpTypeDeposit - Ithaca deposit event
// OpTypeReward - Ithaca endorsing reward
func (b *Builder) AppendImplicitEvents(ctx context.Context) error {
	flows := b.NewImplicitFlows()
	if len(flows) == 0 {
		return nil
	}
	b.block.Flows = append(b.block.Flows, flows...)

	// prepare ops
	ops := make([]*model.Op, flows[len(flows)-1].OpN+1)

	// parse all flows and reverse-assign to ops
	for _, f := range flows {
		if f.OpN < 0 || f.OpN >= len(ops) {
			log.Errorf("Implicit ops: out of range %d/%d", f.OpN, len(ops))
			continue
		}
		id := model.OpRef{
			N: f.OpN,                  // pos in block
			L: model.OPL_BLOCK_EVENTS, // list id
			P: f.OpN,                  // pos in list
		}
		switch f.Operation {
		case model.FlowTypeInvoice:
			// only append additional invoice op post-Florence
			if b.block.Params.Version >= 9 {
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeInvoice
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
					ops[f.OpN].SenderId = f.AccountId
					ops[f.OpN].Volume = f.AmountIn
				}
			}
		case model.FlowTypeBaking:
			if ops[f.OpN] == nil {
				id.Kind = model.OpTypeBake
				ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
				ops[f.OpN].SenderId = f.AccountId
			}
			// assuming only one flow per category per baker
			switch f.Category {
			case model.FlowCategoryDeposits:
				ops[f.OpN].Deposit = f.AmountIn
			case model.FlowCategoryRewards:
				ops[f.OpN].Reward = f.AmountIn
			case model.FlowCategoryBalance:
				// post-Ithaca only: fee is explicit (we hava a flow), so we can
				// add fee here; on pre-Ithaca protocols we sum op fees when updating
				// a block and then later add the block fee in the op indexer
				if f.IsFee {
					ops[f.OpN].Fee += f.AmountIn
				} else {
					ops[f.OpN].Reward += f.AmountIn
				}
			}
		case model.FlowTypeInternal:
			// only create ops for unfreeze-related internal events here
			if f.IsUnfrozen {
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeUnfreeze
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
					ops[f.OpN].SenderId = f.AccountId
				}
				// sum multiple flows per category per baker
				switch f.Category {
				case model.FlowCategoryDeposits:
					ops[f.OpN].Deposit += f.AmountOut
				case model.FlowCategoryRewards:
					ops[f.OpN].Reward += f.AmountOut
				case model.FlowCategoryFees:
					ops[f.OpN].Fee += f.AmountOut
				}
			}
		case model.FlowTypeNonceRevelation:
			// only seed slash events
			if f.IsBurned {
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeSeedSlash
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
				}
				// sum multiple consecutive seed slashes into one op
				switch f.Category {
				case model.FlowCategoryRewards:
					ops[f.OpN].Reward += f.AmountOut
					ops[f.OpN].Burned += f.AmountOut
				case model.FlowCategoryFees:
					ops[f.OpN].Fee += f.AmountOut
					ops[f.OpN].Burned += f.AmountOut
				case model.FlowCategoryBalance:
					ops[f.OpN].Reward += f.AmountIn
					ops[f.OpN].Burned += f.AmountOut
				}
			}
		case model.FlowTypeBonus:
			// Ithaca+
			if ops[f.OpN] == nil {
				id.Kind = model.OpTypeBonus
				ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
				ops[f.OpN].SenderId = f.AccountId
				ops[f.OpN].Reward = f.AmountIn
			} else {
				// add bonus to existing block proposer
				ops[f.OpN].Reward += f.AmountIn
			}
		case model.FlowTypeReward:
			// Ithaca+
			if f.IsBurned {
				// participation burn
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeReward
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
					ops[f.OpN].SenderId = f.AccountId
					ops[f.OpN].Reward = f.AmountIn
					ops[f.OpN].Burned = f.AmountIn
				}
			} else {
				// endorsement reward
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeReward
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
					ops[f.OpN].SenderId = f.AccountId
					ops[f.OpN].Reward = f.AmountIn
				}
			}
		case model.FlowTypeDeposit:
			// Ithaca+
			// explicit deposit payment (positive)
			// refund is translated into an unfreeze event
			if f.Category == model.FlowCategoryDeposits {
				if ops[f.OpN] == nil {
					id.Kind = model.OpTypeDeposit
					ops[f.OpN] = model.NewEventOp(b.block, f.AccountId, id)
					ops[f.OpN].SenderId = f.AccountId
					ops[f.OpN].Deposit = f.AmountIn
				}
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

// generate synthetic ops from block implicit ops (Granada+)
// Originations (on migration)
// Transactions / Subsidy
func (b *Builder) AppendImplicitBlockOps(ctx context.Context) error {
	for _, op := range b.block.TZ.Block.Metadata.ImplicitOperationsResults {
		Errorf := func(format string, args ...interface{}) error {
			return fmt.Errorf(
				"implicit block %s op [%d]: "+format,
				append([]interface{}{op.Kind, b.block.NextN()}, args...)...,
			)
		}
		n := b.block.NextN()
		id := model.OpRef{
			N: n,                      // pos in block
			L: model.OPL_BLOCK_HEADER, // list id
			P: n,                      // pos in list
		}
		switch op.Kind {
		case tezos.OpTypeOrigination:
			// for now we expect a single address only
			dst, ok := b.AccountByAddress(op.OriginatedContracts[0])
			if !ok {
				return Errorf("missing originated contract %s", op.OriginatedContracts[0])
			}
			// load script from RPC
			if op.Script == nil {
				var err error
				op.Script, err = b.rpc.GetContractScript(ctx, dst.Address)
				if err != nil {
					return Errorf("loading contract script %s: %w", dst.Address, err)
				}
			}
			id.Kind = model.MapOpType(op.Kind)
			o := model.NewEventOp(b.block, dst.RowId, id)
			o.IsContract = true
			dst.IsContract = true
			o.GasUsed = op.ConsumedGas
			o.StoragePaid = op.PaidStorageSizeDiff
			if op.Storage.IsValid() {
				o.Storage, _ = op.Storage.MarshalBinary()
			}

			// patch missing bigmap allocs
			typs := op.Script.BigmapTypesByName()
			ids := op.Script.BigmapsByName()
			if len(ids) > 0 {
				bmd := make(micheline.BigmapDiff, 0)
				for n, id := range ids {
					typ, _ := typs[n]
					diff := micheline.BigmapDiffElem{
						Action:    micheline.DiffActionAlloc,
						Id:        id,
						KeyType:   typ.Prim.Args[0],
						ValueType: typ.Prim.Args[1],
					}
					bmd = append(bmd, diff)
				}
				o.Diff, _ = bmd.MarshalBinary()
				o.BigmapDiff = bmd
			}

			// add volume if balance update exists
			for _, v := range op.BalanceUpdates {
				o.Volume += v.Amount()
				f := b.NewSubsidyFlow(dst, v.Amount(), id)
				b.block.Flows = append(b.block.Flows, f)
			}

			b.block.Ops = append(b.block.Ops, o)

			// register new implicit contract
			b.conMap[dst.RowId] = model.NewImplicitContract(dst, op, o)

		case tezos.OpTypeTransaction:
			for _, v := range op.BalanceUpdates {
				addr := v.Address()
				if !addr.IsValid() {
					continue
				}
				dst, ok := b.AccountByAddress(addr)
				if !ok {
					return Errorf("missing account %s", v.Address())
				}
				dCon, err := b.LoadContractByAccountId(ctx, dst.RowId)
				if err != nil {
					return Errorf("loading contract %d %s: %w", dst.RowId, v.Address(), err)
				}
				id.Kind = model.OpTypeSubsidy
				o := model.NewEventOp(b.block, dst.RowId, id)
				o.IsContract = true
				o.Volume = v.Amount()
				o.Reward = v.Amount()
				o.GasUsed = op.ConsumedGas
				o.StoragePaid = op.PaidStorageSizeDiff
				o.Entrypoint = 1
				o.Storage, _ = op.Storage.MarshalBinary()
				dCon.UpdateStorage(o, op.Storage)
				b.block.Ops = append(b.block.Ops, o)
				if o.Volume > 0 {
					b.block.Flows = append(b.block.Flows, b.NewSubsidyFlow(dst, o.Volume, id))
				}
			}
		}
	}
	return nil
}

func (b *Builder) AppendRegularBlockOps(ctx context.Context, rollback bool) error {
	for op_l, ol := range b.block.TZ.Block.Operations {
		for op_p, oh := range ol {
			for op_c, o := range oh.Contents {
				var err error
				id := model.OpRef{
					Hash: oh.Hash,
					Kind: model.MapOpType(o.Kind()),
					N:    b.block.NextN(),
					L:    op_l,
					P:    op_p,
					C:    op_c,
					I:    0,
					Raw:  o,
				}
				switch id.Kind {
				case model.OpTypeEndorsement, model.OpTypePreendorsement:
					err = b.AppendEndorsementOp(ctx, oh, id, rollback)
				case model.OpTypeTransaction:
					err = b.AppendTransactionOp(ctx, oh, id, rollback)
				case model.OpTypeDelegation:
					err = b.AppendDelegationOp(ctx, oh, id, rollback)
				case model.OpTypeReveal:
					err = b.AppendRevealOp(ctx, oh, id, rollback)
				case model.OpTypeNonceRevelation:
					err = b.AppendSeedNonceOp(ctx, oh, id, rollback)
				case model.OpTypeOrigination:
					err = b.AppendOriginationOp(ctx, oh, id, rollback)
				case model.OpTypeActivation:
					err = b.AppendActivationOp(ctx, oh, id, rollback)
				case model.OpTypeProposal:
					err = b.AppendProposalOp(ctx, oh, id, rollback)
				case model.OpTypeBallot:
					err = b.AppendBallotOp(ctx, oh, id, rollback)
				case model.OpTypeDoubleBaking:
					err = b.AppendDoubleBakingOp(ctx, oh, id, rollback)
				case model.OpTypeDoubleEndorsement, model.OpTypeDoublePreendorsement:
					err = b.AppendDoubleEndorsingOp(ctx, oh, id, rollback)
				case model.OpTypeRegisterConstant:
					err = b.AppendRegisterConstantOp(ctx, oh, id, rollback)
				case model.OpTypeDepositsLimit:
					err = b.AppendDepositsLimitOp(ctx, oh, id, rollback)
				}
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (b *Builder) AppendInvoiceOp(ctx context.Context, acc *model.Account, amount int64, p int) error {
	id := model.OpRef{
		Kind: model.OpTypeInvoice,
		N:    b.block.NextN(),
		L:    model.OPL_PROTOCOL_UPGRADE,
		P:    p,
	}
	b.block.Flows = append(b.block.Flows, b.NewInvoiceFlow(acc, amount, id))
	op := model.NewEventOp(b.block, acc.RowId, id)
	op.SenderId = acc.RowId
	op.Reward = amount
	b.block.Ops = append(b.block.Ops, op)
	return nil
}

func (b *Builder) AppendAirdropOp(ctx context.Context, acc *model.Account, amount int64, p int) error {
	id := model.OpRef{
		Kind: model.OpTypeAirdrop,
		N:    b.block.NextN(),
		L:    model.OPL_PROTOCOL_UPGRADE,
		P:    p,
	}
	b.block.Flows = append(b.block.Flows, b.NewAirdropFlow(acc, amount, id))
	op := model.NewEventOp(b.block, acc.RowId, id)
	op.Reward = amount
	b.block.Ops = append(b.block.Ops, op)
	return nil
}

func (b *Builder) AppendMagicBakerRegistrationOp(ctx context.Context, bkr *model.Baker, p int) error {
	id := model.OpRef{
		Kind: model.OpTypeDelegation,
		N:    b.block.NextN(),
		L:    model.OPL_PROTOCOL_UPGRADE,
		P:    p,
	}
	op := model.NewEventOp(b.block, 0, id)
	op.SenderId = bkr.AccountId
	op.BakerId = bkr.AccountId
	b.block.Ops = append(b.block.Ops, op)
	return nil
}

func (b *Builder) AppendContractMigrationOp(ctx context.Context, acc *model.Account, c *model.Contract, p int) error {
	id := model.OpRef{
		Kind: model.OpTypeMigration,
		N:    b.block.NextN(),
		L:    model.OPL_PROTOCOL_UPGRADE,
		P:    p,
	}
	op := model.NewEventOp(b.block, acc.RowId, id)
	op.IsContract = true
	op.Storage = c.Storage
	b.block.Ops = append(b.block.Ops, op)
	return nil
}

func (b *Builder) AppendActivationOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)
	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P}, args...)...,
		)
	}

	aop, ok := o.(*rpc.Activation)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}

	// need to lookup using blinded key
	bkey, err := tezos.BlindAddress(aop.Pkh, aop.Secret)
	if err != nil {
		return Errorf("blinded address creation failed: %w", err)
	}

	acc, ok := b.AccountByAddress(bkey)
	if !ok {
		return Errorf("missing account %s", aop.Pkh)
	}

	// cross-check if account exists under it's implicit address
	origacc, ok := b.AccountByAddress(aop.Pkh)
	if !ok {
		origacc, _ = b.idx.LookupAccount(ctx, aop.Pkh)
	}

	// read activated amount, required for potential re-routing
	fees := aop.Fees()
	activated := fees[len(fees)-1].Amount()

	// build op
	op := model.NewOp(b.block, id)
	op.IsSuccess = true
	op.Status = tezos.OpStatusApplied
	op.Volume = activated
	op.SenderId = acc.RowId
	op.ReceiverId = acc.RowId
	op.Data = hex.EncodeToString(aop.Secret) + "," + bkey.String()
	b.block.Ops = append(b.block.Ops, op)

	// update account
	if !rollback {
		// remove blinded hash from builder
		key := b.accCache.AccountHashKey(acc)
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
			acc.IsActivated = true
			acc.NOps++
			acc.IsDirty = true

			// register original account with builder
			b.accHashMap[b.accCache.AccountHashKey(origacc)] = origacc
			b.accMap[origacc.RowId] = origacc

			// use original account from now
			acc = origacc
		} else {
			// update blinded account with new hash
			acc.Address = aop.Pkh
			acc.Type = aop.Pkh.Type
			acc.FirstSeen = b.block.Height
			acc.LastSeen = b.block.Height
			acc.IsActivated = true
			acc.IsFunded = true
			acc.IsDirty = true
			b.accHashMap[b.accCache.AccountHashKey(acc)] = acc
		}
	} else {
		// check if deactivated blinded account exists
		blindedacc, _ := b.idx.LookupAccount(ctx, bkey)
		if blindedacc != nil {
			// reactivate blinded account
			blindedacc.SpendableBalance = activated

			// register blinded account with builder
			b.accHashMap[b.accCache.AccountHashKey(blindedacc)] = blindedacc
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
			delete(b.accHashMap, b.accCache.AccountHashKey(acc))
			acc.NOps--
			acc.Address = bkey
			acc.Type = bkey.Type
			acc.IsActivated = false
			acc.IsDirty = true
			acc.FirstSeen = 1 // reset to genesis
			acc.LastSeen = 1  // reset to genesis
			b.accHashMap[b.accCache.AccountHashKey(acc)] = acc
		}
	}

	// build flows after rerouting happend
	_ = b.NewActivationFlow(acc, aop, id)
	return nil
}

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
	power := meta.EndorsementPower + meta.PreendorsementPower
	op.Data = strconv.Itoa(power)

	// build flows; post-Ithaca this is empty
	flows := b.NewEndorserFlows(bkr.Account, meta.Balances(), id)

	// fill op amounts from flows
	for _, f := range flows {
		switch f.Category {
		case model.FlowCategoryRewards:
			op.Reward += f.AmountIn
		case model.FlowCategoryDeposits:
			op.Deposit += f.AmountIn
		case model.FlowCategoryBalance:
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
			bkr.SlotsEndorsed -= int64(power)
		}
		bkr.IsDirty = true
		bkr.Account.LastSeen = b.block.Height - 1 // approximation
		bkr.Account.IsDirty = true
	}
	return nil
}

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
	buf := bytes.NewBuffer(make([]byte, 0, len(pop.Proposals)*tezos.HashTypeProtocol.Base58Len()-1))
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
	op.SenderId = b.block.BakerId

	// lookup baker for original level
	if refBlock, err := b.idx.BlockByHeight(ctx, sop.Level); err != nil {
		return Errorf("missing block %d: %w", sop.Level, err)
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
		b.block.Baker.NBakerOps++
		b.block.Baker.NSeedNonce++
		b.block.Baker.IsDirty = true
		b.block.Baker.Account.LastSeen = b.block.Height
		b.block.Baker.Account.IsDirty = true
	} else {
		b.block.Baker.NBakerOps++
		b.block.Baker.NSeedNonce--
		b.block.Baker.IsDirty = true
		b.block.Baker.Account.IsDirty = true
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

	// determine who's who
	// - before Ithaca: last & first (dynamic count of updates when one subaccount is empty)
	// - after Ithaca: second, third (always 4 entries)
	upd := dop.Fees()
	accuserIndex, offenderIndex := 1, 2
	if b.block.Params.Version < 12 {
		accuserIndex, offenderIndex = len(upd)-1, 0
	}
	addr := upd[accuserIndex].Address()
	accuser, ok := b.BakerByAddress(addr)
	if !ok {
		return Errorf("missing accuser account %s", addr)
	}
	addr = upd[offenderIndex].Address()
	offender, ok := b.BakerByAddress(addr)
	if !ok {
		return Errorf("missing offender account %s", addr)
	}

	// build flows first to determine burn
	flows := b.NewDenunciationFlows(accuser, offender, upd, id)

	// build op
	op := model.NewOp(b.block, id)
	op.IsSuccess = true
	op.Status = tezos.OpStatusApplied
	op.SenderId = accuser.AccountId
	op.ReceiverId = offender.AccountId

	// we store both block headers as json array
	buf, err := json.Marshal(dop.Strip())
	if err != nil {
		return Errorf("cannot write data: %w", err)
	}
	op.Data = string(buf)

	// calc burn from flows
	for _, f := range flows {
		if f.IsBurned {
			// track all burned coins
			op.Burned += f.AmountOut

			// track offender losses by category
			switch f.Category {
			case model.FlowCategoryRewards:
				op.Reward -= f.AmountOut
			case model.FlowCategoryDeposits:
				op.Deposit -= f.AmountOut
			case model.FlowCategoryFees:
				op.Fee -= f.AmountOut
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
		accuser.NBakerOps++
		accuser.NAccusations++
		accuser.IsDirty = true
		acc := accuser.Account
		acc.LastSeen = b.block.Height
		acc.IsDirty = true

		offender.NBakerOps++
		offender.N2Baking++
		offender.IsDirty = true
		acc = offender.Account
		acc.LastSeen = b.block.Height
		acc.IsDirty = true
	} else {
		accuser.NBakerOps--
		accuser.NAccusations--
		accuser.IsDirty = true
		acc := accuser.Account
		acc.LastSeen = util.Max64N(acc.LastSeen, acc.LastIn, acc.LastOut)
		acc.IsDirty = true

		offender.NBakerOps--
		offender.N2Baking--
		offender.IsDirty = true
		acc = offender.Account
		acc.LastSeen = util.Max64N(acc.LastSeen, acc.LastIn, acc.LastOut)
		acc.IsDirty = true
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

	// determine who's who
	// - before Ithaca: last & first (dynamic count of updates when one subaccount is empty)
	// - after Ithaca: second, third (always 4 entries)
	upd := dop.Fees()
	accuserIndex, offenderIndex := 1, 2
	if b.block.Params.Version < 12 {
		accuserIndex, offenderIndex = len(upd)-1, 0
	}
	addr := upd[accuserIndex].Address()
	accuser, ok := b.BakerByAddress(addr)
	if !ok {
		return Errorf("missing accuser account %s", addr)
	}
	addr = upd[offenderIndex].Address()
	offender, ok := b.BakerByAddress(addr)
	if !ok {
		return Errorf("missing offender account %s", addr)
	}

	// build flows first to determine burn
	flows := b.NewDenunciationFlows(accuser, offender, upd, id)

	// build op
	op := model.NewOp(b.block, id)
	op.IsSuccess = true
	op.Status = tezos.OpStatusApplied
	op.SenderId = accuser.AccountId
	op.ReceiverId = offender.AccountId

	// we store double-endorsed evidences as JSON
	buf, err := json.Marshal(dop.Strip())
	if err != nil {
		return Errorf("cannot write data: %w", err)
	}
	op.Data = string(buf)

	// calc burn from flows
	for _, f := range flows {
		if f.IsBurned {
			// track all burned coins
			op.Burned += f.AmountOut

			// track offender losses by category
			switch f.Category {
			case model.FlowCategoryRewards:
				op.Reward -= f.AmountOut
			case model.FlowCategoryDeposits:
				op.Deposit -= f.AmountOut
			case model.FlowCategoryFees:
				op.Fee -= f.AmountOut
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
		accuser.NBakerOps++
		accuser.NAccusations++
		accuser.IsDirty = true
		acc := accuser.Account
		acc.LastSeen = b.block.Height
		acc.IsDirty = true

		offender.NBakerOps++
		offender.N2Endorsement++
		offender.IsDirty = true
		acc = offender.Account
		acc.LastSeen = b.block.Height
		acc.IsDirty = true
	} else {
		accuser.NBakerOps--
		accuser.NAccusations--
		accuser.IsDirty = true
		acc := accuser.Account
		acc.LastSeen = util.Max64N(acc.LastSeen, acc.LastIn, acc.LastOut)
		acc.IsDirty = true

		offender.NBakerOps--
		offender.N2Endorsement--
		offender.IsDirty = true
		acc = offender.Account
		acc.LastSeen = util.Max64N(acc.LastSeen, acc.LastIn, acc.LastOut)
		acc.IsDirty = true
	}
	return nil
}

// manager operation, extends grace period
func (b *Builder) AppendRevealOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P}, args...)...,
		)
	}

	rop, ok := o.(*rpc.Reveal)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	src, ok := b.AccountByAddress(rop.Source)
	if !ok {
		return Errorf("missing account %s", rop.Source)
	}
	var sbkr *model.Baker
	if src.BakerId != 0 {
		if sbkr, ok = b.BakerById(src.BakerId); !ok {
			return Errorf("missing baker account %d for source account %d %s", src.BakerId, src.RowId, src)
		}
	}

	// build op
	op := model.NewOp(b.block, id)
	op.SenderId = src.RowId
	op.Counter = rop.Counter
	op.Fee = rop.Fee
	op.GasLimit = rop.GasLimit
	op.StorageLimit = rop.StorageLimit
	op.Data = rop.PublicKey.String()

	res := rop.Result()
	op.GasUsed = res.ConsumedGas
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	_ = b.NewRevealFlows(src, sbkr, rop.Fees(), id)

	// extend grace period for bakers who send reveal ops
	if src.IsBaker {
		if bkr, ok := b.BakerById(src.BakerId); ok {
			bkr.UpdateGracePeriod(b.block.Cycle, b.block.Params)
		}
	}

	// update accounts
	if !rollback {
		src.Counter = op.Counter
		src.LastSeen = b.block.Height
		src.IsDirty = true
		if !op.IsSuccess {
			src.NOps++
			src.NOpsFailed++
			if len(res.Errors) > 0 {
				if buf, err := json.Marshal(res.Errors); err == nil {
					op.Errors = buf
				} else {
					// non-fatal, but error data will be missing from index
					log.Error(Errorf("marshal op errors: %s", err))
				}
			}
		} else {
			src.NOps++
			src.IsRevealed = true
			src.Pubkey = rop.PublicKey.Bytes()
		}
	} else {
		if !op.IsSuccess {
			src.NOps--
			src.NOpsFailed--
			src.IsDirty = true
		} else {
			src.NOps--
			src.IsRevealed = false
			src.Pubkey = nil
			src.IsDirty = true
		}
	}
	b.block.Ops = append(b.block.Ops, op)
	return nil
}

// can implicitly burn a fee when new account is created
// NOTE: this seems to not extend grace period
func (b *Builder) AppendTransactionOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
		)
	}

	top, ok := o.(*rpc.Transaction)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}
	src, ok := b.AccountByAddress(top.Source)
	if !ok {
		return Errorf("missing source account %s", top.Source)
	}
	dst, ok := b.AccountByAddress(top.Destination)
	if !ok {
		return Errorf("missing target account %s", top.Destination)
	}

	var (
		sbkr, dbkr *model.Baker
		sCon, dCon *model.Contract
		err        error
	)
	if src.BakerId != 0 {
		if sbkr, ok = b.BakerById(src.BakerId); !ok {
			return Errorf("missing baker %d for source account %d", src.BakerId, src.RowId)
		}
	}
	if dst.BakerId != 0 {
		if dbkr, ok = b.BakerById(dst.BakerId); !ok {
			return Errorf("missing baker %d for dest account %d", dst.BakerId, dst.RowId)
		}
	}
	if src.IsContract {
		sCon, err = b.LoadContractByAccountId(ctx, src.RowId)
		if err != nil {
			return Errorf("loading contract %s %d: %w", top.Source, src.RowId, err)
		}
	}
	if dst.IsContract {
		dCon, err = b.LoadContractByAccountId(ctx, dst.RowId)
		if err != nil {
			return Errorf("loading contract %s %d: %w", top.Destination, dst.RowId, err)
		}
	}

	// build op
	op := model.NewOp(b.block, id)
	op.SenderId = src.RowId
	op.ReceiverId = dst.RowId
	op.Counter = top.Counter
	op.Fee = top.Fee
	op.GasLimit = top.GasLimit
	op.StorageLimit = top.StorageLimit
	op.IsContract = dst.IsContract
	res := top.Result()
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.ConsumedGas
	op.Volume = top.Amount
	op.StoragePaid = res.PaidStorageSizeDiff

	if top.Parameters.Value.IsValid() {
		op.Parameters, err = top.Parameters.MarshalBinary()
		if err != nil {
			return Errorf("marshal params: %w", err)
		}

		if dCon != nil {
			pTyp, _, err := dCon.LoadType()
			if err != nil {
				return Errorf("loading script: %w", err)
			}
			ep, _, err := top.Parameters.MapEntrypoint(pTyp)
			if op.IsSuccess && err != nil {
				return Errorf("searching entrypoint: %w", err)
			}
			op.Entrypoint = ep.Id
			op.Data = ep.Name
		}
	}
	if res.Storage.IsValid() {
		op.Storage, err = res.Storage.MarshalBinary()
		if err != nil {
			return Errorf("marshal storage: %w", err)
		}
		// dedup (don't store storage when similar to current value)
		if bytes.Compare(op.Storage, dCon.Storage) == 0 {
			op.Storage = nil
		}
	}

	if len(res.BigmapDiff) > 0 {
		op.BigmapDiff, err = b.PatchBigmapDiff(ctx, res.BigmapDiff, dst.Address, nil)
		if err != nil {
			return Errorf("patch bigmap: %w", err)
		}
		op.Diff, err = op.BigmapDiff.MarshalBinary()
		if err != nil {
			return Errorf("marshal bigmap: %w", err)
		}
	}

	var flows []*model.Flow

	if op.IsSuccess {
		flows = b.NewTransactionFlows(
			src, dst, // involved accounts
			sbkr, dbkr, // related bakers (optional)
			sCon, dCon, // contracts (optional)
			top.Fees(),     // fees
			res.Balances(), // move
			b.block,
			id,
		)

		// update burn from burn flow (for implicit originated contracts)
		for _, f := range flows {
			if f.IsBurned {
				op.Burned += f.AmountOut
			}
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

		// fees only
		flows = b.NewTransactionFlows(src, nil, sbkr, nil,
			nil, nil,
			top.Fees(),
			nil, // no result balance updates
			b.block,
			id,
		)
	}

	// update accounts
	if !rollback {
		src.Counter = op.Counter
		src.LastSeen = b.block.Height
		src.NOps++
		src.NTx++
		src.IsDirty = true
		dst.LastSeen = b.block.Height
		dst.NOps++
		dst.NTx++
		dst.IsDirty = true

		if !op.IsSuccess {
			src.NOpsFailed++
			dst.NOpsFailed++
		} else {
			// reactivate inactive bakers (receiver only)
			// - it seems from reverse engineering baker activation rules
			//   that received transactions will reactivate an inactive baker
			//   and extend grace period for active bakers
			// - don't do this for magic bakers (missing self-registration)
			// - support for this feature ends with proto_004
			if dst.IsBaker && b.block.Params.Version <= 3 {
				if dbkr == nil {
					// re-load baker if not set to handle accounts with dst.BakerId == 0
					dbkr, ok = b.BakerById(dst.RowId)
					if !ok {
						log.Warnf("missing baker for account %s %d", dst, dst.RowId)
					}
				}
				if dbkr != nil {
					if !dbkr.IsActive && dst.BakerId == dst.RowId {
						dbkr.IsActive = true
						dbkr.InitGracePeriod(b.block.Cycle, b.block.Params)
					} else {
						dbkr.UpdateGracePeriod(b.block.Cycle, b.block.Params)
					}
				}
			}

			// update contract from op
			if dCon != nil {
				dCon.UpdateStorage(op, res.Storage)
			}
		}
	} else {
		src.Counter = op.Counter - 1
		src.NOps--
		src.NTx--
		src.IsDirty = true
		dst.NOps--
		dst.NTx--
		dst.IsDirty = true
		if !op.IsSuccess {
			src.NOpsFailed--
			dst.NOpsFailed--
		} else {
			// rollback contract from previous op
			if dCon != nil {
				// if nil, will rollback to previous state
				prev, _ := b.idx.FindLastCall(ctx, dst.RowId, dst.FirstSeen, op.Height)
				if prev != nil {
					var store micheline.Prim
					store.UnmarshalBinary(prev.Storage)
					dCon.RollbackStorage(op, prev, store)
				}
			}
		}
	}

	// append before potential internal ops
	b.block.Ops = append(b.block.Ops, op)

	// apply internal operation result (may generate new op and flows)
	for i, v := range top.Metadata.InternalResults {
		id.I = i
		id.Kind = model.MapOpType(v.Kind)
		id.N++
		switch id.Kind {
		case model.OpTypeTransaction:
			if err := b.AppendInternalTransactionOp(ctx, src, sbkr, oh, v, id, rollback); err != nil {
				return err
			}
		case model.OpTypeDelegation:
			if err := b.AppendInternalDelegationOp(ctx, src, sbkr, oh, v, id, rollback); err != nil {
				return err
			}
		case model.OpTypeOrigination:
			if err := b.AppendInternalOriginationOp(ctx, src, sbkr, oh, v, id, rollback); err != nil {
				return err
			}
		default:
			return Errorf("unsupported internal operation type %s", v.Kind)
		}
	}
	return nil
}

func (b *Builder) AppendInternalTransactionOp(
	ctx context.Context,
	origsrc *model.Account,
	origbkr *model.Baker,
	oh *rpc.Operation,
	iop rpc.InternalResult,
	id model.OpRef,
	rollback bool) error {

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"internal %s op [%d:%d:%d:%d]: "+format,
			append([]interface{}{iop.Kind, id.L, id.P, id.C, id.I}, args...)...,
		)
	}

	src, ok := b.AccountByAddress(iop.Source)
	if !ok {
		return Errorf("missing source account %s", iop.Source)
	}
	dst, ok := b.AccountByAddress(iop.Destination)
	if !ok {
		return Errorf("missing source account %s", iop.Destination)
	}

	var (
		sbkr, dbkr *model.Baker
		sCon, dCon *model.Contract
		err        error
	)
	if src.BakerId != 0 {
		if sbkr, ok = b.BakerById(src.BakerId); !ok {
			return Errorf("missing baker %d for source account %d", src.BakerId, src.RowId)
		}
	}
	if dst.BakerId != 0 {
		if dbkr, ok = b.BakerById(dst.BakerId); !ok {
			return Errorf("missing baker %d for dest account %d", dst.BakerId, dst.RowId)
		}
	}
	if src.IsContract {
		sCon, err = b.LoadContractByAccountId(ctx, src.RowId)
		if err != nil {
			return Errorf("loading contract %s %d: %w", iop.Source, src.RowId, err)
		}
	}
	if dst.IsContract {
		dCon, err = b.LoadContractByAccountId(ctx, dst.RowId)
		if err != nil {
			return Errorf("loading contract %s %d: %w", iop.Destination, dst.RowId, err)
		}
	}

	// build op (internal and outer tx share the same hash and block location)
	op := model.NewOp(b.block, id)
	op.IsInternal = true
	op.IsContract = dst.IsContract
	op.SenderId = origsrc.RowId
	op.ReceiverId = dst.RowId
	op.CreatorId = src.RowId
	op.Counter = iop.Nonce
	op.Fee = 0          // n.a. for internal ops
	op.GasLimit = 0     // n.a. for internal ops
	op.StorageLimit = 0 // n.a. for internal ops
	op.Volume = iop.Amount
	res := iop.Result
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.ConsumedGas
	op.StoragePaid = res.PaidStorageSizeDiff

	if iop.Parameters.Value.IsValid() {
		op.Parameters, err = iop.Parameters.MarshalBinary()
		if err != nil {
			return Errorf("marshal params: %w", err)
		}
		if dCon != nil {
			pTyp, _, err := dCon.LoadType()
			if err != nil {
				return Errorf("loading script for %s: %w", dst, err)
			}
			ep, _, err := iop.Parameters.MapEntrypoint(pTyp)
			if op.IsSuccess && err != nil {
				return Errorf("searching entrypoint in %s: %w", dst, err)
			}
			op.Entrypoint = ep.Id
			op.Data = ep.Name
		}
	}
	if res.Storage.IsValid() {
		op.Storage, err = res.Storage.MarshalBinary()
		if err != nil {
			return Errorf("marshal storage: %w", err)
		}
		// dedup (don't store storage when similar to current value)
		if bytes.Compare(op.Storage, dCon.Storage) == 0 {
			op.Storage = nil
		}
	}
	if len(res.BigmapDiff) > 0 {
		// patch original bigmap diff
		op.BigmapDiff, err = b.PatchBigmapDiff(ctx, res.BigmapDiff, dst.Address, nil)
		if err != nil {
			return Errorf("patch bigmap: %w", err)
		}
		op.Diff, err = op.BigmapDiff.MarshalBinary()
		if err != nil {
			return Errorf("marshal bigmap: %w", err)
		}
	}

	var flows []*model.Flow

	// on success, create flows and update accounts
	if op.IsSuccess {
		flows = b.NewInternalTransactionFlows(
			origsrc, src, dst, // outer and inner source, inner dest
			origbkr, sbkr, dbkr, // bakers (optional)
			sCon, dCon, // contracts (optional)
			res.Balances(), // moved and burned amounts
			b.block,
			id,
		)

		// update burn from burn flow (for storage paid)
		for _, f := range flows {
			if f.IsBurned {
				op.Burned += f.AmountOut
			}
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
		// a negative outcome leaves no trace because fees are paid by outer tx
	}

	// update accounts
	if !rollback {
		src.Counter = op.Counter
		src.LastSeen = b.block.Height
		src.NOps++
		src.NTx++
		src.IsDirty = true
		dst.NOps++
		dst.NTx++
		dst.LastSeen = b.block.Height
		dst.IsDirty = true
		if !op.IsSuccess {
			src.NOpsFailed++
			dst.NOpsFailed++
		} else {
			// update contract from op
			if dCon != nil {
				dCon.UpdateStorage(op, res.Storage)
			}
		}
	} else {
		src.Counter = op.Counter - 1
		src.NOps--
		src.NTx--
		src.IsDirty = true
		dst.NOps--
		dst.NTx--
		dst.IsDirty = true
		if !op.IsSuccess {
			src.NOpsFailed--
			dst.NOpsFailed--
		} else {
			// rollback contract from previous op
			if dCon != nil {
				// if nil, will rollback to previous state
				prev, _ := b.idx.FindLastCall(ctx, dst.RowId, dst.FirstSeen, op.Height)
				if prev != nil {
					var store micheline.Prim
					store.UnmarshalBinary(prev.Storage)
					dCon.RollbackStorage(op, prev, store)
				}
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
func (b *Builder) AppendOriginationOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
		)
	}

	oop, ok := o.(*rpc.Origination)
	if !ok {
		return Errorf("unexpected type %T", o)
	}

	src, ok := b.AccountByAddress(oop.Source)
	if !ok {
		return Errorf("missing source account %s", oop.Source)
	}
	var (
		mgr, dst       *model.Account
		srcbkr, newbkr *model.Baker
	)
	if a := oop.ManagerAddress(); a.IsValid() {
		mgr, ok = b.AccountByAddress(a)
		if !ok {
			return Errorf("missing manager account %s", a)
		}
	}
	if src.BakerId != 0 {
		if srcbkr, ok = b.BakerById(src.BakerId); !ok {
			return Errorf("missing baker %d for source account %d", src.BakerId, src.RowId)
		}
	}
	res := oop.Result()
	if oop.Delegate != nil && res.Status.IsSuccess() {
		newbkr, ok = b.BakerByAddress(*oop.Delegate)
		// register self bakers if not registered yet (v002 fixed this)
		if !ok && b.block.Params.Version <= 1 && src.Address.Equal(*oop.Delegate) && !src.IsBaker {
			newbkr = b.RegisterBaker(src, false)
			// log.Infof("Origination bug baker register %s", newbkr)
		}
		if newbkr == nil {
			return Errorf("missing baker account %s", oop.Delegate)
		}
	}

	// build op
	op := model.NewOp(b.block, id)
	op.SenderId = src.RowId
	op.Counter = oop.Counter
	op.Fee = oop.Fee
	op.GasLimit = oop.GasLimit
	op.StorageLimit = oop.StorageLimit
	op.IsContract = oop.Script != nil
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.ConsumedGas
	op.Volume = oop.Balance
	op.StoragePaid = res.PaidStorageSizeDiff

	// store manager and baker
	if mgr != nil {
		op.CreatorId = mgr.RowId
	}
	if newbkr != nil {
		op.BakerId = newbkr.AccountId
	}

	var (
		flows []*model.Flow
		err   error
	)
	if op.IsSuccess {
		if l := len(res.OriginatedContracts); l != 1 {
			return Errorf("%d originated accounts, expected exactly 1", l)
		}

		addr := res.OriginatedContracts[0]
		dst, ok = b.AccountByAddress(addr)
		if !ok {
			return Errorf("missing originated account %s", addr)
		}
		op.ReceiverId = dst.RowId
		flows = b.NewOriginationFlows(
			src, dst,
			srcbkr, newbkr,
			oop.Fees(),
			res.Balances(),
			id,
		)

		// update burn from burn flow
		for _, f := range flows {
			if f.IsBurned {
				op.Burned += f.AmountOut
			}
		}

		// create or extend bigmap diff to inject alloc for proto < v005
		// overwrite original diff
		if len(res.BigmapDiff) > 0 {
			op.BigmapDiff, err = b.PatchBigmapDiff(ctx, res.BigmapDiff, dst.Address, oop.Script)
			if err != nil {
				return Errorf("patch bigmap: %w", err)
			}
			op.Diff, err = op.BigmapDiff.MarshalBinary()
			if err != nil {
				return Errorf("marshal bigmap: %w", err)
			}
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

		// fees flows
		flows = b.NewOriginationFlows(src, nil, srcbkr, nil, oop.Fees(), nil, id)
	}

	b.block.Ops = append(b.block.Ops, op)

	// update accounts
	if !rollback {
		src.Counter = op.Counter
		src.NOps++
		src.NOrigination++
		src.LastSeen = b.block.Height
		src.IsDirty = true
		src.NOps++
		src.NOrigination++
		src.LastSeen = b.block.Height
		src.IsDirty = true

		if !op.IsSuccess {
			src.NOpsFailed++
		} else {
			// initialize originated account
			// pre-Babylon delegator KT1s had no code, post-babylon keep sender
			// as creator, until Athens it was called manager
			dst.IsContract = oop.Script != nil
			if mgr != nil {
				dst.CreatorId = mgr.RowId
				mgr.LastSeen = b.block.Height
				mgr.IsDirty = true
			} else {
				dst.CreatorId = src.RowId
			}
			dst.UnclaimedBalance = int64(oop.BabylonFlags(b.block.Params.Version))
			dst.LastSeen = b.block.Height
			dst.IsDirty = true

			// handle delegation
			if newbkr != nil {
				dst.IsDelegated = true
				dst.BakerId = newbkr.AccountId
				dst.DelegatedSince = b.block.Height

				newbkr.TotalDelegations++
				if op.Volume > 0 {
					newbkr.ActiveDelegations++
				}
				newbkr.IsDirty = true
				newbkr.Account.LastSeen = b.block.Height
				newbkr.Account.IsDirty = true
			}

			// create and register a temporary contract for potential use by
			// subsequent transactions in the same block (this happened once
			// on Carthagenet block 346116); the actual contract table entry
			// is later created & inserted separately in contract index and
			// this temporary object is discarded
			dstCon := model.NewContract(dst, oop, op, nil, b.block.Params)

			// if oop.Script.Code.Storage.ContainsOpCode(micheline.T_BIG_MAP) {
			// 	// re-fetch initial storage to resolve bigmap pointers
			// 	storage, err := b.rpc.GetContractStorage(ctx, dst.Address, rpc.BlockLevel(b.block.Height))
			// 	if err != nil {
			// 		return Errorf("fetching initial storage: %w", err)
			// 	}
			// 	dstCon.SetStorage(storage)
			// }

			// add to map
			b.conMap[dst.RowId] = dstCon
		}
	} else {
		src.Counter = op.Counter - 1
		src.NOps--
		src.NOrigination--
		src.IsDirty = true
		if !op.IsSuccess {
			src.NOpsFailed--
		} else {
			// reverse origination, dst will be deleted
			dst.MustDelete = true
			dst.IsDirty = true
			if newbkr != nil {
				dst.IsDelegated = false
				dst.BakerId = 0
				newbkr.TotalDelegations--
				if op.Volume > 0 {
					newbkr.ActiveDelegations--
				}
				newbkr.IsDirty = true
			}
			// ignore, not possible anymore
			// // handle self-delegate deregistration (note: there is no previous delegate)
			// if b.block.Params.HasOriginationBug && newdlg != nil && newdlg.TotalDelegations == 0 && src.RowId == newdlg.RowId {
			// 	b.UnregisterDelegate(newdlg)
			// }
		}
	}

	return nil
}

func (b *Builder) AppendInternalOriginationOp(
	ctx context.Context,
	origsrc *model.Account,
	origbkr *model.Baker,
	oh *rpc.Operation,
	iop rpc.InternalResult,
	id model.OpRef,
	rollback bool) error {

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"internal %s op [%d:%d:%d:%d]: "+format,
			append([]interface{}{iop.Kind, id.L, id.P, id.C, id.I}, args...)...,
		)
	}

	src, ok := b.AccountByAddress(iop.Source)
	if !ok {
		return Errorf("missing source account %s", iop.Source)
	}

	var (
		srcbkr, newbkr *model.Baker
		dst            *model.Account
	)
	if src.BakerId != 0 {
		if srcbkr, ok = b.BakerById(src.BakerId); !ok {
			return Errorf("missing baker %d for source account %d", src.BakerId, src.RowId)
		}
	}
	if iop.Delegate.IsValid() {
		if newbkr, ok = b.BakerByAddress(iop.Delegate); !ok && iop.Result.Status.IsSuccess() {
			return fmt.Errorf("internal origination op [%d:%d:%d]: missing baker account %s",
				id.L, id.P, id.I, iop.Delegate)
		}
	}

	// build op (internal and outer op share the same hash and block location)
	op := model.NewOp(b.block, id)
	op.IsInternal = true
	op.SenderId = origsrc.RowId
	op.CreatorId = src.RowId
	op.Counter = iop.Nonce
	op.Fee = 0           // n.a. for internal ops
	op.GasLimit = 0      // n.a. for internal ops
	op.StorageLimit = 0  // n.a. for internal ops
	op.IsContract = true // orign is always a contract op!
	res := iop.Result    // internal result
	op.Status = res.Status
	op.GasUsed = res.ConsumedGas
	op.IsSuccess = op.Status.IsSuccess()
	op.Volume = iop.Balance
	op.StoragePaid = res.PaidStorageSizeDiff

	// store baker
	if newbkr != nil {
		op.BakerId = newbkr.AccountId
	}

	var (
		flows []*model.Flow
		err   error
	)
	if op.IsSuccess {
		if l := len(res.OriginatedContracts); l != 1 {
			return Errorf("%d originated accounts", l)
		}

		addr := res.OriginatedContracts[0]
		dst, ok = b.AccountByAddress(addr)
		if !ok {
			return Errorf("missing originated account %s", addr)
		}
		op.ReceiverId = dst.RowId
		flows = b.NewInternalOriginationFlows(
			origsrc,
			src,
			dst,
			origbkr,
			srcbkr,
			newbkr,
			res.Balances(),
			id,
		)

		// update burn from burn flow
		for _, f := range flows {
			if f.IsBurned {
				op.Burned += f.AmountOut
			}
		}

		// create or extend bigmap diff to inject alloc for proto < v005
		// patch original result
		if len(res.BigmapDiff) > 0 {
			op.BigmapDiff, err = b.PatchBigmapDiff(ctx, res.BigmapDiff, dst.Address, iop.Script)
			if err != nil {
				return Errorf("patch bigmap: %w", err)
			}
			op.Diff, err = op.BigmapDiff.MarshalBinary()
			if err != nil {
				return Errorf("marshal bigmap: %w", err)
			}
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

		// no internal fees, no flows on failure
	}

	b.block.Ops = append(b.block.Ops, op)

	// update accounts
	if !rollback {
		src.Counter = op.Counter
		src.NOps++
		src.NOrigination++
		src.LastSeen = b.block.Height
		src.IsDirty = true
		if !op.IsSuccess {
			src.NOpsFailed++
		} else {
			dst.LastSeen = b.block.Height
			dst.IsContract = iop.Script != nil
			dst.IsDirty = true

			// internal originations have no manager, baker and flags (in protocol v5)
			// but we still keep the original caller as manager to track contract ownership
			dst.CreatorId = origsrc.RowId

			// handle delegation
			if newbkr != nil {
				dst.IsDelegated = true
				dst.BakerId = newbkr.AccountId
				dst.DelegatedSince = b.block.Height

				newbkr.TotalDelegations++
				if op.Volume > 0 {
					// delegation becomes active only when dst KT1 is funded
					newbkr.ActiveDelegations++
				}
				newbkr.IsDirty = true
				newbkr.Account.LastSeen = b.block.Height
				newbkr.Account.IsDirty = true
			}

			// create and register temporary a contract for potential use by
			// subsequent transactions in the same block (this happened once
			// on Carthagenet block 346116); the actual contract table entry
			// is later created & inserted separately in contract index and
			// this temporary object is discarded
			dstCon := model.NewInternalContract(dst, iop, op, nil)

			// if iop.Script.Code.Storage.ContainsOpCode(micheline.T_BIG_MAP) {
			// 	// re-fetch initial storage to resolve bigmap pointers
			// 	storage, err := b.rpc.GetContractStorage(ctx, dst.Address, rpc.BlockLevel(b.block.Height))
			// 	if err != nil {
			// 		return Errorf("fetching initial storage: %w", err)
			// 	}
			// 	dstCon.SetStorage(storage)
			// }

			// add to map
			b.conMap[dst.RowId] = dstCon
		}
	} else {
		src.Counter = op.Counter - 1
		src.NOps--
		src.NOrigination--
		src.IsDirty = true
		if !op.IsSuccess {
			src.NOpsFailed--
		} else {
			// reverse origination
			dst.MustDelete = true
			dst.IsDirty = true
			if newbkr != nil {
				dst.IsDelegated = false
				dst.BakerId = 0
				newbkr.TotalDelegations--
				if op.Volume > 0 {
					newbkr.ActiveDelegations--
				}
				newbkr.IsDirty = true
			}
		}
	}

	return nil
}

// Notes
// - manager operation, extends grace period
// - delegations may or may not pay a fee, so BalanceUpdates may be empty
// - early delegations did neither pay nor consume gas
// - status is either `applied` or `failed`
// - failed delegations may still pay fees, but don't consume gas
// - self-delegation (src == ndlg) represents a new/renewed baker registration
// - zero-delegation (ndlg == null) represents a delegation withdraw
//
// Start conditions when building flows           NewBaker       OldBaker
// -----------------------------------------------------------------------
// 1. new baker registration w/ fresh acc             -             -
// 1. new baker registration w/ delegated acc         -             x
// 2. baker re-registration                         self           self
// 3. new delegation                                  x             -
// 4. redelegation                                    x             y
// 5. failed redelegation                             x             x
// 6. delegation withdrawal                           -             x
// 7. failed delegation withdrawal                    -             -
//
func (b *Builder) AppendDelegationOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
		)
	}

	dop, ok := o.(*rpc.Delegation)
	if !ok {
		return Errorf("unexpected type %T ", o)
	}

	src, ok := b.AccountByAddress(dop.Source)
	if !ok {
		return Errorf("missing account %s", dop.Source)
	}

	// on delegator withdraw, new baker is empty!
	// at registration source may be a baker who re-registeres or may not be a baker yet!
	res := dop.Result()
	isRegistration := dop.Source.Equal(dop.Delegate)
	var nbkr, obkr *model.Baker
	if dop.Delegate.IsValid() {
		nbkr, ok = b.BakerByAddress(dop.Delegate)
		if !ok && res.Status.IsSuccess() && !isRegistration {
			return Errorf("missing baker %s", dop.Delegate)
		}
	}
	// re-delegations change baker
	// if !isRegistration && src.BakerId != 0 {
	if src.BakerId != 0 {
		if obkr, ok = b.BakerById(src.BakerId); !ok {
			return Errorf("missing baker %d for source account %d", src.BakerId, src.RowId)
		}
	}

	// build op
	op := model.NewOp(b.block, id)
	op.SenderId = src.RowId
	if isRegistration {
		op.BakerId = src.RowId
	} else if nbkr != nil {
		op.BakerId = nbkr.AccountId
	}
	if obkr != nil {
		op.ReceiverId = obkr.AccountId
	}
	op.Counter = dop.Counter
	op.Fee = dop.Fee
	op.GasLimit = dop.GasLimit
	op.StorageLimit = dop.StorageLimit

	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.ConsumedGas
	op.Volume = src.Balance()
	b.block.Ops = append(b.block.Ops, op)

	// build flows
	// - fee payment by source
	// - fee reception by baker
	// - (re)delegate source balance on success
	if op.IsSuccess {
		b.NewDelegationFlows(
			src,
			nbkr,
			obkr,
			dop.Fees(),
			id,
		)
	} else {
		// on error fees still deduct from old delegation
		b.NewDelegationFlows(
			src,
			obkr, // both =old !!
			obkr, // both =old !!
			dop.Fees(),
			id,
		)

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

	// update accounts
	if !rollback {
		src.Counter = op.Counter
		src.NOps++
		src.NDelegation++
		src.LastSeen = b.block.Height
		src.IsDirty = true
		if nbkr != nil {
			nbkr.Account.LastSeen = b.block.Height
			nbkr.Account.IsDirty = true
			nbkr.IsDirty = true
		}
		if obkr != nil {
			obkr.Account.LastSeen = b.block.Height
			obkr.Account.IsDirty = true
			obkr.IsDirty = true
		}

		if !op.IsSuccess {
			src.NOpsFailed++
		} else {
			// handle baker registration
			if isRegistration {
				if nbkr == nil {
					// first-time register
					nbkr = b.RegisterBaker(src, true)
					// log.Infof("Explicit baker register %s", nbkr)
				} else {
					// reactivate baker
					// log.Infof("Explicit baker reactivate %s", nbkr)
					nbkr.InitGracePeriod(b.block.Cycle, b.block.Params)
					nbkr.IsActive = true

					// due to origination bug on v001 we set again explicitly
					nbkr.Account.BakerId = nbkr.Account.RowId
				}
				// handle withdraw from old baker, exclude re-registrations
				if obkr != nil && obkr.AccountId != nbkr.AccountId {
					if src.Balance() > 0 {
						obkr.ActiveDelegations--
					}
				}

			} else {
				// handle delegator withdraw
				if nbkr == nil {
					src.IsDelegated = false
					src.BakerId = 0
					src.DelegatedSince = 0
				} else {
					// handle switch to new baker
					// only update when this delegation changes baker
					if src.BakerId != nbkr.AccountId {
						src.IsDelegated = true
						src.BakerId = nbkr.AccountId
						src.DelegatedSince = b.block.Height
						nbkr.TotalDelegations++
						// delegation becomes active only when src is funded
						if src.Balance() > 0 {
							nbkr.ActiveDelegations++
						}
					}
				}
				// handle withdraw from old baker if any
				if obkr != nil {
					if src.Balance() > 0 {
						obkr.ActiveDelegations--
					}
				}
			}
		}
	} else {
		// rollback accounts
		src.Counter = op.Counter - 1
		src.NOps--
		src.NDelegation--
		src.IsDirty = true
		if !op.IsSuccess {
			src.NOpsFailed--
		} else {
			if isRegistration {
				// reverse effects of baker registration on base account
				src.BakerId = 0
				src.IsBaker = false
				src.IsDirty = true

				// don't touch baker object, will be dropped from table by index
				// in case this was its first registration, but remove from builder
				// cache
				hashkey := b.accCache.AccountHashKey(src)
				b.accMap[src.RowId] = src
				b.accHashMap[hashkey] = src
				delete(b.bakerMap, src.RowId)
				delete(b.bakerHashMap, hashkey)
			}

			// find previous baker, if any (accounts who are delegated can later
			// become a baker, so we must search either way)
			var lastsince int64
			if prevop, err := b.idx.FindLatestDelegation(ctx, src.RowId); err != nil {
				if err != index.ErrNoOpEntry {
					return Errorf("rollback: failed loading previous delegation op for account %d: %w", src.RowId, err)
				}
				obkr = nil
			} else if prevop.BakerId > 0 {
				lastsince = prevop.Height
				obkr, ok = b.BakerById(prevop.BakerId)
				if !ok {
					return Errorf("rollback: missing previous baker id %d", prevop.BakerId)
				}
			}
			if obkr == nil {
				// we must also look for the sources' origination op that may have
				// set the initial baker
				if prevop, err := b.idx.FindOrigination(ctx, src.RowId, src.FirstSeen); err != nil {
					if err != index.ErrNoOpEntry {
						return Errorf("rollback: failed loading previous origination op for account %d: %w", src.RowId, err)
					}
				} else if prevop.BakerId != 0 {
					obkr, ok = b.BakerById(prevop.BakerId)
					if !ok {
						return Errorf("rollback: missing origin baker %s", prevop.BakerId)
					}
				}
			}

			// reverse new baker setting
			if nbkr != nil {
				nbkr.TotalDelegations--
				nbkr.ActiveDelegations--
			}

			// reverse handle withdraw from old baker
			if obkr != nil {
				src.IsDelegated = true
				src.BakerId = obkr.AccountId
				src.DelegatedSince = lastsince
				obkr.ActiveDelegations++
			} else {
				src.IsDelegated = false
				src.BakerId = 0
				src.DelegatedSince = 0
			}
		}
	}

	return nil
}

func (b *Builder) AppendInternalDelegationOp(
	ctx context.Context,
	origsrc *model.Account,
	origbkr *model.Baker,
	oh *rpc.Operation,
	iop rpc.InternalResult,
	id model.OpRef,
	rollback bool) error {

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"internal %s op [%d:%d:%d:%d]: "+format,
			append([]interface{}{iop.Kind, id.L, id.P, id.C, id.I}, args...)...,
		)
	}

	src, ok := b.AccountByAddress(iop.Source)
	if !ok {
		return Errorf("missing source account %s", iop.Source)
	}

	var obkr, nbkr *model.Baker
	if src.BakerId != 0 {
		if obkr, ok = b.BakerById(src.BakerId); !ok {
			return Errorf("missing baker %d for source account %d", src.BakerId, src.RowId)
		}
	}
	if iop.Delegate.IsValid() {
		nbkr, ok = b.BakerByAddress(iop.Delegate)
		if !ok && iop.Result.Status.IsSuccess() {
			return Errorf("missing account %s", iop.Delegate)
		}
	}

	// build op (internal and outer op share the same hash and block location)
	op := model.NewOp(b.block, id)
	op.IsInternal = true
	op.SenderId = origsrc.RowId
	op.CreatorId = src.RowId
	if nbkr != nil {
		op.BakerId = nbkr.AccountId
	}
	if obkr != nil {
		op.ReceiverId = obkr.AccountId
	}
	op.Counter = iop.Nonce
	op.Fee = 0          // n.a. for internal ops
	op.GasLimit = 0     // n.a. for internal ops
	op.StorageLimit = 0 // n.a. for internal ops
	res := iop.Result   // internal result
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
		b.NewDelegationFlows(src, nbkr, obkr, nil, id)
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

	// update accounts
	if !rollback {
		src.Counter = op.Counter
		src.NOps++
		src.NDelegation++
		src.IsDirty = true
		src.LastSeen = b.block.Height
		if nbkr != nil {
			nbkr.Account.LastSeen = b.block.Height
			nbkr.Account.IsDirty = true
		}
		if obkr != nil {
			obkr.Account.LastSeen = b.block.Height
			obkr.Account.IsDirty = true
		}
		if !op.IsSuccess {
			src.NOpsFailed++
		} else {
			// no baker registration via internal op

			// handle delegator withdraw
			if nbkr == nil {
				src.IsDelegated = false
				src.BakerId = 0
				src.DelegatedSince = 0
			}

			// handle new baker
			if nbkr != nil {
				// only update when this delegation changes baker
				if src.BakerId != nbkr.AccountId {
					src.IsDelegated = true
					src.BakerId = nbkr.AccountId
					src.DelegatedSince = b.block.Height
					nbkr.TotalDelegations++
					if src.Balance() > 0 {
						// delegation becomes active only when src is funded
						nbkr.ActiveDelegations++
					}
				}
			}

			// handle withdraw from old baker (also ensures we're duplicate safe)
			if obkr != nil {
				if src.Balance() > 0 {
					obkr.ActiveDelegations--
				}
			}
		}
	} else {
		// rollback accounts
		src.Counter = op.Counter - 1
		src.NOps--
		src.NDelegation--
		src.IsDirty = true
		if !op.IsSuccess {
			src.NOpsFailed--
		} else {

			// find previous baker, if any
			var lastsince int64
			if prevop, err := b.idx.FindLatestDelegation(ctx, src.RowId); err != nil {
				if err != index.ErrNoOpEntry {
					return Errorf("rollback: failed loading previous delegation op for account %d: %w", src.RowId, err)
				}
				obkr = nil
			} else if prevop.BakerId > 0 {
				lastsince = prevop.Height
				obkr, ok = b.BakerById(prevop.BakerId)
				if !ok {
					return Errorf("rollback: missing previous baker id %d", prevop.BakerId)
				}
			}
			if obkr == nil {
				// we must also look for the sources' origination op that may have
				// set the initial baker
				if prevop, err := b.idx.FindOrigination(ctx, src.RowId, src.FirstSeen); err != nil {
					if err != index.ErrNoOpEntry {
						return Errorf("rollback: failed loading previous origination op for account %d: %w", src.RowId, err)
					}
				} else if prevop.BakerId != 0 {
					obkr, ok = b.BakerById(prevop.BakerId)
					if !ok {
						return Errorf("rollback: missing origin baker %s", prevop.BakerId)
					}
				}
			}

			// reverse new baker
			if nbkr != nil {
				nbkr.TotalDelegations--
				nbkr.ActiveDelegations--
				nbkr.IsDirty = true
			}

			// reverse handle withdraw from old baker
			if obkr != nil {
				src.IsDelegated = true
				src.BakerId = obkr.AccountId
				src.DelegatedSince = lastsince
				obkr.ActiveDelegations++
				obkr.IsDirty = true
			} else {
				src.IsDelegated = false
				src.BakerId = 0
				src.DelegatedSince = 0
			}
		}
	}

	return nil
}

func (b *Builder) AppendRegisterConstantOp(ctx context.Context, oh *rpc.Operation, id model.OpRef, rollback bool) error {
	o := id.Get(oh)

	Errorf := func(format string, args ...interface{}) error {
		return fmt.Errorf(
			"%s op [%d:%d:%d]: "+format,
			append([]interface{}{o.Kind(), id.L, id.P, id.C}, args...)...,
		)
	}

	gop, ok := o.(*rpc.ConstantRegistration)
	if !ok {
		return Errorf("unexpected type %T", o)
	}

	src, ok := b.AccountByAddress(gop.Source)
	if !ok {
		return Errorf("missing source account %s", gop.Source)
	}
	var srcbkr *model.Baker
	if src.BakerId != 0 {
		if srcbkr, ok = b.BakerById(src.BakerId); !ok {
			return Errorf("missing baker %d for source account %d", src.BakerId, src.RowId)
		}
	}

	// build op
	op := model.NewOp(b.block, id)
	op.SenderId = src.RowId
	op.Counter = gop.Counter
	op.Fee = gop.Fee
	op.GasLimit = gop.GasLimit
	op.StorageLimit = gop.StorageLimit
	res := gop.Result()
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.ConsumedGas
	op.StoragePaid = res.StorageSize // sic!

	var (
		flows []*model.Flow
		// err   error
	)
	if op.IsSuccess {
		flows = b.NewConstantRegistrationFlows(
			src, srcbkr,
			gop.Fees(),
			res.Balances(),
			id,
		)

		// update burn from burn flow
		for _, f := range flows {
			if f.IsBurned {
				op.Burned += f.AmountOut
			}
		}

		op.Data = res.GlobalAddress.String()
		// op.Storage, err = gop.Value.MarshalBinary()
		// if err != nil {
		// 	return Errorf("marshal value: %w", err)
		// }

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

		// fees flows
		flows = b.NewConstantRegistrationFlows(src, srcbkr, gop.Fees(), nil, id)
	}

	b.block.Ops = append(b.block.Ops, op)

	// update sender account
	if !rollback {
		src.Counter = op.Counter
		src.NOps++
		src.NConstants++
		src.LastSeen = b.block.Height
		src.IsDirty = true
		if !op.IsSuccess {
			src.NOpsFailed++
		}
	} else {
		src.Counter = op.Counter - 1
		src.NOps--
		src.NConstants--
		src.IsDirty = true
		if !op.IsSuccess {
			src.NOpsFailed--
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
	op.GasUsed = res.ConsumedGas
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
			src.IsDirty = true
		}
	}

	return nil
}
