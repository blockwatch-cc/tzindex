// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

// pre-Ithaca
// - baker pays deposit and receives block rewards+fees
// - on last block in cycle, deposits, rewards and fees from cycle-N are unfrozen
//
// post-Ithaca
// - baker reward is paid directly (to proposer and bonus to baker)
// - baker fee is paid directly via accumulator
// - all bakers pay deposit at migration and at end of cycle
// - endorsing reward is minted and paid at end of cycle or burned for low participation
func (b *Builder) NewImplicitFlows() []*model.Flow {
	// Note: during chain bootstrap there used to be blocks without rewards
	// and no balance updates were issued to bakers
	flows := make([]*model.Flow, 0)
	id := model.OpRef{
		L: model.OPL_BLOCK_HEADER,
		N: -1,
		P: -1,
	}
	var (
		fees     int64 // block fees, deduct from baker reward flow
		last     tezos.Address
		nextType model.FlowType = model.FlowTypeInvalid
	)
	// ignore buggy mainnet receipts at block 1409024
	bu := b.block.TZ.Block.Metadata.BalanceUpdates
	if b.block.Params.IsMainnet() && b.block.Height == 1409024 {
		bu = bu[:len(bu)-2]
	}
	for i, u := range bu {
		// some balance updates (mints) have no address (!)
		// count individual baker updates as separate batch operations, when we later
		// assign flows to implicit ops/events we use OpN to assign multiple flows
		// to the same operation
		addr := u.Address()
		if addr.IsValid() && !last.Equal(addr) {
			last = addr
			id.N++
			id.P++
		}
		switch u.Kind {
		case "minted":
			// Ithaca+
			// on observing mints we identify their category fill the following
			// credit event
			switch u.Category {
			case "endorsing rewards":
				nextType = model.FlowTypeReward
			case "baking rewards":
				nextType = model.FlowTypeBaking
			case "baking bonuses":
				nextType = model.FlowTypeBonus
			}
		case "burned":
			// Ithaca+
			// only endorsing rewards can be burned here
			acc, ok := b.AccountByAddress(addr)
			if !ok {
				log.Errorf("block balance update %d:%d missing account %s", b.block.Height, i, addr)
				continue
			}
			// treat participation burn with higher priority than seed slash
			if u.IsRevelationBurn && !u.IsParticipationBurn {
				// Ithaca+ seed slash (minted reward is burned right away)
				f := model.NewFlow(b.block, acc, nil, id)
				f.Category = model.FlowCategoryBalance
				f.Operation = model.FlowTypeNonceRevelation
				f.AmountIn = u.Change  // event is balance neutral
				f.AmountOut = u.Change // event is balance neutral
				f.IsBurned = true
				flows = append(flows, f)
			} else {
				// Ithaca+ endorsement slash (minted reward is burned right away)
				f := model.NewFlow(b.block, acc, nil, id)
				f.Category = model.FlowCategoryBalance
				f.Operation = model.FlowTypeReward
				f.AmountIn = u.Change  // event is balance neutral
				f.AmountOut = u.Change // event is balance neutral
				f.IsBurned = true
				flows = append(flows, f)
			}
		case "accumulator":
			if u.Category == "block fees" {
				fees = -u.Change
			}
		case "contract":
			acc, ok := b.AccountByAddress(addr)
			if !ok {
				log.Errorf("block balance update %d:%d missing account %s", b.block.Height, i, addr)
				continue
			}
			switch u.Origin {
			case "", "block":
				if u.Change < 0 {
					// baking: deposit paid from balance
					if b.block.Params.Version < 12 {
						// pre-Ithaca deposits go to baking op
						f := model.NewFlow(b.block, acc, nil, id)
						f.Category = model.FlowCategoryBalance
						f.Operation = model.FlowTypeBaking
						f.AmountOut = -u.Change // note the negation!
						flows = append(flows, f)
					} else {
						// post-Ithaca deposits are explicit and go to deposit op
						f := model.NewFlow(b.block, acc, nil, id)
						f.Category = model.FlowCategoryBalance
						f.Operation = model.FlowTypeDeposit
						f.AmountOut = -u.Change // note the negation!
						flows = append(flows, f)
					}
				} else {
					if b.block.Params.Version < 12 {
						// pre-Ithaca payout: credit unfrozen deposits, rewards and fees
						f := model.NewFlow(b.block, acc, nil, id)
						f.Category = model.FlowCategoryBalance
						f.Operation = model.FlowTypeInternal
						f.AmountIn = u.Change
						f.TokenGenMin = 1
						flows = append(flows, f)
					} else {
						if nextType.IsValid() {
							// post-Ithaca payout: credit minted rewards directly to balance
							// deduct block fee from baker reward since we handle fee payments
							// explicitly across all ops. The Op indexer will later add
							// all earned block fees to the first bake operation (which pays the
							// proposer reward).
							f := model.NewFlow(b.block, acc, nil, id)
							f.Category = model.FlowCategoryBalance
							f.Operation = nextType
							f.AmountIn = u.Change - fees
							f.TokenGenMin = 1
							flows = append(flows, f)
							// add fee flow
							if fees > 0 {
								f = model.NewFlow(b.block, acc, nil, id)
								f.Category = model.FlowCategoryBalance
								f.Operation = nextType
								f.AmountIn = fees
								f.IsFee = true
								f.TokenGenMin = 1
								flows = append(flows, f)
								fees = 0
							}
							// reset next type
							nextType = model.FlowTypeInvalid
						} else {
							// post-Ithaca: credit refunded deposit to balance
							// Not: operation is not `deposit` because freezer
							// flow already creates an unfreeze event
							f := model.NewFlow(b.block, acc, nil, id)
							f.Category = model.FlowCategoryBalance
							f.Operation = model.FlowTypeInternal
							f.AmountIn = u.Change
							flows = append(flows, f)
						}
					}
				}
			case "migration":
				if b.block.Params.Version < 12 {
					// Florence v009+
					f := model.NewFlow(b.block, acc, nil, id)
					f.Category = model.FlowCategoryBalance
					f.Operation = model.FlowTypeInvoice
					f.AmountIn = u.Change
					f.TokenGenMin = 1
					flows = append(flows, f)
				} else {
					// Ithanca v012 migration extra deposit freeze or refund
					// keep op=internal to not create operation
					if u.Change < 0 {
						// extra deposit paid
						f := model.NewFlow(b.block, acc, nil, id)
						f.Category = model.FlowCategoryBalance
						f.Operation = model.FlowTypeInternal
						f.AmountOut = -u.Change // note the negation!
						flows = append(flows, f)
					} else {
						// legacy deposit refunded, use op internal
						// to not create a deposit operation
						f := model.NewFlow(b.block, acc, nil, id)
						f.Category = model.FlowCategoryBalance
						f.Operation = model.FlowTypeInternal
						f.AmountIn = u.Change
						flows = append(flows, f)
					}
				}
			}
		case "freezer":
			acc, ok := b.AccountByAddress(addr)
			if !ok {
				log.Errorf("block balance update %d:%d missing account %s", b.block.Height, i, addr)
				continue
			}
			// pre/post-Ithaca
			if u.Change > 0 {
				// baking: frozen deposits and rewards
				switch u.Category {
				case "deposits":
					if b.block.Params.Version < 12 {
						// pre-Ithaca deposits are added to baking op type
						f := model.NewFlow(b.block, acc, nil, id)
						f.Category = model.FlowCategoryDeposits
						f.Operation = model.FlowTypeBaking
						f.AmountIn = u.Change
						f.IsFrozen = true
						flows = append(flows, f)
					} else {
						// post-Ithaca deposits are explicit and go to deposit op type
						f := model.NewFlow(b.block, acc, nil, id)
						f.Category = model.FlowCategoryDeposits
						f.Operation = model.FlowTypeDeposit
						f.AmountIn = u.Change
						f.IsFrozen = true
						flows = append(flows, f)
					}
				case "rewards":
					f := model.NewFlow(b.block, acc, nil, id)
					f.Category = model.FlowCategoryRewards
					f.Operation = model.FlowTypeBaking
					f.AmountIn = u.Change
					f.IsFrozen = true
					flows = append(flows, f)
				}
			} else {
				// payout: deduct unfrozen deposits, rewards and fees
				// when cycle is set and > N-5 then this is a seed nonce slashing
				// because the baker did not publish nonces
				cycle := u.Cycle()
				isSeedNonceSlashing := cycle > 0 && cycle > b.block.Cycle-b.block.Params.PreservedCycles && u.Origin != "migration"
				switch u.Category {
				case "deposits", "legacy_deposits":
					f := model.NewFlow(b.block, acc, nil, id)
					f.Category = model.FlowCategoryDeposits
					f.Operation = model.FlowTypeInternal
					f.AmountOut = -u.Change
					f.IsUnfrozen = true
					flows = append(flows, f)
				case "rewards", "legacy_rewards":
					if isSeedNonceSlashing {
						f := model.NewFlow(b.block, acc, nil, id)
						f.Category = model.FlowCategoryRewards
						f.Operation = model.FlowTypeNonceRevelation
						f.AmountOut = -u.Change
						f.IsBurned = true
						flows = append(flows, f)
					} else {
						f := model.NewFlow(b.block, acc, nil, id)
						f.Category = model.FlowCategoryRewards
						f.Operation = model.FlowTypeInternal
						f.AmountOut = -u.Change
						f.IsUnfrozen = true
						flows = append(flows, f)
					}
				case "fees", "legacy_fees":
					if isSeedNonceSlashing {
						f := model.NewFlow(b.block, acc, nil, id)
						f.Category = model.FlowCategoryFees
						f.Operation = model.FlowTypeNonceRevelation
						f.AmountOut = -u.Change
						f.IsBurned = true
						flows = append(flows, f)
					} else {
						f := model.NewFlow(b.block, acc, nil, id)
						f.Category = model.FlowCategoryFees
						f.Operation = model.FlowTypeInternal
						f.AmountOut = -u.Change
						f.IsUnfrozen = true
						flows = append(flows, f)
					}
				}
			}
		}
	}
	return flows
}

func (b *Builder) NewSubsidyFlow(acc *model.Account, amount int64, id model.OpRef) *model.Flow {
	f := model.NewFlow(b.block, acc, nil, id)
	f.Category = model.FlowCategoryBalance
	f.Operation = model.FlowTypeSubsidy
	f.AmountIn = amount
	return f
}

func (b *Builder) NewInvoiceFlow(acc *model.Account, amount int64, id model.OpRef) *model.Flow {
	f := model.NewFlow(b.block, acc, nil, id)
	f.Category = model.FlowCategoryBalance
	f.Operation = model.FlowTypeInvoice
	f.AmountIn = amount
	return f
}

func (b *Builder) NewAirdropFlow(acc *model.Account, amount int64, id model.OpRef) *model.Flow {
	f := model.NewFlow(b.block, acc, nil, id)
	f.Category = model.FlowCategoryBalance
	f.Operation = model.FlowTypeAirdrop
	f.AmountIn = amount
	return f
}

func (b *Builder) NewActivationFlow(acc *model.Account, aop *rpc.Activation, id model.OpRef) []*model.Flow {
	bal := aop.Fees()
	if len(bal) < 1 {
		log.Warnf("Empty balance update for activation op at height %d", b.block.Height)
	}
	f := model.NewFlow(b.block, acc, nil, id)
	f.Category = model.FlowCategoryBalance
	f.Operation = model.FlowTypeActivation
	for _, u := range bal {
		switch u.Kind {
		case "contract":
			f.AmountIn = u.Amount()
		}
	}
	f.TokenGenMin = 1
	f.TokenGenMax = 1
	b.block.Flows = append(b.block.Flows, f)
	return []*model.Flow{f}
}

// Note: during chain bootstrap there used to be blocks without rewards
//       and no balance updates were issued to endorsers
// Note: on Ithaca balance updates are empty since deposit/reward is paid
//       before cycle start (technically at cycle end)
func (b *Builder) NewEndorserFlows(acc *model.Account, bal rpc.BalanceUpdates, id model.OpRef) []*model.Flow {
	flows := make([]*model.Flow, 0)
	for _, u := range bal {
		switch u.Kind {
		case "contract":
			// deposits paid from balance
			f := model.NewFlow(b.block, acc, nil, id)
			f.Category = model.FlowCategoryBalance
			f.Operation = model.FlowTypeEndorsement
			f.AmountOut = -u.Change // note the negation!
			flows = append(flows, f)
		case "freezer":
			switch u.Category {
			case "deposits":
				f := model.NewFlow(b.block, acc, nil, id)
				f.Category = model.FlowCategoryDeposits
				f.Operation = model.FlowTypeEndorsement
				f.AmountIn = u.Change
				f.IsFrozen = true
				flows = append(flows, f)
			case "rewards":
				f := model.NewFlow(b.block, acc, nil, id)
				f.Category = model.FlowCategoryRewards
				f.Operation = model.FlowTypeEndorsement
				f.AmountIn = u.Change
				f.IsFrozen = true
				flows = append(flows, f)
			}
		}
	}
	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}

// injected by the baker only
func (b *Builder) NewSeedNonceFlows(bal rpc.BalanceUpdates, id model.OpRef) []*model.Flow {
	flows := make([]*model.Flow, 0)
	for _, u := range bal {
		switch u.Kind {
		case "freezer":
			// before Ithaca
			f := model.NewFlow(b.block, b.block.Baker.Account, nil, id)
			f.Category = model.FlowCategoryRewards
			f.Operation = model.FlowTypeNonceRevelation
			f.AmountIn = u.Change
			f.IsFrozen = true
			flows = append(flows, f)
		case "contract":
			// after Ithaca (not frozen, goes to block proposer)
			f := model.NewFlow(b.block, b.block.Proposer.Account, nil, id)
			f.Category = model.FlowCategoryBalance
			f.Operation = model.FlowTypeNonceRevelation
			f.AmountIn = u.Change
			flows = append(flows, f)
		}
	}
	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}

// works for double-bake, double-endorse, double-preendorse
func (b *Builder) NewDenunciationFlows(accuser, offender *model.Baker, bal rpc.BalanceUpdates, id model.OpRef) []*model.Flow {
	flows := make([]*model.Flow, 0)
	for _, u := range bal {
		// penalties
		// pre-Ithaca: kind=freezer & amount < 0 (up to 3 categories)
		// post-Ithaca: kind=freezer & amount < 0
		// rewards
		// pre-Ithaca: kind=freezer & amount > 0
		// post-Ithaca: kind=contract
		switch u.Kind {
		case "freezer":
			switch u.Category {
			case "rewards":
				if u.Change > 0 {
					// pre-Ithaca accuser reward
					f := model.NewFlow(b.block, accuser.Account, offender.Account, id)
					f.Operation = model.FlowTypePenalty
					f.Category = model.FlowCategoryRewards
					f.AmountIn = u.Change
					f.IsFrozen = true
					flows = append(flows, f)
				} else {
					// offender penalty
					f := model.NewFlow(b.block, offender.Account, accuser.Account, id)
					f.Operation = model.FlowTypePenalty
					f.Category = model.FlowCategoryRewards
					f.AmountOut = -u.Change
					f.IsFrozen = true
					f.IsBurned = true
					flows = append(flows, f)
				}
			case "deposits":
				// pre&post-Ithaca offender penalty
				f := model.NewFlow(b.block, offender.Account, accuser.Account, id)
				f.Operation = model.FlowTypePenalty
				f.Category = model.FlowCategoryDeposits
				f.AmountOut = -u.Change
				f.IsFrozen = true
				f.IsBurned = true
				flows = append(flows, f)
			case "fees":
				// pre-Ithaca offender penalty
				f := model.NewFlow(b.block, offender.Account, accuser.Account, id)
				f.Operation = model.FlowTypePenalty
				f.Category = model.FlowCategoryFees
				f.AmountOut = -u.Change
				f.IsFrozen = true
				f.IsBurned = true
				flows = append(flows, f)
			}

		case "contract":
			// post-Ithaca reward
			f := model.NewFlow(b.block, accuser.Account, offender.Account, id)
			f.Operation = model.FlowTypePenalty
			f.Category = model.FlowCategoryBalance // not frozen (!)
			f.AmountIn = u.Change
			flows = append(flows, f)
		}
	}
	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}

// Fees for manager operations (optional, i.e. sender may set to zero,
// for batch ops fees may also be paid by first op in batch)
func (b *Builder) NewFeeFlows(src *model.Account, fees rpc.BalanceUpdates, id model.OpRef) ([]*model.Flow, int64) {
	var sum int64
	flows := make([]*model.Flow, 0)
	typ := model.MapFlowType(id.Kind)
	for _, u := range fees {
		if u.Change == 0 {
			continue
		}
		switch u.Kind {
		case "contract":
			// pre/post-Ithaca fees paid by src
			f := model.NewFlow(b.block, src, b.block.Proposer.Account, id)
			f.Category = model.FlowCategoryBalance
			f.Operation = typ
			f.AmountOut = -u.Change // note the negation!
			f.IsFee = true
			sum += -u.Change
			flows = append(flows, f)
		case "freezer":
			// pre-Ithaca: fees paid to baker
			switch u.Category {
			case "fees":
				f := model.NewFlow(b.block, b.block.Proposer.Account, src, id)
				f.Category = model.FlowCategoryFees
				f.Operation = typ
				f.AmountIn = u.Change
				f.IsFrozen = true
				flows = append(flows, f)
			}
			// case "accumulator":
			// post-Ithaca: unused
		}
	}

	// delegation change is handled outside
	return flows, sum
}

func (b *Builder) NewRevealFlows(src *model.Account, bkr *model.Baker, fees rpc.BalanceUpdates, id model.OpRef) []*model.Flow {
	flows, feespaid := b.NewFeeFlows(src, fees, id)

	// if src is delegated (and not baker, subtract paid fees from delegated balance
	if feespaid > 0 && bkr != nil && !src.IsBaker {
		f := model.NewFlow(b.block, bkr.Account, src, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeReveal
		f.AmountOut = feespaid
		flows = append(flows, f)
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}

// used for internal an non-internal delegations
func (b *Builder) NewDelegationFlows(src *model.Account, newbkr, oldbkr *model.Baker, fees rpc.BalanceUpdates, id model.OpRef) []*model.Flow {
	// apply fees first
	flows, feespaid := b.NewFeeFlows(src, fees, id)
	var pending int64
	balance := src.Balance()

	// if delegation is renewed/duplicate, handle fee out-flow only
	if newbkr != nil && oldbkr != nil && newbkr.AccountId == oldbkr.AccountId {
		// create flow only if fees are paid
		if feespaid > 0 && !src.IsBaker {
			f := model.NewFlow(b.block, oldbkr.Account, src, id)
			f.Category = model.FlowCategoryDelegation
			f.Operation = model.FlowTypeDelegation
			f.AmountOut = feespaid // deduct this operation's fees only
			flows = append(flows, f)
		}
	} else {
		// handle any change in baker

		// source account may have run multiple ops in the same block, so the
		// (re-)delegated balance must be adjusted by all pending updates
		// because they have already created delegation out-flows
		for _, f := range b.block.Flows {
			if f.AccountId == src.RowId && f.Category == model.FlowCategoryBalance {
				pending += f.AmountOut - f.AmountIn
			}
		}

		// if src is already delegated, create an (un)delegation flow from old baker
		// also cover the case where src registers as baker
		if oldbkr != nil {
			f := model.NewFlow(b.block, oldbkr.Account, src, id)
			f.Category = model.FlowCategoryDelegation
			f.Operation = model.FlowTypeDelegation
			f.AmountOut = balance - pending // deduct difference including fees
			flows = append(flows, f)
		}

		// create delegation to new baker using balance minus delegation fees from above
		// and minus pending balance updates, unless its a self-delegation
		// (i.e. baker registration)
		if newbkr != nil && !src.IsBaker && balance-pending-feespaid > 0 {
			f := model.NewFlow(b.block, newbkr.Account, src, id)
			f.Category = model.FlowCategoryDelegation
			f.Operation = model.FlowTypeDelegation
			f.AmountIn = balance - feespaid - pending // add difference without fees
			flows = append(flows, f)
		}
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}

func (b *Builder) NewTransactionFlows(
	src, dst *model.Account,
	sbkr, dbkr *model.Baker,
	srccon, dstcon *model.Contract,
	fees, bal rpc.BalanceUpdates,
	block *model.Block,
	id model.OpRef) []*model.Flow {

	// apply fees
	flows, feespaid := b.NewFeeFlows(src, fees, id)

	// transaction may burn and transfer
	var moved, burnedAndMoved int64
	for _, u := range bal {
		switch u.Kind {
		// we only consider contract in/out flows and calculate
		// the burn even though Ithaca makes this explict
		case "contract":
			if u.Change < 0 {
				burnedAndMoved += -u.Change
			} else {
				moved += u.Change
			}
		}
	}

	// create move and burn flows when necessary
	if moved > 0 && dst != nil {
		// debit from source
		f := model.NewFlow(b.block, src, dst, id)
		f.Category = model.FlowCategoryBalance
		f.Operation = model.FlowTypeTransaction
		f.AmountOut = moved
		f.TokenGenMin = src.TokenGenMin
		f.TokenGenMax = src.TokenGenMax
		f.TokenAge = block.Age(src.LastIn)
		f.IsUnshielded = srccon != nil && srccon.Features.Contains(micheline.FeatureSapling)
		flows = append(flows, f)
		// credit to dest
		f = model.NewFlow(b.block, dst, src, id)
		f.Category = model.FlowCategoryBalance
		f.Operation = model.FlowTypeTransaction
		f.AmountIn = moved
		f.TokenGenMin = src.TokenGenMin
		f.TokenGenMax = src.TokenGenMax
		f.TokenAge = block.Age(src.LastIn)
		f.IsShielded = dstcon != nil && dstcon.Features.Contains(micheline.FeatureSapling)
		flows = append(flows, f)
	}

	if burnedAndMoved-moved > 0 {
		// debit burn from source
		f := model.NewFlow(b.block, src, nil, id)
		f.Category = model.FlowCategoryBalance
		f.Operation = model.FlowTypeTransaction
		f.AmountOut = burnedAndMoved - moved
		f.IsBurned = true
		flows = append(flows, f)
	}

	// debit from source delegation unless source is a baker
	if sbkr != nil && !src.IsBaker && feespaid+burnedAndMoved > 0 {
		f := model.NewFlow(b.block, sbkr.Account, src, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeTransaction
		f.AmountOut = feespaid + burnedAndMoved // fees and amount moved
		flows = append(flows, f)
	}

	// credit to destination baker unless dest is a baker
	if dbkr != nil && !dst.IsBaker && moved > 0 {
		f := model.NewFlow(b.block, dbkr.Account, src, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeTransaction
		f.AmountIn = moved
		flows = append(flows, f)
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}

// fees are already paid by outer tx
// burn is attributed to outer source
func (b *Builder) NewInternalTransactionFlows(
	origsrc, src, dst *model.Account,
	origbkr, srcbkr, dstbkr *model.Baker,
	srccon, dstcon *model.Contract,
	bal rpc.BalanceUpdates,
	block *model.Block,
	id model.OpRef,
) []*model.Flow {
	flows := make([]*model.Flow, 0)

	// transaction may burn and transfer
	var moved, burnedAndMoved int64
	for _, u := range bal {
		switch u.Kind {
		// we only consider contract in/out flows and calculate
		// the burn even though Ithaca makes this explict
		case "contract":
			if u.Change < 0 {
				burnedAndMoved += -u.Change
			} else {
				moved += u.Change
			}
		}
	}

	// create move and burn flows when necessary
	if moved > 0 && dst != nil {
		// deducted from source
		f := model.NewFlow(b.block, src, dst, id)
		f.Category = model.FlowCategoryBalance
		f.Operation = model.FlowTypeTransaction
		f.AmountOut = moved
		f.TokenGenMin = src.TokenGenMin
		f.TokenGenMax = src.TokenGenMax
		f.TokenAge = block.Age(src.LastIn)
		f.IsUnshielded = srccon != nil && srccon.Features.Contains(micheline.FeatureSapling)
		flows = append(flows, f)
		// credit to dest
		f = model.NewFlow(b.block, dst, src, id)
		f.Category = model.FlowCategoryBalance
		f.Operation = model.FlowTypeTransaction
		f.AmountIn = moved
		f.TokenGenMin = src.TokenGenMin
		f.TokenGenMax = src.TokenGenMax
		f.TokenAge = block.Age(src.LastIn)
		f.IsShielded = dstcon != nil && dstcon.Features.Contains(micheline.FeatureSapling)
		flows = append(flows, f)
	}

	if burnedAndMoved-moved > 0 {
		// Note: use outer source to burn
		f := model.NewFlow(b.block, origsrc, nil, id)
		f.Category = model.FlowCategoryBalance
		f.Operation = model.FlowTypeTransaction
		f.AmountOut = burnedAndMoved - moved
		f.IsBurned = true
		flows = append(flows, f)
		// adjust delegated balance if not a baker
		if origbkr != nil && !origsrc.IsBaker {
			f = model.NewFlow(b.block, origbkr.Account, origsrc, id)
			f.Category = model.FlowCategoryDelegation
			f.Operation = model.FlowTypeTransaction
			f.AmountOut = burnedAndMoved - moved
			flows = append(flows, f)
		}
	}

	// handle delegation updates

	// debut moved amount from source delegation if not baker
	if srcbkr != nil && !src.IsBaker && moved > 0 {
		f := model.NewFlow(b.block, srcbkr.Account, src, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeTransaction
		f.AmountOut = moved
		flows = append(flows, f)
	}

	// credit to dest delegate unless its a baker
	if dstbkr != nil && !dst.IsBaker && moved > 0 {
		f := model.NewFlow(b.block, dstbkr.Account, src, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeTransaction
		f.AmountIn = moved
		flows = append(flows, f)
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}

func (b *Builder) NewOriginationFlows(
	src, dst *model.Account,
	srcbkr, newbkr *model.Baker,
	fees, bal rpc.BalanceUpdates,
	id model.OpRef) []*model.Flow {

	flows, feespaid := b.NewFeeFlows(src, fees, id)

	// origination may burn and may transfer funds
	var moved, burnedAndMoved int64
	for _, u := range bal {
		switch u.Kind {
		// we only consider contract in/out flows and calculate
		// the burn even though Ithaca makes this explict
		case "contract":
			if u.Change < 0 {
				burnedAndMoved += -u.Change
			} else {
				moved += u.Change
			}
		}
	}

	// create move and burn flows when necessary
	if moved > 0 && dst != nil {
		// debit from source
		f := model.NewFlow(b.block, src, dst, id)
		f.Category = model.FlowCategoryBalance
		f.Operation = model.FlowTypeOrigination
		f.AmountOut = moved
		flows = append(flows, f)
		// credit to dest
		f = model.NewFlow(b.block, dst, src, id)
		f.Category = model.FlowCategoryBalance
		f.Operation = model.FlowTypeOrigination
		f.AmountIn = moved
		flows = append(flows, f)
	}
	if burnedAndMoved-moved > 0 {
		// debit from source as burn
		f := model.NewFlow(b.block, src, nil, id)
		f.Category = model.FlowCategoryBalance
		f.Operation = model.FlowTypeOrigination
		f.AmountOut = burnedAndMoved - moved
		f.IsBurned = true
		flows = append(flows, f)
	}

	// handle delegation updates

	// debit from source delegation if not baker
	if srcbkr != nil && !src.IsBaker && feespaid+burnedAndMoved > 0 {
		f := model.NewFlow(b.block, srcbkr.Account, src, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeOrigination
		f.AmountOut = feespaid + burnedAndMoved // fees and value moved
		flows = append(flows, f)
	}

	// credit to new baker when set
	if newbkr != nil && moved > 0 {
		f := model.NewFlow(b.block, newbkr.Account, src, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeOrigination
		f.AmountIn = moved
		flows = append(flows, f)
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}

func (b *Builder) NewInternalOriginationFlows(
	origsrc, src, dst *model.Account,
	origbkr, srcbkr, newbkr *model.Baker,
	bal rpc.BalanceUpdates,
	id model.OpRef) []*model.Flow {

	// fees are paid by outer transaction, here we only handle burned coins
	flows := make([]*model.Flow, 0)
	var burned, moved int64
	for _, u := range bal {
		switch u.Kind {
		case "contract":
			addr := u.Address()
			switch true {
			case addr.Equal(origsrc.Address):
				// burned from original source balance
				f := model.NewFlow(b.block, origsrc, nil, id)
				f.Category = model.FlowCategoryBalance
				f.Operation = model.FlowTypeOrigination
				f.AmountOut = -u.Change // note the negation!
				f.IsBurned = true
				flows = append(flows, f)
				burned += -u.Change
			case addr.Equal(src.Address):
				// transfers from src contract to dst contract
				f := model.NewFlow(b.block, src, dst, id)
				f.Category = model.FlowCategoryBalance
				f.Operation = model.FlowTypeOrigination
				f.AmountOut = -u.Change // note the negation!
				flows = append(flows, f)
				moved += -u.Change
			case addr.Equal(dst.Address):
				// transfers from src contract to dst contract
				f := model.NewFlow(b.block, dst, src, id)
				f.Category = model.FlowCategoryBalance
				f.Operation = model.FlowTypeOrigination
				f.AmountIn = u.Change
				flows = append(flows, f)
			}
		}
	}

	// handle delegation updates

	// debit burn from original source delegation iff not baker
	if origbkr != nil && !origsrc.IsBaker && burned > 0 {
		f := model.NewFlow(b.block, origbkr.Account, origsrc, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeOrigination
		f.AmountOut = burned // deduct burned amount
		flows = append(flows, f)
	}

	// debit moved funds from source delegation if not baker
	if srcbkr != nil && !src.IsBaker && moved > 0 {
		f := model.NewFlow(b.block, srcbkr.Account, src, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeOrigination
		f.AmountOut = moved // deduct moved amount
		flows = append(flows, f)
	}

	// credit moved funds to target baker when set
	if newbkr != nil && moved > 0 {
		f := model.NewFlow(b.block, newbkr.Account, src, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeOrigination
		f.AmountIn = moved
		flows = append(flows, f)
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}

func (b *Builder) NewConstantRegistrationFlows(
	src *model.Account,
	srcbkr *model.Baker,
	fees, bal rpc.BalanceUpdates,
	id model.OpRef) []*model.Flow {

	flows, feespaid := b.NewFeeFlows(src, fees, id)

	// rest is burned
	var burned int64
	for _, u := range bal {
		switch u.Kind {
		case "contract":
			// debit burn from source
			f := model.NewFlow(b.block, src, nil, id)
			f.Category = model.FlowCategoryBalance
			f.Operation = model.FlowTypeRegisterConstant
			f.AmountOut = -u.Change
			burned += -u.Change
			f.IsBurned = true
			flows = append(flows, f)
		}
	}

	// debit burn from source delegation if not baker
	if srcbkr != nil && !src.IsBaker && feespaid+burned > 0 {
		f := model.NewFlow(b.block, srcbkr.Account, src, id)
		f.Category = model.FlowCategoryDelegation
		f.Operation = model.FlowTypeRegisterConstant
		f.AmountOut = feespaid + burned
		flows = append(flows, f)
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}

// sent by baker, so no delegation update required
// post-Ithaca only op, so no pre-Ithaca fee handling
func (b *Builder) NewSetDepositsLimitFlows(src *model.Account, fees rpc.BalanceUpdates, id model.OpRef) []*model.Flow {
	flows, _ := b.NewFeeFlows(src, fees, id)
	b.block.Flows = append(b.block.Flows, flows...)
	return flows
}
