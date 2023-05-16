// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
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
		if addr.IsValid() && last != addr {
			last = addr
			id.N++
			id.P++
		}
		// split baking and unfreeze events if baker address did not change
		// this happened on Mainnet for Polychain baker in 6 blocks 548,864 .. 1179648
		if b.block.Params.Version < 12 &&
			i == 3 && bu[2].Address().Equal(addr) &&
			u.Kind == "freezer" && u.Category == "deposits" && u.Amount() < 0 {
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
			case "invoice":
				nextType = model.FlowTypeInvoice
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
			// skip migration updates on block 2, we have already processed
			// initial accounts when decoding the genesis/activation block
			// frozen deposits on block 2 are handled in freezer branch below
			if b.block.Height == 2 && u.Origin == "migration" {
				continue
			}
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
							flows = append(flows, f)
							// add fee flow
							if fees > 0 {
								f = model.NewFlow(b.block, acc, nil, id)
								f.Category = model.FlowCategoryBalance
								f.Operation = nextType
								f.AmountIn = fees
								f.IsFee = true
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
					flows = append(flows, f)
				} else {
					if nextType.IsValid() && nextType == model.FlowTypeInvoice {
						// add one or multiple invoice flows
						f := model.NewFlow(b.block, acc, nil, id)
						f.Category = model.FlowCategoryBalance
						f.Operation = model.FlowTypeInvoice
						f.AmountIn = u.Change
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
						// post-Ithaca deposits are explicit on bootstrap migration
						if b.block.Height == 2 && u.Origin == "migration" {
							f := model.NewFlow(b.block, acc, nil, id)
							f.Category = model.FlowCategoryBalance
							f.Operation = model.FlowTypeDeposit
							f.AmountOut = u.Change
							flows = append(flows, f)
						}
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
						f.IsUnfrozen = true
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
						f.IsUnfrozen = true
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
