// Copyright (c) 2020-2024 Blockwatch Data Inc.
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
//
// post-Oxford
// - each reward flow may be split into two flows (4 balance updates), one for the baker
// and another for the staker pool
// - only bake reward, bake bonus and endorse reward are shared with stakers
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

	// decode balance updates in order of appearance
	for i, u := range bu {
		// 1/ determine who this update is for?
		//
		// priority order is defined in rpc/balance.go
		// - contract (usually balance updates)
		// - delegate (usually pre-oxford baker updates)
		// - committer (rollup)
		// - staker.contract (post-Oxford user stake, user or baker address)
		// - staker.delegate (post-Oxford shared stake, baker address)
		// - staker.baker (post-Oxford baker)
		addr := u.Address()

		// re-assign flows for the same baker to new operations under certain conditions
		switch {
		case b.block.Params.Version < 12:
			// treat updates for different bakers as separate events,
			// but assign flows for the same baker to a single operation
			// Note: some balance updates (mints, burns) have no address (!)
			if addr.IsValid() && last != addr {
				last = addr
				id.N++
				id.P++
			}

			// split baking and unfreeze events if baker address did not change
			// this happened on Mainnet for Polychain baker in 6 blocks 548,864 .. 1179648
			if b.block.Params.IsMainnet() {
				if i == 3 && bu[2].Address().Equal(addr) &&
					u.Kind == "freezer" && u.Category == "deposits" && u.Amount() < 0 {
					id.N++
					id.P++
				}
			}

		case b.block.Params.Version < 18:
			// treat updates for different bakers as separate events,
			// but assign flows for the same baker to a single operation
			// Note: some balance updates (mints, burns) have no address (!)
			if addr.IsValid() && last != addr {
				last = addr
				id.N++
				id.P++
			}

		default:
			// post-oxford we break update streams for the same baker into
			// multiple events when anything leaves unstaked_deposits
			//
			// that means we split
			// - unstake (2) + auto-finalize (2) -> unstake & finalize
			// - re-stake (2,4,6,8,10) + auto-finalize (2) -> {unstake, stake}+ & finalize
			//
			// Rationale: this way updating unstaked, staked and spendable balances
			// is more clear, even though the protocol takes the direct route and
			// re-freezes from unstaked

			// detect re-staking by peeking into the next balance update
			isFinalize := u.Category == "unstaked_deposits" && u.Amount() < 0
			wasFinalize := !isFinalize && i > 0 && bu[i-1].Category == "unstaked_deposits" && bu[i-1].Amount() < 0
			isRestake := wasFinalize && u.Kind == "freezer"

			// determine if this update belongs to the same delayed operation
			isDelayedOpPair := u.Origin == "delayed_operation" && i > 0 && bu[i-1].Origin == "delayed_operation"
			isSameDelayedOp := isDelayedOpPair && u.DelayedOp == bu[i-1].DelayedOp
			isNextDelayedOp := isDelayedOpPair && u.DelayedOp != bu[i-1].DelayedOp
			isEndDelayedOp := i > 0 && bu[i-1].Origin == "delayed_operation" && u.Origin != "delayed_operation"

			// prepare for next balance update
			// if isRestake {
			// 	log.Warnf("%d Restake %s %d", b.block.Height, addr, u.Amount())
			// }
			// if isFinalize {
			// 	log.Warnf("%d Finalize %s %d", b.block.Height, addr, u.Amount())
			// }

			// keep for processing the following balance update
			if isRestake {
				nextType = model.FlowTypeStake
			}

			// treat updates for different bakers as separate events,
			// but assign flows for the same baker to a single operation
			// Note: some balance updates (mints, burns) have no address (!)
			// Note: delayed slashing interleaves 2 bakers on deposits & unstaked_deposits
			if addr.IsValid() {
				switch {
				case isSameDelayedOp:
					// keep current id
				case last != addr:
					// always advance on address change
					last = addr
					id.N++
					id.P++
				case isNextDelayedOp:
					// detect multiple different slashing ops to the same offender
					id.N++
					id.P++
				case isFinalize || isRestake:
					// auto-finalize goes into a separate operation (per finalized cycle)
					// unless it is the only update for this baker, then we have already
					// advanced N & P in the first if branch
					id.N++
					id.P++
				}
			}
			// extra check for end of delayed operation sequence to catch
			// the following endorsing reward burn for the same baker
			if isEndDelayedOp {
				id.N++
				id.P++
			}
		}

		// 2/ Decode balance updates
		switch u.Kind {
		case "minted":
			// Ithaca+
			// on observing mints we identify their category in prepartion
			// for filling the following credit event
			switch u.Category {
			case "endorsing rewards", "attesting rewards":
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
				log.Debugf("block balance %s %d:%d missing account %s", u.Kind, b.block.Height, i, addr)
				continue
			}
			// treat participation burn with higher priority than seed slash
			if u.IsRevelationBurn && !u.IsParticipationBurn {
				// Ithaca+ seed slash (minted reward is burned right away)
				f := model.NewFlow(b.block, acc, nil, id)
				f.Kind = model.FlowKindBalance
				f.Type = model.FlowTypeNonceRevelation
				f.AmountIn = u.Amount()  // event is balance neutral
				f.AmountOut = u.Amount() // event is balance neutral
				f.IsBurned = true
				flows = append(flows, f)
			} else {
				// Ithaca+ endorsement slash (minted reward is burned right away)
				f := model.NewFlow(b.block, acc, nil, id)
				f.Kind = model.FlowKindBalance
				f.Type = model.FlowTypeReward
				f.AmountIn = u.Amount()  // event is balance neutral
				f.AmountOut = u.Amount() // event is balance neutral
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
					switch {
					case b.block.Params.Version < 12:
						// pre-Ithaca deposits go to baking op
						f := model.NewFlow(b.block, acc, acc, id)
						f.Kind = model.FlowKindBalance
						f.Type = model.FlowTypeBaking
						f.AmountOut = -u.Amount() // note the negation!
						flows = append(flows, f)
					case b.block.Params.Version < 18:
						// post-Ithaca deposits are explicit and go to deposit op
						f := model.NewFlow(b.block, acc, acc, id)
						f.Kind = model.FlowKindBalance
						f.Type = model.FlowTypeDeposit
						f.AmountOut = -u.Amount() // note the negation!
						flows = append(flows, f)
					default:
						// post-Oxford deposits go into staking
						f := model.NewFlow(b.block, acc, acc, id)
						f.Kind = model.FlowKindBalance
						f.Type = model.FlowTypeStake
						f.AmountOut = -u.Amount() // note the negation!
						flows = append(flows, f)
						nextType = model.FlowTypeStake // set here for second in pair
					}
				} else {
					switch {
					case b.block.Params.Version < 12:
						// pre-Ithaca payout: credit unfrozen deposits, rewards and fees
						f := model.NewFlow(b.block, acc, acc, id)
						f.Kind = model.FlowKindBalance
						f.Type = model.FlowTypeInternal
						f.AmountIn = u.Amount()
						flows = append(flows, f)
					case b.block.Params.Version < 18:
						// post-Ithaca payout: credit minted rewards directly to balance
						// fees and reward are paid in the same balance update, so we
						// deduct them from amount here and add an extra fee flow
						if nextType.IsValid() {
							f := model.NewFlow(b.block, acc, acc, id)
							f.Kind = model.FlowKindBalance
							f.Type = nextType
							f.AmountIn = u.Amount() - fees
							flows = append(flows, f)
							if fees > 0 {
								// add fee flow
								f = model.NewFlow(b.block, acc, nil, id)
								f.Kind = model.FlowKindBalance
								f.Type = nextType
								f.AmountIn = fees
								f.IsFee = true
								flows = append(flows, f)
								fees = 0
							}
							// reset next type
							nextType = model.FlowTypeInvalid
						} else {
							// post-Ithaca: credit refunded deposit to balance
							// Note: type is not `deposit` because a separate freezer
							// flow already creates an unfreeze event
							f := model.NewFlow(b.block, acc, acc, id)
							f.Kind = model.FlowKindBalance
							f.Type = model.FlowTypeInternal
							f.AmountIn = u.Amount()
							flows = append(flows, f)
						}
					default:
						// post-Oxford payout share to spendable
						// fees are explicitly paid to proposer
						// deposit refund happens via unstake (not in this branch)
						if nextType.IsValid() {
							f := model.NewFlow(b.block, acc, acc, id)
							f.Kind = model.FlowKindBalance
							f.Type = nextType
							f.AmountIn = u.Amount()
							flows = append(flows, f)
							// add fee flow
							if fees > 0 {
								f = model.NewFlow(b.block, acc, nil, id)
								f.Kind = model.FlowKindBalance
								f.Type = nextType
								f.AmountIn = fees
								f.IsFee = true
								flows = append(flows, f)
								fees = 0
							}
							// reset next type
							nextType = model.FlowTypeInvalid
						}
					}
				}
			case "migration":
				switch {
				case b.block.Params.Version < 12:
					// Florence v009+
					f := model.NewFlow(b.block, acc, nil, id)
					f.Kind = model.FlowKindBalance
					f.Type = model.FlowTypeInvoice
					f.AmountIn = u.Amount()
					flows = append(flows, f)
				case b.block.Params.Version < 18:
					if nextType.IsValid() && nextType == model.FlowTypeInvoice {
						// add one or multiple invoice flows
						f := model.NewFlow(b.block, acc, nil, id)
						f.Kind = model.FlowKindBalance
						f.Type = model.FlowTypeInvoice
						f.AmountIn = u.Amount()
						flows = append(flows, f)
					} else {
						// Ithanca v012 migration extra deposit freeze or refund
						// keep op=internal to not create operation
						if u.Amount() < 0 {
							// extra deposit paid
							f := model.NewFlow(b.block, acc, acc, id)
							f.Kind = model.FlowKindBalance
							f.Type = model.FlowTypeInternal
							f.AmountOut = -u.Amount() // note the negation!
							flows = append(flows, f)
						} else {
							// legacy deposit refunded, use op internal
							// to not create a deposit operation
							f := model.NewFlow(b.block, acc, acc, id)
							f.Kind = model.FlowKindBalance
							f.Type = model.FlowTypeInternal
							f.AmountIn = u.Amount()
							flows = append(flows, f)
						}
					}
				default:
					// skip: we already decode the genesis block which
					// funds accounts
				}
			case "delayed_operation":
				// Oxford+ penalty reward
				offender, _ := b.AccountByAddress(bu[i-1].Address())
				f := model.NewFlow(b.block, acc, offender, id)
				f.Kind = model.FlowKindBalance
				f.Type = model.FlowTypePenalty
				f.AmountIn = u.Amount()
				flows = append(flows, f)
			}
		case "freezer":
			acc, ok := b.AccountByAddress(addr)
			if !ok {
				log.Errorf("block balance update %d:%d missing account %s", b.block.Height, i, addr)
				continue
			}
			if u.Amount() > 0 {
				// baking: frozen deposits and rewards
				switch u.Category {
				case "deposits":
					switch {
					case b.block.Params.Version < 12:
						// pre-Ithaca deposits are added to baking op type
						f := model.NewFlow(b.block, acc, nil, id)
						f.Kind = model.FlowKindDeposits
						f.Type = model.FlowTypeBaking
						f.AmountIn = u.Amount()
						f.IsFrozen = true
						flows = append(flows, f)
					case b.block.Params.Version < 18:
						// post-Ithaca bootstrap migration deposits are explicit
						if b.block.Height == 2 && u.Origin == "migration" {
							f := model.NewFlow(b.block, acc, acc, id)
							f.Kind = model.FlowKindBalance
							f.Type = model.FlowTypeDeposit
							f.AmountOut = u.Amount()
							flows = append(flows, f)
						}
						// post-Ithaca deposits are explicit and go to deposit op type
						f := model.NewFlow(b.block, acc, acc, id)
						f.Kind = model.FlowKindDeposits
						f.Type = model.FlowTypeDeposit
						f.AmountIn = u.Amount()
						f.IsFrozen = true
						flows = append(flows, f)
					default:
						// post-Oxford bootstrap migration deposits are explicit
						if b.block.Height == 2 && u.Origin == "migration" {
							f := model.NewFlow(b.block, acc, acc, id)
							f.Kind = model.FlowKindBalance
							f.Type = model.FlowTypeStake
							f.AmountOut = u.Amount()
							flows = append(flows, f)
							nextType = model.FlowTypeStake
						}
						// post-Oxford deposits express rewards paid into the staking pool
						// OR deposit top-ups from auto-staking; the exact type (and hence)
						// operation that will be produced is defined in nextType which is
						// set when processing the first update from this pair
						f := model.NewFlow(b.block, acc, acc, id)
						f.Kind = model.FlowKindStake // target is frozen stake pool
						f.Type = nextType            // type is reward, baking, bonus or stake
						f.AmountIn = u.Amount()
						f.IsFrozen = true
						flows = append(flows, f)
						nextType = model.FlowTypeInvalid
					}
				case "rewards":
					f := model.NewFlow(b.block, acc, nil, id)
					f.Kind = model.FlowKindRewards
					f.Type = model.FlowTypeBaking
					f.AmountIn = u.Amount()
					f.IsFrozen = true
					flows = append(flows, f)
				case "unstaked_deposits":
					// post-oxford unstaked inflow comes from frozen deposits
					f := model.NewFlow(b.block, acc, acc, id)
					f.Kind = model.FlowKindStake
					f.Type = model.FlowTypeUnstake
					f.AmountOut = u.Amount()
					flows = append(flows, f)

					// FIXME: add delegation flow
					// f = model.NewFlow(b.block, acc, acc, id)
					// f.Kind = model.FlowKindDelegation
					// f.Type = model.FlowTypeUnstake
					// f.AmountIn = u.Amount()
					// flows = append(flows, f)
				}
			} else {
				// pre-Oxford
				// payout: deduct unfrozen deposits, rewards and fees
				// when cycle is set and > N-5 then this is a seed nonce slashing
				// because the baker did not publish nonces
				cycle := u.Cycle()
				isSeedNonceSlashing := cycle > 0 && cycle > b.block.Cycle-b.block.Params.PreservedCycles && u.Origin != "migration"
				switch u.Category {
				case "deposits", "legacy_deposits":
					switch {
					case b.block.Params.Version < 18:
						// pre-Oxford deposit outflows are unfrozen deposits that
						// go back to spendable balance
						f := model.NewFlow(b.block, acc, acc, id)
						f.Kind = model.FlowKindDeposits
						f.Type = model.FlowTypeInternal
						f.AmountOut = -u.Amount()
						f.IsUnfrozen = true
						flows = append(flows, f)
					default:
						// detect slash event
						if u.Origin == "delayed_operation" {
							addr := bu[i+1].Address()
							if !addr.IsValid() && len(bu) > i+3 {
								addr = bu[i+3].Address()
							}
							accuser, _ := b.AccountByAddress(addr)
							f := model.NewFlow(b.block, acc, accuser, id)
							f.Kind = model.FlowKindStake
							f.Type = model.FlowTypePenalty
							f.AmountOut = -u.Amount()
							// the next non-freezer balance update must be kind=burned
							var isBurn bool
							for _, next := range bu[i+1:] {
								if next.Kind == "freezer" {
									continue
								}
								isBurn = next.Kind == "burned"
								break
							}
							f.IsBurned = isBurn
							flows = append(flows, f)
						}
						// other post-Oxford deposit outflows are unstaked:
						// we skip this first balance update from the pair and
						// handle the second above in `unstaked_deposits`
					}
				case "rewards", "legacy_rewards":
					if isSeedNonceSlashing {
						f := model.NewFlow(b.block, acc, nil, id)
						f.Kind = model.FlowKindRewards
						f.Type = model.FlowTypeNonceRevelation
						f.AmountOut = -u.Amount()
						f.IsBurned = true
						f.IsUnfrozen = true
						flows = append(flows, f)
					} else {
						f := model.NewFlow(b.block, acc, nil, id)
						f.Kind = model.FlowKindRewards
						f.Type = model.FlowTypeInternal
						f.AmountOut = -u.Amount()
						f.IsUnfrozen = true
						flows = append(flows, f)
					}
				case "fees", "legacy_fees":
					if isSeedNonceSlashing {
						f := model.NewFlow(b.block, acc, nil, id)
						f.Kind = model.FlowKindFees
						f.Type = model.FlowTypeNonceRevelation
						f.AmountOut = -u.Amount()
						f.IsBurned = true
						f.IsUnfrozen = true
						flows = append(flows, f)
					} else {
						f := model.NewFlow(b.block, acc, nil, id)
						f.Kind = model.FlowKindFees
						f.Type = model.FlowTypeInternal
						f.AmountOut = -u.Amount()
						f.IsUnfrozen = true
						flows = append(flows, f)
					}
				case "unstaked_deposits":
					// detect slash event
					if u.Origin == "delayed_operation" {
						addr := bu[i+1].Address()
						if !addr.IsValid() && len(bu) > i+3 {
							addr = bu[i+3].Address()
						}
						accuser, _ := b.AccountByAddress(addr)
						f := model.NewFlow(b.block, acc, accuser, id)
						f.Kind = model.FlowKindStake
						f.Type = model.FlowTypePenalty
						f.AmountOut = -u.Amount()
						// the next non-freezer balance update must be kind=burned
						var isBurn bool
						for _, next := range bu[i+1:] {
							if next.Kind == "freezer" {
								continue
							}
							isBurn = next.Kind == "burned"
							break
						}
						f.IsBurned = isBurn
						f.IsUnfrozen = true // re-use to signal unstake slash
						flows = append(flows, f)
					} else {
						// post-Oxford unstaked out-flow can go to
						// - spendable balance (auto-finalize)
						// - back to frozen deposits (auto-staked)
						f := model.NewFlow(b.block, acc, acc, id)
						f.Kind = model.FlowKindStake
						f.Type = model.FlowTypeFinalizeUnstake
						f.AmountOut = -u.Amount()
						f.IsUnfrozen = true
						flows = append(flows, f)

						// FIXME: add delegation outflow?

						// TODO: staker staking
						// - add delegation out-flow if baker switches
						// - add delegation in-flow to new baker
					}
				}
			}
		}
	}
	return flows
}

func (b *Builder) NewSubsidyFlow(acc *model.Account, amount int64, id model.OpRef) *model.Flow {
	f := model.NewFlow(b.block, acc, nil, id)
	f.Kind = model.FlowKindBalance
	f.Type = model.FlowTypeSubsidy
	f.AmountIn = amount
	return f
}

func (b *Builder) NewInvoiceFlow(acc *model.Account, amount int64, id model.OpRef) *model.Flow {
	f := model.NewFlow(b.block, acc, nil, id)
	f.Kind = model.FlowKindBalance
	f.Type = model.FlowTypeInvoice
	f.AmountIn = amount
	return f
}

func (b *Builder) NewAirdropFlow(acc *model.Account, amount int64, id model.OpRef) *model.Flow {
	f := model.NewFlow(b.block, acc, nil, id)
	f.Kind = model.FlowKindBalance
	f.Type = model.FlowTypeAirdrop
	f.AmountIn = amount
	return f
}
