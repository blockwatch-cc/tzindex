// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"bytes"
	"fmt"

	. "blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/micheline"
	"blockwatch.cc/tzindex/rpc"
)

// - baker pays deposit and receives block rewards+fees
// - on first block in cycle, deposits, rewards and fees are unfrozen
func (b *Builder) NewBakerFlows() ([]*Flow, error) {
	// Note: during chain bootstrap there used to be blocks without rewards
	// and no balance updates were issued to bakers
	flows := make([]*Flow, 0)
	for _, upd := range b.block.TZ.Block.Metadata.BalanceUpdates {
		switch upd.BalanceUpdateKind() {
		case "contract":
			u := upd.(*rpc.ContractBalanceUpdate)
			acc, ok := b.AccountByAddress(u.Contract)
			if !ok {
				return nil, fmt.Errorf("missing account %s", u.Contract)
			}
			if u.Change < 0 {
				// baking: deposits paid from balance
				f := NewFlow(b.block, acc, acc)
				f.Category = FlowCategoryBalance
				f.Operation = FlowTypeBaking
				f.AmountOut = -u.Change // note the negation!
				flows = append(flows, f)
			} else {
				// payout: credit unfrozen deposits, rewards and fees
				f := NewFlow(b.block, acc, acc)
				f.Category = FlowCategoryBalance
				f.Operation = FlowTypeInternal
				f.AmountIn = u.Change
				f.IsUnfrozen = true
				f.TokenGenMin = 1
				flows = append(flows, f)
			}
		case "freezer":
			u := upd.(*rpc.FreezerBalanceUpdate)
			acc, ok := b.AccountByAddress(u.Delegate)
			if !ok {
				return nil, fmt.Errorf("missing account %s", u.Delegate)
			}
			if u.Change > 0 {
				// baking: frozen deposits and rewards
				switch u.Category {
				case "deposits":
					f := NewFlow(b.block, acc, acc)
					f.Category = FlowCategoryDeposits
					f.Operation = FlowTypeBaking
					f.AmountIn = u.Change
					f.IsFrozen = true
					flows = append(flows, f)
				case "rewards":
					f := NewFlow(b.block, acc, acc)
					f.Category = FlowCategoryRewards
					f.Operation = FlowTypeBaking
					f.AmountIn = u.Change
					f.IsFrozen = true
					flows = append(flows, f)
				}
			} else {
				// payout: deduct unfrozen deposits, rewards and fees
				// when cycle is set and > N-5 then this is a seed nonce slashing
				// because the baker did not publish nonces
				cycle := u.Cycle()
				isSeedNonceSlashing := cycle > 0 && cycle > b.block.Cycle-b.block.Params.PreservedCycles
				switch u.Category {
				case "deposits":
					f := NewFlow(b.block, acc, acc)
					f.Category = FlowCategoryDeposits
					f.Operation = FlowTypeInternal
					f.AmountOut = -u.Change
					f.IsUnfrozen = true
					flows = append(flows, f)
				case "rewards":
					if isSeedNonceSlashing {
						f := NewFlow(b.block, acc, acc)
						f.Category = FlowCategoryRewards
						f.Operation = FlowTypeNonceRevelation
						f.AmountOut = -u.Change
						f.IsBurned = true
						flows = append(flows, f)
					} else {
						f := NewFlow(b.block, acc, acc)
						f.Category = FlowCategoryRewards
						f.Operation = FlowTypeInternal
						f.AmountOut = -u.Change
						f.IsUnfrozen = true
						flows = append(flows, f)
					}
				case "fees":
					if isSeedNonceSlashing {
						f := NewFlow(b.block, acc, acc)
						f.Category = FlowCategoryFees
						f.Operation = FlowTypeNonceRevelation
						f.AmountOut = -u.Change
						f.IsBurned = true
						flows = append(flows, f)
					} else {
						f := NewFlow(b.block, acc, acc)
						f.Category = FlowCategoryFees
						f.Operation = FlowTypeInternal
						f.AmountOut = -u.Change
						f.IsUnfrozen = true
						flows = append(flows, f)
					}
				}
			}
		}
	}
	b.block.Flows = append(b.block.Flows, flows...)
	return flows, nil
}

func (b *Builder) NewEndorserFlows(acc *Account, upd rpc.BalanceUpdates) ([]*Flow, error) {
	// Note: during chain bootstrap there used to be blocks without rewards
	// and no balance updates were issued to endorsers
	flows := make([]*Flow, 0)
	for _, v := range upd {
		switch v.BalanceUpdateKind() {
		case "contract":
			// deposits paid from balance
			u := v.(*rpc.ContractBalanceUpdate)
			f := NewFlow(b.block, acc, acc)
			f.Category = FlowCategoryBalance
			f.Operation = FlowTypeEndorsement
			f.AmountOut = -u.Change // note the negation!
			flows = append(flows, f)
		case "freezer":
			u := v.(*rpc.FreezerBalanceUpdate)
			switch u.Category {
			case "deposits":
				f := NewFlow(b.block, acc, acc)
				f.Category = FlowCategoryDeposits
				f.Operation = FlowTypeEndorsement
				f.AmountIn = u.Change
				f.IsFrozen = true
				flows = append(flows, f)
			case "rewards":
				f := NewFlow(b.block, acc, acc)
				f.Category = FlowCategoryRewards
				f.Operation = FlowTypeEndorsement
				f.AmountIn = u.Change
				f.IsFrozen = true
				flows = append(flows, f)
			}
		}
	}
	b.block.Flows = append(b.block.Flows, flows...)
	return flows, nil
}

func (b *Builder) NewInvoiceFlow(acc *Account, amount int64) *Flow {
	f := NewFlow(b.block, acc, acc)
	f.Category = FlowCategoryBalance
	f.Operation = FlowTypeInvoice
	f.AmountIn = amount
	return f
}

func (b *Builder) NewActivationFlow(acc *Account, aop *rpc.AccountActivationOp) ([]*Flow, error) {
	if l := len(aop.Metadata.BalanceUpdates); l != 1 {
		log.Warnf("Unexpected %d balance updates for activation op at height %d", l, b.block.Height)
	}
	f := NewFlow(b.block, acc, acc)
	f.Category = FlowCategoryBalance
	f.Operation = FlowTypeActivation
	f.AmountIn = aop.Metadata.BalanceUpdates[0].(*rpc.ContractBalanceUpdate).Change
	f.TokenGenMin = 1
	f.TokenGenMax = 1
	b.block.Flows = append(b.block.Flows, f)
	return []*Flow{f}, nil
}

func (b *Builder) NewDelegationFlows(src, ndlg, odlg *Account, upd rpc.BalanceUpdates) ([]*Flow, error) {
	flows := make([]*Flow, 0)
	var feespaid int64
	for _, v := range upd {
		switch v.BalanceUpdateKind() {
		case "contract":
			// fees paid by src
			u := v.(*rpc.ContractBalanceUpdate)
			f := NewFlow(b.block, src, src)
			f.Category = FlowCategoryBalance
			f.Operation = FlowTypeDelegation
			f.AmountOut = -u.Change // note the negation!
			f.IsFee = true
			feespaid -= u.Change
			flows = append(flows, f)
		case "freezer":
			// fees received by baker
			u := v.(*rpc.FreezerBalanceUpdate)
			switch u.Category {
			case "fees":
				f := NewFlow(b.block, b.block.Baker, src)
				f.Category = FlowCategoryFees
				f.Operation = FlowTypeDelegation
				f.AmountIn = u.Change
				f.IsFrozen = true
				flows = append(flows, f)
			}
		}
	}

	balance := src.Balance()
	var pending int64

	// if delegation is renewed/duplicate, handle fee out-flow only
	if ndlg != nil && odlg != nil && ndlg.RowId == odlg.RowId {
		// create flow only if fees are paid
		if feespaid > 0 && odlg.RowId != src.RowId {
			f := NewFlow(b.block, odlg, src)
			f.Category = FlowCategoryDelegation
			f.Operation = FlowTypeDelegation
			f.AmountOut = feespaid // deduct this operation's fees only
			flows = append(flows, f)
		}
	} else {
		// handle any change in delegate

		// source account may have run multiple ops in the same block, so the
		// (re-)delegated balance must be adjusted by all pending updates
		// because they have already created delegation out-flows
		for _, f := range b.block.Flows {
			if f.AccountId == src.RowId && f.Category == FlowCategoryBalance {
				pending += f.AmountOut - f.AmountIn
			}
		}

		// if src is already delegated, create an (un)delegation flow for the old delegate
		// unless its self-delegation
		if odlg != nil && odlg.RowId != src.RowId {
			f := NewFlow(b.block, odlg, src)
			f.Category = FlowCategoryDelegation
			f.Operation = FlowTypeDelegation
			f.AmountOut = balance - pending // deduct difference without fees
			flows = append(flows, f)
		}

		// create delegation to new delegate using balance minus delegation fees from above
		// and minus pending balance updates, unless its a self-delegation
		// (i.e. delegate registration)
		if ndlg != nil && ndlg.RowId != src.RowId && balance-pending-feespaid > 0 {
			f := NewFlow(b.block, ndlg, src)
			f.Category = FlowCategoryDelegation
			f.Operation = FlowTypeDelegation
			f.AmountIn = balance - feespaid - pending
			flows = append(flows, f)
		}
	}
	b.block.Flows = append(b.block.Flows, flows...)
	return flows, nil
}

func (b *Builder) NewRevealFlows(src, dlg *Account, upd rpc.BalanceUpdates) ([]*Flow, error) {
	flows := make([]*Flow, 0)
	// fees are optional
	var feespaid int64
	for _, v := range upd {
		switch v.BalanceUpdateKind() {
		case "contract":
			// fees paid by src
			u := v.(*rpc.ContractBalanceUpdate)
			f := NewFlow(b.block, src, src)
			f.Category = FlowCategoryBalance
			f.Operation = FlowTypeReveal
			f.AmountOut = -u.Change // note the negation!
			f.IsFee = true
			feespaid -= u.Change
			flows = append(flows, f)
		case "freezer":
			// fees received by baker
			u := v.(*rpc.FreezerBalanceUpdate)
			switch u.Category {
			case "fees":
				f := NewFlow(b.block, b.block.Baker, src)
				f.Category = FlowCategoryFees
				f.Operation = FlowTypeReveal
				f.AmountIn = u.Change
				f.IsFrozen = true
				flows = append(flows, f)
			}
		}
	}

	// if src is delegated subtract paid fees from delegated balance if not self delegate
	if feespaid > 0 && dlg != nil && dlg.RowId != src.RowId {
		f := NewFlow(b.block, dlg, src)
		f.Category = FlowCategoryDelegation
		f.Operation = FlowTypeReveal
		f.AmountOut = feespaid // deduct fees
		flows = append(flows, f)
	}
	b.block.Flows = append(b.block.Flows, flows...)
	return flows, nil
}

// injected by the baker only
func (b *Builder) NewSeedNonceFlows(upd rpc.BalanceUpdates) ([]*Flow, error) {
	flows := make([]*Flow, 0)
	for _, v := range upd {
		switch v.BalanceUpdateKind() {
		case "freezer":
			u := v.(*rpc.FreezerBalanceUpdate)
			switch u.Category {
			case "rewards":
				f := NewFlow(b.block, b.block.Baker, b.block.Baker)
				f.Category = FlowCategoryRewards
				f.Operation = FlowTypeNonceRevelation
				f.AmountIn = u.Change
				f.IsFrozen = true
				flows = append(flows, f)
			}
		}
	}
	b.block.Flows = append(b.block.Flows, flows...)
	return flows, nil
}

// FIXME: split penalty into burned & moved amounts
func (b *Builder) NewDoubleBakingFlows(accuser, offender *Account, upd rpc.BalanceUpdates) ([]*Flow, error) {
	flows := make([]*Flow, 0)
	for i, v := range upd {
		// freezer updates only
		u := v.(*rpc.FreezerBalanceUpdate)
		if i == len(upd)-1 {
			// accuser reward
			switch u.Category {
			case "rewards":
				f := NewFlow(b.block, accuser, accuser)
				f.Operation = FlowTypeDenounciation
				f.Category = FlowCategoryRewards
				f.AmountIn = u.Change
				f.IsFrozen = true
				flows = append(flows, f)
			}
		} else {
			// offender penalty
			f := NewFlow(b.block, offender, accuser)
			f.Operation = FlowTypeDenounciation
			f.AmountOut = -u.Change
			f.IsFrozen = true
			switch u.Category {
			case "deposits":
				f.Category = FlowCategoryDeposits
			case "rewards":
				f.Category = FlowCategoryRewards
			case "fees":
				f.Category = FlowCategoryFees
			}
			flows = append(flows, f)
		}
	}
	b.block.Flows = append(b.block.Flows, flows...)
	return flows, nil
}

// FIXME: split penalty into burned & moved amounts
func (b *Builder) NewDoubleEndorsingFlows(accuser, offender *Account, upd rpc.BalanceUpdates) ([]*Flow, error) {
	flows := make([]*Flow, 0)
	for i, v := range upd {
		// freezer updates only
		u := v.(*rpc.FreezerBalanceUpdate)
		if i == len(upd)-1 {
			// accuser reward
			switch u.Category {
			case "rewards":
				f := NewFlow(b.block, accuser, accuser)
				f.Operation = FlowTypeDenounciation
				f.Category = FlowCategoryRewards
				f.AmountIn = u.Change
				f.IsFrozen = true
				flows = append(flows, f)
			}
		} else {
			// offender penalty
			f := NewFlow(b.block, offender, accuser)
			f.Operation = FlowTypeDenounciation
			f.AmountOut = -u.Change
			f.IsFrozen = true
			switch u.Category {
			case "deposits":
				f.Category = FlowCategoryDeposits
			case "rewards":
				f.Category = FlowCategoryRewards
			case "fees":
				f.Category = FlowCategoryFees
			}
			flows = append(flows, f)
		}
	}
	b.block.Flows = append(b.block.Flows, flows...)
	return flows, nil
}

func (b *Builder) NewTransactionFlows(src, dst, srcdlg, dstdlg *Account, feeupd, txupd rpc.BalanceUpdates, block *Block) ([]*Flow, error) {
	flows := make([]*Flow, 0)
	// fees are optional
	var fees int64
	for _, v := range feeupd {
		switch v.BalanceUpdateKind() {
		case "contract":
			// fees paid by src
			u := v.(*rpc.ContractBalanceUpdate)
			f := NewFlow(b.block, src, src)
			f.Category = FlowCategoryBalance
			f.Operation = FlowTypeTransaction
			f.AmountOut = -u.Change // note the negation!
			f.IsFee = true
			fees += -u.Change
			flows = append(flows, f)
		case "freezer":
			// fees received by baker
			u := v.(*rpc.FreezerBalanceUpdate)
			switch u.Category {
			case "fees":
				f := NewFlow(b.block, b.block.Baker, src)
				f.Category = FlowCategoryFees
				f.Operation = FlowTypeTransaction
				f.AmountIn = u.Change
				f.IsFrozen = true
				f.IsFee = true
				flows = append(flows, f)
			}
		}
	}

	// transaction may burn, transfer and vest funds
	var moved, burnedAndMoved int64
	for _, v := range txupd {
		switch v.BalanceUpdateKind() {
		case "contract":
			u := v.(*rpc.ContractBalanceUpdate)
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
		f := NewFlow(b.block, src, src)
		f.Category = FlowCategoryBalance
		f.Operation = FlowTypeTransaction
		f.AmountOut = moved
		f.TokenGenMin = src.TokenGenMin
		f.TokenGenMax = src.TokenGenMax
		f.TokenAge = block.Age(src.LastIn)
		flows = append(flows, f)
		// credited to dest
		f = NewFlow(b.block, dst, src)
		f.Category = FlowCategoryBalance
		f.Operation = FlowTypeTransaction
		f.AmountIn = moved
		f.TokenGenMin = src.TokenGenMin
		f.TokenGenMax = src.TokenGenMax
		f.TokenAge = block.Age(src.LastIn)
		flows = append(flows, f)
	}

	if burnedAndMoved-moved > 0 {
		// deducted from source as burn
		f := NewFlow(b.block, src, src)
		f.Category = FlowCategoryBalance
		f.Operation = FlowTypeTransaction
		f.AmountOut = burnedAndMoved - moved
		f.IsBurned = true
		flows = append(flows, f)
	}

	// deduct from source delegation if not self delegate
	if srcdlg != nil && srcdlg.RowId != src.RowId && fees+burnedAndMoved > 0 {
		f := NewFlow(b.block, srcdlg, src)
		f.Category = FlowCategoryDelegation
		f.Operation = FlowTypeTransaction
		f.AmountOut = fees + burnedAndMoved // deduct fees and burned+moved amount
		flows = append(flows, f)
	}

	// delegate to dest delegate unless its a self-delegate
	if dstdlg != nil && dstdlg.RowId != dst.RowId && moved > 0 {
		f := NewFlow(b.block, dstdlg, src)
		f.Category = FlowCategoryDelegation
		f.Operation = FlowTypeTransaction
		f.AmountIn = moved
		flows = append(flows, f)
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows, nil
}

// fees are paid by outer tx
// burn is attributed to outer source
// vesting contracts generate vesting flows
func (b *Builder) NewInternalTransactionFlows(
	origsrc, src, dst,
	origdlg, srcdlg, dstdlg *Account,
	txupd rpc.BalanceUpdates,
	storage *micheline.Prim,
	block *Block,
) ([]*Flow, error) {
	flows := make([]*Flow, 0)

	// transaction may burn, transfer and vest funds
	var moved, burnedAndMoved int64
	for _, v := range txupd {
		switch v.BalanceUpdateKind() {
		case "contract":
			u := v.(*rpc.ContractBalanceUpdate)
			if u.Change < 0 {
				burnedAndMoved += -u.Change
			} else {
				moved += u.Change
			}
		}
	}

	// vesting criteria (specific to Tezos genesis vesting contracts)
	// - src must be a vesting contract
	// - vesting itself (vest) cannot be observed without looking at the
	//   contract storage (.storage.args[1].args[0].args[0].args[0].int)
	// - payout (pour) account may be reset by a previous contract tx which
	//   we do not currently track, so we must assume the given dest is
	//   eligible and the blockchain has properly verified the tx

	// extract contract balance if vesting
	if src.IsVesting {
		vestingBalance, err := GetVestingBalance(storage)
		if err != nil {
			return nil, fmt.Errorf("cannot read vesting balance: %v", err)
		}

		// vest difference into source, cap at unclaimed balance
		//
		// the cap is necessary because a vesting contract cannot vest
		// more than the max unclaimed balance even though the internal
		// vesting balance may be set to an overly high value (this seems
		// to be related to custodian contracts or vesting contracts without
		// pour information set. (e.g. some contracts have a 200M internal
		// vesting balance, no pour data and an initial/max contract balance
		// much smaller)
		//
		// References
		// https://gitlab.com/tezos/tezos/tree/9efc6cbcc0ded0472b9a504f9e927221514dc911/contracts/vesting
		// https://tezos.foundation/wp-content/uploads/2018/09/5223213-genesis.txt
		//

		// assume vest & pour when contract balance after the op is zero
		if vestingBalance == 0 {
			vestingBalance = moved
		}

		// cap at unclaimed balance
		if vestingBalance > src.UnclaimedBalance {
			vestingBalance = src.UnclaimedBalance
		}

		// cross check
		if moved > vestingBalance {
			return nil, fmt.Errorf("vest: %s pour %d is larger than vesting balance %d",
				src, moved, vestingBalance)
		}
		// vest moved amount into source
		f := NewFlow(b.block, src, src)
		f.Category = FlowCategoryBalance
		f.Operation = FlowTypeVest
		f.AmountIn = moved
		flows = append(flows, f)
	}

	// create move and burn flows when necessary, pouring a vested amount
	// is similar to a regular transaction
	if moved > 0 && dst != nil {
		// deducted from source
		f := NewFlow(b.block, src, src)
		f.Category = FlowCategoryBalance
		f.Operation = FlowTypeTransaction
		f.AmountOut = moved
		f.TokenGenMin = src.TokenGenMin
		f.TokenGenMax = src.TokenGenMax
		f.TokenAge = block.Age(src.LastIn)
		flows = append(flows, f)
		// credit to dest
		f = NewFlow(b.block, dst, src)
		f.Category = FlowCategoryBalance
		f.Operation = FlowTypeTransaction
		f.AmountIn = moved
		f.TokenGenMin = src.TokenGenMin
		f.TokenGenMax = src.TokenGenMax
		f.TokenAge = block.Age(src.LastIn)
		flows = append(flows, f)
	}

	if burnedAndMoved-moved > 0 {
		// Note: use outer source to burn
		f := NewFlow(b.block, origsrc, origsrc)
		f.Category = FlowCategoryBalance
		f.Operation = FlowTypeTransaction
		f.AmountOut = burnedAndMoved - moved
		f.IsBurned = true
		flows = append(flows, f)
		// adjust delegated balance if not self-delegated
		if origdlg != nil && origdlg.RowId != origsrc.RowId {
			f = NewFlow(b.block, origdlg, origsrc)
			f.Category = FlowCategoryDelegation
			f.Operation = FlowTypeTransaction
			f.AmountOut = burnedAndMoved - moved
			flows = append(flows, f)
		}
	}

	// deduct moved amount from source delegation if not self delegated
	if srcdlg != nil && srcdlg.RowId != src.RowId && moved > 0 {
		f := NewFlow(b.block, srcdlg, src)
		f.Category = FlowCategoryDelegation
		f.Operation = FlowTypeTransaction
		f.AmountOut = moved
		flows = append(flows, f)
	}

	// delegate to dest delegate unless its a self-delegate
	if dstdlg != nil && dstdlg.RowId != dst.RowId && moved > 0 {
		f := NewFlow(b.block, dstdlg, src)
		f.Category = FlowCategoryDelegation
		f.Operation = FlowTypeTransaction
		f.AmountIn = moved
		flows = append(flows, f)
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows, nil
}

func (b *Builder) NewOriginationFlows(src, dst, srcdlg, newdlg *Account, feeupd, origupd rpc.BalanceUpdates) ([]*Flow, error) {
	flows := make([]*Flow, 0)
	// fees are optional
	var fees int64
	for _, v := range feeupd {
		switch v.BalanceUpdateKind() {
		case "contract":
			// fees paid by src
			u := v.(*rpc.ContractBalanceUpdate)
			f := NewFlow(b.block, src, src)
			f.Category = FlowCategoryBalance
			f.Operation = FlowTypeOrigination
			f.AmountOut = -u.Change // note the negation!
			f.IsFee = true
			fees += -u.Change
			flows = append(flows, f)
		case "freezer":
			// fees received by baker
			u := v.(*rpc.FreezerBalanceUpdate)
			switch u.Category {
			case "fees":
				f := NewFlow(b.block, b.block.Baker, src)
				f.Category = FlowCategoryFees
				f.Operation = FlowTypeOrigination
				f.AmountIn = u.Change
				f.IsFrozen = true
				flows = append(flows, f)
			}
		}
	}

	// origination may burn and may transfer funds
	var moved, burnedAndMoved int64
	for _, v := range origupd {
		switch v.BalanceUpdateKind() {
		case "contract":
			u := v.(*rpc.ContractBalanceUpdate)
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
		f := NewFlow(b.block, src, src)
		f.Category = FlowCategoryBalance
		f.Operation = FlowTypeOrigination
		f.AmountOut = moved
		flows = append(flows, f)
		// credited to dest
		f = NewFlow(b.block, dst, src)
		f.Category = FlowCategoryBalance
		f.Operation = FlowTypeOrigination
		f.AmountIn = moved
		flows = append(flows, f)
	}
	if burnedAndMoved-moved > 0 {
		// deducted from source as burn
		f := NewFlow(b.block, src, src)
		f.Category = FlowCategoryBalance
		f.Operation = FlowTypeOrigination
		f.AmountOut = burnedAndMoved - moved
		f.IsBurned = true
		flows = append(flows, f)
	}

	// deduct from source delegation iff not self-delegated
	if srcdlg != nil && srcdlg.RowId != src.RowId && fees+burnedAndMoved > 0 {
		f := NewFlow(b.block, srcdlg, src)
		f.Category = FlowCategoryDelegation
		f.Operation = FlowTypeOrigination
		f.AmountOut = fees + burnedAndMoved // deduct fees and burned+moved amount
		flows = append(flows, f)
	}

	// delegate when a new delegate was set
	if newdlg != nil && moved > 0 {
		f := NewFlow(b.block, newdlg, src)
		f.Category = FlowCategoryDelegation
		f.Operation = FlowTypeOrigination
		f.AmountIn = moved
		flows = append(flows, f)
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows, nil
}

func (b *Builder) NewInternalOriginationFlows(origsrc, src, dst, origdlg, srcdlg *Account, upd rpc.BalanceUpdates) ([]*Flow, error) {
	flows := make([]*Flow, 0)
	// fees are paid by outer transaction, here we only have burned coins
	var burned, moved int64
	for _, v := range upd {
		switch v.BalanceUpdateKind() {
		case "contract":
			u := v.(*rpc.ContractBalanceUpdate)
			switch true {
			case bytes.Compare(u.Contract.Hash, origsrc.Hash) == 0:
				// burned from original source balance
				f := NewFlow(b.block, origsrc, origsrc)
				f.Category = FlowCategoryBalance
				f.Operation = FlowTypeOrigination
				f.AmountOut = -u.Change // note the negation!
				f.IsBurned = true
				flows = append(flows, f)
				burned += -u.Change
			case bytes.Compare(u.Contract.Hash, src.Hash) == 0:
				// transfers from src contract to dst contract
				f := NewFlow(b.block, src, src)
				f.Category = FlowCategoryBalance
				f.Operation = FlowTypeOrigination
				f.AmountOut = -u.Change // note the negation!
				flows = append(flows, f)
				moved += -u.Change
			case bytes.Compare(u.Contract.Hash, dst.Hash) == 0:
				// transfers from src contract to dst contract
				f := NewFlow(b.block, dst, src)
				f.Category = FlowCategoryBalance
				f.Operation = FlowTypeOrigination
				f.AmountIn = u.Change
				flows = append(flows, f)
			}
		}
	}

	// deduct burn from original source delegation iff not self-delegated
	if origdlg != nil && origdlg.RowId != origsrc.RowId && burned > 0 {
		f := NewFlow(b.block, origdlg, origsrc)
		f.Category = FlowCategoryDelegation
		f.Operation = FlowTypeOrigination
		f.AmountOut = burned // deduct burned amount
		flows = append(flows, f)
	}

	// deduct move from source delegation iff not self-delegated
	if srcdlg != nil && srcdlg.RowId != src.RowId && moved > 0 {
		f := NewFlow(b.block, srcdlg, src)
		f.Category = FlowCategoryDelegation
		f.Operation = FlowTypeOrigination
		f.AmountOut = moved // deduct moved amount
		flows = append(flows, f)
	}

	b.block.Flows = append(b.block.Flows, flows...)
	return flows, nil
}
