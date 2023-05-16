// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"encoding/json"
	"fmt"

	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

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
	isRegistration := dop.Source == dop.Delegate
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
	op.GasUsed = res.Gas()
	b.block.Ops = append(b.block.Ops, op)

	// build flows
	// - fee payment by source
	// - fee reception by baker
	// - (re)delegate source balance on success
	if op.IsSuccess {
		flows := b.NewDelegationFlows(
			src,
			nbkr,
			obkr,
			dop.Fees(),
			id,
		)

		// use the actual flow volume (current src balance may have changed before
		// in the same block)
		for _, f := range flows {
			switch f.AccountId {
			case op.BakerId:
				// new baker add (first or second flow on re-delegation)
				op.Volume = f.AmountIn
			case op.ReceiverId:
				// old baker withdraw (first or only flow on withdraw)
				op.Volume = f.AmountOut
			}
		}

	} else {
		// on error fees still deduct from old delegation
		b.NewDelegationFlows(
			src,
			obkr, // both =old !!
			obkr, // both =old !!
			dop.Fees(),
			id,
		)

		// keep errors
		op.Errors, _ = json.Marshal(res.Errors)
	}

	// update accounts
	if !rollback {
		src.Counter = op.Counter
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

		if op.IsSuccess {
			src.NTxSuccess++
			src.NTxOut++

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
					obkr.ActiveDelegations--
				}

			} else {
				// handle delegator withdraw
				if nbkr == nil {
					src.IsDelegated = false
					src.BakerId = 0
					src.DelegatedSince = 0
				} else if src.BakerId != nbkr.AccountId {
					// handle switch to new baker
					// only update when this delegation changes baker
					src.IsDelegated = true
					src.BakerId = nbkr.AccountId
					src.DelegatedSince = b.block.Height
					nbkr.TotalDelegations++
					nbkr.ActiveDelegations++
				}
				// handle withdraw from old baker if any
				if obkr != nil {
					obkr.ActiveDelegations--
				}
			}
		} else {
			src.NTxFailed++
		}
	} else {
		// rollback accounts
		src.Counter = op.Counter - 1
		src.IsDirty = true

		if op.IsSuccess {
			src.NTxSuccess--
			src.NTxOut--

			// update registrations
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
			if prevop, err := b.idx.FindLatestDelegation(ctx, src.RowId, op.Height); err != nil {
				if err != model.ErrNoOp {
					log.Error(Errorf("rollback: failed loading previous delegation op for account %d: %v", src.RowId, err))
				}
				obkr = nil
			} else if prevop.BakerId > 0 {
				lastsince = prevop.Height
				obkr, ok = b.BakerById(prevop.BakerId)
				if !ok {
					log.Error(Errorf("rollback: missing previous baker id %d", prevop.BakerId))
				}
			}
			if obkr == nil {
				// we must also look for the sources' origination op that may have
				// set the initial baker
				if prevop, err := b.idx.FindOrigination(ctx, src.RowId, src.FirstSeen); err != nil {
					if err != model.ErrNoOp {
						log.Error(Errorf("rollback: failed loading previous origination op for account %d: %v", src.RowId, err))
					}
				} else if prevop.BakerId != 0 {
					obkr, ok = b.BakerById(prevop.BakerId)
					if !ok {
						log.Error(Errorf("rollback: missing origin baker %s", prevop.BakerId))
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
		} else {
			src.NTxFailed--
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
	op.GasUsed = res.Gas()
	op.IsSuccess = op.Status.IsSuccess()
	b.block.Ops = append(b.block.Ops, op)

	if op.IsSuccess {
		// build flows
		// - no fees (paid by outer op)
		// - (re)delegate source balance on success
		// - no fees paid, no flow on failure
		flows := b.NewDelegationFlows(src, nbkr, obkr, nil, id)

		// use the actual flow volume (current src balance may have changed before
		// in the same block)
		for _, f := range flows {
			switch f.AccountId {
			case op.BakerId:
				// new baker add (first or second flow on re-delegation)
				op.Volume = f.AmountIn
			case op.ReceiverId:
				// old baker withdraw (first or only flow on withdraw)
				op.Volume = f.AmountOut
			}
		}
	} else {
		// keep errors
		op.Errors, _ = json.Marshal(res.Errors)
	}

	// update accounts
	if !rollback {
		src.Counter = op.Counter
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

		if op.IsSuccess {
			src.NTxSuccess++
			src.NTxOut++

			// no baker registration via internal op

			// handle delegator withdraw
			if nbkr == nil {
				src.IsDelegated = false
				src.BakerId = 0
				src.DelegatedSince = 0
			}

			// only update when this delegation changes baker
			if nbkr != nil && src.BakerId != nbkr.AccountId {
				src.IsDelegated = true
				src.BakerId = nbkr.AccountId
				src.DelegatedSince = b.block.Height
				nbkr.TotalDelegations++
				nbkr.ActiveDelegations++
			}

			// handle withdraw from old baker (also ensures we're duplicate safe)
			if obkr != nil {
				obkr.ActiveDelegations--
			}
		} else {
			src.NTxFailed++
		}
	} else {
		// rollback accounts
		src.Counter = op.Counter - 1
		src.IsDirty = true

		if op.IsSuccess {
			src.NTxSuccess--
			src.NTxOut--

			// find previous baker, if any
			var lastsince int64
			if prevop, err := b.idx.FindLatestDelegation(ctx, src.RowId, op.Height); err != nil {
				if err != model.ErrNoOp {
					log.Error(Errorf("rollback: failed loading previous delegation op for account %d: %v", src.RowId, err))
				}
				obkr = nil
			} else if prevop.BakerId > 0 {
				lastsince = prevop.Height
				obkr, ok = b.BakerById(prevop.BakerId)
				if !ok {
					log.Error(Errorf("rollback: missing previous baker id %d", prevop.BakerId))
				}
			}
			if obkr == nil {
				// we must also look for the sources' origination op that may have
				// set the initial baker
				if prevop, err := b.idx.FindOrigination(ctx, src.RowId, src.FirstSeen); err != nil {
					if err != model.ErrNoOp {
						log.Error(Errorf("rollback: failed loading previous origination op for account %d: %v", src.RowId, err))
					}
				} else if prevop.BakerId != 0 {
					obkr, ok = b.BakerById(prevop.BakerId)
					if !ok {
						log.Error(Errorf("rollback: missing origin baker %s", prevop.BakerId))
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
		} else {
			src.NTxFailed--
		}
	}

	return nil
}
