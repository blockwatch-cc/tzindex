// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"context"
	"encoding/json"
	"fmt"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

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
			return Errorf("loading contract %s %d: %v", top.Source, src.RowId, err)
		}
	}
	if dst.IsContract {
		dCon, err = b.LoadContractByAccountId(ctx, dst.RowId)
		if err != nil {
			return Errorf("loading contract %s %d: %v", top.Destination, dst.RowId, err)
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
	op.IsContract = dst.IsContract && !dCon.Address.IsRollup()
	op.IsRollup = dst.IsContract && dCon.Address.IsRollup()
	op.IsBurnAddress = top.Destination == tezos.ZeroAddress

	res := top.Result()
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.Gas()
	op.Volume = top.Amount
	op.StoragePaid = res.PaidStorageSizeDiff
	b.block.Ops = append(b.block.Ops, op)

	if top.Parameters.Value.IsValid() {
		op.Parameters, err = top.Parameters.MarshalBinary()
		if err != nil {
			log.Error(Errorf("marshal params: %v", err))
		}

		if dCon != nil && op.IsContract {
			pTyp, _, err := dCon.LoadType()
			if err != nil {
				log.Error(Errorf("loading script: %v", err))
			}
			ep, _, err := top.Parameters.MapEntrypoint(pTyp)
			if op.IsSuccess && err != nil {
				log.Error(Errorf("searching entrypoint: %v", err))
			} else {
				op.Entrypoint = ep.Id
				op.Data = ep.Name
			}
		}
		// ticket deposit
		if dCon != nil && op.IsRollup {
			op.Entrypoint = 0
			op.Data = top.Parameters.Entrypoint
		}
	}
	if res.Storage.IsValid() {
		op.Storage, err = res.Storage.MarshalBinary()
		if err != nil {
			log.Error(Errorf("marshal storage: %v", err))
		}
		op.StorageHash = res.Storage.Hash64()
	}

	// create or extend bigmap diff to inject alloc for proto < v005, overwrite original
	op.BigmapEvents = res.BigmapEvents()
	if b.block.Params.Version <= 4 && len(op.BigmapEvents) > 0 {
		op.BigmapEvents, _ = b.PatchBigmapEvents(ctx, op.BigmapEvents, dst.Address, nil)
	}

	// keep ticket updates
	op.RawTicketUpdates = res.TicketUpdates()

	if op.IsSuccess {
		flows := b.NewTransactionFlows(
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
		// fees only
		b.NewTransactionFlows(src, nil, sbkr, nil,
			nil, nil,
			top.Fees(),
			nil, // no result balance updates
			b.block,
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
		dst.LastSeen = b.block.Height
		dst.IsDirty = true

		if op.IsSuccess {
			src.NTxSuccess++
			src.NTxOut++
			dst.NTxIn++
			dst.TotalFeesUsed += op.Fee

			// reactivate inactive bakers (receiver only)
			// - it seems from reverse engineering baker activation rules
			//   that received transactions will reactivate an inactive baker
			//   and extend grace period for active bakers
			// - don't do this for origination-big bakers (missing self-registration)
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
				op.IsStorageUpdate = dCon.Update(op, b.block.Params)
				op.Contract = dCon
			}
		} else {
			src.NTxFailed++
		}
	} else {
		src.Counter = op.Counter - 1
		src.IsDirty = true
		dst.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess--
			src.NTxOut--
			dst.NTxIn--
			dst.TotalFeesUsed -= op.Fee
			if dCon != nil {
				// rollback contract from previous op
				// if nil, will rollback to previous state
				prev, _ := b.idx.FindLastCall(ctx, dst.RowId, dst.FirstSeen, op.Height)
				if prev != nil {
					store, _ := b.idx.FindPreviousStorage(ctx, dst.RowId, prev.Height, prev.Height)
					if store != nil {
						prev.Storage = store.Storage
						prev.StorageHash = store.Hash
						dCon.Rollback(op, prev, b.block.Params)
					}
				}
			}
		} else {
			src.NTxFailed--
		}
	}

	// apply internal operation result (may generate new op and flows)
	for i, v := range top.Metadata.InternalResults {
		// skip events, they are processed in event index
		if v.Kind == tezos.OpTypeEvent {
			continue
		}
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
			return Errorf("loading contract %s %d: %v", iop.Source, src.RowId, err)
		}
	}
	if dst.IsContract {
		dCon, err = b.LoadContractByAccountId(ctx, dst.RowId)
		if err != nil {
			return Errorf("loading contract %s %d: %v", iop.Destination, dst.RowId, err)
		}
	}

	// build op (internal and outer tx share the same hash and block location)
	op := model.NewOp(b.block, id)
	op.IsInternal = true
	op.IsContract = dst.IsContract && !dCon.Address.IsRollup()
	op.IsRollup = dst.IsContract && dCon.Address.IsRollup()
	op.SenderId = origsrc.RowId
	op.ReceiverId = dst.RowId
	op.CreatorId = src.RowId
	op.Counter = iop.Nonce
	op.Fee = 0          // n.a. for internal ops
	op.GasLimit = 0     // n.a. for internal ops
	op.StorageLimit = 0 // n.a. for internal ops
	op.Volume = iop.Amount
	op.IsBurnAddress = tezos.ZeroAddress == iop.Destination

	res := iop.Result
	op.Status = res.Status
	op.IsSuccess = op.Status.IsSuccess()
	op.GasUsed = res.Gas()
	op.StoragePaid = res.PaidStorageSizeDiff
	b.block.Ops = append(b.block.Ops, op)

	if iop.Parameters.Value.IsValid() {
		op.Parameters, err = iop.Parameters.MarshalBinary()
		if err != nil {
			log.Error(Errorf("marshal params: %v", err))
		}
		if dCon != nil && op.IsContract {
			pTyp, _, err := dCon.LoadType()
			if err != nil {
				log.Error(Errorf("loading script for %s: %v", dst, err))
			}
			ep, _, err := iop.Parameters.MapEntrypoint(pTyp)
			if op.IsSuccess && err != nil {
				log.Error(Errorf("searching entrypoint in %s: %v", dst, err))
			} else {
				op.Entrypoint = ep.Id
				op.Data = ep.Name
			}
		}
		// ticket deposit
		if dCon != nil && op.IsRollup {
			op.Entrypoint = 0
			op.Data = iop.Parameters.Entrypoint
		}
	}
	if res.Storage.IsValid() {
		op.Storage, err = res.Storage.MarshalBinary()
		if err != nil {
			log.Error(Errorf("marshal storage: %v", err))
		}
		op.StorageHash = res.Storage.Hash64()
	}

	// create or extend bigmap diff to inject alloc for proto < v005, overwrite original
	op.BigmapEvents = res.BigmapEvents()
	if b.block.Params.Version <= 4 && len(op.BigmapEvents) > 0 {
		op.BigmapEvents, _ = b.PatchBigmapEvents(ctx, op.BigmapEvents, dst.Address, nil)
	}

	// keep ticket updates
	op.RawTicketUpdates = res.TicketUpdates()

	// on success, create flows and update accounts
	if op.IsSuccess {
		flows := b.NewInternalTransactionFlows(
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
		// keep errors
		op.Errors, _ = json.Marshal(res.Errors)

		// fees are paid by outer tx
	}

	// update accounts
	if !rollback {
		src.Counter = op.Counter
		src.LastSeen = b.block.Height
		src.IsDirty = true
		dst.LastSeen = b.block.Height
		dst.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess++
			src.NTxOut++
			dst.NTxIn++
			dst.TotalFeesUsed += op.Fee
			if dCon != nil {
				op.IsStorageUpdate = dCon.Update(op, b.block.Params)
				op.Contract = dCon
			}
		} else {
			src.NTxFailed++
		}
	} else {
		src.Counter = op.Counter - 1
		src.IsDirty = true
		dst.IsDirty = true
		if op.IsSuccess {
			src.NTxSuccess--
			src.NTxOut--
			dst.NTxIn--
			dst.TotalFeesUsed -= op.Fee
			if dCon != nil {
				// rollback contract from previous op
				// if nil, will rollback to previous state
				prev, _ := b.idx.FindLastCall(ctx, dst.RowId, dst.FirstSeen, op.Height)
				if prev != nil {
					store, _ := b.idx.FindPreviousStorage(ctx, dst.RowId, prev.Height, prev.Height)
					if store != nil {
						prev.Storage = store.Storage
						prev.StorageHash = store.Hash
						dCon.Rollback(op, prev, b.block.Params)
					}
				}
			}
		} else {
			src.NTxFailed--
		}
	}
	return nil
}
