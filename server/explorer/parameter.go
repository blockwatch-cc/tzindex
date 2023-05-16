// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"encoding/json"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
	"blockwatch.cc/tzindex/server"
)

type Parameters struct {
	Entrypoint string          `json:"entrypoint,omitempty"`      // contract, ticket transfer
	Value      interface{}     `json:"value,omitempty"`           // contract
	Prim       *micheline.Prim `json:"prim,omitempty"`            // contract
	Kind       string          `json:"kind,omitempty"`            // rollup kind
	L2Address  *tezos.Address  `json:"l2_address,omitempty"`      // tx rollup
	Method     string          `json:"method,omitempty"`          // tx+smart rollup
	Arguments  json.RawMessage `json:"args,omitempty"`            // tx+smart rollup
	Level      *int64          `json:"level,omitempty"`           // tx rollup
	Result     json.RawMessage `json:"result,omitempty"`          // smart rollup
	Type       *micheline.Prim `json:"ticket_ty,omitempty"`       // ticket transfer
	Contents   *micheline.Prim `json:"ticket_contents,omitempty"` // ticket transfer
	Ticketer   *tezos.Address  `json:"ticket_ticketer,omitempty"` // ticket transfer
	Amount     *tezos.Z        `json:"ticket_amount,omitempty"`   // ticket transfer

}

func NewContractParameters(ctx *server.Context, data []byte, typ micheline.Type, op tezos.OpHash, args server.Options) *Parameters {
	resp := &Parameters{}

	p := &micheline.Parameters{}
	if err := p.UnmarshalBinary(data); err != nil {
		log.Errorf("%s param unmarshal: %v", op, err)
		return resp
	}
	// log.Infof("call entrypoint=%s params=%s", p.Entrypoint)
	// log.Infof("call params=%s", p.Value.Dump())
	// log.Infof("call type %s", typ.Dump())

	ep, prim, err := p.MapEntrypoint(typ)
	if err != nil {
		log.Errorf("%s entrypoint %s: %v", op, p.Entrypoint, err)
		log.Errorf("%s params: %s", op, p.Value.Dump())
		return resp
	}

	var wasPacked bool
	if args.WithUnpack() && prim.IsPackedAny() {
		if up, err := prim.UnpackAll(); err == nil {
			wasPacked = true
			prim = up
		}
	}

	// use resolved entrypoint type and real name
	resp.Entrypoint = ep.Name
	typ = ep.Type()

	// strip entrypoint name annot to prevent duplicate nesting
	typ.Prim.Anno = nil

	val := micheline.NewValue(typ, prim)
	if m, err := val.Map(); err == nil {
		resp.Value = m
	} else {
		log.Errorf("%s param translate: %v", op, err)
	}

	// try with type deduction
	if resp.Value == nil && wasPacked {
		val.FixType()
		if m, err := val.Map(); err == nil {
			resp.Value = m
		} else {
			log.Errorf("%s fixed param translate: %v", op, err)
		}
	}

	// be compatible with legacy API
	if resp.Value == nil && !prim.IsNil() {
		resp.Prim = &prim
	}

	if args.WithPrim() {
		resp.Prim = &prim
	}

	return resp
}

func NewTicketTransferParameters(ctx *server.Context, op *model.Op, args server.Options) *Parameters {
	p := &Parameters{
		Entrypoint: op.Data,
	}
	var prim micheline.Prim
	if err := prim.UnmarshalBinary(op.Parameters); err != nil {
		log.Errorf("%s param unmarshal: %v", op, err)
		return nil
	}
	p.Type = &prim.Args[1].Args[0]
	p.Contents = &prim.Args[1].Args[1].Args[0]
	var addr tezos.Address
	if err := addr.Decode(prim.Args[0].Bytes); err != nil {
		log.Errorf("%s ticketer address: %v", op, err)
	}
	p.Ticketer = &addr
	z := tezos.NewBigZ(prim.Args[1].Args[1].Args[1].Int)
	p.Amount = &z
	return p
}

func NewTxRollupParameters(ctx *server.Context, op *model.Op, args server.Options) *Parameters {
	p := &Parameters{
		Method: op.Data,
		Kind:   "tx_rollup",
	}

	switch op.Data {
	case "deposit":
		// fake deposit entrypoint call with ticket
		p.Method = ""
		p.Entrypoint = op.Data
		var call micheline.Parameters
		_ = call.UnmarshalBinary(op.Parameters)
		if call.Value.IsValid() {
			p.Value = micheline.NewValuePtr(
				micheline.TicketType(call.Value.Args[1]),
				call.Value.Args[0].Args[0],
			)
			addr := tezos.NewAddress(
				tezos.AddressTypeBls12_381,
				call.Value.Args[0].Args[1].Bytes,
			)
			p.L2Address = &addr
		}
	case "tx_rollup_submit_batch":
		if batch, err := rpc.DecodeTxRollupBatch(op.Parameters); err == nil {
			p.Arguments, _ = batch.MarshalJSON()
		}
	case "tx_rollup_commit":
		if commit, err := rpc.DecodeTxRollupCommit(op.Parameters); err == nil {
			p.Arguments, _ = commit.MarshalJSON()
		}
	case "tx_rollup_return_bond":
		// empty, no args
	case "tx_rollup_finalize_commitment", "tx_rollup_remove_commitment":
		var prim micheline.Prim
		if err := prim.UnmarshalBinary(op.Parameters); err == nil {
			lvl := prim.Int.Int64()
			p.Level = &lvl
		}
	case "tx_rollup_rejection":
		if reject, err := rpc.DecodeTxRollupRejection(op.Parameters); err == nil {
			p.Arguments, _ = reject.MarshalJSON()
		}
	case "tx_rollup_dispatch_tickets":
		if dispatch, err := rpc.DecodeTxRollupDispatch(op.Parameters); err == nil {
			p.Arguments, _ = dispatch.MarshalJSON()
		}
	}
	return p
}

func NewSmartRollupParameters(ctx *server.Context, op *model.Op, args server.Options) *Parameters {
	p := &Parameters{
		Method: op.Data,
		Kind:   "smart_rollup",
	}
	var prim micheline.Prim
	if err := prim.UnmarshalBinary(op.Parameters); err == nil {
		if op.Type == model.OpTypeRollupOrigination {
			p.Arguments = prim.Bytes
		} else {
			p.Arguments = prim.Args[0].Bytes
			p.Result = prim.Args[1].Bytes
		}
	}
	return p
}
