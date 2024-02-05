// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

type TicketUpdate struct {
	// Id      TicketUpdateID `json:"row_id"`
	Ticketer tezos.Address  `json:"ticketer"`
	Type     micheline.Prim `json:"type"`
	Content  micheline.Prim `json:"content"`
	Account  tezos.Address  `json:"account"`
	Amount   tezos.Z        `json:"amount"`
}

func NewTicketUpdate(ctx *server.Context, u *model.TicketUpdate, _ server.Options) *TicketUpdate {
	typ, _ := ctx.Indexer.LookupTicket(ctx, u.TicketId)
	return &TicketUpdate{
		Ticketer: typ.Ticketer,
		Type:     typ.Type,
		Content:  typ.Content,
		Account:  ctx.Indexer.LookupAddress(ctx, u.AccountId),
		Amount:   u.Amount,
	}
}
