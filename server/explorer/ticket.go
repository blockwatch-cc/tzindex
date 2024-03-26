// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"errors"
	"net/http"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

type Ticket struct {
	Id           uint64         `json:"id"`
	Address      tezos.Address  `json:"address"`
	Type         micheline.Prim `json:"type"`
	Content      micheline.Prim `json:"content"`
	Hash         string         `json:"hash"`
	Creator      tezos.Address  `json:"creator"`
	FirstBlock   int64          `json:"first_block"`
	FirstTime    time.Time      `json:"first_time"`
	LastBlock    int64          `json:"last_block"`
	LastTime     time.Time      `json:"last_time"`
	Supply       tezos.Z        `json:"total_supply"`
	TotalMint    tezos.Z        `json:"total_mint"`
	TotalBurn    tezos.Z        `json:"total_burn"`
	NumTransfers int            `json:"num_transfers"`
	NumHolders   int            `json:"num_holders"`
}

func NewTicket(ctx *server.Context, tick *model.Ticket) *Ticket {
	return &Ticket{
		Id:           tick.Id.U64(),
		Address:      tick.Address,
		Type:         tick.Type,
		Content:      tick.Content,
		Hash:         util.U64String(tick.Hash).String(),
		Creator:      ctx.Indexer.LookupAddress(ctx, tick.Creator),
		FirstBlock:   tick.FirstBlock,
		FirstTime:    tick.FirstTime,
		LastBlock:    tick.LastBlock,
		LastTime:     tick.LastTime,
		Supply:       tick.Supply,
		TotalMint:    tick.TotalMint,
		TotalBurn:    tick.TotalBurn,
		NumTransfers: tick.NumTransfers,
		NumHolders:   tick.NumHolders,
	}
}

func (t Ticket) LastModified() time.Time {
	return t.LastTime
}

func (_ Ticket) Expires() time.Time {
	return time.Time{}
}

type TicketOwner struct {
	Id           uint64         `json:"id"`
	Address      tezos.Address  `json:"address"`
	Type         micheline.Prim `json:"type"`
	Content      micheline.Prim `json:"content"`
	Hash         string         `json:"hash"`
	Account      tezos.Address  `json:"account"`
	Balance      tezos.Z        `json:"balance"`
	FirstBlock   int64          `json:"first_block"`
	FirstTime    time.Time      `json:"first_time"`
	LastBlock    int64          `json:"last_block"`
	LastTime     time.Time      `json:"last_time"`
	NumTransfers int            `json:"num_transfers"`
	NumMints     int            `json:"num_mints"`
	NumBurns     int            `json:"num_burns"`
	VolSent      tezos.Z        `json:"vol_sent"`
	VolRecv      tezos.Z        `json:"vol_recv"`
	VolMint      tezos.Z        `json:"vol_mint"`
	VolBurn      tezos.Z        `json:"vol_burn"`
}

func NewTicketOwner(ctx *server.Context, ownr *model.TicketOwner, tick *model.Ticket) *TicketOwner {
	return &TicketOwner{
		Id:           ownr.Id.U64(),
		Address:      tick.Address,
		Type:         tick.Type,
		Content:      tick.Content,
		Hash:         util.U64String(tick.Hash).String(),
		Account:      ctx.Indexer.LookupAddress(ctx, ownr.Account),
		Balance:      ownr.Balance,
		FirstBlock:   ownr.FirstBlock,
		FirstTime:    ownr.FirstTime,
		LastBlock:    ownr.LastBlock,
		LastTime:     ownr.LastTime,
		NumTransfers: ownr.NumTransfers,
		NumMints:     ownr.NumMints,
		NumBurns:     ownr.NumBurns,
		VolSent:      ownr.VolSent,
		VolRecv:      ownr.VolRecv,
		VolMint:      ownr.VolMint,
		VolBurn:      ownr.VolBurn,
	}
}

func (t TicketOwner) LastModified() time.Time {
	return t.LastTime
}

func (t TicketOwner) Expires() time.Time {
	return time.Time{}
}

type TicketEvent struct {
	Id        uint64                `json:"id"`
	Address   tezos.Address         `json:"address"`
	Type      micheline.Prim        `json:"type"`
	Content   micheline.Prim        `json:"content"`
	Hash      string                `json:"hash"`
	EventType model.TicketEventType `json:"event_type"`
	Sender    tezos.Address         `json:"sender"`
	Receiver  tezos.Address         `json:"receiver"`
	Amount    tezos.Z               `json:"amount"`
	Height    int64                 `json:"height"`
	Time      time.Time             `json:"time"`
	OpId      model.OpID            `json:"op_id"`
}

func NewTicketEvent(ctx *server.Context, evnt *model.TicketEvent, tick *model.Ticket) *TicketEvent {
	return &TicketEvent{
		Id:        evnt.Id.U64(),
		Address:   tick.Address,
		Type:      tick.Type,
		Content:   tick.Content,
		Hash:      util.U64String(tick.Hash).String(),
		EventType: evnt.Type,
		Sender:    ctx.Indexer.LookupAddress(ctx, evnt.Sender),
		Receiver:  ctx.Indexer.LookupAddress(ctx, evnt.Receiver),
		Amount:    evnt.Amount,
		Height:    evnt.Height,
		Time:      evnt.Time,
		OpId:      evnt.OpId,
	}
}

func (t TicketEvent) LastModified() time.Time {
	return t.Time
}

func (t TicketEvent) Expires() time.Time {
	return time.Time{}
}

type TicketUpdate struct {
	Ticketer tezos.Address  `json:"ticketer"`
	Type     micheline.Prim `json:"type"`
	Content  micheline.Prim `json:"content"`
	Account  tezos.Address  `json:"account"`
	Amount   tezos.Z        `json:"amount"`
}

func NewTicketUpdate(ctx *server.Context, u *model.TicketUpdate, _ server.Options) *TicketUpdate {
	typ, err := ctx.Indexer.LookupTicket(ctx, u.TicketId)
	if err != nil {
		return nil
	}
	return &TicketUpdate{
		Ticketer: typ.Address,
		Type:     typ.Type,
		Content:  typ.Content,
		Account:  ctx.Indexer.LookupAddress(ctx, u.AccountId),
		Amount:   u.Amount,
	}
}

type TicketListRequest struct {
	ListRequest
	Type    tezos.HexBytes `schema:"type"`
	Content tezos.HexBytes `schema:"content"`
	Hash    util.U64String `schema:"hash"`
}

func (r TicketListRequest) Load(ctx *server.Context, issuer tezos.Address) (*model.Ticket, error) {
	if len(r.Type) > 0 && len(r.Content) > 0 && r.Hash == 0 {
		var typ, content micheline.Prim
		if err := typ.UnmarshalBinary(r.Type.Bytes()); err != nil {
			return nil, server.EBadRequest(server.EC_PARAM_INVALID, "invalid type", err)
		}
		if err := content.UnmarshalBinary(r.Content.Bytes()); err != nil {
			return nil, server.EBadRequest(server.EC_PARAM_INVALID, "invalid content", err)
		}
		r.Hash = util.U64String(model.TicketHash(issuer, typ, content))
	}
	tickets, err := ctx.Indexer.Table(model.TicketTableKey)
	if err != nil {
		return nil, server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no ticket table", err)
	}
	tick, err := model.LookupTicket(ctx, tickets, issuer, r.Hash.U64())
	if err != nil {
		if errors.Is(err, model.ErrNoTicket) {
			return nil, server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such ticket", err)
		} else {
			return nil, server.EInternal(server.EC_DATABASE, err.Error(), nil)
		}
	}
	return tick, nil
}

func ListTickets(ctx *server.Context) (any, int) {
	var args TicketListRequest
	ctx.ParseRequestArgs(&args)
	issuer := loadAccount(ctx)

	q := pack.NewQuery("api.list_tickets").
		WithOrder(args.Order).
		WithLimit(int(ctx.Cfg.ClampExplore(args.Limit))).
		WithOffset(int(args.Offset)).
		AndEqual("ticketer", issuer.RowId)

	if args.Cursor > 0 {
		q = q.And("id", args.Mode(), args.Cursor)
	}
	if len(args.Type) > 0 {
		q = q.AndEqual("type", args.Type.Bytes())
	}
	if len(args.Content) > 0 {
		q = q.AndEqual("content", args.Content.Bytes())
	}
	if args.Hash > 0 {
		q = q.AndEqual("hash", args.Hash)
	}

	t, err := ctx.Indexer.Table(model.TicketTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no ticket table", err))
	}
	list, err := model.ListTickets(ctx, t, q)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot list tickets", err))
	}

	resp := make([]*Ticket, 0, len(list))
	for _, v := range list {
		resp = append(resp, NewTicket(ctx, v))
	}
	return resp, http.StatusOK
}

func ListTicketBalances(ctx *server.Context) (any, int) {
	var args TicketListRequest
	ctx.ParseRequestArgs(&args)
	issuer := loadAccount(ctx)

	q := pack.NewQuery("api.list_ticket_balances").
		WithOrder(args.Order).
		WithLimit(int(ctx.Cfg.ClampExplore(args.Limit))).
		WithOffset(int(args.Offset)).
		AndEqual("ticketer", issuer.RowId)

	if args.Cursor > 0 {
		q = q.And("id", args.Mode(), args.Cursor)
	}

	tick, err := args.Load(ctx, issuer.Address)
	if err == nil {
		q = q.AndEqual("ticket", tick.Id)
	}

	owners, err := ctx.Indexer.Table(model.TicketOwnerTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no ticket owners table", err))
	}

	tickets, err := ctx.Indexer.Table(model.TicketTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no ticket table", err))
	}

	list, err := model.ListTicketOwners(ctx, owners, q)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot list ticket balances", err))
	}

	resp := make([]*TicketOwner, 0, len(list))
	for _, v := range list {
		if tick == nil || tick.Id != v.Ticket {
			tick, err = model.GetTicketId(ctx, tickets, v.Ticket)
			if err != nil {
				continue
			}
		}
		resp = append(resp, NewTicketOwner(ctx, v, tick))
	}
	return resp, http.StatusOK
}

type TicketEventListRequest struct {
	TicketListRequest
	EventType model.TicketEventType `schema:"event_type"`
	Sender    tezos.Address         `schema:"sender"`
	Receiver  tezos.Address         `schema:"receiver"`
	Height    int64                 `schema:"height"`
}

func ListTicketEvents(ctx *server.Context) (any, int) {
	var args TicketEventListRequest
	ctx.ParseRequestArgs(&args)
	issuer := loadAccount(ctx)

	q := pack.NewQuery("api.list_ticket_events").
		WithOrder(args.Order).
		WithLimit(int(ctx.Cfg.ClampExplore(args.Limit))).
		WithOffset(int(args.Offset)).
		AndEqual("ticketer", issuer.RowId)

	tick, err := args.Load(ctx, issuer.Address)
	if err == nil {
		q = q.AndEqual("ticket", tick.Id)
	}

	if args.Cursor > 0 {
		q = q.And("id", args.Mode(), args.Cursor)
	}
	if args.EventType.IsValid() {
		q = q.AndEqual("type", args.EventType)
	}
	if args.Sender.IsValid() {
		acc, err := ctx.Indexer.LookupAccount(ctx, args.Sender)
		if err != nil {
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such sender", err))
		}
		q = q.AndEqual("sender", acc.RowId)
	}
	if args.Receiver.IsValid() {
		acc, err := ctx.Indexer.LookupAccount(ctx, args.Sender)
		if err != nil {
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such sender", err))
		}
		q = q.AndEqual("receiver", acc.RowId)
	}
	if args.Height > 0 {
		q = q.AndEqual("height", args.Height)
	}

	events, err := ctx.Indexer.Table(model.TicketEventTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no ticket events table", err))
	}

	tickets, err := ctx.Indexer.Table(model.TicketTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no ticket table", err))
	}

	list, err := model.ListTicketEvents(ctx, events, q)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot list ticket events", err))
	}

	resp := make([]*TicketEvent, 0, len(list))
	for _, v := range list {
		if tick == nil || tick.Id != v.Ticket {
			tick, err = model.GetTicketId(ctx, tickets, v.Ticket)
			if err != nil {
				continue
			}
		}
		resp = append(resp, NewTicketEvent(ctx, v, tick))
	}
	return resp, http.StatusOK
}

type AccountTicketListRequest struct {
	ListRequest
	Ticketer  tezos.Address         `schema:"ticketer"`
	Type      tezos.HexBytes        `schema:"type"`
	Content   tezos.HexBytes        `schema:"content"`
	Hash      util.U64String        `schema:"hash"`
	EventType model.TicketEventType `schema:"event_type"`
	Sender    tezos.Address         `schema:"sender"`
	Receiver  tezos.Address         `schema:"receiver"`
	Height    int64                 `schema:"height"`
}

func (r AccountTicketListRequest) Load(ctx *server.Context) (*model.Ticket, error) {
	if len(r.Type) > 0 && len(r.Content) > 0 && r.Hash == 0 {
		var typ, content micheline.Prim
		if err := typ.UnmarshalBinary(r.Type.Bytes()); err != nil {
			return nil, server.EBadRequest(server.EC_PARAM_INVALID, "invalid type", err)
		}
		if err := content.UnmarshalBinary(r.Content.Bytes()); err != nil {
			return nil, server.EBadRequest(server.EC_PARAM_INVALID, "invalid content", err)
		}
		r.Hash = util.U64String(model.TicketHash(r.Ticketer, typ, content))
	}
	tickets, err := ctx.Indexer.Table(model.TicketTableKey)
	if err != nil {
		return nil, server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no ticket table", err)
	}
	tick, err := model.LookupTicket(ctx, tickets, r.Ticketer, r.Hash.U64())
	if err != nil {
		if errors.Is(err, model.ErrNoTicket) {
			return nil, server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such ticket", err)
		} else {
			return nil, server.EInternal(server.EC_DATABASE, err.Error(), nil)
		}
	}
	return tick, nil
}

func ListAccountTicketBalances(ctx *server.Context) (any, int) {
	var args AccountTicketListRequest
	ctx.ParseRequestArgs(&args)
	acc := loadAccount(ctx)

	q := pack.NewQuery("api.list_account_ticket_balances").
		WithOrder(args.Order).
		WithLimit(int(ctx.Cfg.ClampExplore(args.Limit))).
		WithOffset(int(args.Offset)).
		AndEqual("account", acc.RowId)

	if args.Cursor > 0 {
		q = q.And("id", args.Mode(), args.Cursor)
	}

	var tick *model.Ticket
	if args.Ticketer.IsValid() {
		tick, err := args.Load(ctx)
		if err == nil {
			q = q.AndEqual("ticket", tick.Id)
		}
	}

	owners, err := ctx.Indexer.Table(model.TicketOwnerTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no ticket owners table", err))
	}

	tickets, err := ctx.Indexer.Table(model.TicketTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no ticket table", err))
	}

	list, err := model.ListTicketOwners(ctx, owners, q)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot list ticket balances", err))
	}

	resp := make([]*TicketOwner, 0, len(list))
	for _, v := range list {
		if tick == nil || tick.Id != v.Ticket {
			tick, err = model.GetTicketId(ctx, tickets, v.Ticket)
			if err != nil {
				continue
			}
		}
		resp = append(resp, NewTicketOwner(ctx, v, tick))
	}
	return resp, http.StatusOK
}

func ListAccountTicketEvents(ctx *server.Context) (any, int) {
	var args AccountTicketListRequest
	ctx.ParseRequestArgs(&args)
	acc := loadAccount(ctx)

	q := pack.NewQuery("api.list_account_ticket_events").
		WithOrder(args.Order).
		WithLimit(int(ctx.Cfg.ClampExplore(args.Limit))).
		WithOffset(int(args.Offset)).
		OrCondition(
			pack.Equal("sender", acc.RowId),
			pack.Equal("receiver", acc.RowId),
		)
	if args.Cursor > 0 {
		q = q.And("id", args.Mode(), args.Cursor)
	}

	var tick *model.Ticket
	if args.Ticketer.IsValid() {
		tick, err := args.Load(ctx)
		if err == nil {
			q = q.AndEqual("ticket", tick.Id)
		}
	}

	if args.EventType.IsValid() {
		q = q.AndEqual("type", args.EventType)
	}
	if args.Sender.IsValid() {
		acc, err := ctx.Indexer.LookupAccount(ctx, args.Sender)
		if err != nil {
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such sender", err))
		}
		q = q.AndEqual("sender", acc.RowId)
	}
	if args.Receiver.IsValid() {
		acc, err := ctx.Indexer.LookupAccount(ctx, args.Receiver)
		if err != nil {
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such receiver", err))
		}
		q = q.AndEqual("receiver", acc.RowId)
	}
	if args.Height > 0 {
		q = q.AndEqual("height", args.Height)
	}

	events, err := ctx.Indexer.Table(model.TicketEventTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no ticket events table", err))
	}

	tickets, err := ctx.Indexer.Table(model.TicketTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no ticket table", err))
	}

	list, err := model.ListTicketEvents(ctx, events, q)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot list ticket events", err))
	}

	resp := make([]*TicketEvent, 0, len(list))
	for _, v := range list {
		if tick == nil {
			t2, err := model.GetTicketId(ctx, tickets, v.Ticket)
			if err != nil {
				continue
			}
			resp = append(resp, NewTicketEvent(ctx, v, t2))
		} else {
			resp = append(resp, NewTicketEvent(ctx, v, tick))
		}
	}
	return resp, http.StatusOK
}
