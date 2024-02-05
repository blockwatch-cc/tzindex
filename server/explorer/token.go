// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

func init() {
	server.Register(Token{})
}

var _ server.RESTful = (*Token)(nil)

type Token struct {
	Contract     tezos.Address   `json:"contract"`
	TokenId      tezos.Z         `json:"token_id"`
	Creator      tezos.Address   `json:"creator"`
	Type         model.TokenType `json:"type"`
	FirstBlock   int64           `json:"first_block"`
	FirstTime    time.Time       `json:"first_time"`
	LastBlock    int64           `json:"last_block"`
	LastTime     time.Time       `json:"last_time"`
	Supply       tezos.Z         `json:"total_supply"`
	TotalMint    tezos.Z         `json:"total_mint"`
	TotalBurn    tezos.Z         `json:"total_burn"`
	NumTransfers int             `json:"num_transfers"`
	NumHolders   int             `json:"num_holders"`
	Metadata     json.RawMessage `json:"metadata,omitempty"`
}

func NewToken(ctx *server.Context, tokn *model.Token) *Token {
	md, _ := lookupTokenIdMetadata(ctx, tokn.Id)
	return &Token{
		Contract:     ctx.Indexer.LookupAddress(ctx, tokn.Ledger),
		TokenId:      tokn.TokenId,
		Creator:      ctx.Indexer.LookupAddress(ctx, tokn.Creator),
		Type:         tokn.Type,
		FirstBlock:   tokn.FirstBlock,
		FirstTime:    tokn.FirstTime,
		LastBlock:    tokn.LastBlock,
		LastTime:     tokn.LastTime,
		Supply:       tokn.Supply,
		TotalMint:    tokn.TotalMint,
		TotalBurn:    tokn.TotalBurn,
		NumTransfers: tokn.NumTransfers,
		NumHolders:   tokn.NumHolders,
		Metadata:     md,
	}
}

func (t Token) LastModified() time.Time {
	return t.LastTime
}

func (t Token) Expires() time.Time {
	return time.Time{}
}

func (t Token) RESTPrefix() string {
	return "/explorer/token"
}

func (t Token) RESTPath(r *mux.Router) string {
	path, _ := r.Get("token").URLPath("ident", tezos.NewToken(t.Contract, t.TokenId).String())
	return path.String()
}

func (t Token) RegisterDirectRoutes(r *mux.Router) error {
	r.HandleFunc(t.RESTPrefix(), server.C(ListTokens)).Methods("GET")
	return nil
}

func (t Token) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", server.C(ReadToken)).Methods("GET").Name("token")
	r.HandleFunc("/{ident}/events", server.C(ListTokenEvents)).Methods("GET")
	r.HandleFunc("/{ident}/balances", server.C(ListTokenBalances)).Methods("GET")
	return nil
}

type TokenOwner struct {
	Account      tezos.Address   `json:"account"`
	Contract     tezos.Address   `json:"contract"`
	TokenId      tezos.Z         `json:"token_id"`
	Type         model.TokenType `json:"type"`
	FirstBlock   int64           `json:"first_block"`
	FirstTime    time.Time       `json:"first_time"`
	LastBlock    int64           `json:"last_block"`
	LastTime     time.Time       `json:"last_time"`
	NumTransfers int             `json:"num_transfers"`
	NumMints     int             `json:"num_mints"`
	NumBurns     int             `json:"num_burns"`
	VolSent      tezos.Z         `json:"vol_sent"`
	VolRecv      tezos.Z         `json:"vol_recv"`
	VolMint      tezos.Z         `json:"vol_mint"`
	VolBurn      tezos.Z         `json:"vol_burn"`
	Metadata     json.RawMessage `json:"metadata,omitempty"`
}

func NewTokenOwner(ctx *server.Context, ownr *model.TokenOwner, tokn *model.Token) *TokenOwner {
	md, _ := lookupTokenIdMetadata(ctx, ownr.Token)
	return &TokenOwner{
		Account:      ctx.Indexer.LookupAddress(ctx, ownr.Account),
		Contract:     ctx.Indexer.LookupAddress(ctx, ownr.Ledger),
		TokenId:      tokn.TokenId,
		Type:         tokn.Type,
		FirstBlock:   ownr.FirstBlock,
		FirstTime:    ctx.Indexer.LookupBlockTime(ctx, ownr.FirstBlock),
		LastBlock:    ownr.LastBlock,
		LastTime:     ctx.Indexer.LookupBlockTime(ctx, ownr.LastBlock),
		NumTransfers: ownr.NumTransfers,
		NumMints:     ownr.NumMints,
		NumBurns:     ownr.NumBurns,
		VolSent:      ownr.VolSent,
		VolRecv:      ownr.VolRecv,
		VolMint:      ownr.VolMint,
		VolBurn:      ownr.VolBurn,
		Metadata:     md,
	}
}

func (t TokenOwner) LastModified() time.Time {
	return t.LastTime
}

func (t TokenOwner) Expires() time.Time {
	return time.Time{}
}

type TokenEvent struct {
	Contract tezos.Address        `json:"contract"`
	TokenId  tezos.Z              `json:"token_id"`
	Type     model.TokenEventType `json:"type"`
	Signer   tezos.Address        `json:"signer"`
	Sender   tezos.Address        `json:"sender"`
	Receiver tezos.Address        `json:"receiver"`
	Amount   tezos.Z              `json:"amount"`
	Height   int64                `json:"height"`
	Time     time.Time            `json:"time"`
	OpId     model.OpID           `json:"op_id"`
}

func NewTokenEvent(ctx *server.Context, evnt *model.TokenEvent, tokn *model.Token) *TokenEvent {
	return &TokenEvent{
		Contract: ctx.Indexer.LookupAddress(ctx, evnt.Ledger),
		TokenId:  tokn.TokenId,
		Type:     evnt.Type,
		Signer:   ctx.Indexer.LookupAddress(ctx, evnt.Signer),
		Sender:   ctx.Indexer.LookupAddress(ctx, evnt.Sender),
		Receiver: ctx.Indexer.LookupAddress(ctx, evnt.Receiver),
		Amount:   evnt.Amount,
		Height:   evnt.Height,
		Time:     evnt.Time,
		OpId:     evnt.OpId,
	}
}

func (t TokenEvent) LastModified() time.Time {
	return t.Time
}

func (t TokenEvent) Expires() time.Time {
	return time.Time{}
}

func loadToken(ctx *server.Context) *model.Token {
	id, ok := mux.Vars(ctx.Request)["ident"]
	if !ok || id == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing token address", nil))
	}
	addr, err := tezos.ParseToken(id)
	if err != nil {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid token address", err))
	}
	acc, err := ctx.Indexer.LookupAccountId(ctx, addr.Contract())
	if err != nil {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "no such contract", err))
	}
	table, err := ctx.Indexer.Table(model.TokenTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access token table", err))
	}
	tokn := &model.Token{}
	err = pack.NewQuery("token.find").
		WithTable(table).
		AndEqual("ledger", acc).
		AndEqual("token_id64", addr.TokenId().Int64()).
		Execute(ctx, tokn)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
	}
	if tokn.Id == 0 {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such token", err))
	}
	return tokn
}

func loadTokenId(ctx *server.Context, id model.TokenID) *model.Token {
	table, err := ctx.Indexer.Table(model.TokenTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access token table", err))
	}
	tokn := &model.Token{}
	err = pack.NewQuery("token.find").
		WithTable(table).
		AndEqual("row_id", id).
		Execute(ctx, tokn)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
	}
	if tokn.Id == 0 {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such token", err))
	}
	return tokn
}

func ReadToken(ctx *server.Context) (interface{}, int) {
	tokn := loadToken(ctx)
	return NewToken(ctx, tokn), http.StatusOK
}

type TokenListRequest struct {
	ListRequest
	Contract tezos.Address   `schema:"contract"`
	Type     model.TokenType `schema:"type"`
}

func ListTokens(ctx *server.Context) (interface{}, int) {
	args := &TokenListRequest{}
	ctx.ParseRequestArgs(args)

	table, err := ctx.Indexer.Table(model.TokenTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access token table", err))
	}

	list := make([]*model.Token, 0)
	q := pack.NewQuery("token.list").
		WithTable(table).
		WithLimit(int(ctx.Cfg.ClampExplore(args.Limit))).
		WithOffset(int(args.Offset)).
		AndGt("row_id", args.Cursor)

	if args.Contract.IsValid() {
		id, err := ctx.Indexer.LookupAccountId(ctx, args.Contract)
		if err != nil {
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such contract", err))
		}
		q = q.AndEqual("ledger", id)
	}
	if args.Type.IsValid() {
		q = q.AndEqual("type", args.Type)
	}
	err = q.Execute(ctx, &list)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot list tokens", err))
	}

	resp := make([]*Token, 0, len(list))
	for _, v := range list {
		resp = append(resp, NewToken(ctx, v))
	}
	return resp, http.StatusOK
}

type TokenBalanceListRequest struct {
	ListRequest
	Contract tezos.Address `schema:"contract"`
	WithZero bool          `schema:"zero"`
}

func ListTokenBalances(ctx *server.Context) (interface{}, int) {
	args := &TokenBalanceListRequest{}
	ctx.ParseRequestArgs(args)
	tokn := loadToken(ctx)

	table, err := ctx.Indexer.Table(model.TokenOwnerTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access token owner table", err))
	}

	list := make([]*model.TokenOwner, 0)
	q := pack.NewQuery("token.list.owners").
		WithTable(table).
		AndEqual("token", tokn.Id).
		WithLimit(int(ctx.Cfg.ClampExplore(args.Limit))).
		WithOffset(int(args.Offset)).
		AndGt("row_id", args.Cursor)

	if !args.WithZero {
		q = q.AndNotEqual("balance", tezos.Zero)
	}

	err = q.Execute(ctx, &list)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot list tokens", err))
	}

	resp := make([]*TokenOwner, 0, len(list))
	for _, v := range list {
		resp = append(resp, NewTokenOwner(ctx, v, tokn))
	}
	return resp, http.StatusOK
}

type TokenEventListRequest struct {
	ListRequest
	Contract tezos.Address        `schema:"contract"`
	Type     model.TokenEventType `schema:"type"`
}

func ListTokenEvents(ctx *server.Context) (interface{}, int) {
	args := &TokenEventListRequest{}
	ctx.ParseRequestArgs(args)
	tokn := loadToken(ctx)

	table, err := ctx.Indexer.Table(model.TokenEventTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access token event table", err))
	}

	list := make([]*model.TokenEvent, 0)
	q := pack.NewQuery("token.list.events").
		WithTable(table).
		AndEqual("token", tokn.Id).
		WithLimit(int(ctx.Cfg.ClampExplore(args.Limit))).
		WithOffset(int(args.Offset)).
		AndGt("row_id", args.Cursor)

	if args.Contract.IsValid() {
		id, err := ctx.Indexer.LookupAccountId(ctx, args.Contract)
		if err != nil {
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such contract", err))
		}
		q = q.AndEqual("ledger", id)
	}
	if args.Type.IsValid() {
		q = q.AndEqual("type", args.Type)
	}

	err = q.Execute(ctx, &list)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot list token events", err))
	}

	resp := make([]*TokenEvent, 0, len(list))
	for _, v := range list {
		resp = append(resp, NewTokenEvent(ctx, v, tokn))
	}
	return resp, http.StatusOK
}

func ListAccountTokenBalances(ctx *server.Context) (interface{}, int) {
	args := &TokenBalanceListRequest{}
	ctx.ParseRequestArgs(args)
	acc := loadAccount(ctx)

	table, err := ctx.Indexer.Table(model.TokenOwnerTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access token owner table", err))
	}

	list := make([]*model.TokenOwner, 0)
	err = pack.NewQuery("token.list").
		WithTable(table).
		AndEqual("account", acc.RowId).
		WithLimit(int(ctx.Cfg.ClampExplore(args.Limit))).
		WithOffset(int(args.Offset)).
		AndGt("row_id", args.Cursor).
		Execute(ctx, &list)

	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot list token balances", err))
	}

	resp := make([]*TokenOwner, 0, len(list))
	for _, v := range list {
		tokn := loadTokenId(ctx, v.Token)
		resp = append(resp, NewTokenOwner(ctx, v, tokn))
	}
	return resp, http.StatusOK
}

func ListAccountTokenEvents(ctx *server.Context) (interface{}, int) {
	args := &TokenEventListRequest{}
	ctx.ParseRequestArgs(args)
	acc := loadAccount(ctx)

	table, err := ctx.Indexer.Table(model.TokenEventTableKey)
	if err != nil {
		panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access token event table", err))
	}

	list := make([]*model.TokenEvent, 0)
	q := pack.NewQuery("token.list").
		WithTable(table).
		OrCondition(
			pack.Equal("signer", acc.RowId),
			pack.Equal("sender", acc.RowId),
			pack.Equal("receiver", acc.RowId),
		).
		WithLimit(int(args.Limit)).
		WithOffset(int(args.Offset)).
		AndGt("row_id", args.Cursor)

	if args.Contract.IsValid() {
		id, err := ctx.Indexer.LookupAccountId(ctx, args.Contract)
		if err != nil {
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no such contract", err))
		}
		q = q.AndEqual("ledger", id)
	}
	if args.Type.IsValid() {
		q = q.AndEqual("type", args.Type)
	}

	err = q.Execute(ctx, &list)
	if err != nil {
		panic(server.EInternal(server.EC_DATABASE, "cannot list token events", err))
	}

	resp := make([]*TokenEvent, 0, len(list))
	for _, v := range list {
		tokn := loadTokenId(ctx, v.Token)
		resp = append(resp, NewTokenEvent(ctx, v, tokn))
	}
	return resp, http.StatusOK
}
