// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

const (
	TokenTableKey = "token"
)

var (
	ErrNoToken = errors.New("token not indexed")

	tokenPool = &sync.Pool{
		New: func() interface{} { return new(Token) },
	}
)

type TokenID uint64

func (i TokenID) U64() uint64 {
	return uint64(i)
}

// Token tracks token state and stats
type Token struct {
	Id           TokenID   `pack:"I,pk"      json:"row_id"`
	Ledger       AccountID `pack:"l,bloom=3" json:"ledger"`
	TokenId      tezos.Z   `pack:"i,snappy"  json:"token_id"`
	TokenId64    int64     `pack:"6"         json:"token_id64"`
	Creator      AccountID `pack:"C"         json:"creator"`
	Type         TokenType `pack:"Y"         json:"type"`
	FirstBlock   int64     `pack:"<"         json:"first_block"`
	FirstTime    time.Time `pack:"T"         json:"first_time"`
	LastBlock    int64     `pack:">"         json:"last_block"`
	LastTime     time.Time `pack:"t"         json:"last_time"`
	Supply       tezos.Z   `pack:"S,snappy"  json:"total_supply"`
	TotalMint    tezos.Z   `pack:"m,snappy"  json:"total_mint"`
	TotalBurn    tezos.Z   `pack:"b,snappy"  json:"total_burn"`
	NumTransfers int       `pack:"x,i32"     json:"num_transfers"`
	NumHolders   int       `pack:"y,i32"     json:"num_holders"`
}

// Ensure Token items implement the pack.Item interface.
var _ pack.Item = (*Token)(nil)

func (m *Token) ID() uint64 {
	return uint64(m.Id)
}

func (m *Token) SetID(id uint64) {
	m.Id = TokenID(id)
}

func NewToken() *Token {
	return tokenPool.Get().(*Token)
}

func (m *Token) Reset() {
	*m = Token{}
}

func (m *Token) Free() {
	m.Reset()
	tokenPool.Put(m)
}

func (m Token) TableKey() string {
	return TokenTableKey
}

func (m Token) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    11,  // 2k pack size
		JournalSizeLog2: 11,  // 2k journal size
		CacheSize:       16,  // max MB
		FillLevel:       100, // boltdb fill level to limit reallocations
	}
}

func (m Token) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

func GetToken(ctx context.Context, t *pack.Table, ledger *Contract, id tezos.Z) (*Token, error) {
	tk := NewToken()
	err := pack.NewQuery("find_token").
		WithTable(t).
		AndEqual("ledger", ledger.AccountId).
		AndEqual("token_id64", id.Int64()).
		AndEqual("token_id", id).
		Execute(ctx, tk)
	if err != nil {
		tk.Free()
		return nil, err
	}
	if tk.Id == 0 {
		tk.Free()
		return nil, ErrNoToken
	}
	return tk, nil
}

func GetOrCreateToken(ctx context.Context, t *pack.Table, ledger *Contract, signer *Account, id tezos.Z, height int64, tm time.Time) (*Token, error) {
	tk := NewToken()
	err := pack.NewQuery("find_token").
		WithTable(t).
		AndEqual("ledger", ledger.AccountId).
		AndEqual("token_id64", id.Int64()).
		AndEqual("token_id", id).
		Execute(ctx, tk)
	if err != nil {
		tk.Free()
		return nil, err
	}
	if tk.Id == 0 {
		tk.Ledger = ledger.AccountId
		tk.TokenId = id
		tk.TokenId64 = id.Int64()
		tk.Creator = signer.RowId
		tk.Type = ledger.LedgerType
		tk.FirstBlock = height
		tk.FirstTime = tm
		if err := t.Insert(ctx, tk); err != nil {
			tk.Free()
			return nil, err
		}
		// ctx.Log.Debugf("creating new %s token %s_%s => T_%d",
		// tk.Type, ledger.Address, tk.TokenId, tk.Id)
	}
	return tk, nil
}

func FindTokenId(ctx context.Context, t *pack.Table, id TokenID) (*Token, error) {
	c := NewToken()
	err := pack.NewQuery("find_id").
		WithTable(t).
		AndEqual("row_id", id).
		Execute(ctx, c)
	if err != nil {
		c.Free()
		return nil, err
	}
	return c, nil
}

// FA1, FA1.2, FA2
type TokenType byte

const (
	TokenTypeInvalid TokenType = iota
	TokenTypeFA1
	TokenTypeFA1_2
	TokenTypeFA2
)

var (
	tokenTypeStrings = map[TokenType]string{
		TokenTypeFA1:   "fa1",
		TokenTypeFA1_2: "fa1.2",
		TokenTypeFA2:   "fa2",
	}
	tokenTypeReverseStrings = map[string]TokenType{}
)

func init() {
	for n, v := range tokenTypeStrings {
		tokenTypeReverseStrings[v] = n
	}
}

func (t TokenType) IsValid() bool {
	return t != TokenTypeInvalid
}

func (t TokenType) String() string {
	return tokenTypeStrings[t]
}

func ParseTokenType(s string) TokenType {
	return tokenTypeReverseStrings[s]
}

func (t *TokenType) UnmarshalText(data []byte) error {
	v := ParseTokenType(string(data))
	if !v.IsValid() {
		return fmt.Errorf("invalid token type %q", string(data))
	}
	*t = v
	return nil
}

func (t TokenType) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func DetectTokenType(s micheline.Script) TokenType {
	switch {
	case IsFA2(s):
		return TokenTypeFA2
	case IsFA12(s):
		return TokenTypeFA1_2
	case IsFA1(s):
		return TokenTypeFA1
	default:
		return TokenTypeInvalid
	}
}

func IsFA1(s micheline.Script) bool {
	return s.Implements(micheline.ITzip5)
}

func IsFA12(s micheline.Script) bool {
	return s.Implements(micheline.ITzip7)
}

func IsFA2(s micheline.Script) bool {
	return s.Implements(micheline.ITzip12)
}

var tokenMatch = map[TokenType]micheline.Type{
	TokenTypeFA1:   micheline.ITzip5.TypeOf("transfer"),
	TokenTypeFA1_2: micheline.ITzip7.TypeOf("transfer"),
	TokenTypeFA2:   micheline.ITzip12.TypeOf("transfer"),
}

func (t TokenType) Matches(prim micheline.Prim) bool {
	return prim.Implements(tokenMatch[t])
}

func (t TokenType) DecodeTransfers(prim micheline.Prim) ([]TokenTransfer, error) {
	xfs := TransferList{typ: t}
	err := prim.Decode(&xfs)
	return xfs.Transfers, err
}

type TokenTransfer struct {
	From    tezos.Address
	To      tezos.Address
	TokenId tezos.Z
	Amount  tezos.Z
}

func (t TokenTransfer) IsValid() bool {
	return t.From.IsValid() && t.To.IsValid() && !t.Amount.IsZero()
}

type TransferList struct {
	Transfers []TokenTransfer
	typ       TokenType
}

func (t *TransferList) UnmarshalPrim(prim micheline.Prim) error {
	switch t.typ {
	case TokenTypeFA1:
		return prim.Decode((*FA1Transfer)(t))
	case TokenTypeFA1_2:
		return prim.Decode((*FA1Transfer)(t))
	case TokenTypeFA2:
		return prim.Decode((*FA2Transfer)(t))
	default:
		return fmt.Errorf("unsupported token type %s %d", t.typ, t.typ)
	}
}

type FA1Transfer TransferList

func (t *FA1Transfer) UnmarshalPrim(prim micheline.Prim) error {
	switch {
	case len(prim.Args) == 0:
		return nil
	case PrimMatches(prim, fa1RightPairParams):
		return t.unmarshalRightPair(prim)
	case PrimMatches(prim, fa1LeftPairParams):
		return t.unmarshalLeftPair(prim)
	case PrimMatches(prim, fa1ListParams):
		return t.unmarshalList(prim)
	}
	return fmt.Errorf("unsupported fa1 transfer value %s", prim.Dump())
}

// simple match that just looks if a path exists (no type check)
var (
	fa1LeftPairParams = [][]int{
		{0, 0}, // from
		{0, 1}, // to
		{1},    // value
	}
	fa1RightPairParams = [][]int{
		{0},    // from
		{1, 0}, // to
		{1, 1}, // value
	}
	fa1ListParams = [][]int{
		{0}, // from
		{1}, // to
		{2}, // value
	}
)

func (t *FA1Transfer) unmarshalRightPair(prim micheline.Prim) error {
	var xfer struct {
		From   tezos.Address `prim:"from,path=0"`
		To     tezos.Address `prim:"to,path=1/0"`
		Amount tezos.Z       `prim:"value,path=1/1"`
	}
	err := prim.Decode(&xfer)
	if err == nil {
		t.Transfers = []TokenTransfer{
			{
				From:   xfer.From,
				To:     xfer.To,
				Amount: xfer.Amount,
			},
		}
	}
	return err
}

// very unusual, but seems to ge legit
func (t *FA1Transfer) unmarshalLeftPair(prim micheline.Prim) error {
	var xfer struct {
		From   tezos.Address `prim:"from,path=0/0"`
		To     tezos.Address `prim:"to,path=0/1"`
		Amount tezos.Z       `prim:"value,path=1"`
	}
	err := prim.Decode(&xfer)
	if err == nil {
		t.Transfers = []TokenTransfer{
			{
				From:   xfer.From,
				To:     xfer.To,
				Amount: xfer.Amount,
			},
		}
	}
	return err
}

func (t *FA1Transfer) unmarshalList(prim micheline.Prim) error {
	var xfer struct {
		From   tezos.Address `prim:"from,path=0"`
		To     tezos.Address `prim:"to,path=1"`
		Amount tezos.Z       `prim:"value,path=2"`
	}
	err := prim.Decode(&xfer)
	if err == nil {
		t.Transfers = []TokenTransfer{
			{
				From:   xfer.From,
				To:     xfer.To,
				Amount: xfer.Amount,
			},
		}
	}
	return err
}

type FA2Transfer TransferList

func (t *FA2Transfer) UnmarshalPrim(prim micheline.Prim) error {
	// find the first non-empty txs list item (some calls start with an empty list!)
	var x int
	for i, v := range prim.Args {
		x = i
		if len(v.Args) > 0 && len(v.Args[1].Args) > 0 {
			break // ok
		}
	}
	if PrimMatches(prim, [][]int{
		{x, 1, 0, 1, 0}, // [$x].txs[$y].token_id
		{x, 1, 0, 1, 1}, // [$x].txs[$y].amount
	}) {
		return t.unmarshalNestedPrim(prim)
	}
	return t.unmarshalListPrim(prim)
}

func (t *FA2Transfer) unmarshalNestedPrim(prim micheline.Prim) error {
	var xfer struct {
		Transfers []struct {
			From tezos.Address `prim:"from_,path=0"`
			Txs  []struct {
				To      tezos.Address `prim:"to_,path=0"`
				TokenId tezos.Z       `prim:"token_id,path=1/0"`
				Amount  tezos.Z       `prim:"amount,path=1/1"`
			} `prim:"txs,path=1"`
		} `prim:"transfer"`
	}
	err := prim.Decode(&xfer)
	if err == nil {
		t.Transfers = make([]TokenTransfer, 0)
		for i := range xfer.Transfers {
			for j := range xfer.Transfers[i].Txs {
				tx := TokenTransfer{
					From:    xfer.Transfers[i].From,
					To:      xfer.Transfers[i].Txs[j].To,
					TokenId: xfer.Transfers[i].Txs[j].TokenId,
					Amount:  xfer.Transfers[i].Txs[j].Amount,
				}
				t.Transfers = append(t.Transfers, tx)
			}
		}
	}
	return err
}

func (t *FA2Transfer) unmarshalListPrim(prim micheline.Prim) error {
	var xfer struct {
		Transfers []struct {
			From tezos.Address `prim:"from_,path=0"`
			Txs  []struct {
				To      tezos.Address `prim:"to_,path=0"`
				TokenId tezos.Z       `prim:"token_id,path=1"`
				Amount  tezos.Z       `prim:"amount,path=2"`
			} `prim:"txs,path=1"`
		} `prim:"transfer"`
	}
	err := prim.Decode(&xfer)
	if err == nil {
		t.Transfers = make([]TokenTransfer, 0)
		for i := range xfer.Transfers {
			for j := range xfer.Transfers[i].Txs {
				tx := TokenTransfer{
					From:    xfer.Transfers[i].From,
					To:      xfer.Transfers[i].Txs[j].To,
					TokenId: xfer.Transfers[i].Txs[j].TokenId,
					Amount:  xfer.Transfers[i].Txs[j].Amount,
				}
				t.Transfers = append(t.Transfers, tx)
			}
		}
	}
	return err
}

func PrimMatches(prim micheline.Prim, template [][]int) bool {
	for _, path := range template {
		if !prim.HasIndex(path) {
			return false
		}
	}
	return true
}
