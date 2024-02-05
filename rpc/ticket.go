// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	m "blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

type Ticket struct {
	Ticketer tezos.Address `json:"ticketer"`
	Type     m.Prim        `json:"content_type"`
	Content  m.Prim        `json:"content"`
}

func (t Ticket) Hash() tezos.ExprHash {
	key := m.NewPair(
		m.NewBytes(t.Ticketer.EncodePadded()),
		m.NewPair(t.Type, t.Content),
	)
	buf, _ := key.MarshalBinary()
	return m.KeyHash(buf)
}

type TicketBalanceUpdate struct {
	Account tezos.Address `json:"account"`
	Amount  tezos.Z       `json:"amount"`
}

type TicketUpdate struct {
	Ticket  Ticket                `json:"ticket_token"`
	Updates []TicketBalanceUpdate `json:"updates"`
}

func (u TicketUpdate) Prim() m.Prim {
	p := m.NewCombPair(
		m.NewBytes(u.Ticket.Ticketer.EncodePadded()),
		u.Ticket.Type,
		u.Ticket.Content,
		m.NewSeq(),
	)
	for _, v := range u.Updates {
		p.Args[3].Args = append(p.Args[3].Args, m.NewPair(
			m.NewBytes(v.Account.EncodePadded()),
			m.NewNat(v.Amount.Big()),
		))
	}
	return p
}
