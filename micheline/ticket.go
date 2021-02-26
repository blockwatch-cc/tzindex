// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package micheline

func TicketType(t *Prim) *Prim {
	tt := t.Clone()
	if len(tt.Anno) == 0 {
		tt.Anno = []string{":value"}
	}
	return tpair(
		prim(T_ADDRESS, ":ticketer"),
		tpair(
			tt,
			prim(T_INT, ":amount"),
		),
	)
}
