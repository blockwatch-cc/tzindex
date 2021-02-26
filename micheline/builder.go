// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package micheline

import (
	"math/big"
)

func code(c OpCode, args ...*Prim) *Prim {
	typ := PrimNullary
	switch len(args) {
	case 0:
		typ = PrimNullary
	case 1:
		typ = PrimUnary
	case 2:
		typ = PrimBinary
	default:
		typ = PrimVariadicAnno
	}
	return &Prim{Type: typ, OpCode: c, Args: args}
}

func code_anno(c OpCode, anno string, args ...*Prim) *Prim {
	p := code(c, args...)
	p.Anno = []string{anno}
	if p.Type != PrimVariadicAnno {
		p.Type++
	}
	return p
}

func seq(args ...*Prim) *Prim {
	return &Prim{Type: PrimSequence, Args: args}
}

func i64(i int64) *Prim {
	return ibig(big.NewInt(i))
}

func ibig(i *big.Int) *Prim {
	return &Prim{Type: PrimInt, Int: i}
}

func pbytes(b []byte) *Prim {
	return &Prim{Type: PrimBytes, Bytes: b}
}

func pstring(s string) *Prim {
	return &Prim{Type: PrimString, String: s}
}

func tpair(l, r *Prim, anno ...string) *Prim {
	typ := PrimBinary
	if len(anno) > 0 {
		typ = PrimBinaryAnno
	}
	return &Prim{Type: typ, OpCode: T_PAIR, Args: []*Prim{l, r}, Anno: anno}
}

func dpair(l, r *Prim, anno ...string) *Prim {
	typ := PrimBinary
	if len(anno) > 0 {
		typ = PrimBinaryAnno
	}
	return &Prim{Type: typ, OpCode: D_PAIR, Args: []*Prim{l, r}, Anno: anno}
}

func prim(c OpCode, anno ...string) *Prim {
	typ := PrimNullary
	if len(anno) > 0 {
		typ = PrimNullaryAnno
	}
	return &Prim{Type: typ, OpCode: c, Anno: anno}
}

// Macros
func ASSERT_CMPEQ() *Prim {
	return seq(
		seq(code(I_COMPARE), code(I_EQ)),
		code(I_IF, seq(), seq(seq(code(I_UNIT), code(I_FAILWITH)))),
	)
}

func DUUP() *Prim {
	return seq(code(I_DIP, seq(code(I_DUP)), code(I_SWAP)))
}

func IFCMPNEQ(left, right *Prim) *Prim {
	return seq(
		code(I_COMPARE),
		code(I_EQ),
		code(I_IF, left, right),
	)
}

func UNPAIR() *Prim {
	return seq(seq(
		code(I_DUP),
		code(I_CAR),
		code(I_DIP, seq(code(I_CDR))),
	))
}
