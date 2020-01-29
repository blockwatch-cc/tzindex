// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// little-endian zarith encoding
// https://github.com/ocaml/Zarith

package micheline

import (
	"bytes"
	"io"
	"math/big"
)

type Bool byte

const (
	False Bool = 0x00
	True  Bool = 0xff
)

// A variable length sequence of bytes, encoding a Zarith number.
// Each byte has a running unary size bit: the most significant bit
// of each byte tells if this is the last byte in the sequence (0)
// or if there is more to read (1). The second most significant bit
// of the first byte is reserved for the sign (positive if zero).
// Size and sign bits ignored, data is then the binary representation
// of the absolute value of the number in little endian order.
//
type Z big.Int

func (z *Z) Big() *big.Int {
	return (*big.Int)(z)
}

func (z *Z) Int64() int64 {
	return (*big.Int)(z).Int64()
}

func (z *Z) Set(b *big.Int) *Z {
	(*big.Int)(z).Set(b)
	return z
}

func (z *Z) SetInt64(i int64) *Z {
	(*big.Int)(z).SetInt64(i)
	return z
}

func (z *Z) UnmarshalBinary(data []byte) error {
	return z.DecodeBuffer(bytes.NewBuffer(data))
}

func (z *Z) DecodeBuffer(buf *bytes.Buffer) error {
	var (
		s uint     = 6
		y *big.Int = big.NewInt(0)
	)
	b := buf.Next(1)
	if len(b) == 0 {
		return io.ErrShortBuffer
	}
	x := big.NewInt(int64(b[0] & 0x3f)) // clip two bits
	sign := b[0]&0x40 > 0
	if b[0] >= 0x80 {
		for i := 1; ; i++ {
			b = buf.Next(1)
			if len(b) == 0 {
				return io.ErrShortBuffer
			}
			if b[0] < 0x80 {
				y.SetInt64(int64(b[0]))
				x = x.Or(x, y.Lsh(y, s))
				break
			}
			y.SetInt64(int64(b[0] & 0x7f))
			x = x.Or(x, y.Lsh(y, s))
			s += 7
		}
	}
	if sign {
		(*big.Int)(z).Set(x.Neg(x))
	} else {
		(*big.Int)(z).Set(x)
	}
	return nil
}

func (z *Z) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if err := z.EncodeBuffer(buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (z *Z) EncodeBuffer(buf *bytes.Buffer) error {
	x := big.NewInt(0).Set(z.Big())
	var sign byte
	mask := big.NewInt(0x3f)
	y := big.NewInt(0)
	if x.Sign() < 0 {
		sign = 0x40
		x.Neg(x)
	}
	if x.IsInt64() && x.Int64() < 0x20 {
		buf.WriteByte(byte(x.Int64()) | sign)
		return nil
	} else {
		buf.WriteByte(byte(y.And(x, mask).Int64()) | 0x80 | sign)
		x.Rsh(x, 6)
	}
	mask.SetInt64(0x7f)
	for !x.IsInt64() || x.Int64() >= 0x80 {
		buf.WriteByte(byte(y.And(x, mask).Int64()) | 0x80)
		x.Rsh(x, 7)
	}
	buf.WriteByte(byte(x.Int64()))
	return nil
}
