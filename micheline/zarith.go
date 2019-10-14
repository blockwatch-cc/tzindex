// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

// little-endian zarith encoding
// https://github.com/ocaml/Zarith

package micheline

import (
	"bytes"
	"errors"
	"io"
)

var overflow = errors.New("micheline: zarith overflows a 64-bit integer")

type Bool byte

const (
	False Bool = 0x00
	True  Bool = 0xff
)

// A variable length sequence of bytes, encoding a Zarith number.
// Each byte has a running unary size bit: the most significant bit
// of each byte tells is this is the last byte in the sequence (0)
// or if there is more to read (1). The second most significant bit
// of the first byte is reserved for the sign (positive if zero).
// Size and sign bits ignored, data is then the binary representation
// of the absolute value of the number in little endian order.
//
type Z int64

func (z *Z) DecodeBuffer(buf *bytes.Buffer) error {
	var (
		x int64
		s uint = 6
	)
	b := buf.Next(1)
	if len(b) == 0 {
		return io.ErrShortBuffer
	}
	x = int64(b[0] & 0x3f) // clip two bits
	sign := b[0]&0x40 > 0
	if b[0] >= 0x80 {
		for i := 1; ; i++ {
			b = buf.Next(1)
			if len(b) == 0 {
				return io.ErrShortBuffer
			}
			if b[0] < 0x80 {
				if i > 9 || i == 9 && b[0] > 1 {
					return overflow
				}
				x = x | int64(b[0])<<s
				break
			}
			x |= int64(b[0]&0x7f) << s
			s += 7
		}
	}
	if sign {
		*z = Z(-x)
	} else {
		*z = Z(x)
	}
	return nil
}

func (z Z) EncodeBuffer(buf *bytes.Buffer) error {
	x := int64(z)
	var sign byte
	if x < 0 {
		sign = 0x40
		x = -x
	}
	if x < 0x20 {
		buf.WriteByte(byte(x) | sign)
		return nil
	} else {
		buf.WriteByte(byte(x)&0x3f | 0x80)
		x >>= 6
	}
	for x >= 0x80 {
		buf.WriteByte(byte(x) | 0x80)
		x >>= 7
	}
	buf.WriteByte(byte(x))
	return nil
}

// A variable length sequence of bytes, encoding a Zarith number.
// Each byte has a running unary size bit: the most significant bit
// of each byte tells is this is the last byte in the sequence (0)
// or if there is more to read (1). Size bits ignored, data is then
// the binary representation of the absolute value of the number in
// little endian order.
//
type N int64

func (n *N) DecodeBuffer(buf *bytes.Buffer) error {
	var (
		x int64
		s uint
	)
	for i := 0; ; i++ {
		b := buf.Next(1)
		if len(b) == 0 {
			return io.ErrShortBuffer
		}
		if b[0] < 0x80 {
			if i > 9 || i == 9 && b[0] > 1 {
				return overflow
			}
			x = x | int64(b[0])<<s
			break
		}
		x |= int64(b[0]&0x7f) << s
		s += 7
	}
	*n = N(x)
	return nil
}

func (n N) EncodeBuffer(buf *bytes.Buffer) error {
	x := int64(n)
	for x >= 0x80 {
		buf.WriteByte(byte(x) | 0x80)
		x >>= 7
	}
	buf.WriteByte(byte(x))
	return nil
}
