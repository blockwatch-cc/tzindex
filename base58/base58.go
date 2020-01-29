// Copyright (c) 2020 Blockwatch Data Inc.
// Copyright (c) 2013-2015 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package base58

import (
	"math/big"
	"sync"
)

//go:generate go run genalphabet.go

var bigIntPool = &sync.Pool{
	New: func() interface{} { return big.NewInt(0) },
}

var bigRadix = big.NewInt(58)
var bigZero = big.NewInt(0)

// Decode decodes a modified base58 string to a byte slice.
func Decode(b string, buf []byte) []byte {
	answerIf := bigIntPool.Get()
	jIf := bigIntPool.Get()
	scratchIf := bigIntPool.Get()

	answer := answerIf.(*big.Int).SetInt64(0)
	j := jIf.(*big.Int).SetInt64(1)

	scratch := scratchIf.(*big.Int).SetInt64(0)
	for i := len(b) - 1; i >= 0; i-- {
		tmp := b58[b[i]]
		if tmp == 255 {
			bigIntPool.Put(answer)
			bigIntPool.Put(j)
			bigIntPool.Put(scratch)
			return []byte("")
		}
		scratch.SetInt64(int64(tmp))
		scratch.Mul(j, scratch)
		answer.Add(answer, scratch)
		j.Mul(j, bigRadix)
	}

	tmpval := answer.Bytes()

	var numZeros int
	for numZeros = 0; numZeros < len(b); numZeros++ {
		if b[numZeros] != alphabetIdx0 {
			break
		}
	}
	flen := numZeros + len(tmpval)
	if buf == nil || cap(buf) < flen {
		buf = make([]byte, flen)
	}
	buf = buf[:flen]
	copy(buf[numZeros:], tmpval)
	bigIntPool.Put(answerIf)
	bigIntPool.Put(jIf)
	bigIntPool.Put(scratchIf)

	return buf
}

// Encode encodes a byte slice to a modified base58 string.
func Encode(b []byte) string {
	x := bigIntPool.Get().(*big.Int).SetBytes(b)

	answer := make([]byte, 0, len(b)*136/100)
	for x.Cmp(bigZero) > 0 {
		mod := bigIntPool.Get().(*big.Int).SetInt64(0)
		x.DivMod(x, bigRadix, mod)
		answer = append(answer, alphabet[mod.Int64()])
		bigIntPool.Put(mod)
	}
	bigIntPool.Put(x)

	// leading zero bytes
	for _, i := range b {
		if i != 0 {
			break
		}
		answer = append(answer, alphabetIdx0)
	}

	// reverse
	alen := len(answer)
	for i := 0; i < alen/2; i++ {
		answer[i], answer[alen-1-i] = answer[alen-1-i], answer[i]
	}

	return string(answer)
}
