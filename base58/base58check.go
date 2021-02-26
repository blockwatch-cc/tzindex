// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Copyright (c) 2013-2014 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package base58

import (
	"crypto/sha256"
	"errors"
	"sync"
)

var bufPool = &sync.Pool{
	New: func() interface{} { return make([]byte, 0, 96) },
}

// ErrChecksum indicates that the checksum of a check-encoded string does not verify against
// the checksum.
var ErrChecksum = errors.New("checksum error")

// ErrInvalidFormat indicates that the check-encoded string has an invalid format.
var ErrInvalidFormat = errors.New("invalid format: version and/or checksum bytes missing")

// checksum: first four bytes of sha256^2
func checksum(input []byte) (cksum [4]byte) {
	h := sha256.Sum256(input)
	h2 := sha256.Sum256(h[:])
	copy(cksum[:], h2[:4])
	return
}

// CheckEncode prepends a version byte and appends a four byte checksum.
func CheckEncode(input []byte, version []byte) string {
	bi := bufPool.Get()
	b := bi.([]byte)[:0]
	b = append(b, version[:]...)
	b = append(b, input[:]...)
	cksum := checksum(b)
	b = append(b, cksum[:]...)
	res := Encode(b)
	b = b[:0]
	bufPool.Put(bi)
	return res
}

// CheckDecode decodes a string that was encoded with CheckEncode and verifies the checksum.
// adapted to support multi-length version strings
func CheckDecode(input string, vlen int, buf []byte) ([]byte, []byte, error) {
	decoded := Decode(input, buf)
	if len(decoded) < 4+vlen {
		return nil, nil, ErrInvalidFormat
	}
	version := decoded[0:vlen]
	var cksum [4]byte
	copy(cksum[:], decoded[len(decoded)-4:])
	if checksum(decoded[:len(decoded)-4]) != cksum {
		return nil, nil, ErrChecksum
	}
	payload := decoded[vlen : len(decoded)-4]
	return payload, version, nil
}
