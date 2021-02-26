// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package micheline

import (
// "bytes"
// "encoding/binary"
// "encoding/hex"
// "encoding/json"
// "fmt"
// "math/big"
// "blockwatch.cc/tzindex-pro/chain"
)

type SaplingDiffElem struct {
	Action   DiffAction    `json:"action"`
	Updates  SaplingUpdate `json:"updates"`
	MemoSize int           `json:"memo_size"`
}

type SaplingUpdate struct {
	Commitments [][]byte     `json:"commitments"`
	Ciphertexts []Ciphertext `json:"ciphertexts"`
	Nullifiers  [][]byte     `json:"nullifiers"`
}

type Ciphertext struct {
	Cv         []byte
	Epk        []byte
	PayloadEnc []byte
	NonceEnc   []byte
	PayloadOut []byte
	NonceOut   []byte
}

// TODO
func (c Ciphertext) MarshalJSON() ([]byte, error) {
	return []byte{}, nil
}

func (c *Ciphertext) UnmarshalJSON(data []byte) error {
	return nil
}
