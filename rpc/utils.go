// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// BlockID is an interface to abstract different kinds of block addressing modes
type BlockID interface {
	fmt.Stringer
	Int64() int64
}

// BlockLevel is a block addressing mode that uses the blocks sequence number a.k.a level
type BlockLevel int64

func (b BlockLevel) String() string {
	return strconv.FormatInt(int64(b), 10)
}

func (b BlockLevel) Int64() int64 {
	return int64(b)
}

// BlockAlias is a block addressing mode that uses a constant string
type BlockAlias string

const (
	Genesis BlockAlias = "genesis"
	Head    BlockAlias = "head"
)

func (b BlockAlias) String() string {
	return string(b)
}

func (b BlockAlias) Int64() int64 {
	if b == Genesis {
		return 0
	}
	return -1
}

// BlockOffset is a block addressing mode that uses relative addressing from a given
// base block.
type BlockOffset struct {
	Base   BlockID
	Offset int64
}

func NewBlockOffset(id BlockID, n int64) BlockOffset {
	return BlockOffset{
		Base:   id,
		Offset: n,
	}
}

func (o BlockOffset) String() string {
	ref := o.Base.String()
	if o.Offset > 0 {
		ref += "+" + strconv.FormatInt(o.Offset, 10)
	} else if o.Offset < 0 {
		ref += strconv.FormatInt(o.Offset, 10)
	}
	return ref
}

func (b BlockOffset) Int64() int64 {
	base := b.Base.Int64()
	if base >= 0 {
		return base + b.Offset
	}
	return -1
}

func unmarshalMultiTypeJSONArray(data []byte, vals ...interface{}) (err error) {
	dec := json.NewDecoder(bytes.NewBuffer(data))

	// read open bracket
	_, err = dec.Token()
	if err != nil {
		return
	}

	// while the array contains values
	var i int
	for dec.More() {
		if i >= len(vals) {
			return fmt.Errorf("short JSON data")
		}
		err = dec.Decode(vals[i])
		if err != nil {
			return
		}
		i++
	}

	// read closing bracket
	_, err = dec.Token()
	return
}

func marshalMultiTypeJSONArray(vals ...interface{}) (data []byte, err error) {
	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	buf.WriteByte('[')
	for i, v := range vals {
		if i > 0 {
			buf.WriteByte(',')
		}
		err = enc.Encode(v)
		if err != nil {
			return
		}
	}
	buf.WriteByte(']')
	data = buf.Bytes()
	return
}
