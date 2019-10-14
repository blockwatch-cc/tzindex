// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package micheline

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	"blockwatch.cc/tzindex/chain"
)

type BigMapDiffAction byte

const (
	BigMapDiffActionUpdate BigMapDiffAction = iota
	BigMapDiffActionRemove
	BigMapDiffActionCopy
	BigMapDiffActionAlloc
)

func (a BigMapDiffAction) String() string {
	switch a {
	case BigMapDiffActionUpdate:
		return "update"
	case BigMapDiffActionRemove:
		return "remove"
	case BigMapDiffActionCopy:
		return "copy"
	case BigMapDiffActionAlloc:
		return "alloc"
	}
	return ""
}

func (a BigMapDiffAction) MarshalText() ([]byte, error) {
	return []byte(a.String()), nil
}

func (a *BigMapDiffAction) UnmarshalText(data []byte) error {
	switch string(data) {
	case "update", "":
		*a = BigMapDiffActionUpdate
	case "remove":
		*a = BigMapDiffActionRemove
	case "copy":
		*a = BigMapDiffActionCopy
	case "alloc":
		*a = BigMapDiffActionAlloc
	}
	return nil
}

type BigMapDiff []BigMapDiffElem

type BigMapDiffElem struct {
	Action    BigMapDiffAction
	Id        int64
	KeyType   PrimType
	KeyHash   chain.ExprHash
	IntKey    int64
	StringKey string
	BytesKey  []byte
	Value     *Prim
}

func (e *BigMapDiffElem) UnmarshalJSON(data []byte) error {
	// read JSON struct
	var val struct {
		Id      int64             `json:"big_map,string"`
		Action  BigMapDiffAction  `json:"action"`
		Key     map[string]string `json:"key"`
		KeyHash chain.ExprHash    `json:"key_hash"`
		Value   *Prim             `json:"value"`
	}
	err := json.Unmarshal(data, &val)
	if err != nil {
		return err
	}

	// unpack key type
	for n, v := range val.Key {
		switch n {
		case "int":
			e.KeyType = PrimInt
			e.IntKey, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				fmt.Errorf("micheline: decoding bigmap int key '%s': %v", v, err)
			}
		case "bytes":
			e.KeyType = PrimBytes
			e.BytesKey, err = hex.DecodeString(v)
			if err != nil {
				fmt.Errorf("micheline: decoding bigmap bytes key '%s': %v", v, err)
			}
		case "string":
			e.KeyType = PrimString
			e.StringKey = v
		default:
			fmt.Errorf("micheline: unsupported bigmap key type %s", n)
		}
	}

	// assign remaining values
	e.Id = val.Id
	e.Action = val.Action
	e.KeyHash = val.KeyHash
	e.Value = val.Value

	// pre-v005: set correct action on value deletion (missing value in JSON)
	if e.Value == nil && e.Action == BigMapDiffActionUpdate {
		e.Action = BigMapDiffActionRemove
	}

	return nil
}

func (e BigMapDiffElem) MarshalJSON() ([]byte, error) {
	val := struct {
		Id      int64             `json:"big_map,string"`
		Action  BigMapDiffAction  `json:"action"`
		Key     map[string]string `json:"key"`
		KeyHash chain.ExprHash    `json:"key_hash"`
		Value   *Prim             `json:"value,omitempty"`
	}{
		Id:      e.Id,
		Action:  e.Action,
		Key:     make(map[string]string),
		KeyHash: e.KeyHash,
		Value:   e.Value,
	}
	switch e.KeyType {
	case PrimInt:
		val.Key["int"] = strconv.FormatInt(e.IntKey, 10)
	case PrimBytes:
		val.Key["bytes"] = hex.EncodeToString(e.BytesKey)
	case PrimString:
		val.Key["string"] = e.StringKey
	}

	// be API compatible
	if e.Action == BigMapDiffActionRemove {
		val.Value = nil
	}

	return json.Marshal(val)
}

func (b BigMapDiff) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	for _, v := range b {
		// prefix with id (4 byte) and action (1 byte)
		binary.Write(buf, binary.BigEndian, uint32(v.Id))
		buf.WriteByte(byte(v.Action))
		keypair := &Prim{
			Type:   PrimBinary,
			OpCode: T_PAIR,
			Args: []*Prim{
				&Prim{
					Type:   v.KeyType,
					Int:    v.IntKey,
					Bytes:  v.BytesKey,
					String: v.StringKey,
				},
				&Prim{
					Type:  PrimBytes,
					Bytes: v.KeyHash.Hash.Hash,
				},
			},
		}
		val := v.Value
		if val == nil {
			val = &Prim{
				Type:   PrimNullary,
				OpCode: D_NONE,
			}
		}
		kvpair := &Prim{
			Type:   PrimBinary,
			OpCode: T_PAIR,
			Args:   []*Prim{keypair, val},
		}
		if err := kvpair.EncodeBuffer(buf); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (b *BigMapDiff) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	// unpack elements
	for buf.Len() > 0 {
		elem := BigMapDiffElem{
			Id:     int64(binary.BigEndian.Uint32(buf.Next(4))),
			Action: BigMapDiffAction(buf.Next(1)[0]),
		}
		prim := &Prim{}
		if err := prim.DecodeBuffer(buf); err != nil {
			return err
		}
		// check key pair
		if prim.Args[0].Type != PrimBinary && prim.Args[0].OpCode != T_PAIR {
			return fmt.Errorf("micheline: unexpected big_map_diff key pair type %s opcode %s", prim.Args[0].Type, prim.Args[0].OpCode)
		}
		if l := len(prim.Args[0].Args); l != 2 {
			return fmt.Errorf("micheline: unexpected big_map_diff keypair len %d", l)
		}
		if prim.Args[0].Args[1].Type != PrimBytes {
			return fmt.Errorf("micheline: unexpected big_map_diff key hash type %s", prim.Args[0].Args[1].Type)
		}
		elem.KeyType = prim.Args[0].Args[0].Type
		elem.IntKey = prim.Args[0].Args[0].Int
		elem.BytesKey = prim.Args[0].Args[0].Bytes
		elem.StringKey = prim.Args[0].Args[0].String
		elem.Value = prim.Args[1]
		if err := elem.KeyHash.UnmarshalBinary(prim.Args[0].Args[1].Bytes); err != nil {
			return err
		}
		*b = append(*b, elem)
	}
	return nil
}

func (e BigMapDiffElem) Dump() string {
	var key string
	switch e.KeyType {
	case PrimInt:
		key = strconv.FormatInt(e.IntKey, 10)
	case PrimBytes:
		key = hex.EncodeToString(e.BytesKey)
	case PrimString:
		key = e.StringKey
	}
	return fmt.Sprintf("BigMap keytype=%s key=%s hash=%s", e.KeyType, key, e.KeyHash)
}
