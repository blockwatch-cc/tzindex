// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package micheline

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"

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
	ac, err := ParseBigMapDiffAction(string(data))
	// default to update when empty
	if err != nil && len(data) > 0 {
		return err
	}
	*a = ac
	return nil
}

func ParseBigMapDiffAction(data string) (BigMapDiffAction, error) {
	switch data {
	case "update":
		return BigMapDiffActionUpdate, nil
	case "remove":
		return BigMapDiffActionRemove, nil
	case "copy":
		return BigMapDiffActionCopy, nil
	case "alloc":
		return BigMapDiffActionAlloc, nil
	default:
		return BigMapDiffActionUpdate, fmt.Errorf("micheline: invalid big_map_diff action '%s'", string(data))
	}
}

var BigMapRefType = &Prim{
	Type:   PrimNullary,
	OpCode: T_INT,
}

func NewBigMapRefType(anno string) *Prim {
	r := &Prim{
		Type:   PrimNullary,
		OpCode: T_INT,
	}
	if anno != "" {
		r.Anno = []string{"@" + anno}
	}
	return r
}

func NewBigMapRef(id int64) *Prim {
	return &Prim{
		Type: PrimInt,
		Int:  big.NewInt(id),
	}
}

type BigMapDiff []BigMapDiffElem

type BigMapDiffElem struct {
	Action    BigMapDiffAction
	Id        int64
	SourceId  int64
	DestId    int64
	KeyType   OpCode // can be all Michelson types, but mapped to bytes in updates
	KeyHash   chain.ExprHash
	IntKey    *big.Int
	StringKey string
	BytesKey  []byte
	Value     *Prim
	ValueType *Prim
}

func (e *BigMapDiffElem) Encoding() PrimType {
	return PrimTypeFromTypeCode(e.KeyType)
}

func (e *BigMapDiffElem) Key() *BigMapKey {
	k, err := NewBigMapKey(e.KeyType, e.IntKey, e.StringKey, e.BytesKey, e.Value)
	if err != nil {
		log.Error(err)
	}
	return k
}

func (e *BigMapDiffElem) KeyAs(typ OpCode) *BigMapKey {
	k, err := NewBigMapKey(typ, e.IntKey, e.StringKey, e.BytesKey, e.Value)
	if err != nil {
		log.Error(err)
	}
	return k
}

func (e *BigMapDiffElem) KeyString() string {
	switch true {
	case e.IntKey != nil:
		return e.IntKey.Text(10)
	case e.BytesKey != nil:
		return hex.EncodeToString(e.BytesKey)
	default:
		return e.StringKey
	}
}

func (e *BigMapDiffElem) KeyBytes() []byte {
	switch true {
	case e.IntKey != nil:
		var z Z
		z.Set(e.IntKey)
		k, _ := z.MarshalBinary()
		return k
	case e.BytesKey != nil:
		return e.BytesKey
	default:
		return []byte(e.StringKey)
	}
}

func (e *BigMapDiffElem) DecodeKey(t PrimType, b []byte) error {
	switch t {
	case PrimString:
		e.StringKey = string(b)
	case PrimBytes:
		e.BytesKey = make([]byte, len(b))
		copy(e.BytesKey, b)
	case PrimInt:
		var z Z
		_ = z.UnmarshalBinary(b)
		e.IntKey = z.Big()
	}
	return nil
}

func (e *BigMapDiffElem) UnmarshalJSON(data []byte) error {
	var val struct {
		Id        int64             `json:"big_map,string"`
		Action    BigMapDiffAction  `json:"action"`
		KeyType   *Prim             `json:"key_type"`                   // alloc
		ValueType *Prim             `json:"value_type"`                 // alloc
		Key       map[string]string `json:"key"`                        // update/remove
		KeyHash   chain.ExprHash    `json:"key_hash"`                   // update/remove
		Value     *Prim             `json:"value"`                      // update
		SourceId  int64             `json:"source_big_map,string"`      // copy
		DestId    int64             `json:"destination_big_map,string"` // copy
	}
	err := json.Unmarshal(data, &val)
	if err != nil {
		return err
	}

	// unpack key type
	switch val.Action {
	case BigMapDiffActionUpdate, BigMapDiffActionRemove:
		for n, v := range val.Key {
			switch n {
			case "int":
				e.KeyType = T_INT
				e.IntKey = big.NewInt(0)
				if err := e.IntKey.UnmarshalText([]byte(v)); err != nil {
					return fmt.Errorf("micheline: decoding bigmap int key '%s': %v", v, err)
				}
			case "bytes":
				e.KeyType = T_BYTES
				e.BytesKey, err = hex.DecodeString(v)
				if err != nil {
					return fmt.Errorf("micheline: decoding bigmap bytes key '%s': %v", v, err)
				}
			case "string":
				e.KeyType = T_STRING
				e.StringKey = v
			default:
				return fmt.Errorf("micheline: unsupported bigmap key type %s", n)
			}
		}
		e.KeyHash = val.KeyHash
		e.Value = val.Value

	case BigMapDiffActionAlloc:
		e.KeyType = val.KeyType.OpCode
		if !e.KeyType.IsValid() {
			return fmt.Errorf("micheline: unsupported bigmap key type (opcode) %s [%d]",
				val.KeyType.OpCode, val.KeyType.OpCode)
		}
		e.ValueType = val.ValueType

	case BigMapDiffActionCopy:
		e.SourceId = val.SourceId
		e.DestId = val.DestId
	}

	// assign remaining values
	e.Id = val.Id
	e.Action = val.Action

	// pre-v005: set correct action on value deletion (missing value in JSON)
	if e.Value == nil && e.Action == BigMapDiffActionUpdate {
		e.Action = BigMapDiffActionRemove
	}

	return nil
}

func (e BigMapDiffElem) MarshalJSON() ([]byte, error) {
	var res interface{}
	switch e.Action {
	case BigMapDiffActionUpdate, BigMapDiffActionRemove:
		// set key, keyhash, value
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
		case T_INT:
			val.Key["int"] = e.IntKey.Text(10)
		case T_BYTES:
			val.Key["bytes"] = hex.EncodeToString(e.BytesKey)
		case T_STRING:
			val.Key["string"] = e.StringKey
		}

		// be API compatible
		if e.Action == BigMapDiffActionRemove {
			val.Value = nil
		}
		res = val

	case BigMapDiffActionAlloc:
		// set keytype, valuetype
		res = struct {
			Id        int64            `json:"big_map,string"`
			Action    BigMapDiffAction `json:"action"`
			KeyType   *Prim            `json:"key_type"`   // alloc, copy only
			ValueType *Prim            `json:"value_type"` // alloc, copy only
		}{
			Id:     e.Id,
			Action: e.Action,
			KeyType: &Prim{
				Type:   PrimNullary,
				OpCode: e.KeyType,
			},
			ValueType: e.ValueType,
		}

	case BigMapDiffActionCopy:
		res = struct {
			Action   BigMapDiffAction `json:"action"`
			SourceId int64            `json:"source_big_map,string"`      // copy
			DestId   int64            `json:"destination_big_map,string"` // copy
		}{
			Action:   e.Action,
			SourceId: e.SourceId,
			DestId:   e.DestId,
		}
	}
	return json.Marshal(res)
}

func (b BigMapDiff) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	for _, v := range b {
		// prefix with id (4 byte) and action (1 byte)
		binary.Write(buf, binary.BigEndian, uint32(v.Id))
		buf.WriteByte(byte(v.Action))

		// encoding depends on action
		switch v.Action {
		case BigMapDiffActionUpdate, BigMapDiffActionRemove:
			// pair(pair(key_type, hash), value)
			key := &Prim{
				Type:   PrimBinary,
				OpCode: T_PAIR,
				Args: []*Prim{
					&Prim{
						Type:   PrimTypeFromTypeCode(v.KeyType),
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
				// BigMapDiffActionRemove
				val = &Prim{
					Type:   PrimNullary,
					OpCode: D_NONE,
				}
			}
			kvpair := &Prim{
				Type:   PrimBinary,
				OpCode: T_PAIR,
				Args:   []*Prim{key, val},
			}
			if err := kvpair.EncodeBuffer(buf); err != nil {
				return nil, err
			}

		case BigMapDiffActionAlloc:
			// pair(key_type, value_type)
			keytype := &Prim{
				Type:   PrimNullary,
				OpCode: v.KeyType,
			}
			kvpair := &Prim{
				Type:   PrimBinary,
				OpCode: T_PAIR,
				Args:   []*Prim{keytype, v.ValueType},
			}
			if err := kvpair.EncodeBuffer(buf); err != nil {
				return nil, err
			}

		case BigMapDiffActionCopy:
			// pair(src, dest)
			kvpair := &Prim{
				Type:   PrimBinary,
				OpCode: T_PAIR,
				Args: []*Prim{
					&Prim{
						Type: PrimInt,
						Int:  big.NewInt(v.SourceId),
					},
					&Prim{
						Type: PrimInt,
						Int:  big.NewInt(v.DestId),
					},
				},
			}
			if err := kvpair.EncodeBuffer(buf); err != nil {
				return nil, err
			}
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
		if prim.Type != PrimBinary && prim.OpCode != T_PAIR {
			return fmt.Errorf("micheline: unexpected big_map_diff keypair type %s opcode %s", prim.Type, prim.OpCode)
		}
		if l := len(prim.Args); l != 2 {
			return fmt.Errorf("micheline: unexpected big_map_diff keypair len %d", l)
		}
		// assign value based on action
		switch elem.Action {
		case BigMapDiffActionUpdate, BigMapDiffActionRemove:
			// encoded: pair(pair(key,hash), val)
			if prim.Args[0].Args[1].Type != PrimBytes {
				return fmt.Errorf("micheline: unexpected big_map_diff keyhash type %s", prim.Args[0].Args[1].Type)
			}
			if err := elem.KeyHash.UnmarshalBinary(prim.Args[0].Args[1].Bytes); err != nil {
				return err
			}
			elem.KeyType = prim.Args[0].Args[0].Type.TypeCode()
			elem.IntKey = prim.Args[0].Args[0].Int
			elem.BytesKey = prim.Args[0].Args[0].Bytes
			elem.StringKey = prim.Args[0].Args[0].String
			elem.Value = prim.Args[1]
			if elem.Action == BigMapDiffActionRemove {
				elem.Value = nil
			}
		case BigMapDiffActionAlloc:
			// encoded: pair(key_type, value_type)
			elem.KeyType = prim.Args[0].OpCode
			elem.ValueType = prim.Args[1]
			if !elem.KeyType.IsValid() {
				return fmt.Errorf("micheline: invalid big_map_diff key type opcode %s [%d]",
					prim.Args[0].OpCode, prim.Args[0].OpCode)
			}
		case BigMapDiffActionCopy:
			// encoded: pair(src_id, dest_id)
			elem.SourceId = prim.Args[0].Int.Int64()
			elem.DestId = prim.Args[1].Int.Int64()
		}
		*b = append(*b, elem)
	}
	return nil
}

func (e BigMapDiffElem) Dump() string {
	switch e.Action {
	case BigMapDiffActionAlloc:
		return fmt.Sprintf("BigMap action=%s keytype=%s (%s)",
			e.Action, e.KeyType, PrimTypeFromTypeCode(e.KeyType))
	case BigMapDiffActionCopy:
		return fmt.Sprintf("BigMap action=%s src=%d dst=%d", e.Action, e.SourceId, e.DestId)
	default:
		var key string
		switch e.KeyType {
		case T_INT:
			key = e.IntKey.Text(10)
		case T_STRING:
			key = e.StringKey
		case T_BYTES:
			key = hex.EncodeToString(e.BytesKey)
		}
		return fmt.Sprintf("BigMap action=%s keytype=%s (%s) key=%s hash=%s",
			e.Action, e.KeyType, PrimTypeFromTypeCode(e.KeyType), key, e.KeyHash)
	}
}
