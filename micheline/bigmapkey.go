// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// see http://tezos.gitlab.io/whitedoc/michelson.html#full-grammar
//
// Domain specific data types
//
// - timestamp: Dates in the real world.
// - mutez: A specific type for manipulating tokens.
// - address: An untyped address (implicit account or smart contract).
// - contract 'param: A contract, with the type of its code,
//   contract unit for implicit accounts.
// - operation: An internal operation emitted by a contract.
// - key: A public cryptographic key.
// - key_hash: The hash of a public cryptographic key.
// - signature: A cryptographic signature.
// - chain_id: An identifier for a chain, used to distinguish the test
//   and the main chains.
//
// PACK prefixes with 0x05!
// So that when contracts checking signatures (multisigs etc) do the current
// best practice, PACK; ...; CHECK_SIGNATURE, the 0x05 byte distinguishes the
// message from blocks, endorsements, transactions, or tezos-signer authorization
// requests (0x01-0x04)

package micheline

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/tzindex/chain"
)

type BigMapKey struct {
	Type      *Prim
	Hash      chain.ExprHash
	IntKey    *big.Int
	StringKey string
	BytesKey  []byte
	BoolKey   bool
	AddrKey   chain.Address
	TimeKey   time.Time
	PrimKey   *Prim
}

func (k BigMapKey) IsPacked() bool {
	return k.Type.OpCode == T_BYTES && len(k.BytesKey) > 1 && k.BytesKey[0] == 0x5
}

func (k BigMapKey) UnpackPrim() (*Prim, error) {
	if !k.IsPacked() {
		return nil, fmt.Errorf("key is not packed")
	}
	p := &Prim{}
	if err := p.UnmarshalBinary(k.BytesKey[1:]); err != nil {
		return nil, err
	}
	return p, nil
}

func (k BigMapKey) UnpackKey() (*BigMapKey, error) {
	p, err := k.UnpackPrim()
	if err != nil {
		return nil, err
	}
	return &BigMapKey{
		Type:      p.BuildType(),
		IntKey:    p.Int,
		StringKey: p.String,
		BytesKey:  p.Bytes,
		PrimKey:   p,
	}, nil
}

func ParseBigMapKeyType(typ string) (OpCode, error) {
	t, err := ParseOpCode(typ)
	if err != nil {
		return t, fmt.Errorf("micheline: invalid big_map key type '%s'", typ)
	}
	switch t {
	case T_INT, T_NAT, T_MUTEZ, T_STRING, T_BYTES, T_BOOL, T_KEY_HASH, T_TIMESTAMP, T_ADDRESS, T_PAIR:
		return t, nil
	default:
		return t, fmt.Errorf("micheline: unsupported big_map key type string '%s'", t)
	}
}

// query string parsing used for lookup (does not support Pair keys)
func ParseBigMapKey(typ, val string) (*BigMapKey, error) {
	var err error
	key := &BigMapKey{Type: &Prim{}}
	// try parsing expr hash first
	key.Hash, err = chain.ParseExprHash(val)
	if err == nil {
		key.Type.OpCode = T_KEY
		key.Type.Type = PrimBytes
		return key, nil
	}
	if typ == "" {
		key.Type.OpCode = InferBigMapKeyType(val)
	} else {
		key.Type.OpCode, err = ParseOpCode(typ)
		if err != nil {
			return nil, err
		}
	}
	switch key.Type.OpCode {
	case T_INT, T_NAT, T_MUTEZ:
		key.Type.Type = PrimInt
		key.IntKey = big.NewInt(0)
		if err := key.IntKey.UnmarshalText([]byte(val)); err != nil {
			var z Z
			buf, err2 := hex.DecodeString(val)
			if err2 == nil {
				err2 = z.UnmarshalBinary(buf)
			}
			if err2 != nil {
				return nil, fmt.Errorf("micheline: decoding bigmap %s key '%s': %v", key.Type, val, err)
			}
			key.IntKey = z.Big()
		}
	case T_STRING:
		key.Type.Type = PrimString
		key.StringKey = val
	case T_BYTES:
		key.Type.Type = PrimBytes
		key.BytesKey, err = hex.DecodeString(val)
		if err != nil {
			return nil, fmt.Errorf("micheline: decoding bigmap %s key '%s': %v", key.Type, val, err)
		}
	case T_BOOL:
		key.Type.Type = PrimNullary
		key.BoolKey, err = strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("micheline: decoding bigmap %s key '%s': %v", key.Type, val, err)
		}
	case T_TIMESTAMP:
		key.Type.Type = PrimInt
		// either RFC3339 or UNIX seconds
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			key.TimeKey = time.Unix(i, 0).UTC()
		} else if key.TimeKey, err = time.Parse(time.RFC3339, val); err != nil {
			var z Z
			buf, err2 := hex.DecodeString(val)
			if err2 == nil {
				err2 = z.UnmarshalBinary(buf)
			}
			if err2 != nil {
				return nil, fmt.Errorf("micheline: decoding bigmap %s key '%s': %v", key.Type, val, err)
			}
			key.TimeKey = time.Unix(z.Int64(), 0).UTC()
		}
	case T_KEY_HASH, T_ADDRESS:
		key.Type.Type = PrimBytes
		key.AddrKey, err = chain.ParseAddress(val)
		if err != nil {
			a := chain.Address{}
			buf, err2 := hex.DecodeString(val)
			if err2 == nil {
				err2 = a.UnmarshalBinary(buf)
			}
			if err2 != nil {
				return nil, fmt.Errorf("micheline: decoding bigmap %s key '%s': %v", key.Type, val, err)
			}
			key.AddrKey = a
		}
	default:
		return nil, fmt.Errorf("micheline: unsupported big_map key type '%s' in query", key.Type.OpCode)
	}
	// log.Infof("Parsed Bigmap key %s type=%s opcode=%s %s", key.String(), key.Type.Type, key.Type.OpCode, key.Type.Text())
	return key, nil
}

func InferBigMapKeyType(val string) OpCode {
	if _, err := strconv.ParseBool(val); err == nil {
		return T_BOOL
	}
	if a, err := chain.ParseAddress(val); err == nil {
		if a.Type == chain.AddressTypeContract {
			return T_ADDRESS
		}
		return T_KEY_HASH
	}
	if _, err := time.Parse(time.RFC3339, val); err == nil {
		return T_TIMESTAMP
	}
	i := big.NewInt(0)
	if err := i.UnmarshalText([]byte(val)); err == nil {
		return T_INT
	}
	if _, err := hex.DecodeString(val); err == nil {
		return T_BYTES
	}
	return T_STRING
}

func (k *BigMapKey) Bytes() []byte {
	p := Prim{
		Type:   k.Type.Type,
		OpCode: k.Type.OpCode,
	}
	switch k.Type.OpCode {
	case T_INT, T_NAT, T_MUTEZ:
		p.Int = k.IntKey
	case T_STRING:
		p.String = k.StringKey
	case T_BYTES:
		p.Bytes = k.BytesKey
	case T_BOOL:
		if k.BoolKey {
			p.OpCode = D_TRUE
		} else {
			p.OpCode = D_FALSE
		}
	case T_TIMESTAMP:
		var z Z
		z.SetInt64(k.TimeKey.Unix())
		p.Int = z.Big()
	case T_ADDRESS:
		p.Bytes, _ = k.AddrKey.MarshalBinary()
	case T_KEY_HASH:
		b, _ := k.AddrKey.MarshalBinary()
		p.Bytes = b[1:] // strip leading flag
	case T_KEY: // expression hash
		p.Bytes = k.Hash.Hash.Hash
	case T_PAIR, D_PAIR:
		b, _ := k.PrimKey.MarshalBinary()
		p.Bytes = b
	default:
		return nil
	}
	buf, _ := p.MarshalBinary()
	return buf
}

// Note: this is not the encoding stored in bigmap table! (prim wrapper is missing)
func (k *BigMapKey) MarshalBinary() ([]byte, error) {
	switch k.Type.OpCode {
	case T_INT, T_NAT, T_MUTEZ:
		var z Z
		return z.Set(k.IntKey).MarshalBinary()
	case T_STRING:
		return []byte(k.StringKey), nil
	case T_BYTES:
		return k.BytesKey, nil
	case T_BOOL:
		if k.BoolKey {
			return []byte{byte(D_TRUE)}, nil
		} else {
			return []byte{byte(D_FALSE)}, nil
		}
	case T_TIMESTAMP:
		var z Z
		z.SetInt64(k.TimeKey.Unix())
		return z.MarshalBinary()
	case T_ADDRESS:
		return k.AddrKey.MarshalBinary()
	case T_KEY_HASH:
		b, err := k.AddrKey.MarshalBinary()
		if err != nil {
			return nil, err
		}
		return b[1:], nil // strip leading flag
	case T_KEY: // expression hash
		return k.Hash.Hash.Hash, nil
	case T_PAIR, D_PAIR:
		return k.PrimKey.MarshalBinary()
	default:
		return nil, fmt.Errorf("micheline: no binary marshaller for big_map key type '%s'", k.Type.OpCode)
	}
	return nil, nil
}

func NewBigMapKeyAs(typ *Prim, key *Prim) (*BigMapKey, error) {
	k := &BigMapKey{
		Type: typ,
	}
	switch typ.OpCode {
	case T_INT, T_NAT, T_MUTEZ:
		k.IntKey = key.Int
	case T_STRING:
		// check if string contains ASCII only
		if isASCII(key.String) {
			k.StringKey = key.String
		} else {
			key.Bytes = []byte(key.String)
		}
		// on empty or non-ascii strings convert to bytes
		if len(k.StringKey) == 0 && len(key.Bytes) > 0 {
			k.Type.OpCode = T_BYTES
			k.BytesKey = key.Bytes
			log.Debugf("changed string to bytes key '%x'", key.Bytes)
		}
	case T_BYTES:
		k.BytesKey = key.Bytes
	case T_BOOL:
		switch key.OpCode {
		case D_TRUE:
			k.BoolKey = true
		case D_FALSE:
			k.BoolKey = false
		default:
			return nil, fmt.Errorf("micheline: invalid bool big_map key opcode %s (%[1]d)", key.OpCode)
		}
	case T_TIMESTAMP:
		// in some cases (originated contract storage) timestamps are strings
		if key.Int == nil {
			t, err := time.Parse(time.RFC3339, key.String)
			if err != nil {
				return nil, fmt.Errorf("micheline: invalid big_map key for string timestamp: %v", err)
			}
			k.TimeKey = t
		} else {
			k.TimeKey = time.Unix(key.Int.Int64(), 0).UTC()
		}
	case T_KEY_HASH, T_ADDRESS:
		// in some cases (originated contract storage) addresses are strings
		if len(key.Bytes) == 0 && len(key.String) > 0 {
			a, err := chain.ParseAddress(strings.Split(key.String, "%")[0])
			if err != nil {
				return nil, fmt.Errorf("micheline: invalid big_map key for string type address: %v", err)
			}
			k.AddrKey = a
		} else {
			a := chain.Address{}
			if err := a.UnmarshalBinary(key.Bytes); err != nil {
				return nil, fmt.Errorf("micheline: invalid big_map key for type address: %v", err)
			}
			k.AddrKey = a
		}

	case T_KEY: // expression hash
		k.Hash = chain.NewExprHash(key.Bytes)

	case T_PAIR, D_PAIR:
		k.PrimKey = key

	default:
		return nil, fmt.Errorf("micheline: big_map key type '%s' is not implemented", typ.OpCode)
	}
	return k, nil
}

func DecodeBigMapKey(typ *Prim, b []byte) (*BigMapKey, error) {
	key := &Prim{}
	if err := key.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return NewBigMapKeyAs(typ, key)
}

func (k *BigMapKey) String() string {
	switch k.Type.OpCode {
	case T_INT, T_NAT, T_MUTEZ:
		return k.IntKey.Text(10)
	case T_STRING:
		return k.StringKey
	case T_BYTES:
		return hex.EncodeToString(k.BytesKey)
	case T_BOOL:
		return strconv.FormatBool(k.BoolKey)
	case T_TIMESTAMP:
		return k.TimeKey.Format(time.RFC3339)
	case T_KEY_HASH, T_ADDRESS:
		return k.AddrKey.String()
	case T_KEY:
		return k.Hash.String()
	case T_PAIR, D_PAIR:
		return fmt.Sprintf("%s#%s",
			k.PrimKey.Args[0].Value(k.PrimKey.Args[0].OpCode),
			k.PrimKey.Args[1].Value(k.PrimKey.Args[1].OpCode),
		)
	default:
		return ""
	}
}

func (k *BigMapKey) Encode() string {
	switch k.Type.OpCode {
	case T_STRING:
		return k.StringKey
	case T_INT, T_NAT, T_MUTEZ:
		return k.IntKey.Text(10)
	case T_BYTES:
		return hex.EncodeToString(k.BytesKey)
	default:
		buf, _ := k.MarshalBinary()
		return hex.EncodeToString(buf)
	}
}

func (k *BigMapKey) Prim() *Prim {
	p := &Prim{
		OpCode: k.Type.OpCode,
	}
	switch k.Type.OpCode {
	case T_INT, T_NAT, T_MUTEZ:
		p.Int = k.IntKey
		p.Type = PrimInt
	case T_TIMESTAMP:
		p.Int = big.NewInt(k.TimeKey.Unix())
		p.Type = PrimInt
	case T_STRING:
		p.String = k.StringKey
		p.Type = PrimString
	case T_BYTES:
		p.Bytes = k.BytesKey
		p.Type = PrimBytes
	case T_BOOL:
		p.Type = PrimNullary
		if k.BoolKey {
			p.OpCode = D_TRUE
		} else {
			p.OpCode = D_FALSE
		}
	case T_ADDRESS, T_KEY_HASH, T_KEY:
		p.Bytes, _ = k.MarshalBinary()
		p.Type = PrimBytes
	case T_PAIR:
		p = k.PrimKey
	default:
		if k.BytesKey != nil {
			log.Debugf("micheline: unpacking big_map key type '%s'", k.Type.OpCode)
			if err := p.UnmarshalBinary(k.BytesKey); err == nil {
				break
			}
		}
		p.Bytes, _ = k.MarshalBinary()
		p.Type = PrimBytes
		log.Debugf("micheline: marshalled big_map key type '%s' as %s", k.Type.OpCode, p.Type)
	}
	return p
}

func (k *BigMapKey) MarshalJSON() ([]byte, error) {
	switch k.Type.OpCode {
	case T_INT, T_NAT, T_MUTEZ:
		return []byte(strconv.Quote(k.IntKey.Text(10))), nil
	case T_STRING:
		return []byte(strconv.Quote(k.StringKey)), nil
	case T_BYTES:
		return []byte(strconv.Quote(hex.EncodeToString(k.BytesKey))), nil
	case T_BOOL:
		return []byte(strconv.FormatBool(k.BoolKey)), nil
	case T_TIMESTAMP:
		if y := k.TimeKey.Year(); y < 0 || y >= 10000 {
			return []byte(strconv.Quote(strconv.FormatInt(k.TimeKey.Unix(), 10))), nil
		}
		return []byte(strconv.Quote(k.TimeKey.Format(time.RFC3339))), nil
	case T_KEY_HASH, T_ADDRESS:
		return []byte(strconv.Quote(k.AddrKey.String())), nil
	case T_KEY:
		return []byte(strconv.Quote(k.Hash.String())), nil
	case T_PAIR:
		val := &BigMapValue{
			Type:  k.Type,
			Value: k.PrimKey,
		}
		return json.Marshal(val)
	default:
		key, _ := k.Type.MarshalJSON()
		val, _ := k.PrimKey.MarshalJSON()
		return nil, fmt.Errorf("micheline: unsupported big_map key type '%s': typ=%s val=%s",
			k.Type.OpCode, string(key), string(val),
		)
	}
}
