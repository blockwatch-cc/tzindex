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
	"time"
	"unicode"

	"blockwatch.cc/tzindex/chain"
)

type BigMapKey struct {
	Type      OpCode
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
	return k.Type == T_BYTES && len(k.BytesKey) > 1 && k.BytesKey[0] == 0x5
}

func (k BigMapKey) PackedType() PrimType {
	if !k.IsPacked() {
		return PrimNullary
	}
	return PrimType(k.BytesKey[1])
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
		Type:      p.OpCode,
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
	case T_INT, T_NAT, T_MUTEZ, T_STRING, T_BYTES, T_BOOL, T_KEY_HASH, T_TIMESTAMP, T_ADDRESS:
		return t, nil
	default:
		return t, fmt.Errorf("micheline: unsupported big_map key type '%s'", t)
	}
}

// query string parsing used for lookup (does not support Pair keys)
func ParseBigMapKey(typ, val string) (*BigMapKey, error) {
	var err error
	key := &BigMapKey{}
	// try parsing expr hash first
	key.Hash, err = chain.ParseExprHash(val)
	if err == nil {
		key.Type = T_KEY
		return key, nil
	}
	if typ == "" {
		key.Type = InferBigMapKeyType(val)
	} else {
		key.Type, err = ParseOpCode(typ)
		if err != nil {
			return nil, err
		}
	}
	switch key.Type {
	case T_INT, T_NAT, T_MUTEZ:
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
		key.StringKey = val
	case T_BYTES:
		key.BytesKey, err = hex.DecodeString(val)
		if err != nil {
			return nil, fmt.Errorf("micheline: decoding bigmap %s key '%s': %v", key.Type, val, err)
		}
	case T_BOOL:
		key.BoolKey, err = strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("micheline: decoding bigmap %s key '%s': %v", key.Type, val, err)
		}
	case T_TIMESTAMP:
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
		return nil, fmt.Errorf("micheline: unsupported big_map key type '%s'", key.Type)
	}
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
	buf, _ := k.MarshalBinary()
	return buf
}

// FIXME: check binary encoding
func (k *BigMapKey) MarshalBinary() ([]byte, error) {
	switch k.Type {
	case T_INT, T_NAT, T_MUTEZ:
		var z Z
		return z.Set(k.IntKey).MarshalBinary()
	case T_STRING:
		return []byte(k.StringKey), nil
	case T_BYTES:
		return k.BytesKey, nil
	case T_BOOL:
		if k.BoolKey {
			return []byte{byte(True)}, nil
		} else {
			return []byte{byte(False)}, nil
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
	case T_KEY:
		return k.Hash.Hash.Hash, nil
	default:
		return nil, fmt.Errorf("micheline: unsupported big_map key type '%s'", k.Type)
	}
	return nil, nil
}

func NewBigMapKey(typ OpCode, i *big.Int, s string, b []byte, p *Prim) (*BigMapKey, error) {
	k := &BigMapKey{
		Type: typ,
	}
	switch typ {
	case T_INT, T_NAT, T_MUTEZ:
		k.IntKey = i
	case T_STRING:
		// check if string contains ASCII only
		if isASCII(s) {
			k.StringKey = s
		} else {
			b = []byte(s)
		}
		// on empty or non-ascii strings convert to bytes
		if len(k.StringKey) == 0 && len(b) > 0 {
			k.Type = T_BYTES
			k.BytesKey = b
		}
	case T_BYTES:
		k.BytesKey = b
	case T_BOOL:
		if l := len(b); l != 1 {
			return nil, fmt.Errorf("micheline: invalid bool big_map key len %d", l)
		}
		if b[0] == byte(True) {
			k.BoolKey = true
		} else if b[0] == byte(False) {
			k.BoolKey = false
		} else {
			return nil, fmt.Errorf("micheline: invalid bool big_map key val %x", b[0])
		}
	case T_TIMESTAMP:
		// in some cases (originated contract storage) timestamps are strings
		if i == nil {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return nil, fmt.Errorf("micheline: invalid big_map key for string timestamp: %v", err)
			}
			k.TimeKey = t
		} else {
			k.TimeKey = time.Unix(i.Int64(), 0).UTC()
		}
	case T_KEY_HASH, T_ADDRESS:
		// in some cases (originated contract storage) addresses are strings
		if len(b) == 0 && len(s) > 0 {
			a, err := chain.ParseAddress(s)
			if err != nil {
				return nil, fmt.Errorf("micheline: invalid big_map key for string type address: %v", err)
			}
			k.AddrKey = a
		} else {
			a := chain.Address{}
			if err := a.UnmarshalBinary(b); err != nil {
				return nil, fmt.Errorf("micheline: invalid big_map key for type address: %v", err)
			}
			k.AddrKey = a
		}

	case T_KEY: // expression hash
		k.Hash = chain.NewExprHash(b)

	case T_PAIR:
		k.PrimKey = p

	default:
		return nil, fmt.Errorf("micheline: big_map key type '%s' is not implemented", typ)
	}
	return k, nil
}

func DecodeBigMapKey(typ OpCode, enc PrimType, b []byte) (*BigMapKey, error) {
	var (
		i   *big.Int
		str string
		buf []byte
		p   *Prim
	)
	switch enc {
	case PrimString:
		str = string(b)
	case PrimBytes:
		buf = make([]byte, len(b))
		copy(buf, b)
	case PrimInt:
		var z Z
		err := z.UnmarshalBinary(b)
		if err != nil {
			return nil, err
		}
		i = z.Big()
	case PrimBinary:
		p = &Prim{}
		if err := p.UnmarshalBinary(b); err != nil {
			return nil, err
		}
	}
	return NewBigMapKey(typ, i, str, buf, p)
}

func (k *BigMapKey) String() string {
	switch k.Type {
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
		// FIXME: recurse into pair tree
		return fmt.Sprintf("%s#%s",
			k.PrimKey.Args[0].Value(k.PrimKey.Args[0].OpCode),
			k.PrimKey.Args[1].Value(k.PrimKey.Args[1].OpCode),
		)
	default:
		return ""
	}
}

func (k *BigMapKey) Encode() string {
	switch k.Type {
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
		OpCode: k.Type,
	}
	switch k.Type {
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
	case T_BOOL, T_ADDRESS, T_KEY_HASH, T_KEY:
		p.Bytes, _ = k.MarshalBinary()
		p.Type = PrimBytes
	case T_PAIR, D_PAIR:
		p = k.PrimKey
	default:
		if k.BytesKey != nil {
			if err := p.UnmarshalBinary(k.BytesKey); err == nil {
				break
			}
		}
		p.Bytes, _ = k.MarshalBinary()
		p.Type = PrimBytes
		log.Errorf("micheline: unsupported big_map key type '%s'", k.Type)
	}
	return p
}

func (k *BigMapKey) MarshalJSON() ([]byte, error) {
	switch k.Type {
	case T_INT, T_NAT, T_MUTEZ:
		return []byte(strconv.Quote(k.IntKey.Text(10))), nil
	case T_STRING:
		return []byte(strconv.Quote(k.StringKey)), nil
	case T_BYTES:
		return []byte(strconv.Quote(hex.EncodeToString(k.BytesKey))), nil
	case T_BOOL:
		return []byte(strconv.FormatBool(k.BoolKey)), nil
	case T_TIMESTAMP:
		return []byte(strconv.Quote(k.TimeKey.Format(time.RFC3339))), nil
	case T_KEY_HASH, T_ADDRESS:
		return []byte(strconv.Quote(k.AddrKey.String())), nil
	case T_KEY:
		return []byte(strconv.Quote(k.Hash.String())), nil
	case T_PAIR, D_PAIR:
		return json.Marshal(k.PrimKey)
	default:
		return nil, fmt.Errorf("micheline: unsupported big_map key type '%s'", k.Type)
	}
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > unicode.MaxASCII {
			return false
		}
	}
	return true
}
