// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package chain

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"blockwatch.cc/tzindex/base58"

	"golang.org/x/crypto/blake2b"
)

var (
	// ErrUnknownKeyType describes an error where a type for a
	// public key is undefined.
	ErrUnknownKeyType = errors.New("unknown key type")
)

type KeyType int

const (
	KeyTypeEd25519 KeyType = iota
	KeyTypeSecp256k1
	KeyTypeP256
	KeyTypeInvalid
)

func (t KeyType) IsValid() bool {
	return t >= 0 && t < KeyTypeInvalid
}

func (t KeyType) HashType() HashType {
	switch t {
	case KeyTypeEd25519:
		return HashTypePkhEd25519
	case KeyTypeSecp256k1:
		return HashTypePkhSecp256k1
	case KeyTypeP256:
		return HashTypePkhP256
	default:
		return HashTypeInvalid
	}
}

func (t KeyType) AddressType() AddressType {
	switch t {
	case KeyTypeEd25519:
		return AddressTypeEd25519
	case KeyTypeSecp256k1:
		return AddressTypeSecp256k1
	case KeyTypeP256:
		return AddressTypeP256
	default:
		return AddressTypeInvalid
	}
}

func (t KeyType) PrefixBytes() []byte {
	switch t {
	case KeyTypeEd25519:
		return ED25519_PUBLIC_KEY_ID
	case KeyTypeSecp256k1:
		return SECP256K1_PUBLIC_KEY_ID
	case KeyTypeP256:
		return P256_PUBLIC_KEY_ID
	default:
		return nil
	}
}

func (t KeyType) Prefix() string {
	switch t {
	case KeyTypeEd25519:
		return ED25519_PUBLIC_KEY_PREFIX
	case KeyTypeSecp256k1:
		return SECP256K1_PUBLIC_KEY_PREFIX
	case KeyTypeP256:
		return P256_PUBLIC_KEY_PREFIX
	default:
		return ""
	}
}

func (t KeyType) Tag() byte {
	switch t {
	case KeyTypeEd25519:
		return 0
	case KeyTypeSecp256k1:
		return 1
	case KeyTypeP256:
		return 2
	default:
		return 255
	}
}

func ParseKeyTag(b byte) KeyType {
	switch b {
	case 0:
		return KeyTypeEd25519
	case 1:
		return KeyTypeSecp256k1
	case 2:
		return KeyTypeP256
	default:
		return KeyTypeInvalid
	}
}

func HasKeyPrefix(s string) bool {
	for _, prefix := range []string{
		ED25519_PUBLIC_KEY_PREFIX,
		SECP256K1_PUBLIC_KEY_PREFIX,
		P256_PUBLIC_KEY_PREFIX,
	} {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return false
}

func (t KeyType) Len() int {
	switch t {
	case KeyTypeEd25519:
		return 32
	case KeyTypeSecp256k1:
		return 33
	case KeyTypeP256:
		return 33
	default:
		return 0
	}
}

type Key struct {
	Type KeyType
	Data []byte
}

func NewKey(typ KeyType, data []byte) Key {
	return Key{
		Type: typ,
		Data: data,
	}
}

func (k Key) IsValid() bool {
	return k.Type.Len() == len(k.Data)
}

func (k Key) IsEqual(k2 Key) bool {
	return k.Type == k2.Type && bytes.Compare(k.Data, k2.Data) == 0
}

func (k Key) Clone() Key {
	buf := make([]byte, len(k.Data))
	copy(buf, k.Data)
	return Key{
		Type: k.Type,
		Data: buf,
	}
}

func (k Key) Hash() []byte {
	h, _ := blake2b.New(20, nil)
	h.Write(k.Data)
	return h.Sum(nil)
}

func (k Key) Address() Address {
	return Address{
		Type: k.Type.AddressType(),
		Hash: k.Hash(),
	}
}

func (k Key) String() string {
	if !k.IsValid() {
		return ""
	}
	return base58.CheckEncode(k.Data, k.Type.PrefixBytes())
}

func (k Key) MarshalText() ([]byte, error) {
	return []byte(k.String()), nil
}

func (k *Key) UnmarshalText(data []byte) error {
	key, err := ParseKey(string(data))
	if err != nil {
		return err
	}
	*k = key
	return nil
}

func (k Key) MarshalBinary() ([]byte, error) {
	if !k.Type.IsValid() {
		return nil, ErrUnknownKeyType
	}
	return append([]byte{k.Type.Tag()}, k.Data...), nil
}

func (k *Key) UnmarshalBinary(b []byte) error {
	l := len(b)
	if l < 33 {
		return fmt.Errorf("invalid binary key length %d", l)
	}
	if typ := ParseKeyTag(b[0]); !typ.IsValid() {
		return fmt.Errorf("invalid binary key type %x", b[0])
	} else {
		k.Type = typ
	}
	if cap(k.Data) < l-1 {
		k.Data = make([]byte, l-1)
	} else {
		k.Data = k.Data[:l-1]
	}
	copy(k.Data, b[1:])
	return nil
}

func ParseKey(s string) (Key, error) {
	k := Key{}
	decoded, version, err := base58.CheckDecode(s, 4, nil)
	if err != nil {
		if err == base58.ErrChecksum {
			return k, ErrChecksumMismatch
		}
		return k, fmt.Errorf("unknown key format: %v", err.Error())
	}
	switch true {
	case bytes.Compare(version, ED25519_PUBLIC_KEY_ID) == 0:
		k.Type = KeyTypeEd25519
	case bytes.Compare(version, SECP256K1_PUBLIC_KEY_ID) == 0:
		k.Type = KeyTypeSecp256k1
	case bytes.Compare(version, P256_PUBLIC_KEY_ID) == 0:
		k.Type = KeyTypeP256
	default:
		return k, fmt.Errorf("unknown version %v for key %s", version, s)
	}
	if l := len(decoded); l < k.Type.Len() {
		return k, fmt.Errorf("invalid length %d for %s key data", l, k.Type.Prefix())
	}
	k.Data = decoded[:k.Type.Len()]
	return k, nil
}
