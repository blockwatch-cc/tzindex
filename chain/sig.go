// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package chain

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"blockwatch.cc/tzindex/base58"
)

var (
	// ErrUnknownSignatureType describes an error where a type for a
	// signature is undefined.
	ErrUnknownSignatureType = errors.New("unknown signature type")
)

type SignatureType int

const (
	SignatureTypeEd25519 SignatureType = iota
	SignatureTypeSecp256k1
	SignatureTypeP256
	SignatureTypeGeneric
	SignatureTypeInvalid
)

func (t SignatureType) IsValid() bool {
	return t >= 0 && t < SignatureTypeInvalid
}

func (t SignatureType) HashType() HashType {
	switch t {
	case SignatureTypeEd25519:
		return HashTypeSigEd25519
	case SignatureTypeSecp256k1:
		return HashTypeSigSecp256k1
	case SignatureTypeP256:
		return HashTypeSigP256
	case SignatureTypeGeneric:
		return HashTypeSigGeneric
	default:
		return HashTypeInvalid
	}
}

func (t SignatureType) PrefixBytes() []byte {
	switch t {
	case SignatureTypeEd25519:
		return ED25519_SIGNATURE_ID
	case SignatureTypeSecp256k1:
		return SECP256K1_SIGNATURE_ID
	case SignatureTypeP256:
		return P256_SIGNATURE_ID
	case SignatureTypeGeneric:
		return GENERIC_SIGNATURE_ID
	default:
		return nil
	}
}

func (t SignatureType) Prefix() string {
	switch t {
	case SignatureTypeEd25519:
		return ED25519_SIGNATURE_PREFIX
	case SignatureTypeSecp256k1:
		return SECP256K1_SIGNATURE_PREFIX
	case SignatureTypeP256:
		return P256_SIGNATURE_PREFIX
	case SignatureTypeGeneric:
		return GENERIC_SIGNATURE_PREFIX
	default:
		return ""
	}
}

func (t SignatureType) Tag() byte {
	switch t {
	case SignatureTypeEd25519:
		return 0
	case SignatureTypeSecp256k1:
		return 1
	case SignatureTypeP256:
		return 2
	case SignatureTypeGeneric:
		return 3
	default:
		return 255
	}
}

func ParseSignatureTag(b byte) SignatureType {
	switch b {
	case 0:
		return SignatureTypeEd25519
	case 1:
		return SignatureTypeSecp256k1
	case 2:
		return SignatureTypeP256
	case 3:
		return SignatureTypeGeneric
	default:
		return SignatureTypeInvalid
	}
}

func HasSignaturePrefix(s string) bool {
	for _, prefix := range []string{
		ED25519_SIGNATURE_PREFIX,
		SECP256K1_SIGNATURE_PREFIX,
		P256_SIGNATURE_PREFIX,
		GENERIC_SIGNATURE_PREFIX,
	} {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return false
}

func (t SignatureType) Len() int {
	if t.IsValid() {
		return 64
	}
	return 0
}

type Signature struct {
	Type SignatureType
	Data []byte
}

func NewSignature(typ SignatureType, data []byte) Signature {
	return Signature{
		Type: typ,
		Data: data,
	}
}

func (s Signature) IsValid() bool {
	return s.Type.Len() == len(s.Data)
}

func (s Signature) IsEqual(s2 Signature) bool {
	return s.Type == s2.Type && bytes.Compare(s.Data, s2.Data) == 0
}

func (s Signature) Clone() Signature {
	buf := make([]byte, len(s.Data))
	copy(buf, s.Data)
	return Signature{
		Type: s.Type,
		Data: buf,
	}
}

func (s Signature) String() string {
	if !s.IsValid() {
		return ""
	}
	return base58.CheckEncode(s.Data, s.Type.PrefixBytes())
}

func (s Signature) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func (s *Signature) UnmarshalText(data []byte) error {
	sig, err := ParseSignature(string(data))
	if err != nil {
		return err
	}
	*s = sig
	return nil
}

func (s Signature) MarshalBinary() ([]byte, error) {
	if !s.Type.IsValid() {
		return nil, ErrUnknownSignatureType
	}
	return append([]byte{s.Type.Tag()}, s.Data...), nil
}

func (s *Signature) UnmarshalBinary(b []byte) error {
	l := len(b)
	if l < 65 {
		return fmt.Errorf("invalid binary signature length %d", l)
	}
	if typ := ParseSignatureTag(b[0]); !typ.IsValid() {
		return fmt.Errorf("invalid binary signature type %x", b[0])
	} else {
		s.Type = typ
	}
	if cap(s.Data) < l-1 {
		s.Data = make([]byte, l-1)
	} else {
		s.Data = s.Data[:l-1]
	}
	copy(s.Data, b)
	return nil
}

func ParseSignature(s string) (Signature, error) {
	var (
		dec, ver []byte
		typ      SignatureType
		err      error
	)
	switch true {
	case strings.HasPrefix(s, ED25519_SIGNATURE_PREFIX):
		dec, ver, err = base58.CheckDecode(s, 5, nil)
		typ = SignatureTypeEd25519

	case strings.HasPrefix(s, SECP256K1_SIGNATURE_PREFIX):
		dec, ver, err = base58.CheckDecode(s, 5, nil)
		typ = SignatureTypeSecp256k1

	case strings.HasPrefix(s, P256_SIGNATURE_PREFIX):
		dec, ver, err = base58.CheckDecode(s, 4, nil)
		typ = SignatureTypeP256

	case strings.HasPrefix(s, GENERIC_SIGNATURE_PREFIX):
		dec, ver, err = base58.CheckDecode(s, 3, nil)
		typ = SignatureTypeInvalid

	default:
		return Signature{}, fmt.Errorf("unknown signature prefix %s", s)
	}

	if err != nil {
		if err == base58.ErrChecksum {
			return Signature{}, ErrChecksumMismatch
		}
		return Signature{}, fmt.Errorf("unknown signature format: %v", err)
	}

	if bytes.Compare(ver, typ.PrefixBytes()) != 0 {
		return Signature{}, fmt.Errorf("invalid signature type %s for %s", ver, typ.Prefix())
	}

	if l := len(dec); l < typ.Len() {
		return Signature{}, fmt.Errorf("invalid length %d for %s signature data", l, typ.Prefix())
	}

	return Signature{
		Type: typ,
		Data: dec[:typ.Len()],
	}, nil
}
