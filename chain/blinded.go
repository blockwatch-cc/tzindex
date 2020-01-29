// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package chain

import (
	"bytes"
	"errors"
	"fmt"

	"blockwatch.cc/tzindex/base58"

	"golang.org/x/crypto/blake2b"
)

func BlindHash(hash, secret []byte) ([]byte, error) {
	h, err := blake2b.New(20, secret)
	if err != nil {
		return nil, err
	}
	h.Write(hash)
	return h.Sum(nil), nil
}

func BlindAddress(a Address, secret []byte) (Address, error) {
	bh, err := BlindHash(a.Hash, secret)
	if err != nil {
		return Address{}, err
	}
	return Address{
		Type: AddressTypeBlinded,
		Hash: bh,
	}, nil
}

// Checks if address a when blinded with secret equals blinded address b.
func MatchBlindedAddress(a, b Address, secret []byte) bool {
	bh, _ := BlindHash(a.Hash, secret)
	return bytes.Compare(bh, b.Hash) == 0
}

func (a *Address) DecodeBlindedString(addr string) error {
	a.Type = AddressTypeInvalid
	decoded, version, err := base58.CheckDecode(addr, 4, a.Hash)
	if err != nil {
		if err == base58.ErrChecksum {
			return ErrChecksumMismatch
		}
		return fmt.Errorf("decoded address is of unknown format: %v", err.Error())
	}
	if len(decoded) != 20 {
		return fmt.Errorf("decoded address hash is of invalid length")
	}
	switch true {
	case bytes.Compare(version, BLINDED_PUBLIC_KEY_HASH_ID) == 0:
		a.Type = AddressTypeBlinded
		a.Hash = decoded
	default:
		return fmt.Errorf("decoded blinded address %s is of unknown type %v", addr, version)
	}
	return nil
}

func DecodeBlindedAddress(addr string) (Address, error) {
	a := Address{}
	decoded, version, err := base58.CheckDecode(addr, 4, nil)
	if err != nil {
		if err == base58.ErrChecksum {
			return a, ErrChecksumMismatch
		}
		return a, fmt.Errorf("decoded address is of unknown format: %v", err.Error())
	}
	if len(decoded) != 20 {
		return a, errors.New("decoded address hash is of invalid length")
	}
	switch true {
	case bytes.Compare(version, BLINDED_PUBLIC_KEY_HASH_ID) == 0:
		return Address{Type: AddressTypeBlinded, Hash: decoded}, nil
	default:
		return a, fmt.Errorf("decoded address %s is of unknown type %v", addr, version)
	}
}

func EncodeBlindedAddress(hash, secret []byte) (string, error) {
	bh, err := BlindHash(hash, secret)
	if err != nil {
		return "", err
	}
	return EncodeAddress(AddressTypeBlinded, bh)
}
