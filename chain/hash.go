// Copyright (c) 2020-2021 Blockwatch Data Inc.
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
	// ErrUnknownHashType describes an error where a hash can not
	// decoded as a specific hash type because the string encoding
	// starts with an unknown identifier.
	ErrUnknownHashType = errors.New("unknown hash type")

	// InvalidHash represents an empty invalid hash type
	InvalidHash = Hash{Type: HashTypeInvalid, Hash: nil}

	// ZeroHash
	ZeroOpHash    = NewOperationHash(make([]byte, HashTypeOperation.Len()))
	ZeroBlockHash = NewBlockHash(make([]byte, HashTypeBlock.Len()))
)

type HashType int

const (
	HashTypeInvalid HashType = iota
	HashTypeChainId
	HashTypeId
	HashTypePkhEd25519
	HashTypePkhSecp256k1
	HashTypePkhP256
	HashTypePkhNocurve
	HashTypePkhBlinded
	HashTypeBlock
	HashTypeOperation
	HashTypeOperationList
	HashTypeOperationListList
	HashTypeProtocol
	HashTypeContext
	HashTypeNonce
	HashTypeSeedEd25519
	HashTypePkEd25519
	HashTypeSkEd25519
	HashTypePkSecp256k1
	HashTypeSkSecp256k1
	HashTypePkP256
	HashTypeSkP256
	HashTypeScalarSecp256k1
	HashTypeElementSecp256k1
	HashTypeScriptExpr
	HashTypeEncryptedSeedEd25519
	HashTypeEncryptedSkSecp256k1
	HashTypeEncryptedSkP256
	HashTypeSigEd25519
	HashTypeSigSecp256k1
	HashTypeSigP256
	HashTypeSigGeneric
)

func ParseHashType(s string) HashType {
	switch len(s) {
	case 15:
		if strings.HasPrefix(s, CHAIN_ID_PREFIX) {
			return HashTypeChainId
		}
	case 30:
		if strings.HasPrefix(s, ID_HASH_PREFIX) {
			return HashTypeId
		}
	case 36:
		switch true {
		case strings.HasPrefix(s, ED25519_PUBLIC_KEY_HASH_PREFIX):
			return HashTypePkhEd25519
		case strings.HasPrefix(s, SECP256K1_PUBLIC_KEY_HASH_PREFIX):
			return HashTypePkhSecp256k1
		case strings.HasPrefix(s, P256_PUBLIC_KEY_HASH_PREFIX):
			return HashTypePkhP256
		case strings.HasPrefix(s, NOCURVE_PUBLIC_KEY_HASH_PREFIX):
			return HashTypePkhNocurve
		case strings.HasPrefix(s, BLINDED_PUBLIC_KEY_HASH_PREFIX):
			return HashTypePkhBlinded
		}
	case 51:
		switch true {
		case strings.HasPrefix(s, BLOCK_HASH_PREFIX):
			return HashTypeBlock
		case strings.HasPrefix(s, OPERATION_HASH_PREFIX):
			return HashTypeOperation
		case strings.HasPrefix(s, PROTOCOL_HASH_PREFIX):
			return HashTypeProtocol
		}
	case 52:
		switch true {
		case strings.HasPrefix(s, OPERATION_LIST_HASH_PREFIX):
			return HashTypeOperationList
		case strings.HasPrefix(s, CONTEXT_HASH_PREFIX):
			return HashTypeContext
		}
	case 53:
		switch true {
		case strings.HasPrefix(s, OPERATION_LIST_LIST_HASH_PREFIX):
			return HashTypeOperationListList
		case strings.HasPrefix(s, SECP256K1_SCALAR_PREFIX):
			return HashTypeScalarSecp256k1
		case strings.HasPrefix(s, NONCE_HASH_PREFIX):
			return HashTypeNonce
		}
	case 54:
		switch true {
		case strings.HasPrefix(s, ED25519_SEED_PREFIX):
			return HashTypeSeedEd25519
		case strings.HasPrefix(s, ED25519_PUBLIC_KEY_PREFIX):
			return HashTypePkEd25519
		case strings.HasPrefix(s, SECP256K1_SECRET_KEY_PREFIX):
			return HashTypeSkSecp256k1
		case strings.HasPrefix(s, P256_SECRET_KEY_PREFIX):
			return HashTypeSkP256
		case strings.HasPrefix(s, SECP256K1_ELEMENT_PREFIX):
			return HashTypeElementSecp256k1
		case strings.HasPrefix(s, SCRIPT_EXPR_HASH_PREFIX):
			return HashTypeScriptExpr
		}
	case 55:
		switch true {
		case strings.HasPrefix(s, SECP256K1_PUBLIC_KEY_PREFIX):
			return HashTypePkSecp256k1
		case strings.HasPrefix(s, P256_PUBLIC_KEY_PREFIX):
			return HashTypePkP256
		}
	case 88:
		switch true {
		case strings.HasPrefix(s, ED25519_ENCRYPTED_SEED_PREFIX):
			return HashTypeEncryptedSeedEd25519
		case strings.HasPrefix(s, SECP256K1_ENCRYPTED_SECRET_KEY_PREFIX):
			return HashTypeEncryptedSkSecp256k1
		case strings.HasPrefix(s, P256_ENCRYPTED_SECRET_KEY_PREFIX):
			return HashTypeEncryptedSkP256
		}
	case 96:
		if strings.HasPrefix(s, GENERIC_SIGNATURE_PREFIX) {
			return HashTypeSigGeneric
		}
	case 98:
		switch true {
		case strings.HasPrefix(s, ED25519_SECRET_KEY_PREFIX):
			return HashTypeSkEd25519
		case strings.HasPrefix(s, P256_SIGNATURE_PREFIX):
			return HashTypeSigP256
		}
	case 99:
		switch true {
		case strings.HasPrefix(s, ED25519_SIGNATURE_PREFIX):
			return HashTypeSigEd25519
		case strings.HasPrefix(s, SECP256K1_SIGNATURE_PREFIX):
			return HashTypeSigSecp256k1
		}
	}
	return HashTypeInvalid
}

func (t HashType) IsValid() bool {
	return t != HashTypeInvalid
}

func (t HashType) String() string {
	return t.Prefix()
}

func (t HashType) MatchPrefix(s string) bool {
	return strings.HasPrefix(s, t.Prefix())
}

func (t HashType) Prefix() string {
	switch t {
	case HashTypeChainId:
		return CHAIN_ID_PREFIX
	case HashTypeId:
		return ID_HASH_PREFIX
	case HashTypePkhEd25519:
		return ED25519_PUBLIC_KEY_HASH_PREFIX
	case HashTypePkhSecp256k1:
		return SECP256K1_PUBLIC_KEY_HASH_PREFIX
	case HashTypePkhP256:
		return P256_PUBLIC_KEY_HASH_PREFIX
	case HashTypePkhNocurve:
		return NOCURVE_PUBLIC_KEY_HASH_PREFIX
	case HashTypePkhBlinded:
		return BLINDED_PUBLIC_KEY_HASH_PREFIX
	case HashTypeBlock:
		return BLOCK_HASH_PREFIX
	case HashTypeOperation:
		return OPERATION_HASH_PREFIX
	case HashTypeOperationList:
		return OPERATION_LIST_HASH_PREFIX
	case HashTypeOperationListList:
		return OPERATION_LIST_LIST_HASH_PREFIX
	case HashTypeProtocol:
		return PROTOCOL_HASH_PREFIX
	case HashTypeContext:
		return CONTEXT_HASH_PREFIX
	case HashTypeNonce:
		return NONCE_HASH_PREFIX
	case HashTypeSeedEd25519:
		return ED25519_SEED_PREFIX
	case HashTypePkEd25519:
		return ED25519_PUBLIC_KEY_PREFIX
	case HashTypeSkEd25519:
		return ED25519_SECRET_KEY_PREFIX
	case HashTypePkSecp256k1:
		return SECP256K1_PUBLIC_KEY_PREFIX
	case HashTypeSkSecp256k1:
		return SECP256K1_SECRET_KEY_PREFIX
	case HashTypePkP256:
		return P256_PUBLIC_KEY_PREFIX
	case HashTypeSkP256:
		return P256_SECRET_KEY_PREFIX
	case HashTypeScalarSecp256k1:
		return SECP256K1_SCALAR_PREFIX
	case HashTypeElementSecp256k1:
		return SECP256K1_ELEMENT_PREFIX
	case HashTypeScriptExpr:
		return SCRIPT_EXPR_HASH_PREFIX
	case HashTypeEncryptedSeedEd25519:
		return ED25519_ENCRYPTED_SEED_PREFIX
	case HashTypeEncryptedSkSecp256k1:
		return SECP256K1_ENCRYPTED_SECRET_KEY_PREFIX
	case HashTypeEncryptedSkP256:
		return P256_ENCRYPTED_SECRET_KEY_PREFIX
	case HashTypeSigEd25519:
		return ED25519_SIGNATURE_PREFIX
	case HashTypeSigSecp256k1:
		return SECP256K1_SIGNATURE_PREFIX
	case HashTypeSigP256:
		return P256_SIGNATURE_PREFIX
	case HashTypeSigGeneric:
		return GENERIC_SIGNATURE_PREFIX
	default:
		return ""
	}
}

func (t HashType) PrefixBytes() []byte {
	switch t {
	case HashTypeChainId:
		return CHAIN_ID
	case HashTypeId:
		return ID_HASH_ID
	case HashTypePkhEd25519:
		return ED25519_PUBLIC_KEY_HASH_ID
	case HashTypePkhSecp256k1:
		return SECP256K1_PUBLIC_KEY_HASH_ID
	case HashTypePkhP256:
		return P256_PUBLIC_KEY_HASH_ID
	case HashTypePkhNocurve:
		return NOCURVE_PUBLIC_KEY_HASH_ID
	case HashTypePkhBlinded:
		return BLINDED_PUBLIC_KEY_HASH_ID
	case HashTypeBlock:
		return BLOCK_HASH_ID
	case HashTypeOperation:
		return OPERATION_HASH_ID
	case HashTypeOperationList:
		return OPERATION_LIST_HASH_ID
	case HashTypeOperationListList:
		return OPERATION_LIST_LIST_HASH_ID
	case HashTypeProtocol:
		return PROTOCOL_HASH_ID
	case HashTypeContext:
		return CONTEXT_HASH_ID
	case HashTypeNonce:
		return NONCE_HASH_ID
	case HashTypeSeedEd25519:
		return ED25519_SEED_ID
	case HashTypePkEd25519:
		return ED25519_PUBLIC_KEY_ID
	case HashTypeSkEd25519:
		return ED25519_SECRET_KEY_ID
	case HashTypePkSecp256k1:
		return SECP256K1_PUBLIC_KEY_ID
	case HashTypeSkSecp256k1:
		return SECP256K1_SECRET_KEY_ID
	case HashTypePkP256:
		return P256_PUBLIC_KEY_ID
	case HashTypeSkP256:
		return P256_SECRET_KEY_ID
	case HashTypeScalarSecp256k1:
		return SECP256K1_SCALAR_ID
	case HashTypeElementSecp256k1:
		return SECP256K1_ELEMENT_ID
	case HashTypeScriptExpr:
		return SCRIPT_EXPR_HASH_ID
	case HashTypeEncryptedSeedEd25519:
		return ED25519_ENCRYPTED_SEED_ID
	case HashTypeEncryptedSkSecp256k1:
		return SECP256K1_ENCRYPTED_SECRET_KEY_ID
	case HashTypeEncryptedSkP256:
		return P256_ENCRYPTED_SECRET_KEY_ID
	case HashTypeSigEd25519:
		return ED25519_SIGNATURE_ID
	case HashTypeSigSecp256k1:
		return SECP256K1_SIGNATURE_ID
	case HashTypeSigP256:
		return P256_SIGNATURE_ID
	case HashTypeSigGeneric:
		return GENERIC_SIGNATURE_ID
	default:
		return nil
	}
}

func (t HashType) Len() int {
	switch t {
	case HashTypeChainId:
		return 4
	case HashTypeId:
		return 16
	case HashTypePkhEd25519,
		HashTypePkhSecp256k1,
		HashTypePkhP256,
		HashTypePkhNocurve,
		HashTypePkhBlinded:
		return 20
	case HashTypeBlock,
		HashTypeOperation,
		HashTypeOperationList,
		HashTypeOperationListList,
		HashTypeProtocol,
		HashTypeContext,
		HashTypeNonce,
		HashTypeSeedEd25519,
		HashTypePkEd25519,
		HashTypeSkSecp256k1,
		HashTypeSkP256,
		HashTypeScriptExpr:
		return 32
	case HashTypePkSecp256k1,
		HashTypePkP256,
		HashTypeScalarSecp256k1,
		HashTypeElementSecp256k1:
		return 33
	case HashTypeEncryptedSeedEd25519,
		HashTypeEncryptedSkSecp256k1,
		HashTypeEncryptedSkP256:
		return 56
	case HashTypeSkEd25519,
		HashTypeSigEd25519,
		HashTypeSigSecp256k1,
		HashTypeSigP256,
		HashTypeSigGeneric:
		return 64
	default:
		return 0
	}
}

func (t HashType) Base58Len() int {
	switch t {
	case HashTypeChainId:
		return 15
	case HashTypeId,
		HashTypePkhEd25519,
		HashTypePkhSecp256k1,
		HashTypePkhP256,
		HashTypePkhNocurve:
		return 36
	case HashTypePkhBlinded:
		return 37
	case HashTypeBlock,
		HashTypeOperation,
		HashTypeProtocol:
		return 51
	case HashTypeOperationList,
		HashTypeContext:
		return 52
	case HashTypeOperationListList,
		HashTypeNonce,
		HashTypeScalarSecp256k1:
		return 53
	case HashTypeSeedEd25519,
		HashTypePkEd25519,
		HashTypeSkSecp256k1,
		HashTypeSkP256,
		HashTypeElementSecp256k1,
		HashTypeScriptExpr:
		return 54
	case HashTypePkSecp256k1,
		HashTypePkP256:
		return 55
	case HashTypeEncryptedSeedEd25519,
		HashTypeEncryptedSkSecp256k1,
		HashTypeEncryptedSkP256:
		return 88
	case HashTypeSigGeneric:
		return 96
	case HashTypeSkEd25519,
		HashTypeSigP256:
		return 98
	case HashTypeSigEd25519,
		HashTypeSigSecp256k1:
		return 99
	default:
		return 0
	}
}

// DEPRECATED, will migrate to new key handling in v8.1
func (t HashType) KeyType() KeyType {
	switch t {
	case HashTypePkhEd25519, // technically incorrect, bug in v7/packdb
		HashTypePkEd25519: // correct
		return KeyTypeEd25519
	case HashTypePkhSecp256k1, // technically incorrect, bug in v7/packdb
		HashTypePkSecp256k1: // correct
		return KeyTypeSecp256k1
	case HashTypePkhP256, // technically incorrect, bug in v7/packdb
		HashTypePkP256: // correct
		return KeyTypeP256
	default:
		return KeyTypeInvalid
	}
}

type Hash struct {
	Type HashType
	Hash []byte
}

func NewHash(typ HashType, hash []byte) Hash {
	return Hash{
		Type: typ,
		Hash: hash,
	}
}

func (h Hash) IsValid() bool {
	return h.Type != HashTypeInvalid && len(h.Hash) == h.Type.Len()
}

func (h Hash) IsEqual(h2 Hash) bool {
	return h.Type == h2.Type && bytes.Compare(h.Hash, h2.Hash) == 0
}

func (h Hash) Clone() Hash {
	buf := make([]byte, len(h.Hash))
	copy(buf, h.Hash)
	return Hash{
		Type: h.Type,
		Hash: buf,
	}
}

func (h *Hash) Reset() {
	h.Type = HashTypeInvalid
	h.Hash = nil
}

// String returns the string encoding of the hash.
func (h Hash) String() string {
	s, _ := encodeHash(h.Type, h.Hash)
	return s
}

func ParseHash(s string) (Hash, error) {
	return decodeHash(s)
}

func (h *Hash) UnmarshalText(data []byte) error {
	x, err := decodeHash(string(data))
	if err != nil {
		return err
	}
	*h = x
	return nil
}

func (h Hash) MarshalText() ([]byte, error) {
	if h.IsValid() {
		return []byte(h.String()), nil
	}
	return nil, nil
}

func (h Hash) MarshalBinary() ([]byte, error) {
	return h.Hash, nil
}

// ChainIdHash
type ChainIdHash struct {
	Hash
}

func NewChainIdHash(buf []byte) ChainIdHash {
	b := make([]byte, len(buf))
	copy(b, buf)
	return ChainIdHash{Hash: NewHash(HashTypeChainId, b)}
}

func (h ChainIdHash) IsEqual(h2 ChainIdHash) bool {
	return h.Hash.IsEqual(h2.Hash)
}

func (h ChainIdHash) Clone() ChainIdHash {
	return ChainIdHash{h.Hash.Clone()}
}

func (h *ChainIdHash) UnmarshalText(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if !strings.HasPrefix(string(data), CHAIN_ID_PREFIX) {
		return fmt.Errorf("invalid prefix for chain id hash '%s'", string(data))
	}
	if err := h.Hash.UnmarshalText(data); err != nil {
		return err
	}
	if h.Type != HashTypeChainId {
		return fmt.Errorf("invalid type %s for chain id hash", h.Type.Prefix())
	}
	if len(h.Hash.Hash) != h.Type.Len() {
		return fmt.Errorf("invalid len %d for chain id hash", len(h.Hash.Hash))
	}
	return nil
}

func (h *ChainIdHash) UnmarshalBinary(data []byte) error {
	if l := len(data); l > 0 && l != HashTypeChainId.Len() {
		return fmt.Errorf("invalid len %d for chain id hash", len(data))
	}
	h.Type = HashTypeChainId
	h.Hash.Hash = make([]byte, HashTypeChainId.Len())
	copy(h.Hash.Hash, data)
	return nil
}

func MustParseChainIdHash(s string) ChainIdHash {
	h, err := ParseChainIdHash(s)
	if err != nil {
		panic(err)
	}
	return h
}

func ParseChainIdHash(s string) (ChainIdHash, error) {
	var h ChainIdHash
	if err := h.UnmarshalText([]byte(s)); err != nil {
		return h, err
	}
	return h, nil
}

// BlockHash
type BlockHash struct {
	Hash
}

func NewBlockHash(buf []byte) BlockHash {
	b := make([]byte, len(buf))
	copy(b, buf)
	return BlockHash{Hash: NewHash(HashTypeBlock, b)}
}

func (h BlockHash) Clone() BlockHash {
	return BlockHash{h.Hash.Clone()}
}

func (h BlockHash) IsEqual(h2 BlockHash) bool {
	return h.Hash.IsEqual(h2.Hash)
}

func (h *BlockHash) UnmarshalText(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if !strings.HasPrefix(string(data), BLOCK_HASH_PREFIX) {
		return fmt.Errorf("invalid prefix for block hash '%s'", string(data))
	}
	if err := h.Hash.UnmarshalText(data); err != nil {
		return err
	}
	if h.Type != HashTypeBlock {
		return fmt.Errorf("invalid type %s for block hash", h.Type.Prefix())
	}
	if len(h.Hash.Hash) != h.Type.Len() {
		return fmt.Errorf("invalid len %d for block hash", len(h.Hash.Hash))
	}
	return nil
}

func (h *BlockHash) UnmarshalBinary(data []byte) error {
	if l := len(data); l > 0 && l != HashTypeBlock.Len() {
		return fmt.Errorf("invalid len %d for block hash", len(data))
	}
	h.Type = HashTypeBlock
	h.Hash.Hash = make([]byte, HashTypeBlock.Len())
	copy(h.Hash.Hash, data)
	return nil
}

func MustParseBlockHash(s string) BlockHash {
	h, err := ParseBlockHash(s)
	if err != nil {
		panic(err)
	}
	return h
}

func ParseBlockHash(s string) (BlockHash, error) {
	var h BlockHash
	if err := h.UnmarshalText([]byte(s)); err != nil {
		return h, err
	}
	return h, nil
}

// ProtocolHash
type ProtocolHash struct {
	Hash
}

func NewProtocolHash(buf []byte) ProtocolHash {
	b := make([]byte, len(buf))
	copy(b, buf)
	return ProtocolHash{Hash: NewHash(HashTypeProtocol, b)}
}

func (h ProtocolHash) Clone() ProtocolHash {
	return ProtocolHash{h.Hash.Clone()}
}

func (h ProtocolHash) IsEqual(h2 ProtocolHash) bool {
	return h.Hash.IsEqual(h2.Hash)
}

func (h *ProtocolHash) UnmarshalText(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if !strings.HasPrefix(string(data), PROTOCOL_HASH_PREFIX) {
		return fmt.Errorf("invalid prefix for protocol hash '%s'", string(data))
	}
	if err := h.Hash.UnmarshalText(data); err != nil {
		return err
	}
	if h.Type != HashTypeProtocol {
		return fmt.Errorf("invalid type %s for protocol hash", h.Type.Prefix())
	}
	if len(h.Hash.Hash) != h.Type.Len() {
		return fmt.Errorf("invalid len %d for protocol hash", len(h.Hash.Hash))
	}
	return nil
}

func (h *ProtocolHash) UnmarshalBinary(data []byte) error {
	if l := len(data); l > 0 && l != HashTypeProtocol.Len() {
		return fmt.Errorf("invalid len %d for protocol hash", len(data))
	}
	h.Type = HashTypeProtocol
	h.Hash.Hash = make([]byte, HashTypeProtocol.Len())
	copy(h.Hash.Hash, data)
	return nil
}

func ParseProtocolHash(s string) (ProtocolHash, error) {
	var h ProtocolHash
	if err := h.UnmarshalText([]byte(s)); err != nil {
		return h, err
	}
	return h, nil
}

func MustParseProtocolHash(s string) ProtocolHash {
	b, err := ParseProtocolHash(s)
	if err != nil {
		panic(err)
	}
	return b
}

func ParseProtocolHashSafe(s string) ProtocolHash {
	var h ProtocolHash
	h.UnmarshalText([]byte(s))
	return h
}

// OperationHash
type OperationHash struct {
	Hash
}

func NewOperationHash(buf []byte) OperationHash {
	b := make([]byte, len(buf))
	copy(b, buf)
	return OperationHash{Hash: NewHash(HashTypeOperation, b)}
}

func (h OperationHash) Clone() OperationHash {
	return OperationHash{h.Hash.Clone()}
}

func (h OperationHash) IsEqual(h2 OperationHash) bool {
	return h.Hash.IsEqual(h2.Hash)
}

func (h *OperationHash) UnmarshalText(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if !strings.HasPrefix(string(data), OPERATION_HASH_PREFIX) {
		return fmt.Errorf("invalid prefix for operation hash '%s'", string(data))
	}
	if err := h.Hash.UnmarshalText(data); err != nil {
		return err
	}
	if h.Type != HashTypeOperation {
		return fmt.Errorf("invalid type %s for operation hash", h.Type.Prefix())
	}
	if len(h.Hash.Hash) != h.Type.Len() {
		return fmt.Errorf("invalid len %d for operation hash", len(h.Hash.Hash))
	}
	return nil
}

func (h *OperationHash) UnmarshalBinary(data []byte) error {
	if l := len(data); l > 0 && l != HashTypeOperation.Len() {
		return fmt.Errorf("invalid len %d for operation hash", len(data))
	}
	h.Type = HashTypeOperation
	h.Hash.Hash = make([]byte, HashTypeOperation.Len())
	copy(h.Hash.Hash, data)
	return nil
}

func MustParseOperationHash(s string) OperationHash {
	b, err := ParseOperationHash(s)
	if err != nil {
		panic(err)
	}
	return b
}

func ParseOperationHash(s string) (OperationHash, error) {
	var h OperationHash
	if err := h.UnmarshalText([]byte(s)); err != nil {
		return h, err
	}
	return h, nil
}

// ExprHash
type ExprHash struct {
	Hash
}

func NewExprHash(buf []byte) ExprHash {
	b := make([]byte, len(buf))
	copy(b, buf)
	return ExprHash{Hash: NewHash(HashTypeScriptExpr, b)}
}

func (h ExprHash) Clone() ExprHash {
	return ExprHash{h.Hash.Clone()}
}

func (h ExprHash) IsEqual(h2 ExprHash) bool {
	return h.Hash.IsEqual(h2.Hash)
}

func (h *ExprHash) UnmarshalText(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if !strings.HasPrefix(string(data), SCRIPT_EXPR_HASH_PREFIX) {
		return fmt.Errorf("invalid prefix for script expression hash '%s'", string(data))
	}
	if err := h.Hash.UnmarshalText(data); err != nil {
		return err
	}
	if h.Type != HashTypeScriptExpr {
		return fmt.Errorf("invalid type %s for script expression hash", h.Type.Prefix())
	}
	if len(h.Hash.Hash) != h.Type.Len() {
		return fmt.Errorf("invalid len %d for script expression hash", len(h.Hash.Hash))
	}
	return nil
}

func (h *ExprHash) UnmarshalBinary(data []byte) error {
	if l := len(data); l > 0 && l != HashTypeScriptExpr.Len() {
		return fmt.Errorf("invalid len %d for script expression hash", len(data))
	}
	h.Type = HashTypeScriptExpr
	h.Hash.Hash = make([]byte, HashTypeScriptExpr.Len())
	copy(h.Hash.Hash, data)
	return nil
}

func MustParseExprHash(s string) ExprHash {
	b, err := ParseExprHash(s)
	if err != nil {
		panic(err)
	}
	return b
}

func ParseExprHash(s string) (ExprHash, error) {
	var h ExprHash
	if err := h.UnmarshalText([]byte(s)); err != nil {
		return h, err
	}
	return h, nil
}

// NonceHash
type NonceHash struct {
	Hash
}

func NewNonceHash(buf []byte) NonceHash {
	b := make([]byte, len(buf))
	copy(b, buf)
	return NonceHash{Hash: NewHash(HashTypeNonce, b)}
}

func (h NonceHash) Clone() NonceHash {
	return NonceHash{h.Hash.Clone()}
}

func (h NonceHash) IsEqual(h2 NonceHash) bool {
	return h.Hash.IsEqual(h2.Hash)
}

func (h *NonceHash) UnmarshalText(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if !strings.HasPrefix(string(data), NONCE_HASH_PREFIX) {
		return fmt.Errorf("invalid prefix for nonce hash '%s'", string(data))
	}
	if err := h.Hash.UnmarshalText(data); err != nil {
		return err
	}
	if h.Type != HashTypeNonce {
		return fmt.Errorf("invalid type %s for nonce hash '%s'", h.Type.Prefix(), string(data))
	}
	if len(h.Hash.Hash) != h.Type.Len() {
		return fmt.Errorf("invalid len %d for nonce hash '%s'", len(h.Hash.Hash), string(data))
	}
	return nil
}

func (h *NonceHash) UnmarshalBinary(data []byte) error {
	if l := len(data); l > 0 && l != HashTypeNonce.Len() {
		return fmt.Errorf("invalid len %d for nonce hash '%s'", len(data), string(data))
	}
	h.Type = HashTypeNonce
	h.Hash.Hash = make([]byte, HashTypeNonce.Len())
	copy(h.Hash.Hash, data)
	return nil
}

func ParseNonceHash(s string) (NonceHash, error) {
	var h NonceHash
	if err := h.UnmarshalText([]byte(s)); err != nil {
		return h, err
	}
	return h, nil
}

func MustParseNonceHash(s string) NonceHash {
	b, err := ParseNonceHash(s)
	if err != nil {
		panic(err)
	}
	return b
}

func ParseNonceHashSafe(s string) NonceHash {
	var h NonceHash
	h.UnmarshalText([]byte(s))
	return h
}

func decodeHash(hstr string) (Hash, error) {
	typ := ParseHashType(hstr)
	if typ == HashTypeInvalid {
		return Hash{}, ErrUnknownHashType
	}
	decoded, version, err := base58.CheckDecode(hstr, len(typ.PrefixBytes()), nil)
	if err != nil {
		if err == base58.ErrChecksum {
			return Hash{}, ErrChecksumMismatch
		}
		return Hash{}, fmt.Errorf("unknown hash format: %v", err.Error())
	}
	if bytes.Compare(version, typ.PrefixBytes()) != 0 {
		return Hash{}, fmt.Errorf("invalid prefix '%x' for decoded hash type '%s'", version, typ)
	}
	if have, want := len(decoded), typ.Len(); have != want {
		return Hash{}, fmt.Errorf("invalid length for decoded hash have=%d want=%d", have, want)
	}
	return Hash{
		Type: typ,
		Hash: decoded,
	}, nil
}

func encodeHash(typ HashType, h []byte) (string, error) {
	if typ == HashTypeInvalid {
		return "", ErrUnknownHashType
	}
	if have, want := len(h), typ.Len(); have != want {
		return "", fmt.Errorf("invalid hash length have=%d want=%d", have, want)
	}
	return base58.CheckEncode(h, typ.PrefixBytes()), nil
}
