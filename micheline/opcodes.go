// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package micheline

import (
	"fmt"
)

type OpCode byte

func (o OpCode) Byte() byte {
	return byte(o)
}

// Michelson V1 Primitives
const (
	// Keys
	K_PARAMETER OpCode = iota // 00
	K_STORAGE                 // 01
	K_CODE                    // 02

	// Data
	D_FALSE // 03
	D_ELT   // 04
	D_LEFT  // 05
	D_NONE  // 06
	D_PAIR  // 07
	D_RIGHT // 08
	D_SOME  // 09
	D_TRUE  // 0A
	D_UNIT  // 0B

	// instructions
	I_PACK             // 0C
	I_UNPACK           // 0D
	I_BLAKE2B          // 0E
	I_SHA256           // 0F
	I_SHA512           // 10
	I_ABS              // 11
	I_ADD              // 12
	I_AMOUNT           // 13
	I_AND              // 14
	I_BALANCE          // 15
	I_CAR              // 16
	I_CDR              // 17
	I_CHECK_SIGNATURE  // 18
	I_COMPARE          // 19
	I_CONCAT           // 1A
	I_CONS             // 1B
	I_CREATE_ACCOUNT   // 1C
	I_CREATE_CONTRACT  // 1D
	I_IMPLICIT_ACCOUNT // 1E
	I_DIP              // 1F
	I_DROP             // 20
	I_DUP              // 21
	I_EDIV             // 22
	I_EMPTY_MAP        // 23
	I_EMPTY_SET        // 24
	I_EQ               // 25
	I_EXEC             // 26
	I_FAILWITH         // 27
	I_GE               // 28
	I_GET              // 29
	I_GT               // 2A
	I_HASH_KEY         // 2B
	I_IF               // 2C
	I_IF_CONS          // 2D
	I_IF_LEFT          // 2E
	I_IF_NONE          // 2F
	I_INT              // 30
	I_LAMBDA           // 31
	I_LE               // 32
	I_LEFT             // 33
	I_LOOP             // 34
	I_LSL              // 35
	I_LSR              // 36
	I_LT               // 37
	I_MAP              // 38
	I_MEM              // 39
	I_MUL              // 3A
	I_NEG              // 3B
	I_NEQ              // 3C
	I_NIL              // 3D
	I_NONE             // 3E
	I_NOT              // 3F
	I_NOW              // 40
	I_OR               // 41
	I_PAIR             // 42
	I_PUSH             // 43
	I_RIGHT            // 44
	I_SIZE             // 45
	I_SOME             // 46
	I_SOURCE           // 47
	I_SENDER           // 48
	I_SELF             // 49
	I_STEPS_TO_QUOTA   // 4A
	I_SUB              // 4B
	I_SWAP             // 4C
	I_TRANSFER_TOKENS  // 4D
	I_SET_DELEGATE     // 4E
	I_UNIT             // 4F
	I_UPDATE           // 50
	I_XOR              // 51
	I_ITER             // 52
	I_LOOP_LEFT        // 53
	I_ADDRESS          // 54
	I_CONTRACT         // 55
	I_ISNAT            // 56
	I_CAST             // 57
	I_RENAME           // 58

	// Types
	T_BOOL      // 59
	T_CONTRACT  // 5A
	T_INT       // 5B
	T_KEY       // 5C
	T_KEY_HASH  // 5D
	T_LAMBDA    // 5E
	T_LIST      // 5F
	T_MAP       // 60
	T_BIG_MAP   // 61
	T_NAT       // 62
	T_OPTION    // 63
	T_OR        // 64
	T_PAIR      // 65
	T_SET       // 66
	T_SIGNATURE // 67
	T_STRING    // 68
	T_BYTES     // 69
	T_MUTEZ     // 6A
	T_TIMESTAMP // 6B
	T_UNIT      // 6C
	T_OPERATION // 6D
	T_ADDRESS   // 6E

	// v002 addition
	I_SLICE // 6F

	// v005 addition
	// https://blog.nomadic-labs.com/michelson-updates-in-005.html
	I_DIG           // 70
	I_DUG           // 71
	I_EMPTY_BIG_MAP // 72
	I_APPLY         // 73
	T_CHAIN_ID      // 74
	I_CHAIN_ID      // 75

	// v008 additions
	I_LEVEL                 // 76
	I_SELF_ADDRESS          // 77
	T_NEVER                 // 78
	I_NEVER                 // 79
	I_UNPAIR                // 7A
	I_VOTING_POWER          // 7B
	I_TOTAL_VOTING_POWER    // 7C
	I_KECCAK                // 7D
	I_SHA3                  // 7E
	I_PAIRING_CHECK         // 7F
	T_BLS12_381_G1          // 80
	T_BLS12_381_G2          // 81
	T_BLS12_381_FR          // 82
	T_SAPLING_STATE         // 83
	T_SAPLING_TRANSACTION   // 84
	I_SAPLING_EMPTY_STATE   // 85
	I_SAPLING_VERIFY_UPDATE // 86
	T_TICKET                // 87
	I_TICKET                // 88
	I_READ_TICKET           // 89
	I_SPLIT_TICKET          // 8A
	I_JOIN_TICKETS          // 8B
	I_GET_AND_UPDATE        // 8C
)

func (op OpCode) IsValid() bool {
	return op <= I_GET_AND_UPDATE
}

var (
	opCodeToString = map[OpCode]string{
		K_PARAMETER:             "parameter",
		K_STORAGE:               "storage",
		K_CODE:                  "code",
		D_FALSE:                 "False",
		D_ELT:                   "Elt",
		D_LEFT:                  "Left",
		D_NONE:                  "None",
		D_PAIR:                  "Pair",
		D_RIGHT:                 "Right",
		D_SOME:                  "Some",
		D_TRUE:                  "True",
		D_UNIT:                  "Unit",
		I_PACK:                  "PACK",
		I_UNPACK:                "UNPACK",
		I_BLAKE2B:               "BLAKE2B",
		I_SHA256:                "SHA256",
		I_SHA512:                "SHA512",
		I_ABS:                   "ABS",
		I_ADD:                   "ADD",
		I_AMOUNT:                "AMOUNT",
		I_AND:                   "AND",
		I_BALANCE:               "BALANCE",
		I_CAR:                   "CAR",
		I_CDR:                   "CDR",
		I_CHECK_SIGNATURE:       "CHECK_SIGNATURE",
		I_COMPARE:               "COMPARE",
		I_CONCAT:                "CONCAT",
		I_CONS:                  "CONS",
		I_CREATE_ACCOUNT:        "CREATE_ACCOUNT",
		I_CREATE_CONTRACT:       "CREATE_CONTRACT",
		I_IMPLICIT_ACCOUNT:      "IMPLICIT_ACCOUNT",
		I_DIP:                   "DIP",
		I_DROP:                  "DROP",
		I_DUP:                   "DUP",
		I_EDIV:                  "EDIV",
		I_EMPTY_MAP:             "EMPTY_MAP",
		I_EMPTY_SET:             "EMPTY_SET",
		I_EQ:                    "EQ",
		I_EXEC:                  "EXEC",
		I_FAILWITH:              "FAILWITH",
		I_GE:                    "GE",
		I_GET:                   "GET",
		I_GT:                    "GT",
		I_HASH_KEY:              "HASH_KEY",
		I_IF:                    "IF",
		I_IF_CONS:               "IF_CONS",
		I_IF_LEFT:               "IF_LEFT",
		I_IF_NONE:               "IF_NONE",
		I_INT:                   "INT",
		I_LAMBDA:                "LAMBDA",
		I_LE:                    "LE",
		I_LEFT:                  "LEFT",
		I_LOOP:                  "LOOP",
		I_LSL:                   "LSL",
		I_LSR:                   "LSR",
		I_LT:                    "LT",
		I_MAP:                   "MAP",
		I_MEM:                   "MEM",
		I_MUL:                   "MUL",
		I_NEG:                   "NEG",
		I_NEQ:                   "NEQ",
		I_NIL:                   "NIL",
		I_NONE:                  "NONE",
		I_NOT:                   "NOT",
		I_NOW:                   "NOW",
		I_OR:                    "OR",
		I_PAIR:                  "PAIR",
		I_PUSH:                  "PUSH",
		I_RIGHT:                 "RIGHT",
		I_SIZE:                  "SIZE",
		I_SOME:                  "SOME",
		I_SOURCE:                "SOURCE",
		I_SENDER:                "SENDER",
		I_SELF:                  "SELF",
		I_STEPS_TO_QUOTA:        "STEPS_TO_QUOTA",
		I_SUB:                   "SUB",
		I_SWAP:                  "SWAP",
		I_TRANSFER_TOKENS:       "TRANSFER_TOKENS",
		I_SET_DELEGATE:          "SET_DELEGATE",
		I_UNIT:                  "UNIT",
		I_UPDATE:                "UPDATE",
		I_XOR:                   "XOR",
		I_ITER:                  "ITER",
		I_LOOP_LEFT:             "LOOP_LEFT",
		I_ADDRESS:               "ADDRESS",
		I_CONTRACT:              "CONTRACT",
		I_ISNAT:                 "ISNAT",
		I_CAST:                  "CAST",
		I_RENAME:                "RENAME",
		T_BOOL:                  "bool",
		T_CONTRACT:              "contract",
		T_INT:                   "int",
		T_KEY:                   "key",
		T_KEY_HASH:              "key_hash",
		T_LAMBDA:                "lambda",
		T_LIST:                  "list",
		T_MAP:                   "map",
		T_BIG_MAP:               "big_map",
		T_NAT:                   "nat",
		T_OPTION:                "option",
		T_OR:                    "or",
		T_PAIR:                  "pair",
		T_SET:                   "set",
		T_SIGNATURE:             "signature",
		T_STRING:                "string",
		T_BYTES:                 "bytes",
		T_MUTEZ:                 "mutez",
		T_TIMESTAMP:             "timestamp",
		T_UNIT:                  "unit",
		T_OPERATION:             "operation",
		T_ADDRESS:               "address",
		I_SLICE:                 "SLICE",
		I_DIG:                   "DIG",
		I_DUG:                   "DUG",
		I_EMPTY_BIG_MAP:         "EMPTY_BIG_MAP",
		I_APPLY:                 "APPLY",
		T_CHAIN_ID:              "chain_id",
		I_CHAIN_ID:              "CHAIN_ID",
		I_LEVEL:                 "LEVEL",
		I_SELF_ADDRESS:          "SELF_ADDRESS",
		T_NEVER:                 "never",
		I_NEVER:                 "NEVER",
		I_UNPAIR:                "UNPAIR",
		I_VOTING_POWER:          "VOTING_POWER",
		I_TOTAL_VOTING_POWER:    "TOTAL_VOTING_POWER",
		I_KECCAK:                "KECCAK",
		I_SHA3:                  "SHA3",
		I_PAIRING_CHECK:         "PAIRING_CHECK",
		T_BLS12_381_G1:          "bls12_381_g1",
		T_BLS12_381_G2:          "bls12_381_g2",
		T_BLS12_381_FR:          "bls12_381_fr",
		T_SAPLING_STATE:         "sapling_state",
		T_SAPLING_TRANSACTION:   "sapling_transaction",
		I_SAPLING_EMPTY_STATE:   "SAPLING_EMPTY_STATE",
		I_SAPLING_VERIFY_UPDATE: "SAPLING_VERIFY_UPDATE",
		T_TICKET:                "ticket",
		I_TICKET:                "TICKET",
		I_READ_TICKET:           "READ_TICKET",
		I_SPLIT_TICKET:          "SPLIT_TICKET",
		I_JOIN_TICKETS:          "JOIN_TICKETS",
		I_GET_AND_UPDATE:        "GET_AND_UPDATE",
	}
	stringToOp map[string]OpCode
)

func init() {
	stringToOp = make(map[string]OpCode)
	for n, v := range opCodeToString {
		stringToOp[v] = n
	}
}

func (op OpCode) String() string {
	str, ok := opCodeToString[op]
	if !ok {
		return fmt.Sprintf("Unknown michelson opcode 0x%x", int(op))
	}
	return str
}

func (op OpCode) MarshalText() ([]byte, error) {
	return []byte(op.String()), nil
}

func ParseOpCode(str string) (OpCode, error) {
	op, ok := stringToOp[str]
	if !ok {
		return 255, fmt.Errorf("Unknown michelson primitive %s", str)
	}
	return op, nil
}

func (op OpCode) IsType() bool {
	switch op {
	case T_BOOL,
		T_CONTRACT,
		T_INT,
		T_KEY,
		T_KEY_HASH,
		T_LAMBDA,
		T_LIST,
		T_MAP,
		T_BIG_MAP,
		T_NAT,
		T_OPTION,
		T_OR,
		T_PAIR,
		T_SET,
		T_SIGNATURE,
		T_STRING,
		T_BYTES,
		T_MUTEZ,
		T_TIMESTAMP,
		T_UNIT,
		T_OPERATION,
		T_ADDRESS,
		T_CHAIN_ID,
		T_NEVER,
		T_BLS12_381_G1,
		T_BLS12_381_G2,
		T_BLS12_381_FR,
		T_SAPLING_STATE,
		T_SAPLING_TRANSACTION,
		T_TICKET:
		return true
	default:
		return false
	}
}

func (op OpCode) IsKey() bool {
	switch op {
	case K_PARAMETER, K_STORAGE, K_CODE:
		return true
	default:
		return false
	}
}

func (op OpCode) Type() OpCode {
	if op.IsType() {
		return op
	}
	switch op {
	case K_PARAMETER, K_STORAGE, K_CODE, D_UNIT:
		return T_UNIT
	case D_FALSE, D_TRUE:
		return T_BOOL
	case D_LEFT, D_RIGHT:
		return T_OR
	case D_NONE, D_SOME:
		return T_OPTION
	case D_PAIR:
		return T_PAIR
	case D_ELT:
		return T_MAP // may also be T_SET, T_BIG_MAP
	default:
		return T_OPERATION
	}
}
