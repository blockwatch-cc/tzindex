// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// see
// https://gitlab.com/nomadic-labs/mi-cho-coq/merge_requests/29/diffs
// https://gitlab.com/tezos/tezos/blob/510ae152082334b79d3364079cd466e07172dc3a/specs/migration_004_to_005.md#origination-script-transformation

package micheline

import (
	"encoding/hex"
	"fmt"
	"math/big"
)

// manager.tz
// https://blog.nomadic-labs.com/babylon-update-instructions-for-delegation-wallet-developers.html
const m_tz_script = "000000c602000000c105000764085e036c055f036d0000000325646f046c000000082564656661756c740501035d050202000000950200000012020000000d03210316051f02000000020317072e020000006a0743036a00000313020000001e020000000403190325072c020000000002000000090200000004034f0327020000000b051f02000000020321034c031e03540348020000001e020000000403190325072c020000000002000000090200000004034f0327034f0326034202000000080320053d036d0342"

// empty store (used as placeholder and to satisfy script decoding which expects storage)
const m_tz_store = "0000001a0a00000015000000000000000000000000000000000000000000"

func MakeManagerScript(managerHash []byte) (*Script, error) {
	// unpack manager.tz from binary blob
	buf, err := hex.DecodeString(m_tz_script + m_tz_store)
	if err != nil {
		return nil, fmt.Errorf("micheline: decoding manager script: %v", err)
	}
	script := NewScript()
	if err := script.UnmarshalBinary(buf); err != nil {
		return nil, fmt.Errorf("micheline: unmarshal manager script: %v", err)
	}

	// patch storage
	copy(script.Storage.Bytes, managerHash)

	return script, nil
}

// Patch params, storage and code
func (s *Script) MigrateToBabylonAddDo() {
	// add default entrypoint annotation
	s.Code.Param.Args[0].Anno = append([]string{"%default"}, s.Code.Param.Args[0].Anno...)

	// wrap params
	s.Code.Param.Args[0] = code(
		T_OR,
		code_anno(T_LAMBDA, "%do", code(T_UNIT), code(T_LIST, code(T_OPERATION))),
		s.Code.Param.Args[0],
	)

	// wrap storage
	s.Code.Storage.Args[0] = code(T_PAIR, code(T_KEY_HASH), s.Code.Storage.Args[0])

	// wrap code
	s.Code.Code = seq(
		code(I_DUP),
		code(I_CAR),
		code(I_IF_LEFT,
			DO_ENTRY(),
			seq(
				// # Transform the inputs to the original script types
				code(I_DIP, seq(code(I_CDR), code(I_DUP), code(I_CDR))),
				code(I_PAIR),
				// # 'default' entrypoint - original code
				s.Code.Code,
				// # Transform the outputs to the new script types
				code(I_SWAP),
				code(I_CAR),
				code(I_SWAP),
				UNPAIR(),
				code(I_DIP, seq(code(I_SWAP), code(I_PAIR))),
				code(I_PAIR),
			),
		),
	)
}

func (s *Script) MigrateToBabylonSetDelegate() {
	// add default entrypoint annotation
	s.Code.Param.Args[0].Anno = append([]string{"%default"}, s.Code.Param.Args[0].Anno...)

	// wrap params
	s.Code.Param.Args[0] = code(
		T_OR,
		code(T_OR,
			code_anno(T_KEY_HASH, "%set_delegate"),
			code_anno(T_UNIT, "%remove_delegate"),
		),
		s.Code.Param.Args[0],
	)

	// wrap storage
	s.Code.Storage.Args[0] = code(T_PAIR, code(T_KEY_HASH), s.Code.Storage.Args[0])

	// wrap code
	s.Code.Code = seq(
		code(I_DUP),
		code(I_CAR),
		code(I_IF_LEFT,
			DELEGATE_ENTRY(),
			seq(
				// # Transform the inputs to the original script types
				code(I_DIP, seq(code(I_CDR), code(I_DUP), code(I_CDR))),
				code(I_PAIR),
				// # 'default' entrypoint - original code
				s.Code.Code,
				// # Transform the outputs to the new script types
				code(I_SWAP),
				code(I_CAR),
				code(I_SWAP),
				UNPAIR(),
				code(I_DIP, seq(code(I_SWAP), code(I_PAIR))),
				code(I_PAIR),
			),
		),
	)
}

func code(c OpCode, args ...*Prim) *Prim {
	typ := PrimNullary
	switch len(args) {
	case 0:
		typ = PrimNullary
	case 1:
		typ = PrimUnary
	case 2:
		typ = PrimBinary
	default:
		typ = PrimVariadicAnno
	}
	return &Prim{Type: typ, OpCode: c, Args: args}
}

func code_anno(c OpCode, anno string, args ...*Prim) *Prim {
	p := code(c, args...)
	p.Anno = []string{anno}
	if p.Type != PrimVariadicAnno {
		p.Type++
	}
	return p
}

func seq(args ...*Prim) *Prim {
	return &Prim{Type: PrimSequence, Args: args}
}

func i64(i int64) *Prim {
	return ibig(big.NewInt(i))
}

func ibig(i *big.Int) *Prim {
	return &Prim{Type: PrimInt, Int: i}
}

func pbytes(b []byte) *Prim {
	return &Prim{Type: PrimBytes, Bytes: b}
}

func pstring(s string) *Prim {
	return &Prim{Type: PrimString, String: s}
}

// Macros
func DO_ENTRY() *Prim {
	return seq(
		// # Assert no token was sent:
		code(I_PUSH, code(T_MUTEZ), i64(0)), // PUSH mutez 0 ;
		code(I_AMOUNT),                      // AMOUNT ;
		ASSERT_CMPEQ(),                      // ASSERT_CMPEQ ;
		// # Assert that the sender is the manager
		DUUP(),                   // DUUP ;
		code(I_CDR),              // CDR ;
		code(I_CAR),              // CAR ;
		code(I_IMPLICIT_ACCOUNT), // IMPLICIT_ACCOUNT ;
		code(I_ADDRESS),          // ADDRESS ;
		code(I_SENDER),           // SENDER ;
		IFCMPNEQ( // IFCMPNEQ
			seq(
				code(I_SENDER), //   { SENDER ;
				code(I_PUSH, code(T_STRING), pstring("Only the owner can operate.")), // PUSH string "" ;
				code(I_PAIR),     //     PAIR ;
				code(I_FAILWITH), //     FAILWITH ;
			),
			seq( // # Execute the lambda argument
				code(I_UNIT),                  //     UNIT ;
				code(I_EXEC),                  //     EXEC ;
				code(I_DIP, seq(code(I_CDR))), //     DIP { CDR } ;
				code(I_PAIR),                  //     PAIR ;
			),
		),
	)
}

// 'set_delegate'/'remove_delegate' entrypoints
func DELEGATE_ENTRY() *Prim {
	return seq(
		// # Assert no token was sent:
		code(I_PUSH, code(T_MUTEZ), i64(0)), // PUSH mutez 0 ;
		code(I_AMOUNT),                      // AMOUNT ;
		ASSERT_CMPEQ(),                      // ASSERT_CMPEQ ;
		// # Assert that the sender is the manager
		DUUP(),                   // DUUP ;
		code(I_CDR),              // CDR ;
		code(I_CAR),              // CAR ;
		code(I_IMPLICIT_ACCOUNT), // IMPLICIT_ACCOUNT ;
		code(I_ADDRESS),          // ADDRESS ;
		code(I_SENDER),           // SENDER ;
		IFCMPNEQ( // IFCMPNEQ
			seq(
				code(I_SENDER), // SENDER ;
				code(I_PUSH, code(T_STRING), pstring("Only the owner can operate.")), // PUSH string "" ;
				code(I_PAIR),     // PAIR ;
				code(I_FAILWITH), // FAILWITH ;
			),
			seq( // # entrypoints
				code(I_DIP, seq(code(I_CDR), code(I_NIL, code(T_OPERATION)))), // DIP { CDR ; NIL operation } ;
				code(I_IF_LEFT,
					// # 'set_delegate' entrypoint
					seq(
						code(I_SOME),         // SOME ;
						code(I_SET_DELEGATE), // SET_DELEGATE ;
						code(I_CONS),         // CONS ;
						code(I_PAIR),         // PAIR ;
					),
					// # 'remove_delegate' entrypoint
					seq(
						code(I_DROP),                   // DROP ;
						code(I_NONE, code(T_KEY_HASH)), // NONE key_hash ;
						code(I_SET_DELEGATE),           // SET_DELEGATE ;
						code(I_CONS),                   // CONS ;
						code(I_PAIR),                   // PAIR ;
					),
				),
			),
		),
	)
}

func ASSERT_CMPEQ() *Prim {
	return seq(
		seq(code(I_COMPARE), code(I_EQ)),
		code(I_IF, seq(), seq(seq(code(I_UNIT), code(I_FAILWITH)))),
	)
}

func DUUP() *Prim {
	return seq(code(I_DIP, seq(code(I_DUP)), code(I_SWAP)))
}

func IFCMPNEQ(left, right *Prim) *Prim {
	return seq(
		code(I_COMPARE),
		code(I_EQ),
		code(I_IF, left, right),
	)
}

func UNPAIR() *Prim {
	return seq(seq(
		code(I_DUP),
		code(I_CAR),
		code(I_DIP, seq(code(I_CDR))),
	))
}
