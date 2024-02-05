// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"
	"fmt"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

var (
	ErrLedgerSkip = errors.New("skip")
)

// LedgerSchema describes the internal data storage structure of a token ledger.
// It is used to decode and reconcile balance updates and can be detected from
// matching the ledger's storage spec (i.e. bigmap key and value types).
type LedgerSchema byte

const (
	LedgerSchemaInvalid LedgerSchema = iota
	LedgerSchemaNFT1                 // 1 nft
	LedgerSchemaNFT2                 // 2 nft
	LedgerSchemaNFT3                 // 3 nft
	LedgerSchemaTzBTC                // 4
	LedgerSchemaFAa                  // 5
	LedgerSchemaFAb                  // 6
	LedgerSchemaFAc                  // 7
	LedgerSchemaFAd                  // 8 == nft1
	LedgerSchemaFAe                  // 9
	LedgerSchemaFAf                  // 10
	LedgerSchemaFAg                  // 11
	LedgerSchemaFAh                  // 12
	LedgerSchemaFAi                  // 13
	LedgerSchemaFAj                  // 14
	LedgerSchemaDomain               // 15
	LedgerSchemaTezFin               // 16
)

func (s LedgerSchema) IsValid() bool {
	return s != LedgerSchemaInvalid
}

// Detect legder type from interface and schema from bigmap type.
func DetectLedger(s micheline.Script) (schema LedgerSchema, typ TokenType, lid, mid int64) {
	typ = DetectTokenType(s)
	if typ == TokenTypeInvalid {
		return
	}

	maps := s.BigmapTypes()
	for _, name := range []string{
		"holders",      // tezos mandala uses the ledger bigmap for a different purpose
		"bigmap",       // tzBTC, StakerDAO have unnamed bigmaps
		"bigmap_0",     // tzBTC has an unnamed bigmap
		"ledger",       // most FA2, DEX etc.
		"balances",     // FA1.2 template
		"tokens",       // ctez, liquidity baking
		"account_info", // Quipu Gov token
		"records",      // tezos domains
		"shares",       // Flame DEX
	} {
		m, ok := maps[name]
		if !ok {
			continue
		}
		for t, v := range ledgerSpecs {
			if !v.Args[0].IsEqual(m.Prim.Args[0]) {
				continue
			}
			if !v.Args[1].IsEqual(m.Prim.Args[1]) {
				continue
			}
			schema = t
			lid = s.Bigmaps()[name]
			return
		}
	}
	mid = s.Bigmaps()["token_metadata"]
	return
}

type LedgerBalance struct {
	Owner   tezos.Address
	TokenId tezos.Z
	Balance tezos.Z
	Name    string // use for domain names

	// internal
	OwnerRef *TokenOwner // hook
	TokenRef *Token      // hook
	schema   LedgerSchema
}

func (b LedgerBalance) IsValid() bool {
	return b.Owner.IsValid()
}

func (b *LedgerBalance) UnmarshalPrim(prim micheline.Prim) error {
	// schema switch to select the chosen struct type with custom struct tags
	switch b.schema {
	case LedgerSchemaNFT1:
		return prim.Decode((*LedgerNFT1)(b))
	case LedgerSchemaNFT2:
		return prim.Decode((*LedgerNFT2)(b))
	case LedgerSchemaNFT3:
		return prim.Decode((*LedgerNFT3)(b))
	case LedgerSchemaTzBTC:
		return prim.Decode((*LedgerTzBTC)(b))
	case LedgerSchemaFAa:
		return prim.Decode((*LedgerFAa)(b))
	case LedgerSchemaFAb:
		return prim.Decode((*LedgerFAb)(b))
	case LedgerSchemaFAc:
		return prim.Decode((*LedgerFAc)(b))
	case LedgerSchemaFAd:
		return prim.Decode((*LedgerFAd)(b))
	case LedgerSchemaFAe:
		return prim.Decode((*LedgerFAe)(b))
	case LedgerSchemaFAf:
		return prim.Decode((*LedgerFAf)(b))
	case LedgerSchemaFAg:
		return prim.Decode((*LedgerFAg)(b))
	case LedgerSchemaFAh:
		return prim.Decode((*LedgerFAh)(b))
	case LedgerSchemaFAi:
		return prim.Decode((*LedgerFAi)(b))
	case LedgerSchemaFAj:
		return prim.Decode((*LedgerFAj)(b))
	case LedgerSchemaDomain:
		return prim.Decode((*LedgerDomain)(b))
	case LedgerSchemaTezFin:
		return prim.Decode((*LedgerTezFin)(b))
	default:
		return fmt.Errorf("unsupported ledger type %d", b.schema)
	}
}

func (s LedgerSchema) DecodeBalanceUpdates(upd micheline.BigmapEvents, id int64) ([]*LedgerBalance, error) {
	var out []*LedgerBalance
	for i, v := range upd {
		if v.Id != id {
			continue
		}
		switch v.Action {
		case micheline.DiffActionAlloc, micheline.DiffActionCopy:
			// ignore
		case micheline.DiffActionUpdate:
			// extract from ledger
			bal, err := s.DecodeBalance(micheline.NewPair(v.Key, v.Value))
			if err != nil {
				if err != ErrLedgerSkip {
					return nil, fmt.Errorf("decoding ledger=%d schema=%d upd=%d key=%s val=%s err=%s",
						id, s, i, v.Key.Dump(), v.Value.Dump(), err)
				}
			} else {
				out = append(out, bal)
			}
		case micheline.DiffActionRemove:
			// set balance to zero
			bal, err := s.DecodeBalance(micheline.NewPair(v.Key, micheline.InvalidPrim))
			if err != nil {
				if err != ErrLedgerSkip {
					return nil, fmt.Errorf("ledger=%d remove action for key=%s: %v",
						id, v.Key.Dump(), err)
				}
			} else {
				out = append(out, bal)
			}
		}
	}
	return out, nil

}

func (s LedgerSchema) DecodeBalance(prim micheline.Prim) (bal *LedgerBalance, err error) {
	bal = &LedgerBalance{schema: s}
	err = prim.Decode(bal)
	return
}

// var nftLedgers = []LedgerSchema{
//     LedgerSchemaNFT1,
//     LedgerSchemaNFT2,
//     LedgerSchemaNFT3,
// }

var ledgerSpecs = map[LedgerSchema]micheline.Prim{
	// 1 @key: {0: address, 1: nat}     @value: nat
	LedgerSchemaNFT1: micheline.NewPairType(
		micheline.NewPairType(
			micheline.NewCode(micheline.T_ADDRESS), // owner
			micheline.NewCode(micheline.T_NAT),     // token_id
		),
		micheline.NewCode(micheline.T_NAT), // balance
	),
	// 2 @key: nat  @value: address
	LedgerSchemaNFT2: micheline.NewPairType(
		micheline.NewCode(micheline.T_NAT),     // token_id
		micheline.NewCode(micheline.T_ADDRESS), // owner
	),
	// 3 @key: {0: nat, 1: address}     @value: nat
	LedgerSchemaNFT3: micheline.NewPairType(
		micheline.NewPairType(
			micheline.NewCode(micheline.T_NAT),     // token_id
			micheline.NewCode(micheline.T_ADDRESS), // owner
		),
		micheline.NewCode(micheline.T_NAT), // balance
	),
	// 4 packed tzBTC
	// @key: packed bytes @value: packed bytes
	LedgerSchemaTzBTC: micheline.NewPairType(
		micheline.NewCode(micheline.T_BYTES), // packed key
		micheline.NewCode(micheline.T_BYTES), // packed val
	),
	// 5 FA 1.2 with approvals
	// USDtez ledger KT1LN4LPSqTMS7Sd2CJw4bbDGRkMv2t68Fy9
	// PAUL   ledger KT19DUSZw7mfeEATrbWVPHRrWNVbNnmfFAE6
	// @key: address  @value: {0: nat, 1: map { @key: address @val: nat }
	LedgerSchemaFAa: micheline.NewPairType(
		micheline.NewCode(micheline.T_ADDRESS), // owner
		micheline.NewPairType(
			micheline.NewCode(micheline.T_NAT), // balance
			micheline.NewMapType( // approvals
				micheline.NewCode(micheline.T_ADDRESS), // spender
				micheline.NewCode(micheline.T_NAT),     // value
			),
		),
	),
	// 6 FA 1.2 with approvals (flipped order)
	// - HEH balances KT1G1cCRNBgQ48mVDjopHjEmTN5Sbtar8nn9
	// @key: address  @value: {0: nat, 1: map { @key: address @val: nat }
	LedgerSchemaFAb: micheline.NewPairType(
		micheline.NewCode(micheline.T_ADDRESS), // owner
		micheline.NewPairType(
			micheline.NewMapType( // approvals
				micheline.NewCode(micheline.T_ADDRESS), // spender
				micheline.NewCode(micheline.T_NAT),     // value
			),
			micheline.NewCode(micheline.T_NAT), // balance
		),
	),
	// 7 FA1.2 without approvals
	// - Ctez             tokens KT1SjXiUX63QvdNMcM2m492f7kuf8JxXRLp4
	// - Kalamint         ledger KT1A5P4ejnLix13jtadsfV9GCnXLMNnab8UT (FA2 !!)
	// - liquidity-baking tokens KT1AafHA1C1vk959wvHWBispY9Y2f3fxBUUo
	// - Staker Gov       token.ledger KT1AEfeckNbdEYwaMKkytBwPJPycz7jdSGea
	// @key: address  @value: nat
	LedgerSchemaFAc: micheline.NewPairType(
		micheline.NewCode(micheline.T_ADDRESS), // owner
		micheline.NewCode(micheline.T_NAT),     // balance
	),
	// 8 FA2 without operators/approvals
	// - WRAP assets.ledger KT18fp5rcTW7mbWDmzFwjLDUhs5MeJmagDSZ
	// - hDAO ledger KT1AFA2mwNUMNd4SsujE1YYp29vd8BZejyKW
	// - many NFTs use this too (i.e. Objkt.com collection)
	// @key: {0:address, 1:nat}  @value: nat
	LedgerSchemaFAd: micheline.NewPairType(
		micheline.NewPairType(
			micheline.NewCode(micheline.T_ADDRESS), // owner
			micheline.NewCode(micheline.T_NAT),     // token_id
		),
		micheline.NewCode(micheline.T_NAT), // balance
	),
	// 9 FA2 with balance map and allowance set
	// - QuipuSwap Governance Token (name: account_info) KT193D4vozYnhGJQVtw7CoxxqphqUEEwK6Vb
	// @key: address
	// @value: {map: nat -> nat } // token_id -> balance
	//         {set: address } // allowances
	LedgerSchemaFAe: micheline.NewPairType(
		micheline.NewCode(micheline.T_ADDRESS), // owner
		micheline.NewPairType(
			micheline.NewMapType( // balances
				micheline.NewCode(micheline.T_NAT), // token_id
				micheline.NewCode(micheline.T_NAT), // balance
			),
			micheline.NewSetType(micheline.NewCode(micheline.T_ADDRESS)), // allowances
		),
	),
	// 10 Quipuswap legacy FA2 with allowance set, balance and frozen
	// - QuipuSwap dex    legder  KT1Qm3urGqkRsWsovGzNb2R81c3dSfxpteHG
	// @key: address
	// @value: {set: address } // allowances
	//         {map: nat -> nat } // token_id -> balance
	LedgerSchemaFAf: micheline.NewPairType(
		micheline.NewCode(micheline.T_ADDRESS), // owner
		micheline.NewPairType(
			micheline.NewPairType(
				micheline.NewSetType(micheline.NewCode(micheline.T_ADDRESS)), // allowances
				micheline.NewCode(micheline.T_NAT),                           // balance
			),
			micheline.NewCode(micheline.T_NAT), // frozen_balance
		),
	),
	// 11 FA2 with single balance and allowance set
	// - fDAO Token ledger KT1KPoyzkj82Sbnafm6pfesZKEhyCpXwQfMc
	// - Flame      ledger KT1Wa8yqRBpFCusJWgcQyjhRz7hUQAmFxW7j
	// @key: address
	// @value: {set: address } // allowances
	//         nat             // balance
	LedgerSchemaFAg: micheline.NewPairType(
		micheline.NewCode(micheline.T_ADDRESS), // owner
		micheline.NewPairType(
			micheline.NewSetType(micheline.NewCode(micheline.T_ADDRESS)), // allowances
			micheline.NewCode(micheline.T_NAT),                           // balance
		),
	),
	// 12 FA2 Lugh Stablecoin
	// - ledger KT1JBNFcB5tiycHNdYGYCtR3kk6JaJysUCi8
	// @key: {0:address, 1:nat} // owner, token_id
	// @value: {0:nat, 1:bool}  // balance, lock
	LedgerSchemaFAh: micheline.NewPairType(
		micheline.NewPairType(
			micheline.NewCode(micheline.T_ADDRESS), // owner
			micheline.NewCode(micheline.T_NAT),     // token_id
		),
		micheline.NewPairType(
			micheline.NewCode(micheline.T_NAT),  // balance
			micheline.NewCode(micheline.T_BOOL), // lock
		),
	),

	// 13 Flame Dex
	// - shares KT1PRtrP7pKZ3PSLwgfTwt8hD39bxVojoKuX
	// @key: {0:address, 1:nat} // owner, token_id
	// @value: {nat, nat, nat}  // balance, last_rewards_per_share, rewards
	LedgerSchemaFAi: micheline.NewPairType(
		micheline.NewPairType(
			micheline.NewCode(micheline.T_ADDRESS), // owner
			micheline.NewCode(micheline.T_NAT),     // token_id
		),
		micheline.NewPairType(
			micheline.NewPairType(
				micheline.NewCode(micheline.T_NAT), // balance
				micheline.NewCode(micheline.T_NAT), // last_rewards_per_share
			),
			micheline.NewCode(micheline.T_NAT), // rewards
		),
	),

	// 14 MAG
	// - ledger KT1H5KJDxuM9DURSfttepebb6Cn7GbvAAT45
	// @key: address // owner
	// @value: {map: address => nat} // approvals
	//         nat // balance
	//         nat // frozenBalance
	//         nat // reward
	//         nat // rewardDebt
	//         nat // usedVotes
	LedgerSchemaFAj: micheline.NewPairType(
		micheline.NewCode(micheline.T_ADDRESS), // owner
		micheline.NewPairType(
			micheline.NewPairType(
				micheline.NewMapType( // approvals
					micheline.NewCode(micheline.T_ADDRESS), // spender
					micheline.NewCode(micheline.T_NAT),     // allowance
				),
				micheline.NewPairType(
					micheline.NewCode(micheline.T_NAT), // balance
					micheline.NewCode(micheline.T_NAT), // frozenBalance
				),
			),
			micheline.NewPairType(
				micheline.NewCode(micheline.T_NAT), // reward
				micheline.NewPairType(
					micheline.NewCode(micheline.T_NAT), // rewardDebt
					micheline.NewCode(micheline.T_NAT), // usedVotes
				),
			),
		),
	),

	// 15 Tezos Domains
	// - records KT1GBZmSxmnKJXGMdMLbugPfLyUPmuLSMwKS
	// Note: records contain a tzip12_token_id, but key is bytes (domain name)
	//       sale example: https://tzstats.com/onzVjWA6LeBSEYd4ZEpiyfiHo7z2kcuqmeM94YD2t7fmoxQ1Zyr
	LedgerSchemaDomain: micheline.NewPairType(
		micheline.NewCode(micheline.T_BYTES), // domain name
		micheline.NewPairType(
			micheline.NewPairType(
				micheline.NewPairType(
					micheline.NewOptType(micheline.NewCode(micheline.T_ADDRESS)), // address
					micheline.NewMapType( // data
						micheline.NewCode(micheline.T_STRING), // key
						micheline.NewCode(micheline.T_BYTES),  // value
					),
				),
				micheline.NewPairType(
					micheline.NewOptType(micheline.NewCode(micheline.T_BYTES)), // expiry_key
					micheline.NewMapType( // internal_data
						micheline.NewCode(micheline.T_STRING), // key
						micheline.NewCode(micheline.T_BYTES),  // value
					),
				),
			),
			micheline.NewPairType(
				micheline.NewPairType(
					micheline.NewCode(micheline.T_NAT),     // level
					micheline.NewCode(micheline.T_ADDRESS), // owner
				),
				micheline.NewOptType(micheline.NewCode(micheline.T_NAT)), // tzip12_token_id
			),
		),
	),

	// 16 TezFin
	LedgerSchemaTezFin: micheline.NewPairType(
		micheline.NewCode(micheline.T_ADDRESS), // owner
		micheline.NewPairType(
			micheline.NewPairType( // accountBorrows
				micheline.NewCode(micheline.T_NAT), // interestIndex
				micheline.NewCode(micheline.T_NAT), // principal
			),
			micheline.NewPairType(
				micheline.NewMapType( // approvals
					micheline.NewCode(micheline.T_ADDRESS),
					micheline.NewCode(micheline.T_NAT),
				),
				micheline.NewCode(micheline.T_NAT), // balance
			),
		),
	),
}

// 1 @key: {0: address, 1: nat}     @value: nat
type LedgerNFT1 LedgerBalance

func (b *LedgerNFT1) UnmarshalPrim(prim micheline.Prim) error {
	var alias struct {
		Owner   tezos.Address `prim:"owner,path=0/0"`
		TokenId tezos.Z       `prim:"token_id,path=0/1"`
		Balance tezos.Z       `prim:"balance,path=1,nofail"`
	}
	err := prim.Decode(&alias)
	if err == nil {
		b.Owner = alias.Owner
		b.TokenId = alias.TokenId
		b.Balance = alias.Balance
	}
	return err
}

// 2 @key: nat  @value: address
type LedgerNFT2 LedgerBalance

func (b *LedgerNFT2) UnmarshalPrim(prim micheline.Prim) error {
	var alias struct {
		TokenId tezos.Z       `prim:"token_id,path=0"`
		Owner   tezos.Address `prim:"owner,path=1"`
	}
	err := prim.Decode(&alias)
	if err == nil {
		b.Owner = alias.Owner
		b.TokenId = alias.TokenId
		if b.Owner.IsValid() {
			b.Balance.SetInt64(1)
		} else {
			b.Balance.SetInt64(0)
		}
	}
	return err
}

// 3 @key: {0: nat, 1: address}     @value: nat
type LedgerNFT3 LedgerBalance

func (b *LedgerNFT3) UnmarshalPrim(prim micheline.Prim) error {
	var alias struct {
		TokenId tezos.Z       `prim:"token_id,path=0/0"`
		Owner   tezos.Address `prim:"owner,path=0/1"`
		Balance tezos.Z       `prim:"balance,path=1,nofail"`
	}
	err := prim.Decode(&alias)
	if err == nil {
		b.Owner = alias.Owner
		b.TokenId = alias.TokenId
		b.Balance = alias.Balance
	}
	return err
}

// 4 tzBTC (packed bytes)
// @key:{0: string = ledger, 1: address}
// @value: {0: int, 1: map { @key: address @val: nat }
type LedgerTzBTC LedgerBalance

func (b *LedgerTzBTC) UnmarshalPrim(prim micheline.Prim) error {
	var alias struct {
		Key     string        `prim:"key,path=0/0,nofail"`
		Owner   tezos.Address `prim:"owner,path=0/1,nofail"`
		Balance tezos.Z       `prim:"balance,path=1/0,nofail"`
	}
	up, _ := prim.UnpackAll()
	err := up.Decode(&alias)
	if err == nil {
		if alias.Key == "ledger" {
			b.Owner = alias.Owner
			b.Balance = alias.Balance
		} else {
			err = ErrLedgerSkip
		}
	}
	return err
}

// 5 FA 1.2 with approvals
// USDtez ledger KT1LN4LPSqTMS7Sd2CJw4bbDGRkMv2t68Fy9
// PAUL   ledger KT19DUSZw7mfeEATrbWVPHRrWNVbNnmfFAE6
// @key: address  @value: {0: nat, 1: map { @key: address @val: nat }
type LedgerFAa LedgerBalance

func (b *LedgerFAa) UnmarshalPrim(prim micheline.Prim) error {
	var alias struct {
		Owner   tezos.Address `prim:"owner,path=0"`
		Balance tezos.Z       `prim:"balance,path=1/0,nofail"`
	}
	err := prim.Decode(&alias)
	if err == nil {
		b.Owner = alias.Owner
		b.Balance = alias.Balance
	}
	return err
}

// 6 FA 1.2 with approvals (flipped order)
// - HEH balances KT1G1cCRNBgQ48mVDjopHjEmTN5Sbtar8nn9
// @key: address  @value: {0: nat, 1: map { @key: address @val: nat }
type LedgerFAb LedgerBalance

func (b *LedgerFAb) UnmarshalPrim(prim micheline.Prim) error {
	var alias struct {
		Owner   tezos.Address `prim:"owner,path=0"`
		Balance tezos.Z       `prim:"balance,path=1/1,nofail"`
	}
	err := prim.Decode(&alias)
	if err == nil {
		b.Owner = alias.Owner
		b.Balance = alias.Balance
	}
	return err
}

// 7 FA1.2 without approvals
// - Ctez             tokens KT1SjXiUX63QvdNMcM2m492f7kuf8JxXRLp4
// - Kalamint         ledger KT1A5P4ejnLix13jtadsfV9GCnXLMNnab8UT (FA2 !!)
// - liquidity-baking tokens KT1AafHA1C1vk959wvHWBispY9Y2f3fxBUUo
// - Staker Gov       token.ledger KT1AEfeckNbdEYwaMKkytBwPJPycz7jdSGea
// @key: address  @value: nat
type LedgerFAc LedgerBalance

func (b *LedgerFAc) UnmarshalPrim(prim micheline.Prim) error {
	var alias struct {
		Owner   tezos.Address `prim:"owner,path=0"`
		Balance tezos.Z       `prim:"balance,path=1,nofail"`
	}
	err := prim.Decode(&alias)
	if err == nil {
		b.Owner = alias.Owner
		b.Balance = alias.Balance
	}
	return err
}

// 8 FA2 without operators/approvals (== NFT1)
// - WRAP assets.ledger KT18fp5rcTW7mbWDmzFwjLDUhs5MeJmagDSZ
// - hDAO ledger KT1AFA2mwNUMNd4SsujE1YYp29vd8BZejyKW
// @key: {0:address, 1:nat}  @value: nat
type LedgerFAd LedgerBalance

func (b *LedgerFAd) UnmarshalPrim(prim micheline.Prim) error {
	var alias struct {
		Owner   tezos.Address `prim:"owner,path=0/0"`
		TokenId tezos.Z       `prim:"token_id,path=0/1"`
		Balance tezos.Z       `prim:"balance,path=1,nofail"`
	}
	err := prim.Decode(&alias)
	if err == nil {
		b.Owner = alias.Owner
		b.TokenId = alias.TokenId
		b.Balance = alias.Balance
	}
	return err
}

// 9 FA2 with balance map and allowance set
// - QuipuSwap Governance Token (name: account_info) KT193D4vozYnhGJQVtw7CoxxqphqUEEwK6Vb
// @key: address
// @value: {map: nat -> nat } // token_id -> balance
//
//	{set: address } // allowances
type LedgerFAe LedgerBalance

// ERRO KT193D4vozYnhGJQVtw7CoxxqphqUEEwK6Vb: decoding ledger=12043 schema=9 upd=0
// key={"bytes":"00000254027db659dff16a82c0dfd62a1c0cb2172a93"}
// val={
//     "prim":"Pair",
//     "args":[
//         [
//             {
//                 "prim":"Elt",
//                 "args":[
//                     {"int":"0"},
//                     {"int":"0"}
//                 ]
//             }
//         ],
//         []
//     ]} err=math/big: cannot unmarshal "" into a *big.Int

func (b *LedgerFAe) UnmarshalPrim(prim micheline.Prim) error {
	var alias struct {
		Owner   tezos.Address `prim:"owner,path=0"`
		TokenId tezos.Z       `prim:"token_id,path=1/0/0/0,nofail"`
		Balance tezos.Z       `prim:"balance,path=1/0/0/1,nofail"`
	}
	err := prim.Decode(&alias)
	if err == nil {
		b.Owner = alias.Owner
		b.TokenId = alias.TokenId
		b.Balance = alias.Balance
	}
	return err
}

// 10 Quipuswap legacy FA2 with allowance set, balance and frozen
// - QuipuSwap dex    legder  KT1Qm3urGqkRsWsovGzNb2R81c3dSfxpteHG
// @key: address
// @value: {0: { 0:{set: address}, 1:nat } // allowances, balance
//
//	 1: nat                         // frozen_balance
//	}
type LedgerFAf LedgerBalance

func (b *LedgerFAf) UnmarshalPrim(prim micheline.Prim) error {
	var alias struct {
		Owner   tezos.Address `prim:"owner,path=0"`
		Balance tezos.Z       `prim:"balance,path=1/0/1,nofail"`
	}
	err := prim.Decode(&alias)
	if err == nil {
		b.Owner = alias.Owner
		b.Balance = alias.Balance
	}
	return err
}

// 11 FA2 with single balance and allowance set
// - fDAO Token ledger KT1KPoyzkj82Sbnafm6pfesZKEhyCpXwQfMc
// - Flame      ledger KT1Wa8yqRBpFCusJWgcQyjhRz7hUQAmFxW7j
// @key: address
// @value: {set: address } // allowances
//
//	nat             // balance
type LedgerFAg LedgerBalance

func (b *LedgerFAg) UnmarshalPrim(prim micheline.Prim) error {
	var alias struct {
		Owner   tezos.Address `prim:"owner,path=0"`
		Balance tezos.Z       `prim:"balance,path=1/1,nofail"`
	}
	err := prim.Decode(&alias)
	if err == nil {
		b.Owner = alias.Owner
		b.Balance = alias.Balance
	}
	return err
}

// 12 FA2 Lugh Stablecoin
// - ledger KT1JBNFcB5tiycHNdYGYCtR3kk6JaJysUCi8
// @key: {0:address, 1:nat} // owner, token_id
// @value: {0:nat, 1:bool}  // balance, lock
type LedgerFAh LedgerBalance

func (b *LedgerFAh) UnmarshalPrim(prim micheline.Prim) error {
	var alias struct {
		Owner   tezos.Address `prim:"owner,path=0/0"`
		TokenId tezos.Z       `prim:"token_id,path=0/1"`
		Balance tezos.Z       `prim:"balance,path=1/0,nofail"`
	}
	err := prim.Decode(&alias)
	if err == nil {
		b.Owner = alias.Owner
		b.TokenId = alias.TokenId
		b.Balance = alias.Balance
	}
	return err
}

// 13 Flame Dex
// - shares KT1PRtrP7pKZ3PSLwgfTwt8hD39bxVojoKuX
// @key: {0:address, 1:nat} // owner, token_id
// @value: {nat, nat, nat}  // balance, last_rewards_per_share, rewards
type LedgerFAi LedgerBalance

func (b *LedgerFAi) UnmarshalPrim(prim micheline.Prim) error {
	var alias struct {
		Owner   tezos.Address `prim:"owner,path=0/0"`
		TokenId tezos.Z       `prim:"token_id,path=0/1"`
		Balance tezos.Z       `prim:"balance,path=1/0/0,nofail"`
	}
	err := prim.Decode(&alias)
	if err == nil {
		b.Owner = alias.Owner
		b.TokenId = alias.TokenId
		b.Balance = alias.Balance
	}
	return err
}

// 14 MAG
// - ledger KT1H5KJDxuM9DURSfttepebb6Cn7GbvAAT45
// @key: address // owner
// @value: {map: address => nat} // approvals
//
//	nat // balance
//	nat // frozenBalance
//	nat // reward
//	nat // rewardDebt
//	nat // usedVotes
type LedgerFAj LedgerBalance

func (b *LedgerFAj) UnmarshalPrim(prim micheline.Prim) error {
	var alias struct {
		Owner   tezos.Address `prim:"owner,path=0"`
		Balance tezos.Z       `prim:"balance,path=1/0/1/0,nofail"`
	}
	err := prim.Decode(&alias)
	if err == nil {
		b.Owner = alias.Owner
		b.Balance = alias.Balance
	}
	return err
}

// // 15 Tezos Domains
// // - records KT1GBZmSxmnKJXGMdMLbugPfLyUPmuLSMwKS
// // Note: records contain a tzip12_token_id, but key is bytes (domain name)
// //       sale example: https://tzstats.com/onzVjWA6LeBSEYd4ZEpiyfiHo7z2kcuqmeM94YD2t7fmoxQ1Zyr
// LedgerSchemaDomain: micheline.NewPairType(
//
//	micheline.NewCode(micheline.T_BYTES), // domain name
//	micheline.NewPairType(
//	    micheline.NewPairType(
//	        micheline.NewPairType(
//	            micheline.NewOptType(micheline.NewCode(micheline.T_ADDRESS)), // address
//	            micheline.NewMapType( // data
//	                micheline.NewCode(micheline.T_STRING), // key
//	                micheline.NewCode(micheline.T_BYTES),  // value
//	            ),
//	        ),
//	        micheline.NewPairType(
//	            micheline.NewOptType(micheline.NewCode(micheline.T_BYTES)), // expiry_key
//	            micheline.NewMapType( // internal_data
//	                micheline.NewCode(micheline.T_STRING), // key
//	                micheline.NewCode(micheline.T_BYTES),  // value
//	            ),
//	        ),
//	    ),
//	    micheline.NewPairType(
//	        micheline.NewPairType(
//	            micheline.NewCode(micheline.T_NAT),     // level
//	            micheline.NewCode(micheline.T_ADDRESS), // owner
//	        ),
//	        micheline.NewOptType(micheline.NewCode(micheline.T_NAT)), // tzip12_token_id
//	    ),
//	),
//
// ),
type LedgerDomain LedgerBalance

func (b *LedgerDomain) UnmarshalPrim(prim micheline.Prim) error {
	var alias struct {
		Name    []byte        `prim:"name,path=0"`
		Owner   tezos.Address `prim:"owner,path=1/1/0/1"`
		TokenId tezos.Z       `prim:"token_id,path=1/1/1/0,nofail"`
	}
	alias.TokenId.SetInt64(-1)
	err := prim.Decode(&alias)
	if err == nil {
		if alias.TokenId.Int64() < 0 {
			err = ErrLedgerSkip
		} else {
			b.Name, err = UnpackTnsString(string(alias.Name))
			b.Owner = alias.Owner
			b.TokenId = alias.TokenId
			b.Balance.SetInt64(1)
		}
	}
	return err
}

// // 16 TezFin
// LedgerSchemaTezFin: micheline.NewPairType(
//
//	micheline.NewCode(micheline.T_ADDRESS), // owner
//	micheline.NewPairType(
//	    micheline.NewPairType( // accountBorrows
//	        micheline.NewCode(micheline.T_NAT), // interestIndex
//	        micheline.NewCode(micheline.T_NAT), // principal
//	    ),
//	    micheline.NewPairType(
//	        micheline.NewMapType( // approvals
//	            micheline.NewCode(micheline.T_ADDRESS),
//	            micheline.NewCode(micheline.T_NAT),
//	        ),
//	        micheline.NewCode(micheline.T_NAT), // balance
//	    ),
//	),
//
// ),
type LedgerTezFin LedgerBalance

func (b *LedgerTezFin) UnmarshalPrim(prim micheline.Prim) error {
	var alias struct {
		Owner   tezos.Address `prim:"owner,path=0"`
		Balance tezos.Z       `prim:"balance,path=1/1/1"`
	}
	err := prim.Decode(&alias)
	if err == nil {
		b.Owner = alias.Owner
		b.Balance = alias.Balance
	}
	return err
}
