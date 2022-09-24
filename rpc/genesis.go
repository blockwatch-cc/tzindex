// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/echa/bson"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

// lacking the algorithm to compute KT1 addresses from content,
// we hard-code all mainnet vesting KT1 addresses here
var vestingContractAddrs = []tezos.Address{
	tezos.MustParseAddress("KT1QuofAgnsWffHzLA7D78rxytJruGHDe7XG"),
	tezos.MustParseAddress("KT1CSKPf2jeLpMmrgKquN2bCjBTkAcAdRVDy"),
	tezos.MustParseAddress("KT1SLWhfqPtQq7f4zLomh8BNgDeprF9B6d2M"),
	tezos.MustParseAddress("KT1WPEis2WhAc2FciM2tZVn8qe6pCBe9HkDp"),
	tezos.MustParseAddress("KT1Um7ieBEytZtumecLqGeL56iY6BuWoBgio"),
	tezos.MustParseAddress("KT1Cz7TyVFvHxXpxLS57RFePrhTGisUpPhvD"),
	tezos.MustParseAddress("KT1Q1kfbvzteafLvnGz92DGvkdypXfTGfEA3"),
	tezos.MustParseAddress("KT1PDAELuX7CypUHinUgFgGFskKs7ytwh5Vw"),
	tezos.MustParseAddress("KT1A56dh8ivKNvLiLVkjYPyudmnY2Ti5Sba3"),
	tezos.MustParseAddress("KT1RUT25eGgo9KKWXfLhj1xYjghAY1iZ2don"),
	tezos.MustParseAddress("KT1FuFDZGdw86p6krdBUKoZfEMkcUmezqX5o"),
	tezos.MustParseAddress("KT1THsDNgHtN56ew9VVCAUWnqPC81pqAxCEp"),
	tezos.MustParseAddress("KT1EWLAQGPMF2uhtVRPaCH2vtFVN36Njdr6z"),
	tezos.MustParseAddress("KT1FN5fcNNcgieGjzxbVEPWUpJGwZEpzNGA8"),
	tezos.MustParseAddress("KT1TcAHw5gpejyemwRtdNyFKGBLc4qwA5gtw"),
	tezos.MustParseAddress("KT1VsSxSXUkgw6zkBGgUuDXXuJs9ToPqkrCg"),
	tezos.MustParseAddress("KT1Msatnmdy24sQt6knzpALs4tvHfSPPduA2"),
	tezos.MustParseAddress("KT1LZFMGrdnPjRLsCZ1aEDUAF5myA5Eo4rQe"),
	tezos.MustParseAddress("KT1LQ99RfGcmFe98PiBcGXuyjBkWzAcoXXhW"),
	tezos.MustParseAddress("KT1Kfbk3B6NYPCPohPBDU3Hxf5Xeyy9PdkNp"),
	tezos.MustParseAddress("KT1DnfT4hfikoMY3uiPE9mQV4y3Xweramb2k"),
	tezos.MustParseAddress("KT19xDbLsvQKnp9xqfDNPWJbKJJmV93dHDUa"),
	tezos.MustParseAddress("KT1HvwFnXteMbphi7mfPDhCWkZSDvXEz8iyv"),
	tezos.MustParseAddress("KT1KRyTaxCAM3YRquifEe29BDbUKNhJ6hdtx"),
	tezos.MustParseAddress("KT1Gow8VzXZx3Akn5kvjACqnjnyYBxQpzSKr"),
	tezos.MustParseAddress("KT1W148mcjmfvr9J2RvWcGHxsAFApq9mcfgT"),
	tezos.MustParseAddress("KT1D5NmtDtgCwPxYNb2ZK2But6dhNLs1T1bV"),
	tezos.MustParseAddress("KT1TzamC1SCj68ia2E4q2GWZeT24yRHvUZay"),
	tezos.MustParseAddress("KT1CM1g1o9RKDdtDKgcBWE59X2KgTc2TcYtC"),
	tezos.MustParseAddress("KT1FL3C6t9Lyfskyb6rQrCRQTnf7M9t587VM"),
	tezos.MustParseAddress("KT1JW6PwhfaEJu6U3ENsxUeja48AdtqSoekd"),
	tezos.MustParseAddress("KT1VvXEpeBpreAVpfp4V8ZujqWu2gVykwXBJ"),
}

type GenesisData struct {
	Accounts    []*X0
	Contracts   []*X1
	Commitments []*X2
}

// bootstrap account with or without known public key
type X0 struct {
	Addr  tezos.Address
	Key   tezos.Key
	Value int64
}

// bootstrap contract
type X1 struct {
	Addr     tezos.Address
	Delegate tezos.Address
	Value    int64
	Script   micheline.Script
}

// commitment
type X2 struct {
	Addr  tezos.Address
	Value int64
}

func (b *GenesisData) Supply() int64 {
	var s int64
	for _, v := range b.Accounts {
		s += v.Value
	}
	for _, v := range b.Contracts {
		s += v.Value
	}
	for _, v := range b.Commitments {
		s += v.Value
	}
	return s
}

func (b *GenesisData) UnmarshalText(data []byte) error {
	buf := make([]byte, hex.DecodedLen(len(data)))
	if _, err := hex.Decode(buf, data); err != nil {
		return err
	}
	// decode BSON
	encoded := &bootstrap{}
	if err := bson.Unmarshal(buf[4:], encoded); err != nil {
		return err
	}
	// convert BSON to Structs
	acc, err := encoded.DecodeAccounts()
	if err != nil {
		return err
	}
	b.Accounts = acc
	contracts, err := encoded.DecodeContracts()
	if err != nil {
		return err
	}
	b.Contracts = contracts
	commit, err := encoded.DecodeCommitments()
	if err != nil {
		return err
	}
	b.Commitments = commit
	return nil
}

// BSON data types
type bootstrap struct {
	Accounts    [][]string  `bson:"bootstrap_accounts"`
	Contracts   []*contract `bson:"bootstrap_contracts"`
	Commitments [][]string  `bson:"commitments"`
}

type contract struct {
	Delegate string `bson:"delegate"`
	Value    string `bson:"amount"`
	Script   bson.M `bson:"script"`
}

func (b *bootstrap) DecodeContracts() ([]*X1, error) {
	// ignore non-mainnet contract lists (we don't know their addresses)
	if len(b.Contracts) != len(vestingContractAddrs) {
		return nil, nil
	}
	c := make([]*X1, len(b.Contracts))
	for i, v := range b.Contracts {
		c[i] = &X1{
			Addr: vestingContractAddrs[i],
		}
		addr, err := tezos.ParseAddress(v.Delegate)
		if err != nil {
			return nil, err
		}
		c[i].Delegate = addr
		value, err := strconv.ParseInt(v.Value, 10, 64)
		if err != nil {
			return nil, err
		}
		c[i].Value = value

		// script unmarshalling BSON -> JSON -> Micheline
		buf, err := json.Marshal(v.Script)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(buf, &c[i].Script); err != nil {
			return nil, err
		}

		// skip when this does not look like a vesting contract
		isVesting := true
		switch {
		case !c[i].Script.Storage.IsValid():
			isVesting = false
		case len(c[i].Script.Storage.Args) == 0:
			isVesting = false
		case !c[i].Script.Storage.Args[0].IsValid():
			isVesting = false
		case len(c[i].Script.Storage.Args[0].Args) == 0:
			isVesting = false
		case !c[i].Script.Storage.Args[0].Args[1].IsValid():
			isVesting = false
		case len(c[i].Script.Storage.Args[0].Args[1].Args) == 0:
			isVesting = false
		case !c[i].Script.Storage.Args[0].Args[1].Args[0].IsValid():
			isVesting = false
		case len(c[i].Script.Storage.Args[0].Args[1].Args[0].Args) == 0:
			isVesting = false
		}

		if !isVesting {
			continue
		}

		// patch initial storage (convert strings to bytes) to circumvent tezos
		// origination bug
		// - replace edpk strings with byte sequences
		// - replace delegate addesses with binary pkh 00 TT AAAA...

		// keygroups >> signatories
		for _, v := range c[i].Script.Storage.Args[0].Args[1].Args[0].Args {
			for _, vv := range v.Args[0].Args {
				edpk, err := tezos.ParseHash(vv.String)
				if err != nil {
					return nil, fmt.Errorf("decoding signatory key %s: %w", vv.String, err)
				}
				vv.Type = micheline.PrimBytes
				vv.Bytes = append([]byte{0}, edpk.Hash...)
				vv.String = ""
			}
		}

		// only the first 8 contracts have authorizers set
		if i < 8 {
			// pour_dest
			pair := c[i].Script.Storage.Args[1].Args[1].Args[0].Args
			dest, err := tezos.ParseAddress(pair[0].String)
			if err != nil {
				return nil, fmt.Errorf("decoding pour_dest %s: %w", pair[0].String, err)
			}
			pair[0].Type = micheline.PrimBytes
			pair[0].Bytes, _ = dest.MarshalBinary()
			pair[0].String = ""

			// pour_authorizer
			edpk, err := tezos.ParseHash(pair[1].String)
			if err != nil {
				return nil, fmt.Errorf("decoding pour_authorizer key %s: %w", pair[1].String, err)
			}
			// replace with byte sequence
			pair[1].Type = micheline.PrimBytes
			pair[1].Bytes = append([]byte{0}, edpk.Hash...)
			pair[1].String = ""
		}
	}
	return c, nil
}

func (b *bootstrap) DecodeAccounts() ([]*X0, error) {
	acc := make([]*X0, len(b.Accounts))
	for i, v := range b.Accounts {
		acc[i] = &X0{}
		pk := v[0]
		switch {
		case tezos.HasKeyPrefix(pk):
			key, err := tezos.ParseKey(pk)
			if err != nil {
				return nil, err
			}
			acc[i].Key = key
			acc[i].Addr = key.Address()
		case tezos.HasAddressPrefix(pk):
			addr, err := tezos.ParseAddress(pk)
			if err != nil {
				return nil, err
			}
			acc[i].Addr = addr
		}
		amount, err := strconv.ParseInt(v[1], 10, 64)
		if err != nil {
			return nil, err
		}
		acc[i].Value = amount
	}
	return acc, nil
}

func (b *bootstrap) DecodeCommitments() ([]*X2, error) {
	c := make([]*X2, len(b.Commitments))
	for i, v := range b.Commitments {
		c[i] = &X2{}
		// [ $Blinded public key hash, $mutez ]
		addr, err := tezos.ParseAddress(v[0])
		if err != nil {
			return nil, err
		}
		c[i].Addr = addr
		amount, err := strconv.ParseInt(v[1], 10, 64)
		if err != nil {
			return nil, err
		}
		c[i].Value = amount
	}
	return c, nil
}
