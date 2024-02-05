// Copyright (c) 2024 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package profile

import (
	"fmt"

	m "blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

var (
	StorageType = m.MustParseType(`{"prim":"pair","args":[{"prim":"pair","args":[{"prim":"set","annots":["%claims"],"args":[{"prim":"pair","args":[{"prim":"pair","args":[{"prim":"string","annots":["%url"]},{"prim":"bytes","annots":["%sign"]}]},{"prim":"string"}]}]},{"prim":"string","annots":["%contract_type"]}]},{"prim":"pair","args":[{"prim":"big_map","annots":["%metadata"],"args":[{"prim":"string"},{"prim":"bytes"}]},{"prim":"address","annots":["%owner"]}]}]}`)
	ParamsType  = m.MustParseType(`{"prim":"pair","args":[{"prim":"list","args":[{"prim":"pair","args":[{"prim":"pair","args":[{"prim":"string"},{"prim":"bytes"}]},{"prim":"string"}]}]},{"prim":"bool"}]}`)
)

type Claim struct {
	Url  string         `json:"url"   prim:"url,path=0/0"`
	Sign tezos.HexBytes `json:"sign"  prim:"sign,path=0/1"`
}

type Params struct {
	Claims   []Claim `prim:"claims,path=0"`
	IsUpdate bool    `prim:"updated,path=1"`
}

type Storage struct {
	Claims       []Claim       `json:"claims"`
	ContractType string        `json:"contract_type"`
	Owner        tezos.Address `json:"owner"`
}

func (d *Decoder) DecodeParams(buf []byte) (p Params, err error) {
	var params m.Parameters
	if err = params.UnmarshalBinary(buf); err != nil {
		err = fmt.Errorf("%s unmarshal params: %v", d.Namespace(), err)
		return
	}
	err = params.Value.Decode(&p)
	if err != nil {
		err = fmt.Errorf("%s decoding params from %s: %v", d.Namespace(), params.Value.Dump(), err)
	}
	return
}

func (d *Decoder) DecodeStorage(buf []byte) (c Storage, err error) {
	var prim m.Prim
	if err = prim.UnmarshalBinary(buf); err != nil {
		err = fmt.Errorf("%s unmarshal storage: %v", d.Namespace(), err)
		return
	}
	val := m.NewValue(StorageType, prim)
	err = val.Unmarshal(&c)
	if err != nil {
		err = fmt.Errorf("%s decoding storage from %s: %v", d.Namespace(), prim.Dump(), err)
	}
	return
}
