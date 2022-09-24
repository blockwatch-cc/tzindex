// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"encoding/binary"
	// "fmt"
	"strconv"
	"sync"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/rpc"
)

var contractPool = &sync.Pool{
	New: func() interface{} { return new(Contract) },
}

type ContractID uint64

func (id ContractID) Value() uint64 {
	return uint64(id)
}

// Contract holds code and info about smart contracts on the Tezos blockchain.
type Contract struct {
	RowId         ContractID           `pack:"I,pk"     json:"row_id"`
	Address       tezos.Address        `pack:"H,snappy" json:"address"`
	AccountId     AccountID            `pack:"A"        json:"account_id"`
	CreatorId     AccountID            `pack:"C"        json:"creator_id"`
	FirstSeen     int64                `pack:"f"        json:"first_seen"`
	LastSeen      int64                `pack:"l"        json:"last_seen"`
	StorageSize   int64                `pack:"z"        json:"storage_size"`
	StoragePaid   int64                `pack:"y"        json:"storage_paid"`
	StorageBurn   int64                `pack:"Y"        json:"storage_burn"`
	Script        []byte               `pack:"s,snappy" json:"script"`
	Storage       []byte               `pack:"g,snappy" json:"storage"`
	InterfaceHash uint64               `pack:"i,snappy" json:"iface_hash"`
	CodeHash      uint64               `pack:"c,snappy" json:"code_hash"`
	StorageHash   uint64               `pack:"x,snappy" json:"storage_hash"`
	CallStats     []byte               `pack:"S,snappy" json:"call_stats"`
	Features      micheline.Features   `pack:"F,snappy" json:"features"`
	Interfaces    micheline.Interfaces `pack:"n,snappy" json:"interfaces"`

	IsDirty bool              `pack:"-" json:"-"` // indicates an update happened
	IsNew   bool              `pack:"-" json:"-"` // new contract, used during migration
	script  *micheline.Script `pack:"-" json:"-"` // cached decoded script
	params  micheline.Type    `pack:"-" json:"-"` // cached param type
	storage micheline.Type    `pack:"-" json:"-"` // cached storage type
}

// Ensure Contract implements the pack.Item interface.
var _ pack.Item = (*Contract)(nil)

// assuming the op was successful!
func NewContract(acc *Account, oop *rpc.Origination, op *Op, dict micheline.ConstantDict, p *tezos.Params) *Contract {
	c := AllocContract()
	c.Address = acc.Address.Clone()
	c.AccountId = acc.RowId
	c.CreatorId = op.SenderId
	c.FirstSeen = op.Height
	c.LastSeen = op.Height
	res := oop.Result()
	c.StorageSize = res.StorageSize
	c.StoragePaid = res.PaidStorageSizeDiff
	c.StorageBurn += c.StoragePaid * p.CostPerByte
	if oop.Script != nil {
		c.Features = oop.Script.Features()
		if c.Features.Contains(micheline.FeatureGlobalConstant) {
			oop.Script.ExpandConstants(dict)
			c.Features |= oop.Script.Features()
		}
		c.Script, _ = oop.Script.MarshalBinary()
		c.Storage, _ = oop.Script.Storage.MarshalBinary()
		c.InterfaceHash = oop.Script.InterfaceHash()
		c.CodeHash = oop.Script.CodeHash()
		c.StorageHash = oop.Script.StorageHash()
		c.Interfaces = oop.Script.Interfaces()
		ep, _ := oop.Script.Entrypoints(false)
		c.CallStats = make([]byte, 4*len(ep))
	}
	flags := oop.BabylonFlags(p.Version)
	if flags.IsSpendable() {
		c.Features |= micheline.FeatureSpendable
	}
	if flags.IsDelegatable() {
		c.Features |= micheline.FeatureDelegatable
	}
	c.IsNew = true
	c.IsDirty = true
	return c
}

func NewInternalContract(acc *Account, iop rpc.InternalResult, op *Op, dict micheline.ConstantDict, p *tezos.Params) *Contract {
	c := AllocContract()
	c.Address = acc.Address.Clone()
	c.AccountId = acc.RowId
	c.CreatorId = op.CreatorId // may be another KT1
	c.FirstSeen = op.Height
	c.LastSeen = op.Height
	res := iop.Result
	c.StorageSize = res.StorageSize
	c.StoragePaid = res.PaidStorageSizeDiff
	c.StorageBurn += c.StoragePaid * p.CostPerByte
	if iop.Script != nil {
		c.Features = iop.Script.Features()
		if c.Features.Contains(micheline.FeatureGlobalConstant) {
			iop.Script.ExpandConstants(dict)
			c.Features |= iop.Script.Features()
		}
		c.Script, _ = iop.Script.MarshalBinary()
		c.Storage, _ = iop.Script.Storage.MarshalBinary()
		c.InterfaceHash = iop.Script.InterfaceHash()
		c.CodeHash = iop.Script.CodeHash()
		c.StorageHash = iop.Script.StorageHash()
		c.Interfaces = iop.Script.Interfaces()
		ep, _ := iop.Script.Entrypoints(false)
		c.CallStats = make([]byte, 4*len(ep))
	}
	// pre-babylon did not have any internal originations
	// c.Features |= micheline.FeatureSpendable | micheline.FeatureDelegatable
	c.IsNew = true
	c.IsDirty = true
	return c
}

func NewImplicitContract(acc *Account, res rpc.ImplicitResult, op *Op, p *tezos.Params) *Contract {
	c := AllocContract()
	c.Address = acc.Address.Clone()
	c.AccountId = acc.RowId
	c.CreatorId = acc.CreatorId
	c.FirstSeen = op.Height
	c.LastSeen = op.Height
	c.StorageSize = res.StorageSize
	c.StoragePaid = res.PaidStorageSizeDiff
	c.StorageBurn += c.StoragePaid * p.CostPerByte
	if res.Script != nil {
		c.Script, _ = res.Script.MarshalBinary()
		c.Storage, _ = res.Script.Storage.MarshalBinary()
		c.InterfaceHash = res.Script.InterfaceHash()
		c.CodeHash = res.Script.CodeHash()
		c.StorageHash = res.Script.StorageHash()
		c.Features = res.Script.Features()
		c.Interfaces = res.Script.Interfaces()
		ep, _ := res.Script.Entrypoints(false)
		c.CallStats = make([]byte, 4*len(ep))
	}
	c.IsNew = true
	c.IsDirty = true
	return c
}

// create manager.tz contract, used during migration only
func NewManagerTzContract(a *Account, height int64) (*Contract, error) {
	c := AllocContract()
	c.Address = a.Address.Clone()
	c.AccountId = a.RowId
	c.CreatorId = a.CreatorId
	c.FirstSeen = a.FirstSeen
	c.LastSeen = height
	script, _ := micheline.MakeManagerScript(a.Address.Bytes())
	c.Script, _ = script.MarshalBinary()
	c.Storage, _ = script.Storage.MarshalBinary()
	c.InterfaceHash = script.InterfaceHash()
	c.CodeHash = script.CodeHash()
	c.StorageHash = script.StorageHash()
	c.Features = script.Features()
	c.Interfaces = script.Interfaces()
	c.StorageSize = 232           // fixed 232 bytes
	c.StoragePaid = 0             // noone paid for this
	c.CallStats = make([]byte, 8) // 2 entrypoints, 'do' (0) and 'default' (1)
	binary.BigEndian.PutUint32(c.CallStats[4:8], uint32(a.NTx))
	c.IsNew = true
	c.IsDirty = true
	return c, nil
}

func NewRollupContract(acc *Account, op *Op, p *tezos.Params) *Contract {
	c := AllocContract()
	c.Address = acc.Address.Clone()
	c.AccountId = acc.RowId
	c.CreatorId = op.SenderId
	c.FirstSeen = op.Height
	c.LastSeen = op.Height
	c.StorageSize = 4000 // toru fixed, tx_rollup_origination_size
	c.StoragePaid = 4000 // toru fixed, tx_rollup_origination_size
	c.StorageBurn += c.StoragePaid * p.CostPerByte
	// no script
	// 7+1 toru ops excl origination are defined
	// first is the fake `deposit` op for tickets
	c.CallStats = make([]byte, 4*8)
	c.IsNew = true
	c.IsDirty = true
	return c
}

func AllocContract() *Contract {
	return contractPool.Get().(*Contract)
}

func (c *Contract) Free() {
	c.Reset()
	contractPool.Put(c)
}

func (c Contract) ID() uint64 {
	return uint64(c.RowId)
}

func (c *Contract) SetID(id uint64) {
	c.RowId = ContractID(id)
}

func (c Contract) String() string {
	return c.Address.String()
}

func (c *Contract) Reset() {
	c.RowId = 0
	c.Address = tezos.Address{}
	c.AccountId = 0
	c.CreatorId = 0
	c.FirstSeen = 0
	c.LastSeen = 0
	c.StorageSize = 0
	c.StoragePaid = 0
	c.StorageBurn = 0
	c.Script = nil
	c.Storage = nil
	c.InterfaceHash = 0
	c.CodeHash = 0
	c.StorageHash = 0
	c.CallStats = nil
	c.Features = 0
	c.Interfaces = nil
	c.IsDirty = false
	c.IsNew = false
	c.script = nil
	c.params = micheline.Type{}
	c.storage = micheline.Type{}
}

func (c *Contract) Update(op *Op, p *tezos.Params) bool {
	c.LastSeen = op.Height
	c.IncCallStats(op.Entrypoint)
	c.IsDirty = true
	if op.Storage != nil && op.StorageHash != c.StorageHash {
		c.Storage = op.Storage
		c.StorageHash = op.StorageHash
		c.StorageSize = int64(len(c.Storage))
		c.StoragePaid += op.StoragePaid
		c.StorageBurn += op.StoragePaid * p.CostPerByte
		return true
	}
	return false
}

func (c *Contract) Rollback(drop, last *Op, p *tezos.Params) {
	if last != nil {
		c.LastSeen = last.Height
		if last.Storage != nil {
			c.Storage = last.Storage
			c.StorageSize = int64(len(c.Storage))
			c.StorageHash = last.StorageHash
		}
	} else if c.script != nil {
		// back to origination
		c.Storage, _ = c.script.Storage.MarshalBinary()
		c.StorageHash = c.script.Storage.Hash64()
		c.LastSeen = c.FirstSeen
		c.StorageSize = int64(len(c.Storage))
	}
	c.StoragePaid -= drop.StoragePaid
	c.StorageBurn -= drop.StoragePaid * p.CostPerByte
	c.DecCallStats(drop.Entrypoint)
	c.IsDirty = true
}

func (c *Contract) ListRollupCallStats() map[string]int {
	res := make(map[string]int, len(c.CallStats)>>2)
	for i, v := range []string{
		"deposit", // fake, /sigh/ :(
		"tx_rollup_submit_batch",
		"tx_rollup_commit",
		"tx_rollup_return_bond",
		"tx_rollup_finalize_commitment",
		"tx_rollup_remove_commitment",
		"tx_rollup_rejection",
		"tx_rollup_dispatch_tickets",
	} {
		res[v] = int(binary.BigEndian.Uint32(c.CallStats[i*4:]))
	}
	return res
}

func (c *Contract) ListCallStats() map[string]int {
	if c.Address.Type == tezos.AddressTypeToru {
		return c.ListRollupCallStats()
	}
	// list entrypoint names first
	pTyp, _, err := c.LoadType()
	if err != nil {
		return nil
	}

	ep, err := pTyp.Entrypoints(false)
	if err != nil {
		return nil
	}

	// sort entrypoint map by id, we only need names here
	byId := make([]string, len(ep))
	for _, v := range ep {
		byId[v.Id] = v.Name
	}

	res := make(map[string]int, len(c.CallStats)>>2)
	for i, name := range byId {
		res[name] = int(binary.BigEndian.Uint32(c.CallStats[i*4:]))
	}
	return res
}

func (c *Contract) NamedBigmaps(m []*BigmapAlloc) map[string]int64 {
	if len(m) == 0 {
		return nil
	}
	_, sTyp, err := c.LoadType()
	if err != nil {
		return nil
	}
	named := make(map[string]int64)

	// find bigmap typedefs in script
	bigmaps, _ := sTyp.FindOpCodes(micheline.T_BIG_MAP)

	// unpack micheline types into tzgo types for matching
	// this resolves ambiguities from different comb pair expressions
	types := make([]micheline.Typedef, len(bigmaps))
	for i, v := range bigmaps {
		types[i] = micheline.Type{Prim: v}.Typedef("")
	}

	// match bigmap allocs to type annotations using type comparison
	for i, v := range m {
		kt, vt := v.GetKeyType().Typedef(""), v.GetValueType().Typedef("")
		var name string
		for _, typ := range types {
			if !typ.Left().Equal(kt) {
				continue
			}
			if !typ.Right().Equal(vt) {
				continue
			}
			name = typ.Name
			// some bigmap types may be reused (different bigmap use the same type)
			// so be carful not overwriting existing matches
			if _, ok := named[name]; !ok {
				break
			}
		}
		// generate a unique name when annots are missing
		if name == "" {
			name = "bigmap_" + strconv.Itoa(i)
		}
		// make sure name is not a duplicate
		if _, ok := named[name]; ok {
			var c int
			for {
				n := name + "_" + strconv.Itoa(c)
				if _, ok := named[n]; !ok {
					name = n
					break
				}
				c++
			}
		}
		named[name] = v.BigmapId
	}
	return named
}

// stats are stored as uint32 in a byte slice limit entrypoint count to 255
func (c *Contract) IncCallStats(entrypoint int) {
	offs := entrypoint * 4
	if cap(c.CallStats) <= offs+4 {
		// grow slice if necessary
		buf := make([]byte, offs+4)
		copy(buf, c.CallStats)
		c.CallStats = buf
	}
	c.CallStats = c.CallStats[0:util.Max(len(c.CallStats), offs+4)]
	val := binary.BigEndian.Uint32(c.CallStats[offs:])
	binary.BigEndian.PutUint32(c.CallStats[offs:], val+1)
	c.IsDirty = true
}

func (c *Contract) DecCallStats(entrypoint int) {
	offs := entrypoint * 4
	if cap(c.CallStats) <= offs+4 {
		// grow slice if necessary
		buf := make([]byte, offs+4)
		copy(buf, c.CallStats)
		c.CallStats = buf
	}
	c.CallStats = c.CallStats[0:util.Max(len(c.CallStats), offs+4)]
	val := binary.BigEndian.Uint32(c.CallStats[offs:])
	binary.BigEndian.PutUint32(c.CallStats[offs:], val-1)
	c.IsDirty = true
}

// Loads type data from already unmarshaled script or from optimized unmarshaler
func (c *Contract) LoadType() (micheline.Type, micheline.Type, error) {
	var err error
	if !c.params.IsValid() {
		if c.script != nil {
			c.params = c.script.ParamType()
			c.storage = c.script.StorageType()
		} else if c.Script != nil {
			c.params, c.storage, err = micheline.UnmarshalScriptType(c.Script)
		}
	}
	return c.params, c.storage, err
}

// loads script and upgrades to babylon on-the-fly if originated earlier
func (c *Contract) LoadScript() (*micheline.Script, error) {
	// already cached?
	if c.script != nil {
		return c.script, nil
	}

	// should not happen
	if len(c.Script) == 0 {
		return nil, nil
	}

	// unmarshal script
	s := micheline.NewScript()
	if err := s.UnmarshalBinary(c.Script); err != nil {
		return nil, err
	}
	c.script = s
	return s, nil
}
