// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/rpc"
	"blockwatch.cc/tzgo/tezos"
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
	RowId         ContractID           `pack:"I,pk,snappy"   json:"row_id"`
	Address       tezos.Address        `pack:"H"             json:"address"`
	AccountId     AccountID            `pack:"A,snappy"      json:"account_id"`
	CreatorId     AccountID            `pack:"C,snappy"      json:"creator_id"`
	FirstSeen     int64                `pack:"f,snappy"      json:"first_seen"`
	LastSeen      int64                `pack:"l,snappy"      json:"last_seen"`
	IsSpendable   bool                 `pack:"p,snappy"      json:"is_spendable"`
	IsDelegatable bool                 `pack:"?,snappy"      json:"is_delegatable"`
	StorageSize   int64                `pack:"z,snappy"      json:"storage_size"`
	StoragePaid   int64                `pack:"y,snappy"      json:"storage_paid"`
	Script        []byte               `pack:"s,snappy"      json:"script"`
	Storage       []byte               `pack:"g,snappy"      json:"storage"`
	InterfaceHash []byte               `pack:"i,snappy"      json:"iface_hash"`
	CodeHash      []byte               `pack:"c,snappy"      json:"code_hash"`
	CallStats     []byte               `pack:"S,snappy"      json:"call_stats"`
	Features      micheline.Features   `pack:"F,snappy"      json:"features"`
	Interfaces    micheline.Interfaces `pack:"n,snappy"      json:"interfaces"`

	IsDirty bool              `pack:"-" json:"-"` // indicates an update happened
	script  *micheline.Script `pack:"-" json:"-"` // cached decoded script
	IsNew   bool              `pack:"-" json:"-"` // new contract, used during migration
}

// Ensure Contract implements the pack.Item interface.
var _ pack.Item = (*Contract)(nil)

// assuming the op was successful!
func NewContract(acc *Account, oop *rpc.OriginationOp, op *Op) *Contract {
	c := AllocContract()
	c.Address = acc.Address.Clone()
	c.AccountId = acc.RowId
	c.CreatorId = acc.CreatorId
	c.FirstSeen = op.Height
	c.LastSeen = op.Height
	res := oop.Metadata.Result
	c.IsSpendable = acc.IsSpendable
	c.IsDelegatable = acc.IsDelegatable
	c.StorageSize = res.StorageSize
	c.StoragePaid = res.PaidStorageSizeDiff
	if oop.Script != nil {
		c.Script, _ = oop.Script.MarshalBinary()
		c.Storage, _ = oop.Script.Storage.MarshalBinary()
		c.InterfaceHash = oop.Script.InterfaceHash()
		c.CodeHash = oop.Script.CodeHash()
		c.Features = oop.Script.Features()
		c.Interfaces = oop.Script.Interfaces()
		ep, _ := oop.Script.Entrypoints(false)
		c.CallStats = make([]byte, 4*len(ep))
	}
	c.IsNew = true
	c.IsDirty = true
	return c
}

func NewInternalContract(acc *Account, iop *rpc.InternalResult, op *Op) *Contract {
	c := AllocContract()
	c.Address = acc.Address.Clone()
	c.AccountId = acc.RowId
	c.CreatorId = acc.CreatorId
	c.FirstSeen = op.Height
	c.LastSeen = op.Height
	res := iop.Result
	c.IsSpendable = acc.IsSpendable
	c.IsDelegatable = acc.IsDelegatable
	c.StorageSize = res.StorageSize
	c.StoragePaid = res.PaidStorageSizeDiff
	if iop.Script != nil {
		c.Script, _ = iop.Script.MarshalBinary()
		c.Storage, _ = iop.Script.Storage.MarshalBinary()
		c.InterfaceHash = iop.Script.InterfaceHash()
		c.CodeHash = iop.Script.CodeHash()
		c.Features = iop.Script.Features()
		c.Interfaces = iop.Script.Interfaces()
		ep, _ := iop.Script.Entrypoints(false)
		c.CallStats = make([]byte, 4*len(ep))
	}
	c.IsNew = true
	c.IsDirty = true
	return c
}

func NewImplicitContract(acc *Account, res rpc.ImplicitResult, op *Op) *Contract {
	c := AllocContract()
	c.Address = acc.Address.Clone()
	c.AccountId = acc.RowId
	c.CreatorId = acc.CreatorId
	c.FirstSeen = op.Height
	c.LastSeen = op.Height
	c.IsSpendable = acc.IsSpendable
	c.IsDelegatable = acc.IsDelegatable
	c.StorageSize = res.StorageSize
	c.StoragePaid = res.PaidStorageSizeDiff
	if res.Script != nil {
		c.Script, _ = res.Script.MarshalBinary()
		c.Storage, _ = res.Script.Storage.MarshalBinary()
		c.InterfaceHash = res.Script.InterfaceHash()
		c.CodeHash = res.Script.CodeHash()
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
	c.IsSpendable = a.IsSpendable
	c.IsDelegatable = a.IsDelegatable
	script, _ := micheline.MakeManagerScript(a.Address.Bytes())
	c.Script, _ = script.MarshalBinary()
	c.Storage, _ = script.Storage.MarshalBinary()
	c.InterfaceHash = script.InterfaceHash()
	c.CodeHash = script.CodeHash()
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
	c.IsSpendable = false
	c.IsDelegatable = false
	c.StorageSize = 0
	c.StoragePaid = 0
	c.Script = nil
	c.Storage = nil
	c.InterfaceHash = nil
	c.CodeHash = nil
	c.CallStats = nil
	c.Features = 0
	c.Interfaces = nil
	c.IsDirty = false
	c.IsNew = false
	c.script = nil
}

// update storage size and size paid
func (c *Contract) Update(op *Op) {
	c.LastSeen = op.Height
	c.StorageSize = op.StorageSize
	c.StoragePaid += op.StoragePaid
	c.IncCallStats(op.Entrypoint)
	if op.Storage != nil {
		// careful: will recycle on builder cleanup
		c.Storage = op.Storage
	}
	c.IsDirty = true
}

func (c *Contract) Rollback(drop, last *Op) {
	if last != nil {
		c.LastSeen = last.Height
		c.StorageSize = last.StorageSize
		if last.Storage != nil {
			c.Storage = last.Storage
		}
	} else {
		// back to origination
		c.Storage, _ = c.script.Storage.MarshalBinary()
		c.LastSeen = c.FirstSeen
		c.StorageSize = int64(len(c.Storage)) // FIXME: is this correct?
	}
	c.StoragePaid -= drop.StoragePaid
	c.DecCallStats(drop.Entrypoint)
	c.IsDirty = true
}

func (c *Contract) ListCallStats() map[string]int {
	// list entrypoint names first
	script, err := c.LoadScript()
	if err != nil {
		return nil
	}

	ep, err := script.Entrypoints(false)
	if err != nil {
		return nil
	}

	// sort entrypoint map by id, we only need names here
	byId := make([]string, len(ep))
	for _, v := range ep {
		byId[v.Id] = v.Call
	}

	res := make(map[string]int, len(c.CallStats)>>2)
	for i, name := range byId {
		res[name] = int(binary.BigEndian.Uint32(c.CallStats[i*4:]))
	}
	return res
}

func (c *Contract) ListFeatures() micheline.Features {
	script, err := c.LoadScript()
	if err != nil {
		return 0
	}
	return script.Features()
}

func (c *Contract) ListInterfaces() micheline.Interfaces {
	script, err := c.LoadScript()
	if err != nil {
		return nil
	}
	return script.Interfaces()
}

func (c *Contract) NamedBigmaps(ids []int64) map[string]int64 {
	if len(ids) == 0 {
		return nil
	}
	script, err := c.LoadScript()
	if err != nil {
		return nil
	}
	named := make(map[string]int64)
	bigmaps, _ := script.Code.Storage.FindOpCodes(micheline.T_BIG_MAP)
	for i := 0; i < util.Min(len(ids), len(bigmaps)); i++ {
		n := bigmaps[i].GetVarAnnoAny()
		if n == "" {
			n = strconv.Itoa(i)
		}
		if _, ok := named[n]; ok {
			n += "_" + strconv.Itoa(i)
		}
		named[n] = ids[i]
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

// loads script and upgrades to babylon on-the-fly if originated earlier
func (c *Contract) LoadScript() (*micheline.Script, error) {
	// already migrated and cached?
	if c.script != nil {
		return c.script, nil
	}

	// should not happen
	if len(c.Script) == 0 {
		return nil, fmt.Errorf("empty script on %s", c.String())
	}

	// unmarshal script
	s := micheline.NewScript()
	if err := s.UnmarshalBinary(c.Script); err != nil {
		return nil, err
	}
	c.script = s
	return s, nil
}
