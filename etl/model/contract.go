// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"sync"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/micheline"
	"blockwatch.cc/tzindex/rpc"
)

var contractPool = &sync.Pool{
	New: func() interface{} { return new(Contract) },
}

// Contract holds code and info about smart contracts on the Tezos blockchain.
type Contract struct {
	RowId         uint64    `pack:"I,pk,snappy"   json:"row_id"`
	Hash          []byte    `pack:"H"             json:"hash"`
	AccountId     AccountID `pack:"A,snappy"      json:"account_id"`
	ManagerId     AccountID `pack:"M,snappy"      json:"manager_id"`
	Height        int64     `pack:"h,snappy"      json:"height"`
	Fee           int64     `pack:"f,snappy"      json:"fee"`
	GasLimit      int64     `pack:"l,snappy"      json:"gas_limit"`
	GasUsed       int64     `pack:"G,snappy"      json:"gas_used"`
	GasPrice      float64   `pack:"g,convert,precision=5,snappy"   json:"gas_price"`
	StorageLimit  int64     `pack:"s,snappy"      json:"storage_limit"`
	StorageSize   int64     `pack:"z,snappy"      json:"storage_size"`
	StoragePaid   int64     `pack:"y,snappy"      json:"storage_paid"`
	Script        []byte    `pack:"S,snappy"      json:"script"`
	IsSpendable   bool      `pack:"p,snappy"      json:"is_spendable"`   // manager can move funds without running any code
	IsDelegatable bool      `pack:"d,snappy"      json:"is_delegatable"` // manager can delegate funds
	OpL           int       `pack:"L,snappy"      json:"op_l"`
	OpP           int       `pack:"P,snappy"      json:"op_p"`
	OpI           int       `pack:"i,snappy"      json:"op_i"`
	InterfaceHash []byte    `pack:"F,snappy"      json:"iface_hash"`
}

// Ensure Account implements the pack.Item interface.
var _ pack.Item = (*Contract)(nil)

// assuming the op was successful!
func NewContract(acc *Account, oop *rpc.OriginationOp, l, p int) *Contract {
	c := AllocContract()
	c.Hash = acc.Hash
	c.AccountId = acc.RowId
	c.ManagerId = acc.ManagerId
	c.Height = acc.FirstSeen
	c.Fee = oop.Fee
	c.GasLimit = oop.GasLimit
	c.StorageLimit = oop.StorageLimit
	res := oop.Metadata.Result
	c.GasUsed = res.ConsumedGas
	if c.GasUsed > 0 && c.Fee > 0 {
		c.GasPrice = float64(c.Fee) / float64(c.GasUsed)
	}
	c.StorageSize = res.StorageSize
	c.StoragePaid = res.PaidStorageSizeDiff
	if oop.Script != nil {
		c.Script, _ = oop.Script.MarshalBinary()
		c.InterfaceHash = oop.Script.InterfaceHash()
		ep, _ := oop.Script.Entrypoints(false)
		acc.CallStats = make([]byte, 4*len(ep))
	}
	c.IsSpendable = acc.IsSpendable
	c.IsDelegatable = acc.IsDelegatable
	c.OpL = l
	c.OpP = p
	c.OpI = 0
	return c
}

func NewInternalContract(acc *Account, iop *rpc.InternalResult, l, p, i int) *Contract {
	c := AllocContract()
	c.Hash = acc.Hash
	c.AccountId = acc.RowId
	c.ManagerId = acc.ManagerId
	c.Height = acc.FirstSeen
	res := iop.Result
	c.GasUsed = res.ConsumedGas
	c.StorageSize = res.StorageSize
	c.StoragePaid = res.PaidStorageSizeDiff
	if iop.Script != nil {
		c.Script, _ = iop.Script.MarshalBinary()
		c.InterfaceHash = iop.Script.InterfaceHash()
		ep, _ := iop.Script.Entrypoints(false)
		acc.CallStats = make([]byte, 4*len(ep))
	}
	c.OpL = l
	c.OpP = p
	c.OpI = i
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
	return c.RowId
}

func (c *Contract) SetID(id uint64) {
	c.RowId = id
}

func (c Contract) String() string {
	s, _ := chain.EncodeAddress(chain.AddressTypeContract, c.Hash)
	return s
}

// origination operation must exist and code must be available
func (c Contract) IsNonExist() bool {
	return len(c.Script) == 0 && c.OpL == 0 && !c.IsDelegatable && !c.IsSpendable
}

func (c *Contract) Reset() {
	c.RowId = 0
	c.Hash = nil
	c.AccountId = 0
	c.ManagerId = 0
	c.Height = 0
	c.GasLimit = 0
	c.GasUsed = 0
	c.GasPrice = 0
	c.StorageLimit = 0
	c.StorageSize = 0
	c.StoragePaid = 0
	c.Script = nil
	c.IsSpendable = false
	c.IsDelegatable = false
	c.OpL = 0
	c.OpP = 0
	c.OpI = 0
	c.InterfaceHash = nil
}

func (c *Contract) NeedsBabylonUpgrade(p *chain.Params, height int64) bool {
	// babylon activation
	isEligible := p.IsMainnet() && p.Version >= 5 && height < 655361
	// contract upgrade criteria
	isEligible = isEligible && (c.IsSpendable || (!c.IsSpendable && c.IsDelegatable))
	return isEligible
}

// loads script and upgrades to babylon on-the-fly if originated earlier
func (c *Contract) LoadScript(tip *ChainTip, height int64, manager []byte) (*micheline.Script, error) {
	script := micheline.NewScript()

	// patch empty manager.tz
	if len(c.Script) == 0 {
		if tip.ChainId.IsEqual(chain.Mainnet) && height >= 655361 && c.Height < 655361 {
			return micheline.MakeManagerScript(manager)
		}
		// empty script before Babylon
		return nil, nil
	}

	// unmarshal script
	if err := script.UnmarshalBinary(c.Script); err != nil {
		return nil, err
	}

	// must upgrade?
	// - only applies to mainnet and contracts originated before babylon
	// - don't upgrade when requested height is < babylon so we can handle
	//   old params/storage properly
	if tip.ChainId.IsEqual(chain.Mainnet) && height >= 655361 && c.Height < 655361 {
		switch true {
		case c.IsSpendable:
			script.MigrateToBabylonAddDo(manager)
		case !c.IsSpendable && c.IsDelegatable:
			script.MigrateToBabylonSetDelegate(manager)
		}
	}
	return script, nil
}
