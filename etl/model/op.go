// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/rpc"
)

var opPool = &sync.Pool{
	New: func() interface{} { return new(Op) },
}

type OpID uint64

func (id OpID) Value() uint64 {
	return uint64(id)
}

type Op struct {
	RowId        OpID                `pack:"I,pk,snappy"   json:"row_id"`                         // internal: unique row id
	Timestamp    time.Time           `pack:"T,snappy"      json:"time"`                           // bc: op block time
	Height       int64               `pack:"h,snappy"      json:"height"`                         // bc: block height op was mined at
	Cycle        int64               `pack:"c,snappy"      json:"cycle"`                          // bc: block cycle (tezos specific)
	Hash         chain.OperationHash `pack:"H"             json:"hash"`                           // bc: unique op_id (op hash)
	Counter      int64               `pack:"j,snappy"      json:"counter"`                        // bc: counter
	OpN          int                 `pack:"n,snappy"      json:"op_n"`                           // bc: position in block (block.Operations.([][]*OperationHeader) list position)
	OpC          int                 `pack:"o,snappy"      json:"op_c"`                           // bc: position in OperationHeader.Contents.([]Operation) list
	OpI          int                 `pack:"i,snappy"      json:"op_i"`                           // bc: position in internal operation result list
	Type         chain.OpType        `pack:"t,snappy"      json:"type"`                           // stats: operation type as defined byprotocol
	Status       chain.OpStatus      `pack:"?,snappy"      json:"status"`                         // stats: operation status
	IsSuccess    bool                `pack:"!,snappy"      json:"is_success"`                     // bc: operation succesful flag
	IsContract   bool                `pack:"C,snappy"      json:"is_contract"`                    // bc: operation succesful flag
	GasLimit     int64               `pack:"l,snappy"      json:"gas_limit"`                      // stats: gas limit
	GasUsed      int64               `pack:"G,snappy"      json:"gas_used"`                       // stats: gas used
	GasPrice     float64             `pack:"g,convert,precision=5,snappy"      json:"gas_price"`  // stats: gas price in tezos per unit gas, relative to tx fee
	StorageLimit int64               `pack:"Z,snappy"      json:"storage_limit"`                  // stats: storage size limit
	StorageSize  int64               `pack:"z,snappy"      json:"storage_size"`                   // stats: storage size used/allocated by this op
	StoragePaid  int64               `pack:"$,snappy"      json:"storage_paid"`                   // stats: extra storage size paid by this op
	Volume       int64               `pack:"v,snappy"      json:"volume"`                         // stats: sum of transacted tezos volume
	Fee          int64               `pack:"f,snappy"      json:"fee"`                            // stats: transaction fees
	Reward       int64               `pack:"r,snappy"      json:"reward"`                         // stats: baking and endorsement rewards
	Deposit      int64               `pack:"d,snappy"      json:"deposit"`                        // stats: bonded deposits for baking and endorsement
	Burned       int64               `pack:"b,snappy"      json:"burned"`                         // stats: burned tezos
	SenderId     AccountID           `pack:"S,snappy"      json:"sender_id"`                      // internal: op sender
	ReceiverId   AccountID           `pack:"R,snappy"      json:"receiver_id"`                    // internal: op receiver
	ManagerId    AccountID           `pack:"M,snappy"      json:"manager_id"`                     // internal: op manager for originations
	DelegateId   AccountID           `pack:"D,snappy"      json:"delegate_id"`                    // internal: op delegate for originations and delegations
	IsInternal   bool                `pack:"N,snappy"      json:"is_internal"`                    // bc: internal chain/funds management
	HasData      bool                `pack:"w,snappy"      json:"has_data"`                       // internal: flag to signal if data is available
	Data         string              `pack:"a,snappy"      json:"data"`                           // bc: extra op data
	Parameters   []byte              `pack:"p,snappy"      json:"parameters"`                     // bc: input params
	Storage      []byte              `pack:"s,snappy"      json:"storage"`                        // bc: result storage
	BigMapDiff   []byte              `pack:"B,snappy"      json:"big_map_diff"`                   // bc: result big map diff
	Errors       string              `pack:"e,snappy"      json:"errors"`                         // bc: result errors
	TDD          float64             `pack:"x,convert,precision=6,snappy"  json:"days_destroyed"` // stats: token days destroyed
	BranchId     uint64              `pack:"X,snappy"      json:"branch_id"`                      // bc: branch block the op is based on
	BranchHeight int64               `pack:"#,snappy"      json:"branch_height"`                  // bc: height of the branch block
	BranchDepth  int64               `pack:"<,snappy"      json:"branch_depth"`                   // stats: diff between branch block and current block
}

// Ensure Op implements the pack.Item interface.
var _ pack.Item = (*Op)(nil)

func AllocOp() *Op {
	return opPool.Get().(*Op)
}

func NewOp(block, branch *Block, head *rpc.OperationHeader, op_n, op_c, op_i int) *Op {
	o := AllocOp()
	o.RowId = 0
	o.Timestamp = block.Timestamp
	o.Height = block.Height
	o.Cycle = block.Cycle
	o.Hash = head.Hash
	o.OpN = op_n
	o.OpC = op_c
	o.OpI = op_i
	o.Type = head.Contents[op_c].OpKind()
	if branch != nil {
		o.BranchId = branch.RowId
		o.BranchHeight = branch.Height
		o.BranchDepth = block.Height - branch.Height
	}
	// other fields are type specific and will be set by builder
	return o
}

func (o Op) ID() uint64 {
	return uint64(o.RowId)
}

func (o *Op) SetID(id uint64) {
	o.RowId = OpID(id)
}

func (o *Op) Free() {
	o.Reset()
	opPool.Put(o)
}

func (o *Op) Reset() {
	o.RowId = 0
	o.Timestamp = time.Time{}
	o.Height = 0
	o.Cycle = 0
	o.Hash = chain.OperationHash{chain.ZeroHash}
	o.OpN = 0
	o.OpC = 0
	o.OpI = 0
	o.Counter = 0
	o.Type = 0
	o.Status = 0
	o.IsSuccess = false
	o.IsContract = false
	o.GasLimit = 0
	o.GasUsed = 0
	o.GasPrice = 0
	o.StorageLimit = 0
	o.StorageSize = 0
	o.StoragePaid = 0
	o.Volume = 0
	o.Fee = 0
	o.Reward = 0
	o.Deposit = 0
	o.Burned = 0
	o.SenderId = 0
	o.ReceiverId = 0
	o.ManagerId = 0
	o.DelegateId = 0
	o.IsInternal = false
	o.HasData = false
	o.Data = ""
	o.Parameters = nil
	o.Storage = nil
	o.BigMapDiff = nil
	o.Errors = ""
	o.TDD = 0
	o.BranchId = 0
	o.BranchHeight = 0
	o.BranchDepth = 0
}
