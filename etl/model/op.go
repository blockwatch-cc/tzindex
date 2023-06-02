// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/rpc"
)

const (
	OpTableKey        = "op"
	EndorseOpTableKey = "endorsement"
)

var (
	ErrNoOp          = errors.New("op not indexed")
	ErrNoEndorsement = errors.New("endorsement not indexed")
	ErrInvalidOpID   = errors.New("invalid op id")
	ErrInvalidOpHash = errors.New("invalid op hash")
)

// Helper to uniquely identify an operation while indexing
type OpRef struct {
	Hash tezos.OpHash
	Kind OpType
	N    int
	L    int
	P    int
	C    int
	I    int
	Raw  rpc.TypedOperation
}

func (id OpRef) Get(op *rpc.Operation) rpc.TypedOperation {
	return op.Contents[id.C]
}

func (id OpRef) Result(op *rpc.Operation) rpc.OperationResult {
	return id.Get(op).Result()
}

func (id OpRef) InternalResult(op *rpc.Operation) rpc.InternalResult {
	o := id.Get(op)
	return o.Meta().InternalResults[id.I]
}

var opPool = &sync.Pool{
	New: func() interface{} { return new(Op) },
}

type OpID uint64

func (id OpID) U64() uint64 {
	return uint64(id)
}

type Op struct {
	RowId        OpID           `pack:"I,pk"              json:"row_id"`        // internal: unique row id
	Type         OpType         `pack:"t,bloom"           json:"type"`          // indexer op type
	Hash         tezos.OpHash   `pack:"H,snappy,bloom=3"  json:"hash"`          // op hash
	Height       int64          `pack:"h"                 json:"height"`        // block height
	Cycle        int64          `pack:"c"                 json:"cycle"`         // block cycle
	Timestamp    time.Time      `pack:"T"                 json:"time"`          // block time
	OpN          int            `pack:"n"                 json:"op_n"`          // unique in-block pos
	OpP          int            `pack:"P"                 json:"op_p"`          // op list pos (list can be derived from type)
	Status       tezos.OpStatus `pack:"?"                 json:"status"`        // op status
	IsSuccess    bool           `pack:"!"                 json:"is_success"`    // success flag
	IsContract   bool           `pack:"C"                 json:"is_contract"`   // contract call flag (target is contract)
	IsInternal   bool           `pack:"N"                 json:"is_internal"`   // internal contract call or op
	IsEvent      bool           `pack:"m"                 json:"is_event"`      // this is an implicit event
	IsRollup     bool           `pack:"u"                 json:"is_rollup"`     // this is an rollup operation
	Counter      int64          `pack:"j"                 json:"counter"`       // signer counter
	GasLimit     int64          `pack:"l"                 json:"gas_limit"`     // gas limit
	GasUsed      int64          `pack:"G"                 json:"gas_used"`      // gas used
	StorageLimit int64          `pack:"Z"                 json:"storage_limit"` // storage size limit
	StoragePaid  int64          `pack:"$"                 json:"storage_paid"`  // storage allocated/paid
	Volume       int64          `pack:"v"                 json:"volume"`        // transacted tez volume
	Fee          int64          `pack:"f"                 json:"fee"`           // tx fees
	Reward       int64          `pack:"r"                 json:"reward"`        // baking/endorsement reward
	Deposit      int64          `pack:"d"                 json:"deposit"`       // baker deposit
	Burned       int64          `pack:"b"                 json:"burned"`        // burned tez (for storage allocation)
	SenderId     AccountID      `pack:"S,bloom"           json:"sender_id"`     // sender id, also on internal ops
	ReceiverId   AccountID      `pack:"R,bloom"           json:"receiver_id"`   // receiver id
	CreatorId    AccountID      `pack:"M"                 json:"creator_id"`    // creator id, direct source for internal ops
	BakerId      AccountID      `pack:"D,bloom"           json:"baker_id"`      // delegate id
	Data         string         `pack:"a,snappy"          json:"data"`          // custom op data
	Parameters   []byte         `pack:"p,snappy"          json:"parameters"`    // call params
	StorageHash  uint64         `pack:"s"                 json:"storage_hash"`  // storage hash
	Errors       []byte         `pack:"e,snappy"          json:"errors"`        // call errors
	Entrypoint   int            `pack:"E"                 json:"entrypoint_id"` // update contract counters, search by entrypoint

	// internal
	OpC              int                    `pack:"-"  json:"-"` // contents list pos
	OpI              int                    `pack:"-"  json:"-"` // internal op result list pos
	Raw              rpc.TypedOperation     `pack:"-"  json:"-"` // cache
	RawTicketUpdates []rpc.TicketUpdate     `pack:"-"  json:"-"` // rpc ticket updates
	BigmapEvents     micheline.BigmapEvents `pack:"-"  json:"-"` // cache here for bigmap index
	Storage          []byte                 `pack:"-"  json:"-"` // storage update
	IsStorageUpdate  bool                   `pack:"-"  json:"-"` // true when contract storage changed
	Contract         *Contract              `pack:"-"  json:"-"` // cached contract
	BigmapUpdates    []BigmapUpdate         `pack:"-"  json:"-"` // cached query result
	Events           []*Event               `pack:"-"  json:"-"` // cached query result
	TicketUpdates    []*TicketUpdate        `pack:"-"  json:"-"` // cached query result
	IsBurnAddress    bool                   `pack:"-"  json:"-"` // target is known burn address
}

// Ensure Op implements the pack.Item interface.
var _ pack.Item = (*Op)(nil)

func AllocOp() *Op {
	return opPool.Get().(*Op)
}

func NewOp(block *Block, id OpRef) *Op {
	o := AllocOp()
	o.RowId = 0
	o.Type = id.Kind
	o.Hash = id.Hash
	o.Height = block.Height
	o.Cycle = block.Cycle
	o.Timestamp = block.Timestamp
	o.OpN = id.N
	o.OpP = id.P
	o.OpC = id.C
	o.OpI = id.I
	o.Raw = id.Raw
	// other fields are type specific and will be set by builder
	return o
}

func NewEventOp(block *Block, recv AccountID, id OpRef) *Op {
	o := AllocOp()
	o.RowId = 0
	o.Type = id.Kind
	o.Height = block.Height
	o.Cycle = block.Cycle
	o.Timestamp = block.Timestamp
	o.OpN = id.N
	o.OpP = id.P
	o.ReceiverId = recv
	o.Status = tezos.OpStatusApplied
	o.IsSuccess = true
	o.IsEvent = true
	return o
}

// be compatible with time series interface
func (o Op) Time() time.Time {
	return o.Timestamp
}

func (o Op) ID() uint64 {
	return uint64(o.RowId)
}

func (o *Op) SetID(id uint64) {
	o.RowId = OpID(id)
}

func (o Op) Id() uint64 {
	return uint64(o.Height)<<16 | uint64(o.OpN)
}

func (m Op) TableKey() string {
	return OpTableKey
}

func (m Op) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    15,
		JournalSizeLog2: 15,
		CacheSize:       1024,
		FillLevel:       100,
	}
}

func (m Op) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

func (o *Op) Free() {
	o.Reset()
	opPool.Put(o)
}

func (o *Op) Reset() {
	*o = Op{}
}

// Separate endorsement table for better cache locality
type Endorsement struct {
	RowId    OpID         `pack:"I,pk"             json:"row_id"`            // internal: unique row id
	Hash     tezos.OpHash `pack:"H,snappy,bloom=3" json:"hash"`              // op hash
	Height   int64        `pack:"h"                json:"height"`            // block height
	OpN      int          `pack:"n"                json:"op_n"`              // unique in-block pos
	OpP      int          `pack:"P"                json:"op_p"`              // op list pos (list can be derived from type)
	Reward   int64        `pack:"r"                json:"reward"`            // baking/endorsement reward
	Deposit  int64        `pack:"d"                json:"deposit"`           // baker deposit
	SenderId AccountID    `pack:"S,bloom"          json:"sender_id"`         // sender id, also on internal ops
	Power    int64        `pack:"p"                json:"power"`             // power
	IsPre    bool         `pack:"i"                json:"is_preendorsement"` // Ithaca pre-endorsement
}

func (o Endorsement) ID() uint64 {
	return uint64(o.RowId)
}

func (o *Endorsement) SetID(id uint64) {
	o.RowId = OpID(id)
}

func (m Endorsement) TableKey() string {
	return EndorseOpTableKey
}

func (m Endorsement) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    15,
		JournalSizeLog2: 15,
		CacheSize:       2,
		FillLevel:       100,
	}
}

func (m Endorsement) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

func (o Op) ToEndorsement() *Endorsement {
	switch o.Type {
	case OpTypeEndorsement, OpTypePreendorsement:
	default:
		return nil
	}
	power, _ := strconv.ParseInt(o.Data, 10, 64)
	return &Endorsement{
		Hash:     o.Hash,
		Height:   o.Height,
		OpN:      o.OpN,
		OpP:      o.OpP,
		Reward:   o.Reward,
		Deposit:  o.Deposit,
		SenderId: o.SenderId,
		Power:    power,
		IsPre:    o.Type == OpTypePreendorsement,
	}
}

func (e Endorsement) ToOp() *Op {
	typ := OpTypeEndorsement
	if e.IsPre {
		typ = OpTypePreendorsement
	}
	return &Op{
		Hash:      e.Hash,
		Type:      typ,
		Height:    e.Height,
		OpN:       e.OpN,
		OpP:       e.OpP,
		Reward:    e.Reward,
		Deposit:   e.Deposit,
		SenderId:  e.SenderId,
		Data:      strconv.FormatInt(e.Power, 10),
		Status:    tezos.OpStatusApplied,
		IsSuccess: true,
	}
}

// Ensure Endorsement implements the pack.Item interface.
var _ pack.Item = (*Endorsement)(nil)
