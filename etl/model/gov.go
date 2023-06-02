// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"errors"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
)

const (
	ElectionTableKey = "election"
	ProposalTableKey = "proposal"
	VoteTableKey     = "vote"
	BallotTableKey   = "ballot"
	StakeTableKey    = "stake"
)

var (
	// ErrNoElection is an error that indicates a requested entry does
	// not exist in the election table.
	ErrNoElection = errors.New("election not indexed")

	// ErrNoProposal is an error that indicates a requested entry does
	// not exist in the proposal table.
	ErrNoProposal = errors.New("proposal not indexed")

	// ErrNoVote is an error that indicates a requested entry does
	// not exist in the vote table.
	ErrNoVote = errors.New("vote not indexed")

	// ErrNoBallot is an error that indicates a requested entry does
	// not exist in the ballot table.
	ErrNoBallot = errors.New("ballot not indexed")

	ErrInvalidProtocolHash = errors.New("invalid protocol hash")
)

// An Election represents a unique voting cycle which may be between 1 and 4
// voting periods in length. Elections finish with the activation of the winning
// proposal protocol after period 4 or abort at the end of period 1 when no
// proposal was published, at the end of periods 2 and 4 when no quorum or
// supermajority was reached. Period 3 always ends successfully.
type ElectionID uint64

func (id ElectionID) U64() uint64 {
	return uint64(id)
}

type Election struct {
	RowId        ElectionID `pack:"I,pk,snappy"   json:"row_id"`        // unique id
	ProposalId   ProposalID `pack:"P,snappy"      json:"proposal_id"`   // winning proposal id (after first vote)
	NumPeriods   int        `pack:"n,snappy"      json:"num_periods"`   // number of periods processed (so far)
	NumProposals int        `pack:"N,snappy"      json:"num_proposals"` // number of sumbitted proposals
	VotingPeriod int64      `pack:"p,snappy"      json:"voting_period"` // protocol (proposal) voting period starting the election
	StartTime    time.Time  `pack:"T,snappy"      json:"start_time"`    // proposal voting period start
	EndTime      time.Time  `pack:"t,snappy"      json:"end_time"`      // last voting perid end, estimate when open
	StartHeight  int64      `pack:"H,snappy"      json:"start_height"`  // proposal voting period start block
	EndHeight    int64      `pack:"h,snappy"      json:"end_height"`    // last voting perid end block, estimate when open
	IsEmpty      bool       `pack:"e,snappy"      json:"is_empty"`      // no proposal published during this period
	IsOpen       bool       `pack:"o,snappy"      json:"is_open"`       // flag, election in progress
	IsFailed     bool       `pack:"f,snappy"      json:"is_failed"`     // flag, election aborted du to missing proposal or missed quorum/supermajority
	NoQuorum     bool       `pack:"!,snappy"      json:"no_quorum"`     // flag, quorum not reached
	NoMajority   bool       `pack:"m,snappy"      json:"no_majority"`   // flag, supermajority not reached
}

// Ensure Election implements the pack.Item interface.
var _ pack.Item = (*Election)(nil)

func (e *Election) ID() uint64 {
	return uint64(e.RowId)
}

func (e *Election) SetID(id uint64) {
	e.RowId = ElectionID(id)
}

func (m Election) TableKey() string {
	return ElectionTableKey
}

func (m Election) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    8,
		JournalSizeLog2: 8,
		CacheSize:       2,
		FillLevel:       100,
	}
}

func (m Election) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

// Proposal implements unique individual proposals, a baker can choose to publish
// multiple proposals in one operation, which results in multiple rows been created.
type ProposalID uint64

func (id ProposalID) U64() uint64 {
	return uint64(id)
}

type Proposal struct {
	RowId        ProposalID         `pack:"I,pk,snappy"   json:"row_id"`        // unique id
	Hash         tezos.ProtocolHash `pack:"H,snappy"      json:"hash"`          // unique proposal hash
	Height       int64              `pack:"h,snappy"      json:"height"`        // proposal publishing block
	Time         time.Time          `pack:"T,snappy"      json:"time"`          // proposal publishing time
	SourceId     AccountID          `pack:"S,snappy"      json:"source_id"`     // proposal publisher
	OpId         OpID               `pack:"O,snappy"      json:"op_id"`         // operation publishing this proposal
	ElectionId   ElectionID         `pack:"E,snappy"      json:"election_id"`   // custom: election sequence number (same for all voting periods)
	VotingPeriod int64              `pack:"p,snappy"      json:"voting_period"` // protocol: proposal period sequence number
	Rolls        int64              `pack:"r,snappy"      json:"rolls"`         // number of rolls accumulated by this proposal
	Stake        int64              `pack:"s,snappy"      json:"stake"`         // stake accumulated by this proposal
	Voters       int64              `pack:"v,snappy"      json:"voters"`        // number of voters who voted for this proposal
}

// Ensure Proposal implements the pack.Item interface.
var _ pack.Item = (*Proposal)(nil)

func (p *Proposal) ID() uint64 {
	return uint64(p.RowId)
}

func (p *Proposal) SetID(id uint64) {
	p.RowId = ProposalID(id)
}

func (m Proposal) TableKey() string {
	return ProposalTableKey
}

func (m Proposal) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    8,
		JournalSizeLog2: 8,
		CacheSize:       2,
		FillLevel:       100,
	}
}

func (m Proposal) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

// Vote represent the most recent state of a voting period during elections
// or, when closed, the final result of a voting period. Votes contain the
// cummulative aggregate state at the current block.
type Vote struct {
	RowId            uint64                 `pack:"I,pk,snappy"     json:"row_id"`              // unique id
	ElectionId       ElectionID             `pack:"E,snappy"        json:"election_id"`         // related election id
	ProposalId       ProposalID             `pack:"P,snappy"        json:"proposal_id"`         // related proposal id
	VotingPeriod     int64                  `pack:"p,snappy"        json:"voting_period"`       // on-chain sequence number
	VotingPeriodKind tezos.VotingPeriodKind `pack:"k,snappy"        json:"voting_period_kind"`  // on-chain period
	StartTime        time.Time              `pack:"T,snappy"        json:"period_start_time"`   // start time (block time) of voting period
	EndTime          time.Time              `pack:"t,snappy"        json:"period_end_time"`     // end time (block time), estimate when polls are open
	StartHeight      int64                  `pack:"H,snappy"        json:"period_start_height"` // start block height of voting period
	EndHeight        int64                  `pack:"h,snappy"        json:"period_end_height"`   // end block height
	EligibleRolls    int64                  `pack:"r,snappy"        json:"eligible_rolls"`      // total number of rolls at start of perid
	EligibleStake    int64                  `pack:"s,snappy"        json:"eligible_stake"`      // stake at start of period
	EligibleVoters   int64                  `pack:"v,snappy"        json:"eligible_voters"`     // total number of roll owners at start of period
	QuorumPct        int64                  `pack:"q,snappy"        json:"quorum_pct"`          // required quorum in percent (store as integer with 2 digits)
	QuorumRolls      int64                  `pack:"Q,snappy"        json:"quorum_rolls"`        // required quorum in rolls (0 for proposal_period)
	QuorumStake      int64                  `pack:"S,snappy"        json:"quorum_stake"`        // required quorum in stake (0 for proposal_period)
	TurnoutRolls     int64                  `pack:"u,snappy"        json:"turnout_rolls"`       // actual participation in rolls
	TurnoutStake     int64                  `pack:"R,snappy"        json:"turnout_stake"`       // actual participation in rolls
	TurnoutVoters    int64                  `pack:"U,snappy"        json:"turnout_voters"`      // actual participation in voters
	TurnoutPct       int64                  `pack:"c,snappy"        json:"turnout_pct"`         // actual participation in percent
	TurnoutEma       int64                  `pack:"e,snappy"        json:"turnout_ema"`         // EMA (80/20) of participation in percent
	YayRolls         int64                  `pack:"y,snappy"        json:"yay_rolls"`
	YayStake         int64                  `pack:"V,snappy"        json:"yay_stake"`
	YayVoters        int64                  `pack:"Y,snappy"        json:"yay_voters"`
	NayRolls         int64                  `pack:"n,snappy"        json:"nay_rolls"`
	NayStake         int64                  `pack:"W,snappy"        json:"nay_stake"`
	NayVoters        int64                  `pack:"N,snappy"        json:"nay_voters"`
	PassRolls        int64                  `pack:"a,snappy"        json:"pass_rolls"`
	PassStake        int64                  `pack:"X,snappy"        json:"pass_stake"`
	PassVoters       int64                  `pack:"A,snappy"        json:"pass_voters"`
	IsOpen           bool                   `pack:"o,snappy"        json:"is_open"`     // flag, polls are open (only current period)
	IsFailed         bool                   `pack:"f,snappy"        json:"is_failed"`   // flag, failed reaching quorum or supermajority
	IsDraw           bool                   `pack:"d,snappy"        json:"is_draw"`     // flag, draw between at least two proposals
	NoProposal       bool                   `pack:"?,snappy"        json:"no_proposal"` // flag, no proposal submitted
	NoQuorum         bool                   `pack:"!,snappy"        json:"no_quorum"`   // flag, quorum not reached
	NoMajority       bool                   `pack:"m,snappy"        json:"no_majority"` // flag, supermajority not reached
}

// Ensure Vote implements the pack.Item interface.
var _ pack.Item = (*Vote)(nil)

func (v *Vote) ID() uint64 {
	return v.RowId
}

func (v *Vote) SetID(id uint64) {
	v.RowId = id
}

func (m Vote) TableKey() string {
	return VoteTableKey
}

func (m Vote) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    8,
		JournalSizeLog2: 8,
		CacheSize:       2,
		FillLevel:       100,
	}
}

func (m Vote) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

// Ballot represent a single vote cast by a baker during a voting period.
// Only periods 1, 2 and 4 support casting votes, period 1 uses `proposals`
// operations to vote on up to 20 proposals, periods 2 and 4 use `ballot`
// operations to vote on progressing with a single winning proposal.
type Ballot struct {
	RowId            uint64                 `pack:"I,pk,snappy"  json:"row_id"`             // unique id
	ElectionId       ElectionID             `pack:"E,snappy"     json:"election_id"`        // related election id
	ProposalId       ProposalID             `pack:"P,snappy"     json:"proposal_id"`        // related proposal id
	VotingPeriod     int64                  `pack:"p,snappy"     json:"voting_period"`      // on-chain sequence number
	VotingPeriodKind tezos.VotingPeriodKind `pack:"k,snappy"     json:"voting_period_kind"` // on-chain period
	Height           int64                  `pack:"h,snappy"     json:"height"`             // proposal/ballot operation block height
	Time             time.Time              `pack:"T,snappy"     json:"time"`               // proposal/ballot operation block time
	SourceId         AccountID              `pack:"S,snappy"     json:"source_id"`          // voting account
	OpId             OpID                   `pack:"O,snappy"     json:"op_id"`              // proposal/ballot operation id
	Rolls            int64                  `pack:"r,snappy"     json:"rolls"`              // number of rolls for voter (at beginning of voting period)
	Stake            int64                  `pack:"s,snappy"     json:"stake"`              // number of rolls for voter (at beginning of voting period)
	Ballot           tezos.BallotVote       `pack:"b,snappy"     json:"ballot"`             // yay, nay, pass; proposal period uses yay only
}

// Ensure Ballot implements the pack.Item interface.
var _ pack.Item = (*Ballot)(nil)

func (b *Ballot) ID() uint64 {
	return b.RowId
}

func (b *Ballot) SetID(id uint64) {
	b.RowId = id
}

func (m Ballot) TableKey() string {
	return BallotTableKey
}

func (m Ballot) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    8,
		JournalSizeLog2: 8,
		CacheSize:       2,
		FillLevel:       100,
	}
}

func (m Ballot) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

// Stake snapshots
type Stake struct {
	RowId     uint64    `pack:"I,pk"  json:"row_id"`
	Height    int64     `pack:"h"     json:"height"`
	AccountId AccountID `pack:"A"     json:"account_id"`
	Rolls     int64     `pack:"r"     json:"rolls"`
	Stake     int64     `pack:"s"     json:"stake"`
}

var _ pack.Item = (*Stake)(nil)

func (b *Stake) ID() uint64 {
	return b.RowId
}

func (b *Stake) SetID(id uint64) {
	b.RowId = id
}

func (m Stake) TableKey() string {
	return StakeTableKey
}

func (m Stake) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    8,
		JournalSizeLog2: 8,
		CacheSize:       2,
		FillLevel:       100,
	}
}

func (m Stake) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

type Voter struct {
	RowId     AccountID
	Rolls     int64
	Stake     int64
	Ballot    tezos.BallotVote
	HasVoted  bool
	Time      time.Time
	Proposals []ProposalID
}
