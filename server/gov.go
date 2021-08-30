// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"

	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"
)

func init() {
	register(ExplorerElection{})
}

type ExplorerVote struct {
	VotingPeriod     int64                  `json:"voting_period"`
	VotingPeriodKind tezos.VotingPeriodKind `json:"voting_period_kind"`
	StartTime        time.Time              `json:"period_start_time"`
	EndTime          time.Time              `json:"period_end_time"`
	StartHeight      int64                  `json:"period_start_block"`
	EndHeight        int64                  `json:"period_end_block"`
	EligibleRolls    int64                  `json:"eligible_rolls"`
	EligibleVoters   int64                  `json:"eligible_voters"`
	QuorumPct        int64                  `json:"quorum_pct"`
	QuorumRolls      int64                  `json:"quorum_rolls"`
	TurnoutRolls     int64                  `json:"turnout_rolls"`
	TurnoutVoters    int64                  `json:"turnout_voters"`
	TurnoutPct       int64                  `json:"turnout_pct"`
	TurnoutEma       int64                  `json:"turnout_ema"`
	YayRolls         int64                  `json:"yay_rolls"`
	YayVoters        int64                  `json:"yay_voters"`
	NayRolls         int64                  `json:"nay_rolls"`
	NayVoters        int64                  `json:"nay_voters"`
	PassRolls        int64                  `json:"pass_rolls"`
	PassVoters       int64                  `json:"pass_voters"`
	IsOpen           bool                   `json:"is_open"`
	IsFailed         bool                   `json:"is_failed"`
	IsDraw           bool                   `json:"is_draw"`
	NoProposal       bool                   `json:"no_proposal"`
	NoQuorum         bool                   `json:"no_quorum"`
	NoMajority       bool                   `json:"no_majority"`
	Proposals        []*ExplorerProposal    `json:"proposals"`
}

func NewExplorerVote(ctx *ApiContext, v *model.Vote) *ExplorerVote {
	vote := &ExplorerVote{
		VotingPeriod:     v.VotingPeriod,
		VotingPeriodKind: v.VotingPeriodKind,
		StartTime:        v.StartTime,
		EndTime:          v.EndTime,
		StartHeight:      v.StartHeight,
		EndHeight:        v.EndHeight,
		EligibleRolls:    v.EligibleRolls,
		EligibleVoters:   v.EligibleVoters,
		QuorumPct:        v.QuorumPct,
		QuorumRolls:      v.QuorumRolls,
		TurnoutRolls:     v.TurnoutRolls,
		TurnoutVoters:    v.TurnoutVoters,
		TurnoutPct:       v.TurnoutPct,
		TurnoutEma:       v.TurnoutEma,
		YayRolls:         v.YayRolls,
		YayVoters:        v.YayVoters,
		NayRolls:         v.NayRolls,
		NayVoters:        v.NayVoters,
		PassRolls:        v.PassRolls,
		PassVoters:       v.PassVoters,
		IsOpen:           v.IsOpen,
		IsFailed:         v.IsFailed,
		IsDraw:           v.IsDraw,
		NoProposal:       v.NoProposal,
		NoQuorum:         v.NoQuorum,
		NoMajority:       v.NoMajority,
	}
	// estimate end time for open votes
	if vote.IsOpen {
		params := ctx.Params
		diff := params.BlocksPerVotingPeriod - (ctx.Tip.BestHeight - v.StartHeight) - 1
		vote.EndTime = ctx.Tip.BestTime.Add(time.Duration(diff) * params.BlockTime())
	}
	return vote
}

type ExplorerProposal struct {
	Hash          tezos.ProtocolHash `json:"hash"`
	SourceAddress tezos.Address      `json:"source"`
	BlockHash     tezos.BlockHash    `json:"block_hash"`
	OpHash        tezos.OpHash       `json:"op_hash"`
	Height        int64              `json:"height"`
	Time          time.Time          `json:"time"`
	Rolls         int64              `json:"rolls"`
	Voters        int64              `json:"voters"`
}

func NewExplorerProposal(ctx *ApiContext, p *model.Proposal) *ExplorerProposal {
	return &ExplorerProposal{
		Hash:          p.Hash,
		Height:        p.Height,
		Time:          p.Time,
		Rolls:         p.Rolls,
		Voters:        p.Voters,
		BlockHash:     ctx.Indexer.LookupBlockHash(ctx, p.Height),
		OpHash:        govLookupOpHash(ctx, p.OpId),
		SourceAddress: ctx.Indexer.LookupAddress(ctx, p.SourceId),
	}
}

// keep a cache of op and proposal hashes
type (
	govOpMap       map[model.OpID]tezos.OpHash
	govProposalMap map[model.ProposalID]tezos.ProtocolHash
)

var (
	govOpMapStore       atomic.Value
	govProposalMapStore atomic.Value
	govMutex            sync.Mutex
)

func init() {
	govOpMapStore.Store(make(govOpMap))
	govProposalMapStore.Store(make(govProposalMap))
}

func purgeGovStore() {
	govMutex.Lock()
	defer govMutex.Unlock()
	govOpMapStore.Store(make(govOpMap))
	govProposalMapStore.Store(make(govProposalMap))
}

func govLookupOpHash(ctx *ApiContext, id model.OpID) tezos.OpHash {
	if id == 0 {
		return tezos.OpHash{}
	}
	m := govOpMapStore.Load().(govOpMap)
	h, ok := m[id]
	if !ok {
		govMutex.Lock()
		defer govMutex.Unlock()
		ops, err := ctx.Indexer.LookupOpIds(ctx.Context, []uint64{id.Value()})
		if err != nil || len(ops) == 0 {
			log.Errorf("explorer: cannot resolve op id %d: %v", id, err)
		} else {
			h = ops[0].Hash.Clone()
			m2 := make(govOpMap) // create a new map
			for k, v := range m {
				m2[k] = v // copy all data
			}
			m2[id] = h // add new hash
			govOpMapStore.Store(m2)
		}
	}
	return h
}

func govLookupProposalHash(ctx *ApiContext, id model.ProposalID) tezos.ProtocolHash {
	if id == 0 {
		return tezos.ProtocolHash{}
	}
	m := govProposalMapStore.Load().(govProposalMap)
	h, ok := m[id]
	if !ok {
		govMutex.Lock()
		defer govMutex.Unlock()
		props, err := ctx.Indexer.LookupProposalIds(ctx.Context, []uint64{id.Value()})
		if err != nil || len(props) == 0 {
			log.Errorf("explorer: cannot resolve proposal id %d: %v", id, err)
		} else {
			h = props[0].Hash.Clone()
			m2 := make(govProposalMap) // create a new map
			for k, v := range m {
				m2[k] = v // copy all data
			}
			m2[id] = h // add new hash
			govProposalMapStore.Store(m2)
		}
	}
	return h
}

type ExplorerBallot struct {
	RowId            uint64                 `json:"row_id"`
	Height           int64                  `json:"height"`
	Timestamp        time.Time              `json:"time"`
	ElectionId       int                    `json:"election_id"`
	VotingPeriod     int64                  `json:"voting_period"`
	VotingPeriodKind tezos.VotingPeriodKind `json:"voting_period_kind"`
	Proposal         tezos.ProtocolHash     `json:"proposal"`
	OpHash           tezos.OpHash           `json:"op"`
	Ballot           tezos.BallotVote       `json:"ballot"`
	Rolls            int64                  `json:"rolls"`
	Sender           string                 `json:"sender"`
}

type ExplorerBallotList struct {
	list     []*ExplorerBallot
	modified time.Time
	expires  time.Time
}

func (l ExplorerBallotList) MarshalJSON() ([]byte, error) { return json.Marshal(l.list) }
func (l ExplorerBallotList) LastModified() time.Time      { return l.modified }
func (l ExplorerBallotList) Expires() time.Time           { return l.expires }

var _ Resource = (*ExplorerBallotList)(nil)

func NewExplorerBallot(ctx *ApiContext, b *model.Ballot, p tezos.ProtocolHash, o tezos.OpHash) *ExplorerBallot {
	return &ExplorerBallot{
		RowId:            b.RowId,
		Height:           b.Height,
		Timestamp:        b.Time,
		ElectionId:       int(b.ElectionId),
		VotingPeriod:     b.VotingPeriod,
		VotingPeriodKind: b.VotingPeriodKind,
		Proposal:         p,
		OpHash:           o,
		Ballot:           b.Ballot,
		Rolls:            b.Rolls,
		Sender:           ctx.Indexer.LookupAddress(ctx, b.SourceId).String(),
	}
}

type ExplorerElection struct {
	Id                int                    `json:"election_id"`
	MaxPeriods        int                    `json:"max_periods"`
	NumPeriods        int                    `json:"num_periods"`
	NumProposals      int                    `json:"num_proposals"`
	StartTime         time.Time              `json:"start_time"`
	EndTime           time.Time              `json:"end_time"`
	StartHeight       int64                  `json:"start_height"`
	EndHeight         int64                  `json:"end_height"`
	IsEmpty           bool                   `json:"is_empty"`
	IsOpen            bool                   `json:"is_open"`
	IsFailed          bool                   `json:"is_failed"`
	NoQuorum          bool                   `json:"no_quorum"`
	NoMajority        bool                   `json:"no_majority"`
	NoProposal        bool                   `json:"no_proposal"`
	VotingPeriodKind  tezos.VotingPeriodKind `json:"voting_period"`
	ProposalPeriod    *ExplorerVote          `json:"proposal"`
	ExplorationPeriod *ExplorerVote          `json:"exploration"`
	CooldownPeriod    *ExplorerVote          `json:"cooldown"`
	PromotionPeriod   *ExplorerVote          `json:"promotion"`
	AdoptionPeriod    *ExplorerVote          `json:"adoption"`
	expires           time.Time              `json:"-"`
}

var _ RESTful = (*ExplorerElection)(nil)

func NewExplorerElection(ctx *ApiContext, e *model.Election) *ExplorerElection {
	election := &ExplorerElection{
		Id:               int(e.RowId),
		MaxPeriods:       ctx.Params.NumVotingPeriods,
		NumPeriods:       e.NumPeriods,
		NumProposals:     e.NumProposals,
		StartTime:        e.StartTime,
		EndTime:          e.EndTime,
		StartHeight:      e.StartHeight,
		EndHeight:        e.EndHeight,
		IsEmpty:          e.IsEmpty,
		IsOpen:           e.IsOpen,
		IsFailed:         e.IsFailed,
		NoQuorum:         e.NoQuorum,
		NoMajority:       e.NoMajority,
		NoProposal:       e.NumProposals == 0,
		VotingPeriodKind: tezos.ToVotingPeriod(e.NumPeriods),
	}
	// estimate end time for open elections
	p := ctx.Params
	tm := ctx.Tip.BestTime
	if election.IsOpen {
		diff := int64(p.NumVotingPeriods)*p.BlocksPerVotingPeriod - (ctx.Tip.BestHeight - e.StartHeight)
		election.EndTime = tm.Add(time.Duration(diff) * p.BlockTime())
		election.EndHeight = election.StartHeight + int64(p.NumVotingPeriods)*p.BlocksPerVotingPeriod - 1
		election.expires = tm.Add(p.BlockTime())
	} else {
		election.MaxPeriods = election.NumPeriods
		height := ctx.Tip.BestHeight
		if election.EndHeight >= height {
			election.expires = tm.Add(p.BlockTime())
		} else {
			election.expires = ctx.Now.Add(maxCacheExpires)
		}
	}
	return election
}

func (b ExplorerElection) LastModified() time.Time {
	if b.IsOpen {
		return time.Now().UTC()
	}
	return b.EndTime
}

func (b ExplorerElection) Expires() time.Time {
	return b.expires
}

func (b ExplorerElection) RESTPrefix() string {
	return "/explorer/election"
}

func (b ExplorerElection) RESTPath(r *mux.Router) string {
	path, _ := r.Get("election").URLPath("ident", strconv.Itoa(b.Id))
	return path.String()
}

func (b ExplorerElection) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (b ExplorerElection) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", C(ReadElection)).Methods("GET").Name("election")
	r.HandleFunc("/{ident}/{stage}/ballots", C(ListBallots)).Methods("GET")
	r.HandleFunc("/{ident}/{stage}/voters", C(ListVoters)).Methods("GET")
	return nil

}

func loadElection(ctx *ApiContext) *model.Election {
	// from number or block height
	if id, ok := mux.Vars(ctx.Request)["ident"]; !ok || id == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing election identifier", nil))
	} else {
		var (
			err      error
			election *model.Election
		)
		switch true {
		case id == "head":
			election, err = ctx.Indexer.ElectionByHeight(ctx.Context, ctx.Tip.BestHeight)
		case strings.HasPrefix(id, tezos.HashTypeProtocol.Prefix()):
			var p tezos.ProtocolHash
			p, err = tezos.ParseProtocolHash(id)
			if err != nil {
				panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid proposal", err))
			}
			var proposal *model.Proposal
			proposal, err = ctx.Indexer.LookupProposal(ctx.Context, p)
			if err != nil {
				switch err {
				case etl.ErrNoTable:
					panic(ENotFound(EC_RESOURCE_NOTFOUND, "cannot access proposal table", err))
				case index.ErrNoProposalEntry:
					panic(ENotFound(EC_RESOURCE_NOTFOUND, "no proposal", err))
				default:
					panic(EInternal(EC_DATABASE, err.Error(), nil))
				}
			}
			election, err = ctx.Indexer.ElectionById(ctx.Context, proposal.ElectionId)
		default:
			var i int64
			i, err = strconv.ParseInt(id, 10, 64)
			if err != nil {
				panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid election id", err))
			}
			election, err = ctx.Indexer.ElectionById(ctx.Context, model.ElectionID(i))
		}
		if err != nil {
			switch err {
			case etl.ErrNoTable:
				panic(ENotFound(EC_RESOURCE_NOTFOUND, "cannot access election table", err))
			case index.ErrNoElectionEntry:
				panic(ENotFound(EC_RESOURCE_NOTFOUND, "no election", err))
			default:
				panic(EInternal(EC_DATABASE, err.Error(), nil))
			}
		}
		return election
	}
	return nil
}

func loadStage(ctx *ApiContext, election *model.Election, maxPeriods int) int {
	var stage int // 1 .. 4 (5 in Edo) (same as tezos.VotingPeriodKind)
	if s, ok := mux.Vars(ctx.Request)["stage"]; !ok || s == "" {
		panic(EBadRequest(EC_RESOURCE_ID_MISSING, "missing voting period identifier", nil))
	} else {
		if i, err := strconv.Atoi(s); err != nil {
			panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid voting period identifier", err))
		} else if i < 1 || i > maxPeriods {
			panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid voting period identifier", err))
		} else if i > election.NumPeriods {
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "voting period does not exist", nil))
		} else {
			stage = i
		}
	}
	// adjust to 0..3 (4 from Edo)
	return stage - 1
}

func ReadElection(ctx *ApiContext) (interface{}, int) {
	election := loadElection(ctx)
	votes, err := ctx.Indexer.VotesByElection(ctx, election.RowId)
	if err != nil {
		switch err {
		case etl.ErrNoTable:
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "cannot access vote table", err))
		default:
			panic(EInternal(EC_DATABASE, err.Error(), nil))
		}
	}
	proposals, err := ctx.Indexer.ProposalsByElection(ctx, election.RowId)
	if err != nil {
		switch err {
		case etl.ErrNoTable:
			panic(ENotFound(EC_RESOURCE_NOTFOUND, "cannot access proposal table", err))
		default:
			panic(EInternal(EC_DATABASE, err.Error(), nil))
		}
	}
	ee := NewExplorerElection(ctx, election)
	ee.NoProposal = ee.NumProposals == 0
	var winner *ExplorerProposal
	for _, v := range votes {
		switch v.VotingPeriodKind {
		case tezos.VotingPeriodProposal:
			ee.ProposalPeriod = NewExplorerVote(ctx, v)
			ee.ProposalPeriod.Proposals = make([]*ExplorerProposal, 0)
			for _, vv := range proposals {
				p := NewExplorerProposal(ctx, vv)
				if winner == nil || winner.Rolls < p.Rolls {
					winner = p
				}
				ee.ProposalPeriod.Proposals = append(ee.ProposalPeriod.Proposals, p)
			}
			// copy winner for subsequent periods
			if winner != nil {
				winner = &ExplorerProposal{
					Hash:          winner.Hash,
					SourceAddress: winner.SourceAddress,
					BlockHash:     winner.BlockHash,
					OpHash:        winner.OpHash,
					Height:        winner.Height,
					Time:          winner.Time,
					Rolls:         0,
					Voters:        0,
				}
			}
			if len(ee.ProposalPeriod.Proposals) > 0 {
				ee.IsEmpty = false
			}
		case tezos.VotingPeriodExploration:
			ee.ExplorationPeriod = NewExplorerVote(ctx, v)
			ee.ExplorationPeriod.Proposals = []*ExplorerProposal{winner}
		case tezos.VotingPeriodCooldown:
			ee.CooldownPeriod = NewExplorerVote(ctx, v)
			ee.CooldownPeriod.Proposals = []*ExplorerProposal{winner}
		case tezos.VotingPeriodPromotion:
			ee.PromotionPeriod = NewExplorerVote(ctx, v)
			ee.PromotionPeriod.Proposals = []*ExplorerProposal{winner}
		case tezos.VotingPeriodAdoption:
			ee.AdoptionPeriod = NewExplorerVote(ctx, v)
			ee.AdoptionPeriod.Proposals = []*ExplorerProposal{winner}
		}
	}
	return ee, http.StatusOK
}

type ExplorerVoter struct {
	RowId     model.AccountID      `json:"row_id"`
	Address   tezos.Address        `json:"address"`
	Rolls     int64                `json:"rolls"`
	Stake     int64                `json:"stake"`
	HasVoted  bool                 `json:"has_voted"`
	Ballot    tezos.BallotVote     `json:"ballot,omitempty"`
	Proposals []tezos.ProtocolHash `json:"proposals,omitempty"`
}

type ExplorerVoterList struct {
	list     []*ExplorerVoter
	modified time.Time
	expires  time.Time
}

func (l ExplorerVoterList) MarshalJSON() ([]byte, error) { return json.Marshal(l.list) }
func (l ExplorerVoterList) LastModified() time.Time      { return l.modified }
func (l ExplorerVoterList) Expires() time.Time           { return l.expires }

var _ Resource = (*ExplorerVoterList)(nil)

func NewExplorerVoter(ctx *ApiContext, v *model.Voter) *ExplorerVoter {
	voter := &ExplorerVoter{
		RowId:    v.RowId,
		Address:  ctx.Indexer.LookupAddress(ctx, v.RowId),
		Rolls:    v.Rolls,
		Stake:    v.Stake,
		Ballot:   v.Ballot,
		HasVoted: v.HasVoted,
	}
	if v.HasVoted {
		voter.Proposals = make([]tezos.ProtocolHash, 0)
		for _, p := range v.Proposals {
			voter.Proposals = append(voter.Proposals, govLookupProposalHash(ctx, p))
		}
	}
	return voter
}

func ListVoters(ctx *ApiContext) (interface{}, int) {
	args := &ListRequest{}
	ctx.ParseRequestArgs(args)
	election := loadElection(ctx)
	params := ctx.Indexer.ParamsByHeight(election.StartHeight)
	stage := loadStage(ctx, election, params.NumVotingPeriods)

	r := etl.ListRequest{
		Since:  election.StartHeight + int64(stage)*params.BlocksPerVotingPeriod,
		Period: election.VotingPeriod + int64(stage),
		Offset: args.Offset,
		Limit:  args.Limit, // allow higher limit to fetch all voters at once
		Cursor: args.Cursor,
		Order:  args.Order,
	}

	voters, err := ctx.Indexer.ListVoters(ctx, r)
	if err != nil {
		panic(EInternal(EC_DATABASE, err.Error(), nil))
	}

	resp := &ExplorerVoterList{
		list: make([]*ExplorerVoter, 0, len(voters)),
	}

	if election.IsOpen {
		resp.expires = ctx.Tip.BestTime.Add(params.BlockTime())
	} else {
		resp.expires = ctx.Now.Add(maxCacheExpires)
	}

	for _, v := range voters {
		if v.Rolls == 0 {
			continue
		}
		// TODO: list may contain deactivated bakers who were still in the
		// previous snapshot, but have been deactivated afterwards
		// need to confirm how the protocol works here
		resp.list = append(resp.list, NewExplorerVoter(ctx, v))
		resp.modified = util.MaxTime(resp.modified, v.Time)
	}
	return resp, http.StatusOK
}

func ListBallots(ctx *ApiContext) (interface{}, int) {
	args := &ListRequest{}
	ctx.ParseRequestArgs(args)
	election := loadElection(ctx)
	p := ctx.Indexer.ParamsByHeight(election.StartHeight)
	stage := loadStage(ctx, election, p.NumVotingPeriods)

	r := etl.ListRequest{
		Period: election.VotingPeriod + int64(stage),
		Offset: args.Offset,
		Limit:  args.Limit, // allow higher limit to fetch all voters at once
		Cursor: args.Cursor,
		Order:  args.Order,
	}

	ballots, err := ctx.Indexer.ListBallots(ctx, r)
	if err != nil {
		panic(EInternal(EC_DATABASE, "cannot read ballots", err))
	}

	// fetch op hashes for each ballot
	oids := make([]uint64, 0)
	for _, v := range ballots {
		oids = append(oids, v.OpId.Value())
	}

	// lookup
	ops, err := ctx.Indexer.LookupOpIds(ctx, vec.UniqueUint64Slice(oids))
	if err != nil && err != index.ErrNoOpEntry {
		panic(EInternal(EC_DATABASE, "cannot read ops for ballots", err))
	}

	// prepare for lookup
	opMap := make(map[model.OpID]tezos.OpHash)
	for _, v := range ops {
		opMap[v.RowId] = v.Hash
	}

	resp := &ExplorerBallotList{
		list: make([]*ExplorerBallot, 0, len(ballots)),
	}

	if election.IsOpen {
		resp.expires = ctx.Tip.BestTime.Add(p.BlockTime())
	} else {
		resp.expires = ctx.Now.Add(maxCacheExpires)
	}

	for _, v := range ballots {
		o, _ := opMap[v.OpId]
		b := NewExplorerBallot(
			ctx,
			v,
			govLookupProposalHash(ctx, v.ProposalId),
			o,
		)
		resp.list = append(resp.list, b)
		resp.modified = util.MaxTime(resp.modified, v.Time)
	}
	return resp, http.StatusOK
}
