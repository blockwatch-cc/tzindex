// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/server"
)

func init() {
	server.Register(Election{})
}

type Vote struct {
	VotingPeriod     int64                  `json:"voting_period"`
	VotingPeriodKind tezos.VotingPeriodKind `json:"voting_period_kind"`
	StartTime        time.Time              `json:"period_start_time"`
	EndTime          time.Time              `json:"period_end_time"`
	StartHeight      int64                  `json:"period_start_block"`
	EndHeight        int64                  `json:"period_end_block"`
	EligibleRolls    int64                  `json:"eligible_rolls"`
	EligibleStake    float64                `json:"eligible_stake"`
	EligibleVoters   int64                  `json:"eligible_voters"`
	QuorumPct        int64                  `json:"quorum_pct"`
	QuorumRolls      int64                  `json:"quorum_rolls"`
	QuorumStake      float64                `json:"quorum_stake"`
	TurnoutRolls     int64                  `json:"turnout_rolls"`
	TurnoutStake     float64                `json:"turnout_stake"`
	TurnoutVoters    int64                  `json:"turnout_voters"`
	TurnoutPct       int64                  `json:"turnout_pct"`
	TurnoutEma       int64                  `json:"turnout_ema"`
	YayRolls         int64                  `json:"yay_rolls"`
	YayStake         float64                `json:"yay_stake"`
	YayVoters        int64                  `json:"yay_voters"`
	NayRolls         int64                  `json:"nay_rolls"`
	NayStake         float64                `json:"nay_stake"`
	NayVoters        int64                  `json:"nay_voters"`
	PassRolls        int64                  `json:"pass_rolls"`
	PassStake        float64                `json:"pass_stake"`
	PassVoters       int64                  `json:"pass_voters"`
	IsOpen           bool                   `json:"is_open"`
	IsFailed         bool                   `json:"is_failed"`
	IsDraw           bool                   `json:"is_draw"`
	NoProposal       bool                   `json:"no_proposal"`
	NoQuorum         bool                   `json:"no_quorum"`
	NoMajority       bool                   `json:"no_majority"`
	Proposals        []*Proposal            `json:"proposals"`
}

func NewVote(ctx *server.Context, v *model.Vote) *Vote {
	vote := &Vote{
		VotingPeriod:     v.VotingPeriod,
		VotingPeriodKind: v.VotingPeriodKind,
		StartTime:        v.StartTime,
		EndTime:          v.EndTime,
		StartHeight:      v.StartHeight,
		EndHeight:        v.EndHeight,
		EligibleRolls:    v.EligibleRolls,
		EligibleStake:    ctx.Params.ConvertValue(v.EligibleStake),
		EligibleVoters:   v.EligibleVoters,
		QuorumPct:        v.QuorumPct,
		QuorumRolls:      v.QuorumRolls,
		QuorumStake:      ctx.Params.ConvertValue(v.QuorumStake),
		TurnoutRolls:     v.TurnoutRolls,
		TurnoutStake:     ctx.Params.ConvertValue(v.TurnoutStake),
		TurnoutVoters:    v.TurnoutVoters,
		TurnoutPct:       v.TurnoutPct,
		TurnoutEma:       v.TurnoutEma,
		YayRolls:         v.YayRolls,
		YayStake:         ctx.Params.ConvertValue(v.YayStake),
		YayVoters:        v.YayVoters,
		NayRolls:         v.NayRolls,
		NayStake:         ctx.Params.ConvertValue(v.NayStake),
		NayVoters:        v.NayVoters,
		PassRolls:        v.PassRolls,
		PassStake:        ctx.Params.ConvertValue(v.PassStake),
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
		params := ctx.Indexer.ParamsByHeight(v.StartHeight)
		diff := params.BlocksPerVotingPeriod - (ctx.Tip.BestHeight - v.StartHeight) - 1
		vote.EndTime = ctx.Tip.BestTime.Add(time.Duration(diff) * params.BlockTime())
	}
	return vote
}

type Proposal struct {
	Hash          tezos.ProtocolHash `json:"hash"`
	SourceAddress tezos.Address      `json:"source"`
	BlockHash     tezos.BlockHash    `json:"block_hash"`
	OpHash        tezos.OpHash       `json:"op_hash"`
	Height        int64              `json:"height"`
	Time          time.Time          `json:"time"`
	Rolls         int64              `json:"rolls"`
	Stake         float64            `json:"stake"`
	Voters        int64              `json:"voters"`
}

func NewProposal(ctx *server.Context, p *model.Proposal) *Proposal {
	return &Proposal{
		Hash:          p.Hash,
		Height:        p.Height,
		Time:          p.Time,
		Rolls:         p.Rolls,
		Stake:         ctx.Params.ConvertValue(p.Stake),
		Voters:        p.Voters,
		BlockHash:     ctx.Indexer.LookupBlockHash(ctx, p.Height),
		OpHash:        ctx.Indexer.LookupOpHash(ctx, p.OpId),
		SourceAddress: ctx.Indexer.LookupAddress(ctx, p.SourceId),
	}
}

type Ballot struct {
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
	Stake            float64                `json:"stake"`
	Sender           string                 `json:"sender"`
}

type BallotList struct {
	list     []*Ballot
	modified time.Time
	expires  time.Time
}

func (l BallotList) MarshalJSON() ([]byte, error) { return json.Marshal(l.list) }
func (l BallotList) LastModified() time.Time      { return l.modified }
func (l BallotList) Expires() time.Time           { return l.expires }

var _ server.Resource = (*BallotList)(nil)

func NewBallot(ctx *server.Context, b *model.Ballot, p tezos.ProtocolHash, o tezos.OpHash) *Ballot {
	return &Ballot{
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
		Stake:            ctx.Params.ConvertValue(b.Stake),
		Sender:           ctx.Indexer.LookupAddress(ctx, b.SourceId).String(),
	}
}

type Election struct {
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
	ProposalPeriod    *Vote                  `json:"proposal"`
	ExplorationPeriod *Vote                  `json:"exploration"`
	CooldownPeriod    *Vote                  `json:"cooldown"`
	PromotionPeriod   *Vote                  `json:"promotion"`
	AdoptionPeriod    *Vote                  `json:"adoption"`
	expires           time.Time              `json:"-"`
}

var _ server.RESTful = (*Election)(nil)
var _ server.Resource = (*Election)(nil)

func NewElection(ctx *server.Context, e *model.Election) *Election {
	p := ctx.Indexer.ParamsByHeight(e.StartHeight)
	election := &Election{
		Id:               int(e.RowId),
		MaxPeriods:       p.NumVotingPeriods,
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
			election.expires = ctx.Now.Add(ctx.Cfg.Http.CacheMaxExpires)
		}
	}
	return election
}

func (b Election) LastModified() time.Time {
	if b.IsOpen {
		return time.Now().UTC()
	}
	return b.EndTime
}

func (b Election) Expires() time.Time {
	return b.expires
}

func (b Election) RESTPrefix() string {
	return "/explorer/election"
}

func (b Election) RESTPath(r *mux.Router) string {
	path, _ := r.Get("election").URLPath("ident", strconv.Itoa(b.Id))
	return path.String()
}

func (b Election) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (b Election) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/{ident}", server.C(ReadElection)).Methods("GET").Name("election")
	r.HandleFunc("/{ident}/{stage}/ballots", server.C(ListBallots)).Methods("GET")
	r.HandleFunc("/{ident}/{stage}/voters", server.C(ListVoters)).Methods("GET")
	return nil

}

func loadElection(ctx *server.Context) *model.Election {
	// from number or block height
	if id, ok := mux.Vars(ctx.Request)["ident"]; !ok || id == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing election identifier", nil))
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
				panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid proposal", err))
			}
			var proposal *model.Proposal
			proposal, err = ctx.Indexer.LookupProposal(ctx.Context, p)
			if err != nil {
				switch err {
				case etl.ErrNoTable:
					panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access proposal table", err))
				case index.ErrNoProposalEntry:
					panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no proposal", err))
				default:
					panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
				}
			}
			election, err = ctx.Indexer.ElectionById(ctx.Context, proposal.ElectionId)
		default:
			var i int64
			i, err = strconv.ParseInt(id, 10, 64)
			if err != nil {
				panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid election id", err))
			}
			election, err = ctx.Indexer.ElectionById(ctx.Context, model.ElectionID(i))
		}
		if err != nil {
			switch err {
			case etl.ErrNoTable:
				panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access election table", err))
			case index.ErrNoElectionEntry:
				panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "no election", err))
			default:
				panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
			}
		}
		return election
	}
}

func loadStage(ctx *server.Context, election *model.Election, maxPeriods int) int {
	var stage int // 1 .. 4 (5 in Edo) (same as tezos.VotingPeriodKind)
	if s, ok := mux.Vars(ctx.Request)["stage"]; !ok || s == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing voting period identifier", nil))
	} else {
		if i, err := strconv.Atoi(s); err != nil {
			panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid voting period identifier", err))
		} else if i < 1 || i > maxPeriods {
			panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid voting period identifier", err))
		} else if i > election.NumPeriods {
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "voting period does not exist", nil))
		} else {
			stage = i
		}
	}
	// adjust to 0..3 (4 from Edo)
	return stage - 1
}

func ReadElection(ctx *server.Context) (interface{}, int) {
	election := loadElection(ctx)
	votes, err := ctx.Indexer.VotesByElection(ctx, election.RowId)
	if err != nil {
		switch err {
		case etl.ErrNoTable:
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access vote table", err))
		default:
			panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
		}
	}
	proposals, err := ctx.Indexer.ProposalsByElection(ctx, election.RowId)
	if err != nil {
		switch err {
		case etl.ErrNoTable:
			panic(server.ENotFound(server.EC_RESOURCE_NOTFOUND, "cannot access proposal table", err))
		default:
			panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
		}
	}
	ee := NewElection(ctx, election)
	ee.NoProposal = ee.NumProposals == 0
	var winner *Proposal
	for _, v := range votes {
		switch v.VotingPeriodKind {
		case tezos.VotingPeriodProposal:
			ee.ProposalPeriod = NewVote(ctx, v)
			ee.ProposalPeriod.Proposals = make([]*Proposal, 0)
			for _, vv := range proposals {
				p := NewProposal(ctx, vv)
				if winner == nil || winner.Rolls < p.Rolls {
					winner = p
				}
				ee.ProposalPeriod.Proposals = append(ee.ProposalPeriod.Proposals, p)
			}
			// copy winner for subsequent periods
			if winner != nil {
				winner = &Proposal{
					Hash:          winner.Hash,
					SourceAddress: winner.SourceAddress,
					BlockHash:     winner.BlockHash,
					OpHash:        winner.OpHash,
					Height:        winner.Height,
					Time:          winner.Time,
					Rolls:         0,
					Stake:         0,
					Voters:        0,
				}
			}
			if len(ee.ProposalPeriod.Proposals) > 0 {
				ee.IsEmpty = false
			}
		case tezos.VotingPeriodExploration:
			ee.ExplorationPeriod = NewVote(ctx, v)
			ee.ExplorationPeriod.Proposals = []*Proposal{winner}
		case tezos.VotingPeriodCooldown:
			ee.CooldownPeriod = NewVote(ctx, v)
			ee.CooldownPeriod.Proposals = []*Proposal{winner}
		case tezos.VotingPeriodPromotion:
			ee.PromotionPeriod = NewVote(ctx, v)
			ee.PromotionPeriod.Proposals = []*Proposal{winner}
		case tezos.VotingPeriodAdoption:
			ee.AdoptionPeriod = NewVote(ctx, v)
			ee.AdoptionPeriod.Proposals = []*Proposal{winner}
		}
	}
	return ee, http.StatusOK
}

type Voter struct {
	RowId     model.AccountID      `json:"row_id"`
	Address   tezos.Address        `json:"address"`
	Rolls     int64                `json:"rolls"`
	Stake     float64              `json:"stake"`
	HasVoted  bool                 `json:"has_voted"`
	Ballot    tezos.BallotVote     `json:"ballot,omitempty"`
	Proposals []tezos.ProtocolHash `json:"proposals,omitempty"`
}

type VoterList struct {
	list     []*Voter
	modified time.Time
	expires  time.Time
}

func (l VoterList) MarshalJSON() ([]byte, error) { return json.Marshal(l.list) }
func (l VoterList) LastModified() time.Time      { return l.modified }
func (l VoterList) Expires() time.Time           { return l.expires }

var _ server.Resource = (*VoterList)(nil)

func NewVoter(ctx *server.Context, v *model.Voter) *Voter {
	voter := &Voter{
		RowId:    v.RowId,
		Address:  ctx.Indexer.LookupAddress(ctx, v.RowId),
		Rolls:    v.Rolls,
		Stake:    ctx.Params.ConvertValue(v.Stake),
		Ballot:   v.Ballot,
		HasVoted: v.HasVoted,
	}
	if v.HasVoted {
		voter.Proposals = make([]tezos.ProtocolHash, 0)
		for _, p := range v.Proposals {
			voter.Proposals = append(voter.Proposals, ctx.Indexer.LookupProposalHash(ctx, p))
		}
	}
	return voter
}

func ListVoters(ctx *server.Context) (interface{}, int) {
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
		panic(server.EInternal(server.EC_DATABASE, err.Error(), nil))
	}

	resp := &VoterList{
		list: make([]*Voter, 0, len(voters)),
	}

	if election.IsOpen {
		resp.expires = ctx.Tip.BestTime.Add(params.BlockTime())
	} else {
		resp.expires = ctx.Now.Add(ctx.Cfg.Http.CacheMaxExpires)
	}

	for _, v := range voters {
		if v.Rolls == 0 {
			continue
		}
		resp.list = append(resp.list, NewVoter(ctx, v))
		resp.modified = util.MaxTime(resp.modified, v.Time)
	}
	return resp, http.StatusOK
}

func ListBallots(ctx *server.Context) (interface{}, int) {
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
		panic(server.EInternal(server.EC_DATABASE, "cannot read ballots", err))
	}

	// fetch op hashes for each ballot
	oids := make([]uint64, 0)
	for _, v := range ballots {
		oids = append(oids, v.OpId.Value())
	}

	// lookup
	ops, err := ctx.Indexer.LookupOpIds(ctx, vec.UniqueUint64Slice(oids))
	if err != nil && err != index.ErrNoOpEntry {
		panic(server.EInternal(server.EC_DATABASE, "cannot read ops for ballots", err))
	}

	// prepare for lookup
	opMap := make(map[model.OpID]tezos.OpHash)
	for _, v := range ops {
		opMap[v.RowId] = v.Hash
	}

	resp := &BallotList{
		list: make([]*Ballot, 0, len(ballots)),
	}

	if election.IsOpen {
		resp.expires = ctx.Tip.BestTime.Add(p.BlockTime())
	} else {
		resp.expires = ctx.Now.Add(ctx.Cfg.Http.CacheMaxExpires)
	}

	for _, v := range ballots {
		o, _ := opMap[v.OpId]
		b := NewBallot(ctx, v, ctx.Indexer.LookupProposalHash(ctx, v.ProposalId), o)
		resp.list = append(resp.list, b)
		resp.modified = util.MaxTime(resp.modified, v.Time)
	}
	return resp, http.StatusOK
}
