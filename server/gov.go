// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/tzindex/chain"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
)

func init() {
	register(ExplorerElection{})
}

type ExplorerVote struct {
	VotingPeriod     int64                  `json:"voting_period"`
	VotingPeriodKind chain.VotingPeriodKind `json:"voting_period_kind"`
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
		params := ctx.Crawler.ParamsByHeight(-1)
		diff := params.BlocksPerVotingPeriod - (ctx.Crawler.Height() - v.StartHeight) - 1
		vote.EndTime = ctx.Crawler.Time().Add(time.Duration(diff) * params.TimeBetweenBlocks[0])
	}
	return vote
}

type ExplorerProposal struct {
	Hash          chain.ProtocolHash  `json:"hash"`
	SourceAddress chain.Address       `json:"source"`
	BlockHash     chain.BlockHash     `json:"block_hash"`
	OpHash        chain.OperationHash `json:"op_hash"`
	Height        int64               `json:"height"`
	Time          time.Time           `json:"time"`
	Rolls         int64               `json:"rolls"`
	Voters        int64               `json:"voters"`
}

func govLookupBlockHash(ctx *ApiContext, height int64) chain.BlockHash {
	block, err := ctx.Indexer.BlockByHeight(ctx.Context, height)
	if err != nil {
		log.Errorf("explorer: cannot resolve block height %d: %v", height, err)
		return chain.BlockHash{}
	} else {
		return block.Hash.Clone()
	}
}

func govLookupOpHash(ctx *ApiContext, id model.OpID) chain.OperationHash {
	if id == 0 {
		return chain.OperationHash{}
	}
	ops, err := ctx.Indexer.LookupOpIds(ctx.Context, []uint64{id.Value()})
	if err != nil || len(ops) == 0 {
		log.Errorf("explorer: cannot resolve op id %d: %v", id, err)
		return chain.OperationHash{}
	} else {
		return ops[0].Hash.Clone()
	}
}

func govLookupProposalHash(ctx *ApiContext, id model.ProposalID) chain.ProtocolHash {
	if id == 0 {
		return chain.ProtocolHash{}
	}
	props, err := ctx.Indexer.LookupProposalIds(ctx.Context, []uint64{id.Value()})
	if err != nil || len(props) == 0 {
		log.Errorf("explorer: cannot resolve proposal id %d: %v", id, err)
		return chain.ProtocolHash{}
	} else {
		return props[0].Hash.Clone()
	}
}

func NewExplorerProposal(ctx *ApiContext, p *model.Proposal) *ExplorerProposal {
	return &ExplorerProposal{
		Hash:          p.Hash,
		Height:        p.Height,
		Time:          p.Time,
		Rolls:         p.Rolls,
		Voters:        p.Voters,
		BlockHash:     govLookupBlockHash(ctx, p.Height),
		OpHash:        govLookupOpHash(ctx, p.OpId),
		SourceAddress: lookupAddress(ctx, p.SourceId),
	}
}

var _ RESTful = (*ExplorerElection)(nil)

type ExplorerElection struct {
	Id                  int                    `json:"election_id"`
	NumPeriods          int                    `json:"num_periods"`
	NumProposals        int                    `json:"num_proposals"`
	StartTime           time.Time              `json:"start_time"`
	EndTime             time.Time              `json:"end_time"`
	StartHeight         int64                  `json:"start_height"`
	EndHeight           int64                  `json:"end_height"`
	IsEmpty             bool                   `json:"is_empty"`
	IsOpen              bool                   `json:"is_open"`
	IsFailed            bool                   `json:"is_failed"`
	NoQuorum            bool                   `json:"no_quorum"`
	NoMajority          bool                   `json:"no_majority"`
	NoProposal          bool                   `json:"no_proposal"`
	VotingPeriodKind    chain.VotingPeriodKind `json:"voting_period"`
	ProposalPeriod      *ExplorerVote          `json:"proposal"`
	TestingVotePeriod   *ExplorerVote          `json:"testing_vote"`
	TestingPeriod       *ExplorerVote          `json:"testing"`
	PromotionVotePeriod *ExplorerVote          `json:"promotion_vote"`
	expires             time.Time              `json:"-"`
}

func NewExplorerElection(ctx *ApiContext, e *model.Election) *ExplorerElection {
	election := &ExplorerElection{
		Id:               int(e.RowId),
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
		VotingPeriodKind: chain.ToVotingPeriod(e.NumPeriods),
	}
	// estimate end time for open elections
	if election.IsOpen {
		p := ctx.Crawler.ParamsByHeight(-1)
		tm := ctx.Crawler.Time()
		diff := 4*p.BlocksPerVotingPeriod - (ctx.Crawler.Height() - e.StartHeight)
		election.EndTime = tm.Add(time.Duration(diff) * p.TimeBetweenBlocks[0])
		election.EndHeight = election.StartHeight + 4*p.BlocksPerVotingPeriod - 1
		election.expires = tm.Add(p.TimeBetweenBlocks[0])
	}
	return election
}

type ExplorerBallot struct {
	Height           int64                  `json:"height"`
	Timestamp        time.Time              `json:"time"`
	ElectionId       int                    `json:"election_id"`
	VotingPeriod     int64                  `json:"voting_period"`
	VotingPeriodKind chain.VotingPeriodKind `json:"voting_period_kind"`
	Proposal         chain.ProtocolHash     `json:"proposal"`
	OpHash           chain.OperationHash    `json:"op"`
	Ballot           chain.BallotVote       `json:"ballot"`
	Rolls            int64                  `json:"rolls"`
	Bond             float64                `json:"bond"`
	Delegated        float64                `json:"delegated"`
	NDelegations     int64                  `json:"n_delegations"`
}

func NewExplorerBallot(ctx *ApiContext, b *model.Ballot, p chain.ProtocolHash, o chain.OperationHash) *ExplorerBallot {
	// we need params at the time of operation
	params := ctx.Crawler.ParamsByHeight(b.Height)
	ballot := &ExplorerBallot{
		Height:           b.Height,
		Timestamp:        b.Time,
		ElectionId:       int(b.ElectionId),
		VotingPeriod:     b.VotingPeriod,
		VotingPeriodKind: b.VotingPeriodKind,
		Proposal:         p,
		OpHash:           o,
		Ballot:           b.Ballot,
		Rolls:            b.Rolls,
	}
	// fetch voting roll snapshot at beginning of the election cycle
	// which is index 15 at the end of the cycle before
	snapcycle := params.VotingStartCycleFromHeight(b.Height)
	snap, err := ctx.Indexer.LookupSnapshot(ctx, b.SourceId, snapcycle-1, 15)
	if err != nil && err != index.ErrNoSnapshotEntry {
		log.Errorf("cannot read voting snapshot [%d/15] for ballot %d: %v",
			snapcycle-1, b.RowId, err)
	} else {
		ballot.Bond = params.ConvertValue(snap.Balance)
		ballot.Delegated = params.ConvertValue(snap.Delegated)
		ballot.NDelegations = snap.NDelegations
	}
	return ballot
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
			election, err = ctx.Indexer.ElectionByHeight(ctx.Context, ctx.Crawler.Height())
		case strings.HasPrefix(id, chain.HashTypeProtocol.Prefix()):
			var p chain.ProtocolHash
			p, err = chain.ParseProtocolHash(id)
			if err != nil {
				panic(EBadRequest(EC_RESOURCE_ID_MALFORMED, "invalid proposal", err))
			}
			var proposal *model.Proposal
			proposal, err = ctx.Indexer.LookupProposal(ctx.Context, p)
			if err != nil {
				switch err {
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

func ReadElection(ctx *ApiContext) (interface{}, int) {
	election := loadElection(ctx)
	votes, err := ctx.Indexer.VotesByElection(ctx, election.RowId)
	if err != nil {
		panic(EInternal(EC_DATABASE, err.Error(), nil))
	}
	proposals, err := ctx.Indexer.ProposalsByElection(ctx, election.RowId)
	if err != nil {
		panic(EInternal(EC_DATABASE, err.Error(), nil))
	}
	ee := NewExplorerElection(ctx, election)
	ee.NoProposal = ee.NumProposals == 0
	var winner *ExplorerProposal
	for _, v := range votes {
		switch v.VotingPeriodKind {
		case chain.VotingPeriodProposal:
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
		case chain.VotingPeriodTestingVote:
			ee.TestingVotePeriod = NewExplorerVote(ctx, v)
			ee.TestingVotePeriod.Proposals = []*ExplorerProposal{winner}
		case chain.VotingPeriodTesting:
			ee.TestingPeriod = NewExplorerVote(ctx, v)
			ee.TestingPeriod.Proposals = []*ExplorerProposal{winner}
		case chain.VotingPeriodPromotionVote:
			ee.PromotionVotePeriod = NewExplorerVote(ctx, v)
			ee.PromotionVotePeriod.Proposals = []*ExplorerProposal{winner}
		}
	}
	return ee, http.StatusOK
}
