// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package index

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"

	"blockwatch.cc/tzindex/chain"
	. "blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

var (
	GovPackSizeLog2    = 14 // 16k
	GovJournalSizeLog2 = 15 // 32k
	GovCacheSize       = 2
	GovFillLevel       = 100
	GovIndexKey        = "gov"
	ElectionTableKey   = "election"
	ProposalTableKey   = "proposal"
	VoteTableKey       = "vote"
	BallotTableKey     = "ballot"
)

var (
	// ErrNoElectionEntry is an error that indicates a requested entry does
	// not exist in the election table.
	ErrNoElectionEntry = errors.New("election not found")

	// ErrNoProposalEntry is an error that indicates a requested entry does
	// not exist in the proposal table.
	ErrNoProposalEntry = errors.New("proposal not found")

	// ErrNoVoteEntry is an error that indicates a requested entry does
	// not exist in the vote table.
	ErrNoVoteEntry = errors.New("vote not found")

	// ErrNoBallotEntry is an error that indicates a requested entry does
	// not exist in the ballot table.
	ErrNoBallotEntry = errors.New("ballot not found")
)

type GovIndex struct {
	db            *pack.DB
	opts          pack.Options
	electionTable *pack.Table
	proposalTable *pack.Table
	voteTable     *pack.Table
	ballotTable   *pack.Table
}

var _ BlockIndexer = (*GovIndex)(nil)

func NewGovIndex(opts pack.Options) *GovIndex {
	return &GovIndex{opts: opts}
}

func (idx *GovIndex) DB() *pack.DB {
	return idx.db
}

func (idx *GovIndex) Tables() []*pack.Table {
	return []*pack.Table{
		idx.electionTable,
		idx.proposalTable,
		idx.voteTable,
		idx.ballotTable,
	}
}

func (idx *GovIndex) Key() string {
	return GovIndexKey
}

func (idx *GovIndex) Name() string {
	return GovIndexKey + " index"
}

func (idx *GovIndex) Create(path, label string, opts interface{}) error {
	electionFields, err := pack.Fields(Election{})
	if err != nil {
		return err
	}
	proposalFields, err := pack.Fields(Proposal{})
	if err != nil {
		return err
	}
	voteFields, err := pack.Fields(Vote{})
	if err != nil {
		return err
	}
	ballotFields, err := pack.Fields(Ballot{})
	if err != nil {
		return err
	}
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %v", idx.Key(), err)
	}
	defer db.Close()

	_, err = db.CreateTableIfNotExists(
		ElectionTableKey,
		electionFields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, GovPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, GovJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, GovCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, GovFillLevel),
		})
	_, err = db.CreateTableIfNotExists(
		ProposalTableKey,
		proposalFields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, GovPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, GovJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, GovCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, GovFillLevel),
		})
	_, err = db.CreateTableIfNotExists(
		VoteTableKey,
		voteFields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, GovPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, GovJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, GovCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, GovFillLevel),
		})
	_, err = db.CreateTableIfNotExists(
		BallotTableKey,
		ballotFields,
		pack.Options{
			PackSizeLog2:    util.NonZero(idx.opts.PackSizeLog2, GovPackSizeLog2),
			JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, GovJournalSizeLog2),
			CacheSize:       util.NonZero(idx.opts.CacheSize, GovCacheSize),
			FillLevel:       util.NonZero(idx.opts.FillLevel, GovFillLevel),
		})
	return err
}

func (idx *GovIndex) Init(path, label string, opts interface{}) error {
	var err error
	idx.db, err = pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.electionTable, err = idx.db.Table(ElectionTableKey, pack.Options{
		JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, GovJournalSizeLog2),
		CacheSize:       util.NonZero(idx.opts.CacheSize, GovCacheSize),
	})
	if err != nil {
		idx.Close()
		return err
	}
	idx.proposalTable, err = idx.db.Table(ProposalTableKey, pack.Options{
		JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, GovJournalSizeLog2),
		CacheSize:       util.NonZero(idx.opts.CacheSize, GovCacheSize),
	})
	if err != nil {
		idx.Close()
		return err
	}
	idx.voteTable, err = idx.db.Table(VoteTableKey, pack.Options{
		JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, GovJournalSizeLog2),
		CacheSize:       util.NonZero(idx.opts.CacheSize, GovCacheSize),
	})
	if err != nil {
		idx.Close()
		return err
	}
	idx.ballotTable, err = idx.db.Table(BallotTableKey, pack.Options{
		JournalSizeLog2: util.NonZero(idx.opts.JournalSizeLog2, GovJournalSizeLog2),
		CacheSize:       util.NonZero(idx.opts.CacheSize, GovCacheSize),
	})
	if err != nil {
		idx.Close()
		return err
	}
	return nil
}

func (idx *GovIndex) Close() error {
	for _, v := range idx.Tables() {
		if v != nil {
			if err := v.Close(); err != nil {
				log.Errorf("Closing %s table: %v", v.Name(), err)
			}
		}
	}
	idx.electionTable = nil
	idx.proposalTable = nil
	idx.voteTable = nil
	idx.ballotTable = nil
	if idx.db != nil {
		if err := idx.db.Close(); err != nil {
			return err
		}
		idx.db = nil
	}
	return nil
}

func (idx *GovIndex) ConnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	// detect first and last block of a voting period
	isPeriodStart := block.Height == 0 || block.Height%block.Params.BlocksPerVotingPeriod == 0
	isPeriodEnd := block.Height > 0 && (block.Height+1)%block.Params.BlocksPerVotingPeriod == 0

	// open a new election or vote on first block
	if isPeriodStart {
		if block.VotingPeriodKind == chain.VotingPeriodProposal {
			if err := idx.openElection(ctx, block, builder); err != nil {
				log.Errorf("Open election at block %d %s: %v", block.Height, block.VotingPeriodKind, err)
				return err
			}
		}
		if err := idx.openVote(ctx, block, builder); err != nil {
			log.Errorf("Open vote at block %d %s: %v", block.Height, block.VotingPeriodKind, err)
			return err
		}
	}

	// process proposals (1) or ballots (2, 4)
	var err error
	switch block.VotingPeriodKind {
	case chain.VotingPeriodProposal:
		err = idx.processProposals(ctx, block, builder)
	case chain.VotingPeriodTestingVote:
		err = idx.processBallots(ctx, block, builder)
	case chain.VotingPeriodTesting:
		// nothing to do here
	case chain.VotingPeriodPromotionVote:
		err = idx.processBallots(ctx, block, builder)
	}
	if err != nil {
		return err
	}

	// close any previous period after last block
	if isPeriodEnd {
		// close the last vote
		success, err := idx.closeVote(ctx, block, builder)
		if err != nil {
			log.Errorf("Open close at block %d %s: %v", block.Height, block.VotingPeriodKind, err)
			return err
		}

		// on failure or on end, close last election
		if !success || block.VotingPeriodKind == chain.VotingPeriodPromotionVote {
			if err := idx.closeElection(ctx, block, builder); err != nil {
				log.Errorf("Close election at block %d %s: %v", block.Height, block.VotingPeriodKind, err)
				return err
			}
		}
	}
	return nil
}

func (idx *GovIndex) DisconnectBlock(ctx context.Context, block *Block, _ BlockBuilder) error {
	return idx.DeleteBlock(ctx, block.Height)
}

func (idx *GovIndex) DeleteBlock(ctx context.Context, height int64) error {
	// delete ballots by height
	q := pack.Query{
		Name: "etl.ballots.delete",
		Conditions: pack.ConditionList{pack.Condition{
			Field: idx.ballotTable.Fields().Find("h"), // block height (!)
			Mode:  pack.FilterModeEqual,
			Value: height,
		}},
	}
	if _, err := idx.ballotTable.Delete(ctx, q); err != nil {
		return nil
	}

	// delete proposals by height
	q = pack.Query{
		Name: "etl.proposals.delete",
		Conditions: pack.ConditionList{pack.Condition{
			Field: idx.proposalTable.Fields().Find("h"), // block height (!)
			Mode:  pack.FilterModeEqual,
			Value: height,
		}},
	}
	if _, err := idx.proposalTable.Delete(ctx, q); err != nil {
		return nil
	}

	// on vote period start, delete vote by start height
	q = pack.Query{
		Name: "etl.vote.delete",
		Conditions: pack.ConditionList{pack.Condition{
			Field: idx.voteTable.Fields().Find("H"), // start height (!)
			Mode:  pack.FilterModeEqual,
			Value: height,
		}},
	}
	if _, err := idx.voteTable.Delete(ctx, q); err != nil {
		return nil
	}

	// on election start, delete election (maybe not because we use row id as counter)
	q = pack.Query{
		Name: "etl.election.delete",
		Conditions: pack.ConditionList{pack.Condition{
			Field: idx.electionTable.Fields().Find("H"), // start height (!)
			Mode:  pack.FilterModeEqual,
			Value: height,
		}},
	}
	if _, err := idx.electionTable.Delete(ctx, q); err != nil {
		return nil
	}
	return nil
}

func (idx *GovIndex) openElection(ctx context.Context, block *Block, builder BlockBuilder) error {
	election := &Election{
		NumPeriods:   1,
		VotingPeriod: block.TZ.Block.Metadata.Level.VotingPeriod,
		StartTime:    block.Timestamp,
		EndTime:      time.Time{}.UTC(), // set on close
		StartHeight:  block.Height,
		EndHeight:    0, // set on close
		IsEmpty:      true,
		IsOpen:       true,
		IsFailed:     false,
		NoQuorum:     false,
		NoMajority:   false,
	}
	return idx.electionTable.Insert(ctx, election)
}

func (idx *GovIndex) closeElection(ctx context.Context, block *Block, builder BlockBuilder) error {
	// load current election
	election, err := idx.electionByHeight(ctx, block.Height, block.VotingPeriodKind, block.Params)
	if err != nil {
		return err
	}
	// check its open
	if !election.IsOpen {
		return fmt.Errorf("closing election: election %d already closed", election.RowId)
	}
	// load current vote
	vote, err := idx.voteByHeight(ctx, block.Height, block.Params)
	if err != nil {
		return err
	}
	// check its closed
	if vote.IsOpen {
		return fmt.Errorf("closing election: vote %d/%d is not closed", vote.ElectionId, vote.VotingPeriod)
	}
	election.EndHeight = vote.EndHeight
	election.EndTime = vote.EndTime
	election.IsOpen = false
	election.IsEmpty = election.NumPeriods == 1 && vote.IsFailed
	election.IsFailed = vote.IsFailed
	election.NoQuorum = vote.NoQuorum
	election.NoMajority = vote.NoMajority
	return idx.electionTable.Update(ctx, election)
}

func (idx *GovIndex) openVote(ctx context.Context, block *Block, builder BlockBuilder) error {
	// load current election, must exist
	election, err := idx.electionByHeight(ctx, block.Height, block.VotingPeriodKind, block.Params)
	if err != nil {
		return err
	}
	if !election.IsOpen {
		return fmt.Errorf("opening vote: election %d already closed", election.RowId)
	}

	// update election
	switch block.VotingPeriodKind {
	case chain.VotingPeriodTestingVote:
		election.NumPeriods = 2
	case chain.VotingPeriodTesting:
		election.NumPeriods = 3
	case chain.VotingPeriodPromotionVote:
		election.NumPeriods = 4
	}

	p := block.Params
	vote := &Vote{
		ElectionId:       election.RowId,
		ProposalId:       election.ProposalId, // voted proposal, zero in first voting period
		VotingPeriod:     block.TZ.Block.Metadata.Level.VotingPeriod,
		VotingPeriodKind: block.VotingPeriodKind,
		StartTime:        block.Timestamp,
		EndTime:          time.Time{}.UTC(), // set on close
		StartHeight:      block.Height,
		EndHeight:        block.Height + p.BlocksPerVotingPeriod - 1,
		IsOpen:           true,
	}

	// add rolls and calculate quorum
	// - for roll snapshot we use the parent block's chain data
	// - at genesis there is no parent block, we use defaults here
	if block.Height == 0 {
		cd := block.Chain
		vote.EligibleRolls = cd.Rolls
		vote.EligibleVoters = cd.RollOwners
		vote.QuorumPct = 8000
		vote.QuorumRolls = vote.EligibleRolls * vote.QuorumPct / 10000
	} else {
		cd := block.Parent.Chain
		vote.EligibleRolls = cd.Rolls
		vote.EligibleVoters = cd.RollOwners
		switch vote.VotingPeriodKind {
		case chain.VotingPeriodProposal:
			// fixed min proposal quorum as defined by protocol
			vote.QuorumPct = p.MinProposalQuorum
		case chain.VotingPeriodTesting:
			// no quorum
			vote.QuorumPct = 0
		case chain.VotingPeriodTestingVote, chain.VotingPeriodPromotionVote:
			// from most recent (testing_vote or promotion_vote) period
			quorumPct, turnoutEma, err := idx.quorumByHeight(ctx, block.Height, p)
			if err != nil {
				return err
			}
			// quorum adjusts at the end of each voting period (exploration & promotion)
			vote.QuorumPct = quorumPct
			vote.TurnoutEma = turnoutEma
		}
		vote.QuorumRolls = vote.EligibleRolls * vote.QuorumPct / 10000
	}

	// insert vote
	if err := idx.voteTable.Insert(ctx, vote); err != nil {
		return err
	}

	// update election
	return idx.electionTable.Update(ctx, election)
}

func (idx *GovIndex) closeVote(ctx context.Context, block *Block, builder BlockBuilder) (bool, error) {
	// load current vote
	vote, err := idx.voteByHeight(ctx, block.Height, block.Params)
	if err != nil {
		return false, err
	}
	// check its open
	if !vote.IsOpen {
		return false, fmt.Errorf("closing vote: vote %d/%d is already closed", vote.ElectionId, vote.VotingPeriod)
	}
	params := block.Params

	// determine result
	switch vote.VotingPeriodKind {
	case chain.VotingPeriodProposal:
		// select the winning proposal if any and update election
		var isDraw bool
		if vote.TurnoutRolls > 0 {
			// load ballots (technically they are not ballots on-chain, but we store them as such)
			ballots, err := idx.ballotsByVote(ctx, vote.VotingPeriod)
			if err != nil {
				return false, err
			}
			proposals, err := idx.proposalsByElection(ctx, vote.ElectionId)
			if err != nil {
				return false, err
			}

			proposalMap := make(map[ProposalID]*Proposal)
			for _, v := range proposals {
				proposalMap[v.RowId] = v
			}

			// count proposal votes
			for _, v := range ballots {
				if p, ok := proposalMap[v.ProposalId]; ok {
					p.Rolls += v.Rolls
					p.Voters++
				} else {
					return false, fmt.Errorf("closing vote: %d/%d unknown proposal id %d from ballot %d",
						vote.ElectionId, vote.VotingPeriod, v.ProposalId, v.RowId)
				}
			}

			// update proposals
			items := make([]pack.Item, len(proposals))
			for i, v := range proposals {
				items[i] = v
			}
			if err := idx.proposalTable.Update(ctx, items); err != nil {
				return false, err
			}

			// select the winner
			var (
				winner ProposalID
				count  int64
			)
			for _, v := range proposals {
				if v.Rolls < count {
					continue
				}
				if v.Rolls > count {
					isDraw = false
					count = v.Rolls
					winner = v.RowId
				} else {
					isDraw = true
				}
			}

			if !isDraw {
				// load current election, must exist
				election, err := idx.electionByHeight(ctx, block.Height, block.VotingPeriodKind, block.Params)
				if err != nil {
					return false, err
				}
				// store winner and update election
				election.ProposalId = winner
				if err := idx.electionTable.Update(ctx, election); err != nil {
					return false, err
				}
				vote.ProposalId = winner
			}
		}
		vote.NoProposal = vote.TurnoutRolls == 0
		vote.NoQuorum = params.MinProposalQuorum > 0 && vote.TurnoutRolls < vote.QuorumRolls
		vote.IsDraw = isDraw
		vote.IsFailed = vote.NoProposal || vote.NoQuorum || vote.IsDraw

	case chain.VotingPeriodTestingVote, chain.VotingPeriodPromotionVote:
		vote.NoQuorum = vote.TurnoutRolls < vote.QuorumRolls
		vote.NoMajority = vote.YayRolls < (vote.YayRolls+vote.NayRolls)*8/10
		vote.IsFailed = vote.NoQuorum || vote.NoMajority

	case chain.VotingPeriodTesting:
		// empty, cannot fail
	}

	vote.EndTime = block.Timestamp
	vote.IsOpen = false

	if err := idx.voteTable.Update(ctx, vote); err != nil {
		return false, err
	}

	return !vote.IsFailed, nil
}

func (idx *GovIndex) processProposals(ctx context.Context, block *Block, builder BlockBuilder) error {
	// skip blocks without proposals
	if block.NProposal == 0 {
		return nil
	}

	// load current vote
	vote, err := idx.voteByHeight(ctx, block.Height, block.Params)
	if err != nil {
		return err
	}

	// load known proposals
	proposals, err := idx.proposalsByElection(ctx, vote.ElectionId)
	if err != nil {
		return err
	}
	proposalMap := make(map[string]*Proposal)
	for _, v := range proposals {
		proposalMap[v.Hash.String()] = v
	}

	// find unknown proposals
	insProposals := make([]pack.Item, 0)
	for _, op := range block.Ops {
		if op.Type != chain.OpTypeProposals {
			continue
		}
		cop, ok := block.GetRPCOp(op.OpN, op.OpC)
		if !ok {
			return fmt.Errorf("missing proposal op [%d:%d]", op.OpN, op.OpC)
		}
		pop, ok := cop.(*rpc.ProposalsOp)
		if !ok {
			return fmt.Errorf("ballot op [%d:%d]: unexpected type %T ", op.OpN, op.OpC, cop)
		}
		acc, aok := builder.AccountByAddress(pop.Source)
		if !aok {
			return fmt.Errorf("missing account %s in proposal op [%d:%d]", pop.Source, op.OpN, op.OpC)
		}
		for _, v := range pop.Proposals {
			if _, ok := proposalMap[v.String()]; ok {
				continue
			}
			prop := &Proposal{
				Hash:         v,
				Height:       block.Height,
				Time:         block.Timestamp,
				SourceId:     acc.RowId,
				OpId:         op.RowId,
				ElectionId:   vote.ElectionId,
				VotingPeriod: vote.VotingPeriod,
			}
			insProposals = append(insProposals, prop)
		}
	}

	// insert unknown proposals to create ids
	if len(insProposals) > 0 {
		if err := idx.proposalTable.Insert(ctx, insProposals); err != nil {
			return err
		}
		for _, v := range insProposals {
			p := v.(*Proposal)
			proposalMap[p.Hash.String()] = p
		}
		// update election, counting proposals
		election, err := idx.electionByHeight(ctx, block.Height, block.VotingPeriodKind, block.Params)
		if err != nil {
			return err
		}
		// check its open
		if !election.IsOpen {
			return fmt.Errorf("update election: election %d already closed", election.RowId)
		}
		election.IsEmpty = false
		election.NumProposals += len(insProposals)
		if err := idx.electionTable.Update(ctx, election); err != nil {
			return err
		}
	}

	// create and store ballots for each proposal
	insBallots := make([]pack.Item, 0)
	for _, op := range block.Ops {
		if op.Type != chain.OpTypeProposals {
			continue
		}
		cop, ok := block.GetRPCOp(op.OpN, op.OpC)
		if !ok {
			return fmt.Errorf("missing proposal op [%d:%d]", op.OpN, op.OpC)
		}
		pop, ok := cop.(*rpc.ProposalsOp)
		if !ok {
			return fmt.Errorf("proposals op [%d:%d]: unexpected type %T ", op.OpN, op.OpC, cop)
		}
		acc, aok := builder.AccountByAddress(pop.Source)
		if !aok {
			return fmt.Errorf("missing account %s in proposal op [%d:%d]", pop.Source, op.OpN, op.OpC)
		}
		// load account rolls at snapshot block (i.e. at current voting period start)
		rolls, err := idx.rollsByHeight(ctx, acc.RowId, vote.StartHeight, builder)
		if err != nil {
			return fmt.Errorf("missing roll snapshot for %s in vote period %d (%s) start %d",
				acc, vote.VotingPeriod, vote.VotingPeriodKind, vote.StartHeight)
		}
		// update vote
		vote.TurnoutRolls += rolls
		vote.TurnoutVoters++

		// create ballots for all proposals
		for _, v := range pop.Proposals {
			prop, ok := proposalMap[v.String()]
			if !ok {
				return fmt.Errorf("missing proposal %s in op [%d:%d]", v, op.OpN, op.OpC)
			}
			b := &Ballot{
				ElectionId:       vote.ElectionId,
				ProposalId:       prop.RowId,
				VotingPeriod:     vote.VotingPeriod,
				VotingPeriodKind: vote.VotingPeriodKind,
				Height:           block.Height,
				Time:             block.Timestamp,
				SourceId:         acc.RowId,
				OpId:             op.RowId,
				Rolls:            rolls,
				Ballot:           chain.BallotVoteYay,
			}
			insBallots = append(insBallots, b)

			// update proposal too
			prop.Voters++
			prop.Rolls += rolls
		}
	}

	// finalize vote for this round and safe
	vote.TurnoutPct = vote.TurnoutRolls * 10000 / vote.EligibleRolls
	if idx.voteTable.Update(ctx, vote); err != nil {
		return err
	}

	// finalize proposals, reuse slice
	insProposals = insProposals[:0]
	for _, v := range proposalMap {
		insProposals = append(insProposals, v)
	}
	if idx.proposalTable.Update(ctx, insProposals); err != nil {
		return err
	}
	return idx.ballotTable.Insert(ctx, insBallots)
}

func (idx *GovIndex) processBallots(ctx context.Context, block *Block, builder BlockBuilder) error {
	// skip blocks without ballots
	if block.NBallot == 0 {
		return nil
	}

	// load current vote
	vote, err := idx.voteByHeight(ctx, block.Height, block.Params)
	if err != nil {
		return err
	}

	insBallots := make([]pack.Item, 0)
	for _, op := range block.Ops {
		if op.Type != chain.OpTypeBallot {
			continue
		}
		cop, ok := block.GetRPCOp(op.OpN, op.OpC)
		if !ok {
			return fmt.Errorf("missing ballot op [%d:%d]", op.OpN, op.OpC)
		}
		bop, ok := cop.(*rpc.BallotOp)
		if !ok {
			return fmt.Errorf("ballot op [%d:%d]: unexpected type %T ", op.OpN, op.OpC, cop)
		}
		acc, aok := builder.AccountByAddress(bop.Source)
		if !aok {
			return fmt.Errorf("missing account %s in proposal op [%d:%d]", bop.Source, op.OpN, op.OpC)
		}
		// load account rolls at snapshot block (i.e. at current voting period start)
		rolls, err := idx.rollsByHeight(ctx, acc.RowId, vote.StartHeight, builder)
		if err != nil {
			return fmt.Errorf("missing roll snapshot for %s in vote period %d (%s) start %d",
				acc, vote.VotingPeriod, vote.VotingPeriodKind, vote.StartHeight)
		}

		// update vote
		vote.TurnoutRolls += rolls
		vote.TurnoutVoters++
		switch bop.Ballot {
		case chain.BallotVoteYay:
			vote.YayRolls += rolls
			vote.YayVoters++
		case chain.BallotVoteNay:
			vote.NayRolls += rolls
			vote.NayVoters++
		case chain.BallotVotePass:
			vote.PassRolls += rolls
			vote.PassVoters++
		}

		b := &Ballot{
			ElectionId:       vote.ElectionId,
			ProposalId:       vote.ProposalId,
			VotingPeriod:     vote.VotingPeriod,
			VotingPeriodKind: vote.VotingPeriodKind,
			Height:           block.Height,
			Time:             block.Timestamp,
			SourceId:         acc.RowId,
			OpId:             op.RowId,
			Rolls:            rolls,
			Ballot:           bop.Ballot,
		}
		insBallots = append(insBallots, b)
	}
	// finalize vote for this round and safe
	vote.TurnoutPct = vote.TurnoutRolls * 10000 / vote.EligibleRolls
	if idx.voteTable.Update(ctx, vote); err != nil {
		return err
	}
	return idx.ballotTable.Insert(ctx, insBallots)
}

func (idx *GovIndex) electionByHeight(ctx context.Context, height int64, period chain.VotingPeriodKind, params *chain.Params) (*Election, error) {
	var off int64
	switch period {
	case chain.VotingPeriodTestingVote:
		off = 1
	case chain.VotingPeriodTesting:
		off = 2
	case chain.VotingPeriodPromotionVote:
		off = 3
	}
	var startHeight int64
	if height > 0 {
		startHeight = (height/params.BlocksPerVotingPeriod - off) * params.BlocksPerVotingPeriod
	}
	q := pack.Query{
		Name:    "find_election",
		NoCache: true,
		Fields:  idx.electionTable.Fields(),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: idx.electionTable.Fields().Find("H"), // start height
				Mode:  pack.FilterModeEqual,
				Value: startHeight,
			},
		},
	}
	election := &Election{}
	err := idx.electionTable.Stream(ctx, q, func(r pack.Row) error {
		return r.Decode(election)
	})
	if err != nil {
		return nil, err
	}
	if election.RowId == 0 {
		return nil, ErrNoElectionEntry
	}
	return election, nil
}

func (idx *GovIndex) voteByHeight(ctx context.Context, height int64, params *chain.Params) (*Vote, error) {
	q := pack.Query{
		Name:    "find_vote",
		NoCache: true,
		Fields:  idx.voteTable.Fields(),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: idx.voteTable.Fields().Find("H"), // start height
				Mode:  pack.FilterModeEqual,
				Value: height / params.BlocksPerVotingPeriod * params.BlocksPerVotingPeriod,
			},
		},
	}
	vote := &Vote{}
	err := idx.voteTable.Stream(ctx, q, func(r pack.Row) error {
		return r.Decode(vote)
	})
	if err != nil {
		return nil, err
	}
	if vote.RowId == 0 {
		return nil, ErrNoVoteEntry
	}
	return vote, nil
}

func (idx *GovIndex) proposalsByElection(ctx context.Context, eid ElectionID) ([]*Proposal, error) {
	q := pack.Query{
		Name:    "list_proposals",
		NoCache: true,
		Fields:  idx.proposalTable.Fields(),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: idx.proposalTable.Fields().Find("E"), // election id
				Mode:  pack.FilterModeEqual,
				Value: eid.Value(),
			},
		},
	}
	proposals := make([]*Proposal, 0, 20)
	err := idx.proposalTable.Stream(ctx, q, func(r pack.Row) error {
		p := &Proposal{}
		if err := r.Decode(p); err != nil {
			return err
		}
		proposals = append(proposals, p)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return proposals, nil
}

func (idx *GovIndex) ballotsByVote(ctx context.Context, period int64) ([]*Ballot, error) {
	q := pack.Query{
		Name:    "list_ballots",
		NoCache: true,
		Fields:  idx.ballotTable.Fields(),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: idx.ballotTable.Fields().Find("p"), // voting period
				Mode:  pack.FilterModeEqual,
				Value: period,
			},
		},
	}
	ballots := make([]*Ballot, 0)
	err := idx.ballotTable.Stream(ctx, q, func(r pack.Row) error {
		b := &Ballot{}
		if err := r.Decode(b); err != nil {
			return err
		}
		ballots = append(ballots, b)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ballots, nil
}

func (idx *GovIndex) rollsByHeight(ctx context.Context, aid AccountID, height int64, builder BlockBuilder) (int64, error) {
	table, err := builder.Table(SnapshotTableKey)
	if err != nil {
		return 0, err
	}
	q := pack.Query{
		Name:    "find_rolls",
		NoCache: true,
		Fields:  table.Fields().Select("r"),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: table.Fields().Find("h"), // height
				Mode:  pack.FilterModeEqual,
				Value: height,
			},
			pack.Condition{
				Field: table.Fields().Find("a"), // account id
				Mode:  pack.FilterModeEqual,
				Value: aid.Value(),
			},
		},
	}
	type XSnapshot struct {
		Rolls int64 `pack:"r,snappy"`
	}
	var count int
	xr := &XSnapshot{}
	err = table.Stream(ctx, q, func(r pack.Row) error {
		count++
		return r.Decode(xr)
	})
	if err != nil {
		return 0, err
	}
	if count != 1 {
		log.Errorf("Roll snapshot for %d at height %d returned %d results", aid, height, count)
	}
	return xr.Rolls, nil
}

// quorums adjust at the end of each exploration & promotion voting period
// starting in v005 the algorithm changes to track participation as EMA (80/20)
func (idx *GovIndex) quorumByHeight(ctx context.Context, height int64, params *chain.Params) (int64, int64, error) {
	// find most recent exploration or promotion period
	q := pack.Query{
		Name:    "find_quorum_vote",
		NoCache: true,
		Order:   pack.OrderDesc,
		Fields:  idx.voteTable.Fields(),
		Conditions: pack.ConditionList{
			pack.Condition{
				Field: idx.voteTable.Fields().Find("H"), // start height
				Mode:  pack.FilterModeLt,
				Value: height,
			},
		},
	}
	var lastQuorum, lastTurnout, lastTurnoutEma, nextQuorum, nextEma int64
	vote := &Vote{}
	err := idx.voteTable.Stream(ctx, q, func(r pack.Row) error {
		if err := r.Decode(vote); err != nil {
			return err
		}
		switch vote.VotingPeriodKind {
		case chain.VotingPeriodTestingVote, chain.VotingPeriodPromotionVote:
			lastQuorum = vote.QuorumPct
			lastTurnout = vote.TurnoutPct
			lastTurnoutEma = vote.TurnoutEma
			return io.EOF
		}
		return nil
	})
	if err != io.EOF {
		if err != nil {
			return 0, 0, err
		} else {
			// initial protocol quorum
			return 8000, params.QuorumMax, nil
		}
	}
	// calculate next quorum
	switch true {
	case params.Version >= 5:
		// Babylon v005 changed this to participation EMA and min/max caps
		if lastTurnoutEma == 0 {
			// init from upper bound
			nextEma = params.QuorumMax
			lastTurnoutEma = params.QuorumMax
		} else {
			// update using actual turnout
			nextEma = (8*lastTurnoutEma + 2*lastTurnout) / 10
		}
		// q = q_min + participation_ema * (q_max - q_min)
		nextQuorum = params.QuorumMin + lastTurnoutEma*(params.QuorumMax-params.QuorumMin)

	default:
		// 80/20 until Athens v004
		nextQuorum = (8*lastQuorum + 2*lastTurnout) / 10
	}

	return nextQuorum, nextEma, nil
}
