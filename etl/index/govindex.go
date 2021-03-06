// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// Note on voting_period_kind field in block headers:
//
// This may be counter intuitive but the value returned by "voting_period_kind"
// is "what kind of voting operation are accepted at that level". At the last block
// of a period, if you inject a operation, it will be part of the first block of the
// next period and therefore must be an operation of the "to come" period and that's
// what "voting_period_kind" returns.

package index

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/tzgo/rpc"
	"blockwatch.cc/tzgo/tezos"

	. "blockwatch.cc/tzindex/etl/model"
)

const (
	firstVoteBlock int64 = 2 // chain is initialized at block 1 !
)

var (
	GovPackSizeLog2    = 15 // 32k
	GovJournalSizeLog2 = 16 // 64k
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
	// skip genesis and bootstrap blocks
	if block.Height < firstVoteBlock {
		return nil
	}

	// detect first and last block of a voting period
	isPeriodStart := block.Height == firstVoteBlock || block.Params.IsVoteStart(block.Height)
	isPeriodEnd := block.Height > firstVoteBlock && block.Params.IsVoteEnd(block.Height)

	// open a new election or vote on first block
	if isPeriodStart {
		if block.VotingPeriodKind == tezos.VotingPeriodProposal {
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
	case tezos.VotingPeriodProposal:
		err = idx.processProposals(ctx, block, builder)
	case tezos.VotingPeriodExploration:
		err = idx.processBallots(ctx, block, builder)
	case tezos.VotingPeriodCooldown:
		// nothing to do here
	case tezos.VotingPeriodPromotion:
		err = idx.processBallots(ctx, block, builder)
	case tezos.VotingPeriodAdoption:
		// nothing to do here
	}
	if err != nil {
		return err
	}

	// close any previous period after last block
	if isPeriodEnd {
		// close the last vote
		success, err := idx.closeVote(ctx, block, builder)
		if err != nil {
			log.Errorf("Close %s vote at block %d: %v", block.VotingPeriodKind, block.Height, err)
			return err
		}

		// on failure or on end, close last election
		if !success || block.VotingPeriodKind == tezos.VotingPeriodProposal {
			if err := idx.closeElection(ctx, block, builder); err != nil {
				log.Errorf("Close election at block %d: %v", block.Height, err)
				return err
			}
		}
	}

	// // warn at end of each cycle if indexer does not support the selected proposal yet
	// if block.Params.IsCycleEnd(block.Height) && block.VotingPeriodKind {
	// 	// fetch the winning proposal
	// 	// TODO
	// }

	return nil
}

func (idx *GovIndex) DisconnectBlock(ctx context.Context, block *Block, builder BlockBuilder) error {
	// clear state first (also removes any new vote/election periods)
	if err := idx.DeleteBlock(ctx, block.Height); err != nil {
		return err
	}

	// re-open vote/election when at end of cycle
	isPeriodEnd := block.Height > firstVoteBlock && block.Params.IsVoteEnd(block.Height)
	if isPeriodEnd {
		success, err := idx.reopenVote(ctx, block, builder)
		if err != nil {
			log.Errorf("Rollback: reopen %s vote at block %d: %v", block.VotingPeriodKind, block.Height, err)
			return err
		}
		if !success || block.VotingPeriodKind == tezos.VotingPeriodProposal {
			if err := idx.reopenElection(ctx, block, builder); err != nil {
				log.Errorf("Rollback: reopen election at block %d: %v", block.Height, err)
				return err
			}
		}
	}

	return nil
}

func (idx *GovIndex) DeleteBlock(ctx context.Context, height int64) error {
	// delete ballots by height
	_, err := pack.NewQuery("etl.ballots.delete", idx.ballotTable).
		AndEqual("height", height).
		Delete(ctx)
	if err != nil {
		return nil
	}

	// delete proposals by height
	_, err = pack.NewQuery("etl.proposals.delete", idx.proposalTable).
		AndEqual("height", height).
		Delete(ctx)
	if err != nil {
		return nil
	}

	// on vote period start, delete vote by start height
	_, err = pack.NewQuery("etl.vote.delete", idx.voteTable).
		AndEqual("period_start_height", height).
		Delete(ctx)
	if err != nil {
		return nil
	}

	// on election start, delete election (maybe not because we use row id as counter)
	_, err = pack.NewQuery("etl.election.delete", idx.electionTable).
		AndEqual("start_height", height).
		Delete(ctx)
	if err != nil {
		return nil
	}
	return nil
}

func (idx *GovIndex) openElection(ctx context.Context, block *Block, builder BlockBuilder) error {
	election := &Election{
		NumPeriods:   1,
		VotingPeriod: block.TZ.Block.GetVotingPeriod(),
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
	election, err := idx.electionByHeight(ctx, block.Height, block.Params)
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
	election.IsEmpty = election.NumProposals == 0 && vote.IsFailed
	election.IsFailed = vote.IsFailed
	election.NoQuorum = vote.NoQuorum
	election.NoMajority = vote.NoMajority
	return idx.electionTable.Update(ctx, election)
}

func (idx *GovIndex) reopenElection(ctx context.Context, block *Block, builder BlockBuilder) error {
	// load current election
	election, err := idx.electionByHeight(ctx, block.Height, block.Params)
	if err != nil {
		return err
	}
	// check its closed
	if election.IsOpen {
		return fmt.Errorf("reopen election: election %d already open", election.RowId)
	}
	// just update state (will roll forward at end of reorg)
	election.IsOpen = false
	return idx.electionTable.Update(ctx, election)
}

func (idx *GovIndex) openVote(ctx context.Context, block *Block, builder BlockBuilder) error {
	// load current election, must exist
	election, err := idx.electionByHeight(ctx, block.Height, block.Params)
	if err != nil {
		return err
	}
	if !election.IsOpen {
		return fmt.Errorf("opening vote: election %d already closed", election.RowId)
	}

	// update election
	election.NumPeriods = block.VotingPeriodKind.Num()

	// Note: this adjusts end height of first cycle (we run this func at height 2 instead of 0)
	//       otherwise the formula could be simpler
	p := block.Params
	endHeight := (block.Height - (block.Height-p.StartBlockOffset)%p.BlocksPerVotingPeriod) + p.BlocksPerVotingPeriod

	vote := &Vote{
		ElectionId:       election.RowId,
		ProposalId:       election.ProposalId, // voted proposal, zero in first voting period
		VotingPeriod:     block.TZ.Block.GetVotingPeriod(),
		VotingPeriodKind: block.VotingPeriodKind,
		StartTime:        block.Timestamp,
		EndTime:          time.Time{}.UTC(), // set on close
		StartHeight:      block.Height,
		EndHeight:        endHeight,
		IsOpen:           true,
	}

	// add rolls and calculate quorum
	// - use current (cycle resp. vote start block) for roll snapshot
	// - at genesis there is no parent block, we use defaults here
	cd := block.Chain
	vote.EligibleRolls = cd.Rolls
	vote.EligibleVoters = cd.RollOwners
	switch vote.VotingPeriodKind {
	case tezos.VotingPeriodProposal:
		// fixed min proposal quorum as defined by protocol
		vote.QuorumPct = p.MinProposalQuorum
	case tezos.VotingPeriodCooldown, tezos.VotingPeriodAdoption:
		// no quorum
		vote.QuorumPct = 0
	case tezos.VotingPeriodExploration, tezos.VotingPeriodPromotion:
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
	case tezos.VotingPeriodProposal:
		// select the winning proposal if any and update election
		var isDraw bool
		if vote.TurnoutRolls > 0 {
			proposals, err := idx.proposalsByElection(ctx, vote.ElectionId)
			if err != nil {
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
				election, err := idx.electionByHeight(ctx, block.Height, block.Params)
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

	case tezos.VotingPeriodExploration, tezos.VotingPeriodPromotion:
		vote.NoQuorum = vote.TurnoutRolls < vote.QuorumRolls
		vote.NoMajority = vote.YayRolls < (vote.YayRolls+vote.NayRolls)*8/10
		vote.IsFailed = vote.NoQuorum || vote.NoMajority

	case tezos.VotingPeriodCooldown, tezos.VotingPeriodAdoption:
		// empty, cannot fail
	}

	vote.EndTime = block.Timestamp
	vote.IsOpen = false

	if err := idx.voteTable.Update(ctx, vote); err != nil {
		return false, err
	}

	return !vote.IsFailed, nil
}

func (idx *GovIndex) reopenVote(ctx context.Context, block *Block, builder BlockBuilder) (bool, error) {
	// load current vote
	vote, err := idx.voteByHeight(ctx, block.Height, block.Params)
	if err != nil {
		return false, err
	}
	// check its closed
	if vote.IsOpen {
		return false, fmt.Errorf("reopen vote: vote %d/%d is already open", vote.ElectionId, vote.VotingPeriod)
	}

	// just reset state flag
	vote.IsOpen = true
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
		if op.Type != tezos.OpTypeProposals {
			continue
		}
		cop, ok := block.GetRpcOp(op.OpL, op.OpP, op.OpC)
		if !ok {
			return fmt.Errorf("missing proposal op [%d:%d]", op.OpL, op.OpP)
		}
		pop, ok := cop.(*rpc.ProposalsOp)
		if !ok {
			return fmt.Errorf("ballot op [%d:%d]: unexpected type %T ", op.OpL, op.OpP, cop)
		}
		acc, aok := builder.AccountByAddress(pop.Source)
		if !aok {
			return fmt.Errorf("missing account %s in proposal op [%d:%d]", pop.Source, op.OpL, op.OpP)
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
		election, err := idx.electionByHeight(ctx, block.Height, block.Params)
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
		if op.Type != tezos.OpTypeProposals {
			continue
		}
		cop, ok := block.GetRpcOp(op.OpL, op.OpP, op.OpC)
		if !ok {
			return fmt.Errorf("missing proposal op [%d:%d]", op.OpL, op.OpP)
		}
		pop, ok := cop.(*rpc.ProposalsOp)
		if !ok {
			return fmt.Errorf("proposals op [%d:%d]: unexpected type %T ", op.OpP, op.OpP, cop)
		}
		acc, aok := builder.AccountByAddress(pop.Source)
		if !aok {
			return fmt.Errorf("missing account %s in proposal op [%d:%d]", pop.Source, op.OpL, op.OpP)
		}
		// load account rolls at snapshot block (i.e. at current voting period start - 1)
		// using params that were active at that block (after protocol upgrades that change
		// rolls size like Athens is is essential)
		rollParams := builder.Params(vote.StartHeight - 1)
		rolls, err := idx.rollsByHeight(ctx, acc.RowId, vote.StartHeight-1, builder, rollParams)
		if err != nil {
			return fmt.Errorf("missing roll snapshot for %s in vote period %d (%s) start %d",
				acc, vote.VotingPeriod, vote.VotingPeriodKind, vote.StartHeight)
		}
		// fix for missing pre-genesis snapshot
		if block.Cycle == 0 && rolls == 0 {
			rolls = acc.Rolls(block.Params)
		}

		// create ballots for all proposals
		for _, v := range pop.Proposals {
			prop, ok := proposalMap[v.String()]
			if !ok {
				return fmt.Errorf("missing proposal %s in op [%d:%d]", v, op.OpL, op.OpP)
			}

			// skip when the same account voted for the same proposal already
			cnt, err := pack.NewQuery("etl.count_account_ballots", idx.ballotTable).
				AndEqual("source_id", acc.RowId).
				AndEqual("voting_period", vote.VotingPeriod).
				AndEqual("proposal_id", prop.RowId).
				Count(ctx)
			if err != nil {
				return err
			} else if cnt > 0 {
				// log.Infof("Skipping voter %s for proposal %s with %d rolls (already voted %d times)", acc, v, rolls, cnt)
				continue
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
				Ballot:           tezos.BallotVoteYay,
			}
			insBallots = append(insBallots, b)

			// update proposal too
			// log.Infof("New voter %s for proposal %s with %d rolls (add to %d voters, %d rolls)",
			// 	acc, v, rolls, prop.Voters, prop.Rolls)
			prop.Voters++
			prop.Rolls += rolls
		}

		// update vote, skip when the same account voted already
		cnt, err := pack.NewQuery("etl.count_account_ballots", idx.ballotTable).
			AndEqual("source_id", acc.RowId).
			AndEqual("voting_period", vote.VotingPeriod).
			Count(ctx)
		if err != nil {
			return err
		} else if cnt == 0 {
			// log.Infof("Update turnout for period %d voter %s with %d rolls (add to %d voters, %d rolls)",
			// 	vote.VotingPeriod, acc, rolls, vote.TurnoutVoters, vote.TurnoutRolls)
			vote.TurnoutRolls += rolls
			vote.TurnoutVoters++
		} else {
			// log.Infof("Skipping turnout calc for period %d voter %s  with %d rolls (already voted %d times)", vote.VotingPeriod, acc, rolls, cnt)
		}
	}

	// update eligible rolls when zero (happens when vote opens on genesis)
	if vote.EligibleRolls == 0 {
		vote.EligibleRolls = block.Chain.Rolls
		vote.EligibleVoters = block.Chain.RollOwners
		vote.QuorumPct, _, _ = idx.quorumByHeight(ctx, block.Height, block.Params)
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
		if op.Type != tezos.OpTypeBallot {
			continue
		}
		cop, ok := block.GetRpcOp(op.OpL, op.OpP, op.OpC)
		if !ok {
			return fmt.Errorf("missing ballot op [%d:%d]", op.OpL, op.OpP)
		}
		bop, ok := cop.(*rpc.BallotOp)
		if !ok {
			return fmt.Errorf("ballot op [%d:%d]: unexpected type %T ", op.OpL, op.OpP, cop)
		}
		acc, aok := builder.AccountByAddress(bop.Source)
		if !aok {
			return fmt.Errorf("missing account %s in proposal op [%d:%d]", bop.Source, op.OpL, op.OpP)
		}
		// load account rolls at snapshot block (i.e. at current voting period start - 1)
		rollParams := builder.Params(vote.StartHeight - 1)
		rolls, err := idx.rollsByHeight(ctx, acc.RowId, vote.StartHeight-1, builder, rollParams)
		if err != nil {
			return fmt.Errorf("missing roll snapshot for %s in vote period %d (%s) start %d",
				acc, vote.VotingPeriod, vote.VotingPeriodKind, vote.StartHeight)
		}
		// fix for missing pre-genesis snapshot
		if block.Cycle == 0 && rolls == 0 {
			rolls = acc.Rolls(block.Params)
		}

		// update vote
		vote.TurnoutRolls += rolls
		vote.TurnoutVoters++
		switch bop.Ballot {
		case tezos.BallotVoteYay:
			vote.YayRolls += rolls
			vote.YayVoters++
		case tezos.BallotVoteNay:
			vote.NayRolls += rolls
			vote.NayVoters++
		case tezos.BallotVotePass:
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

	// update eligible rolls when zero (happens when vote opens on genesis)
	if vote.EligibleRolls == 0 {
		vote.EligibleRolls = block.Chain.Rolls
		vote.EligibleVoters = block.Chain.RollOwners
		vote.QuorumPct, _, _ = idx.quorumByHeight(ctx, block.Height, block.Params)
	}

	// finalize vote for this round and safe
	vote.TurnoutPct = vote.TurnoutRolls * 10000 / vote.EligibleRolls
	if idx.voteTable.Update(ctx, vote); err != nil {
		return err
	}
	return idx.ballotTable.Insert(ctx, insBallots)
}

func (idx *GovIndex) electionByHeight(ctx context.Context, height int64, params *tezos.Params) (*Election, error) {
	election := &Election{}
	err := pack.NewQuery("find_election", idx.electionTable).
		WithoutCache().
		WithLimit(1).
		WithDesc().
		AndLte("start_height", height). // start height; first cycle starts at 2!
		Execute(ctx, election)
	if err != nil {
		return nil, err
	}
	if election.RowId == 0 {
		return nil, ErrNoElectionEntry
	}
	return election, nil
}

func (idx *GovIndex) voteByHeight(ctx context.Context, height int64, params *tezos.Params) (*Vote, error) {
	vote := &Vote{}
	err := pack.NewQuery("find_vote", idx.voteTable).
		WithoutCache().
		WithLimit(1).
		WithDesc().
		AndLte("period_start_height", height). // start height; first cycle starts at 2!
		Execute(ctx, vote)
	if err != nil {
		return nil, err
	}
	if vote.RowId == 0 {
		return nil, ErrNoVoteEntry
	}
	return vote, nil
}

func (idx *GovIndex) proposalsByElection(ctx context.Context, id ElectionID) ([]*Proposal, error) {
	proposals := make([]*Proposal, 0)
	err := pack.NewQuery("list_proposals", idx.proposalTable).
		WithoutCache().
		AndEqual("election_id", id).
		Execute(ctx, &proposals)
	if err != nil {
		return nil, err
	}
	return proposals, nil
}

func (idx *GovIndex) ballotsByVote(ctx context.Context, period int64) ([]*Ballot, error) {
	ballots := make([]*Ballot, 0)
	err := pack.NewQuery("list_ballots", idx.ballotTable).
		WithoutCache().
		AndEqual("voting_period", period).
		Execute(ctx, &ballots)
	if err != nil {
		return nil, err
	}
	return ballots, nil
}

func (idx *GovIndex) rollsByHeight(ctx context.Context, aid AccountID, height int64, builder BlockBuilder, params *tezos.Params) (int64, error) {
	table, err := builder.Table(SnapshotTableKey)
	if err != nil {
		return 0, err
	}
	type XSnapshot struct {
		Balance   int64 `pack:"B"`
		Delegated int64 `pack:"D"`
	}
	var count int
	xs := &XSnapshot{}
	err = pack.NewQuery("gov_find_rolls", table).
		WithoutCache().
		WithFields("B", "D").
		AndEqual("height", height).
		AndEqual("account_id", aid).
		Stream(ctx, func(r pack.Row) error {
			count++
			return r.Decode(xs)
		})
	if err != nil {
		return 0, err
	}
	if count != 1 {
		log.Warnf("govindex: roll snapshot for account %d at height %d returned %d results", aid, height, count)
	}

	// adjust rolls up since this snapshot is always index 15 which means rewards are
	// not yet unfrozen, but for voting snapshot rewards are unfrozen
	table, err = builder.Table(FlowTableKey)
	if err != nil {
		return 0, err
	}
	type XFlow struct {
		AmountOut int64 `pack:"o"`
	}
	xf := &XFlow{}
	err = pack.NewQuery("gov_find_unfreeze", table).
		WithoutCache().
		WithFields("o").
		AndEqual("height", height).
		AndEqual("account_id", aid).
		AndEqual("category", FlowCategoryRewards).
		AndEqual("operation", FlowTypeInternal).
		Stream(ctx, func(r pack.Row) error {
			count++
			return r.Decode(xf)
		})
	if count != 2 {
		log.Debugf("govindex: no unfreeze flow for account %d at height %d (expected for new bakers)", aid, height)
	}

	// calculate rolls
	stakingBalance := xs.Balance + xs.Delegated + xf.AmountOut
	rolls := stakingBalance / params.TokensPerRoll

	if rolls == 0 {
		log.Errorf("govindex: zero rolls for account %d at height %d with balance own=%f delegated=%f corrected=%f",
			aid, height, params.ConvertValue(xs.Balance), params.ConvertValue(xs.Delegated), params.ConvertValue(xf.AmountOut))
	}
	return rolls, nil
}

// quorums adjust at the end of each exploration & promotion voting period
// starting in v005 the algorithm changes to track participation as EMA (80/20)
func (idx *GovIndex) quorumByHeight(ctx context.Context, height int64, params *tezos.Params) (int64, int64, error) {
	// find most recent exploration or promotion period
	var lastQuorum, lastTurnout, lastTurnoutEma, nextQuorum, nextEma int64
	vote := &Vote{}
	err := pack.NewQuery("find_quorum_vote", idx.voteTable).
		WithoutCache().
		WithDesc().
		AndLt("period_start_height", height).
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(vote); err != nil {
				return err
			}
			switch vote.VotingPeriodKind {
			case tezos.VotingPeriodExploration, tezos.VotingPeriodPromotion:
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
			if params.Version < 5 {
				return 8000, 0, nil
			} else {
				lastTurnoutEma = params.QuorumMax
				nextQuorum = params.QuorumMin + lastTurnoutEma*(params.QuorumMax-params.QuorumMin)/10000
				return nextQuorum, lastTurnoutEma, nil
			}
		}
	}
	// calculate next quorum
	switch true {
	case params.Version >= 5:
		// Babylon v005 changed this to participation EMA and min/max caps
		if lastTurnoutEma == 0 {
			if lastTurnout == 0 {
				// init from upper bound on chains that never had Athens votes
				// nextEma = params.QuorumMax
				lastTurnoutEma = params.QuorumMax
			} else {
				// init from last Athens quorum
				lastTurnoutEma = (8*lastQuorum + 2*lastTurnout) / 10
			}
			nextEma = lastTurnoutEma
		} else {
			// update using actual turnout
			nextEma = (8*lastTurnoutEma + 2*lastTurnout) / 10
		}

		// q = q_min + participation_ema * (q_max - q_min)
		nextQuorum = params.QuorumMin + nextEma*(params.QuorumMax-params.QuorumMin)/10000

	default:
		// 80/20 until Athens v004
		nextQuorum = (8*lastQuorum + 2*lastTurnout) / 10
	}

	return nextQuorum, nextEma, nil
}
