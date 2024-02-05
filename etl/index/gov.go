// Copyright (c) 2024 Blockwatch Data Inc.
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
	"fmt"
	"io"
	"time"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/etl/task"
	"blockwatch.cc/tzindex/rpc"
)

const GovIndexKey = "gov"

type GovIndex struct {
	db     *pack.DB
	tables map[string]*pack.Table
}

var _ model.BlockIndexer = (*GovIndex)(nil)

func NewGovIndex() *GovIndex {
	return &GovIndex{
		tables: make(map[string]*pack.Table),
	}
}

func (idx *GovIndex) DB() *pack.DB {
	return idx.db
}

func (idx *GovIndex) Tables() []*pack.Table {
	t := []*pack.Table{}
	for _, v := range idx.tables {
		t = append(t, v)
	}
	return t
}

func (idx *GovIndex) Key() string {
	return GovIndexKey
}

func (idx *GovIndex) Name() string {
	return GovIndexKey + " index"
}

func (idx *GovIndex) Create(path, label string, opts interface{}) error {
	db, err := pack.CreateDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return fmt.Errorf("creating %s database: %w", idx.Key(), err)
	}
	defer db.Close()

	for _, m := range []model.Model{
		model.Election{},
		model.Proposal{},
		model.Vote{},
		model.Ballot{},
		model.Stake{},
	} {
		key := m.TableKey()
		fields, err := pack.Fields(m)
		if err != nil {
			return fmt.Errorf("reading fields for table %q from type %T: %v", key, m, err)
		}
		opts := m.TableOpts().Merge(model.ReadConfigOpts(key))
		_, err = db.CreateTableIfNotExists(key, fields, opts)
		if err != nil {
			return err
		}
	}
	return nil
}

func (idx *GovIndex) Init(path, label string, opts interface{}) error {
	db, err := pack.OpenDatabase(path, idx.Key(), label, opts)
	if err != nil {
		return err
	}
	idx.db = db

	for _, m := range []model.Model{
		model.Election{},
		model.Proposal{},
		model.Vote{},
		model.Ballot{},
		model.Stake{},
	} {
		key := m.TableKey()
		topts := m.TableOpts().Merge(model.ReadConfigOpts(key))
		table, err := idx.db.Table(key, topts)
		if err != nil {
			idx.Close()
			return err
		}
		idx.tables[key] = table
	}

	return nil
}

func (idx *GovIndex) FinalizeSync(_ context.Context) error {
	return nil
}

func (idx *GovIndex) Close() error {
	for n, v := range idx.tables {
		if v != nil {
			if err := v.Close(); err != nil {
				log.Errorf("Closing %s table: %s", n, err)
			}
		}
		delete(idx.tables, n)
	}
	if idx.db != nil {
		if err := idx.db.Close(); err != nil {
			return err
		}
		idx.db = nil
	}
	return nil
}

func (idx *GovIndex) ConnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// detect first and last block of a voting period
	isPeriodStart := block.TZ.IsVoteStart()
	isPeriodEnd := block.TZ.IsVoteEnd()

	// open a new election or vote on first block
	if isPeriodStart {
		if block.VotingPeriodKind == tezos.VotingPeriodProposal {
			if err := idx.openElection(ctx, block, builder); err != nil {
				log.Errorf("gov: open election at block %d %s: %s", block.Height, block.VotingPeriodKind, err)
				// return err
				return nil
			}
		}
		if err := idx.openVote(ctx, block, builder); err != nil {
			log.Errorf("gov: open vote at block %d %s: %s", block.Height, block.VotingPeriodKind, err)
			// return err
			return nil
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
		log.Errorf("gov: processing block %d: %s", block.Height, err)
		// return err
		return nil
	}

	// close any previous period after last block
	if isPeriodEnd {
		// close the last vote
		success, err := idx.closeVote(ctx, block, builder)
		if err != nil {
			log.Errorf("gov: close %s vote at block %d: %s", block.VotingPeriodKind, block.Height, err)
			// return err
			return nil
		}

		// on failure or on end, close last election
		// Note: must call on RPC block since IsProtocolUpgrade() has a different
		// meaning on model.Block (there we compare parent proto vs current proto)
		// whereas in RPC we compare block.Metadata.NextProto
		if !success || block.TZ.Block.IsProtocolUpgrade() {
			if err := idx.closeElection(ctx, block, builder); err != nil {
				log.Errorf("gov: close election at block %d: %s", block.Height, err)
				// return err
				return nil
			}
		}
	}

	return nil
}

func (idx *GovIndex) DisconnectBlock(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// clear state first (also removes any new vote/election periods)
	if err := idx.DeleteBlock(ctx, block.Height); err != nil {
		log.Errorf("gov: rollback block %d: %s", block.Height, err)
		// return err
		return nil
	}

	// re-open vote/election when at end of cycle
	isPeriodEnd := block.TZ.IsVoteEnd()
	if isPeriodEnd {
		success, err := idx.reopenVote(ctx, block, builder)
		if err != nil {
			log.Errorf("gov: rollback reopen %s vote at block %d: %s", block.VotingPeriodKind, block.Height, err)
			// return err
			return nil
		}
		if !success || block.IsProtocolUpgrade() {
			if err := idx.reopenElection(ctx, block, builder); err != nil {
				log.Errorf("gov: rollback reopen election at block %d: %s", block.Height, err)
				// return err
				return nil
			}
		}
	}

	return nil
}

func (idx *GovIndex) DeleteBlock(ctx context.Context, height int64) error {
	// delete ballots by height
	_, err := pack.NewQuery("etl.delete").
		WithTable(idx.tables[model.BallotTableKey]).
		AndEqual("height", height).
		Delete(ctx)
	if err != nil {
		return err
	}

	// delete proposals by height
	_, err = pack.NewQuery("etl.delete").
		WithTable(idx.tables[model.ProposalTableKey]).
		AndEqual("height", height).
		Delete(ctx)
	if err != nil {
		return err
	}

	// on vote period start, delete vote by start height
	_, err = pack.NewQuery("etl.delete").
		WithTable(idx.tables[model.VoteTableKey]).
		AndEqual("period_start_height", height).
		Delete(ctx)
	if err != nil {
		return err
	}

	// on election start, delete election (Note: will shift row id counter!!)
	_, err = pack.NewQuery("etl.delete").
		WithTable(idx.tables[model.ElectionTableKey]).
		AndEqual("start_height", height).
		Delete(ctx)
	if err != nil {
		return err
	}

	// on vote period end delete snapshot
	_, err = pack.NewQuery("etl.delete").
		WithTable(idx.tables[model.StakeTableKey]).
		AndEqual("height", height).
		Delete(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (idx *GovIndex) DeleteCycle(ctx context.Context, cycle int64) error {
	return nil
}

func (idx *GovIndex) openElection(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	log.Debugf("gov: open election at height %d", block.Height)
	election := &model.Election{
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
	return idx.tables[model.ElectionTableKey].Insert(ctx, election)
}

func (idx *GovIndex) closeElection(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
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
	log.Debugf("gov: close election %d at height %d", election.RowId, block.Height)
	election.EndHeight = vote.EndHeight
	election.EndTime = vote.EndTime
	election.IsOpen = false
	election.IsEmpty = election.NumProposals == 0 && vote.IsFailed
	election.IsFailed = vote.IsFailed
	election.NoQuorum = vote.NoQuorum
	election.NoMajority = vote.NoMajority
	return idx.tables[model.ElectionTableKey].Update(ctx, election)
}

func (idx *GovIndex) reopenElection(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
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
	return idx.tables[model.ElectionTableKey].Update(ctx, election)
}

func (idx *GovIndex) openVote(ctx context.Context, block *model.Block, _ model.BlockBuilder) error {
	// load current election, must exist
	election, err := idx.electionByHeight(ctx, block.Height, block.Params)
	if err != nil {
		return err
	}
	if !election.IsOpen {
		return fmt.Errorf("opening vote: election %d already closed", election.RowId)
	}
	log.Debugf("gov: open %s vote in election %d at height %d with %d steps of %d blocks",
		block.VotingPeriodKind, election.RowId, block.Height,
		block.Params.NumVotingPeriods, block.Params.BlocksPerVotingPeriod,
	)
	log.Debugf("gov: vote counters %#v", block.TZ.Block.GetVotingInfo())

	// update election
	election.NumPeriods = block.VotingPeriodKind.Num()

	// Note: this adjusts end height of first cycle (we run this func at height 2 instead of 0)
	//       otherwise the formula could be simpler
	p := block.Params

	vote := &model.Vote{
		ElectionId:       election.RowId,
		ProposalId:       election.ProposalId, // voted proposal, zero in first voting period
		VotingPeriod:     block.TZ.Block.GetVotingPeriod(),
		VotingPeriodKind: block.VotingPeriodKind,
		StartTime:        block.Timestamp,
		EndTime:          time.Time{}.UTC(), // set on close
		StartHeight:      block.Height,
		EndHeight:        block.Height + block.Params.BlocksPerVotingPeriod - 1,
		IsOpen:           true,
	}

	stake, err := idx.sumStake(ctx, block.Height)
	if err != nil {
		return err
	}

	// add stake and calculate quorum
	// - use current (cycle resp. vote start block) for stake snapshot
	// - at genesis there is no parent block, we use defaults here
	vote.EligibleStake = stake
	vote.EligibleVoters = block.Chain.EligibleBakers
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
	vote.QuorumStake = vote.EligibleStake * vote.QuorumPct / 10000

	// insert vote
	if err := idx.tables[model.VoteTableKey].Insert(ctx, vote); err != nil {
		return err
	}

	// update election
	return idx.tables[model.ElectionTableKey].Update(ctx, election)
}

func (idx *GovIndex) closeVote(ctx context.Context, block *model.Block, builder model.BlockBuilder) (bool, error) {
	// load current vote
	vote, err := idx.voteByHeight(ctx, block.Height, block.Params)
	if err != nil {
		return false, err
	}
	// check its open
	if !vote.IsOpen {
		return false, fmt.Errorf("closing vote: vote %d/%d is already closed", vote.ElectionId, vote.VotingPeriod)
	}

	log.Debugf("gov: close %s vote in election %d at height %d", vote.VotingPeriodKind, vote.ElectionId, block.Height)
	log.Debugf("gov: vote counters %#v", block.TZ.Block.GetVotingInfo())

	// determine result
	params := block.Params
	switch vote.VotingPeriodKind {
	case tezos.VotingPeriodProposal:
		// select the winning proposal if any and update election
		var isDraw bool
		if vote.TurnoutStake > 0 {
			proposals, err := idx.proposalsByElection(ctx, vote.ElectionId)
			if err != nil {
				return false, err
			}

			// select the winner
			var (
				winner model.ProposalID
				stake  int64
			)
			for _, v := range proposals {
				if v.Stake < stake {
					continue
				}
				if v.Stake > stake {
					isDraw = false
					stake = v.Stake
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
				if err := idx.tables[model.ElectionTableKey].Update(ctx, election); err != nil {
					return false, err
				}
				vote.ProposalId = winner
			}
		}
		vote.NoProposal = vote.TurnoutStake == 0
		vote.NoQuorum = params.MinProposalQuorum > 0 && vote.TurnoutStake < vote.QuorumStake
		vote.IsDraw = isDraw
		vote.IsFailed = vote.NoProposal || vote.NoQuorum || vote.IsDraw

	case tezos.VotingPeriodExploration, tezos.VotingPeriodPromotion:
		vote.NoQuorum = vote.TurnoutStake < vote.QuorumStake
		vote.NoMajority = vote.YayStake < (vote.YayStake+vote.NayStake)*8/10
		vote.IsFailed = vote.NoQuorum || vote.NoMajority

	case tezos.VotingPeriodCooldown, tezos.VotingPeriodAdoption:
		// empty, cannot fail
	}

	vote.EndTime = block.Timestamp
	vote.IsOpen = false

	if err := idx.tables[model.VoteTableKey].Update(ctx, vote); err != nil {
		return false, err
	}

	// create stake snapshot after vote end as preparation for next vote
	if err := idx.makeStakeSnapshot(ctx, block, builder); err != nil {
		return false, err
	}
	return !vote.IsFailed, nil
}

func (idx *GovIndex) reopenVote(ctx context.Context, block *model.Block, _ model.BlockBuilder) (bool, error) {
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
	if err := idx.tables[model.VoteTableKey].Update(ctx, vote); err != nil {
		return false, err
	}

	return !vote.IsFailed, nil
}

func (idx *GovIndex) processProposals(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// skip blocks without proposals
	if !block.HasProposals {
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
	proposalMap := make(map[string]*model.Proposal)
	for _, v := range proposals {
		proposalMap[v.Hash.String()] = v
	}

	// find unknown proposals
	insProposals := make([]pack.Item, 0)
	for _, op := range block.Ops {
		if op.Type != model.OpTypeProposal {
			continue
		}
		pop, ok := op.Raw.(*rpc.Proposals)
		if !ok {
			return fmt.Errorf("ballot op [%d:%d]: unexpected type %T ", 1, op.OpP, op.Raw)
		}
		acc, aok := builder.AccountByAddress(pop.Source)
		if !aok {
			return fmt.Errorf("missing account %s in proposal op [%d:%d]", pop.Source, 1, op.OpP)
		}
		for _, v := range pop.Proposals {
			if _, ok := proposalMap[v.String()]; ok {
				continue
			}
			prop := &model.Proposal{
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
		if err := idx.tables[model.ProposalTableKey].Insert(ctx, insProposals); err != nil {
			return err
		}
		for _, v := range insProposals {
			p := v.(*model.Proposal)
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
		if err := idx.tables[model.ElectionTableKey].Update(ctx, election); err != nil {
			return err
		}
	}

	// create and store ballots for each proposal
	insBallots := make([]pack.Item, 0)
	for _, op := range block.Ops {
		if op.Type != model.OpTypeProposal {
			continue
		}
		pop, ok := op.Raw.(*rpc.Proposals)
		if !ok {
			return fmt.Errorf("proposals op [%d:%d]: unexpected type %T ", 1, op.OpP, op.Raw)
		}
		bkr, aok := builder.BakerByAddress(pop.Source)
		if !aok {
			return fmt.Errorf("missing account %s in proposal op [%d:%d]", pop.Source, 1, op.OpP)
		}
		// load account stake at snapshot block (i.e. the last vote close block)
		stake, err := idx.snapshotByHeight(ctx, bkr.AccountId, vote.StartHeight)
		if err != nil {
			return fmt.Errorf("missing stake snapshot for %s in vote period %d (%s) start %d",
				bkr, vote.VotingPeriod, vote.VotingPeriodKind, vote.StartHeight-1)
		}
		// fix for missing pre-genesis snapshot
		if block.Cycle == 0 && stake == 0 {
			stake = bkr.StakingBalance()
		}

		// create ballots for all proposals
		for _, v := range pop.Proposals {
			prop, ok := proposalMap[v.String()]
			if !ok {
				return fmt.Errorf("missing proposal %s in op [%d:%d]", v, 1, op.OpP)
			}

			// skip when the same account voted for the same proposal already
			cnt, err := pack.NewQuery("etl.count_account_ballots").
				WithTable(idx.tables[model.BallotTableKey]).
				AndEqual("source_id", bkr.AccountId).
				AndEqual("voting_period", vote.VotingPeriod).
				AndEqual("proposal_id", prop.RowId).
				Count(ctx)
			if err != nil {
				return err
			} else if cnt > 0 {
				// log.Debugf("Skipping voter %s for proposal %s with %d stake (already voted %d times)", acc, v, stake, cnt)
				continue
			}

			b := &model.Ballot{
				ElectionId:       vote.ElectionId,
				ProposalId:       prop.RowId,
				VotingPeriod:     vote.VotingPeriod,
				VotingPeriodKind: vote.VotingPeriodKind,
				Height:           block.Height,
				Time:             block.Timestamp,
				SourceId:         bkr.AccountId,
				OpId:             op.RowId,
				Stake:            stake,
				Ballot:           tezos.BallotVoteYay,
			}
			insBallots = append(insBallots, b)

			// update proposal too
			// log.Debugf("New voter %s for proposal %s with %d stake (add to %d voters, %d stake)",
			// 	acc, v, stake, prop.Voters, prop.Stake)
			prop.Voters++
			prop.Stake += stake
		}

		// update vote, skip when the same account voted already
		cnt, err := pack.NewQuery("etl.count_account_ballots").
			WithTable(idx.tables[model.BallotTableKey]).
			AndEqual("source_id", bkr.AccountId).
			AndEqual("voting_period", vote.VotingPeriod).
			Count(ctx)
		if err != nil {
			return err
		} else if cnt == 0 {
			// log.Debugf("Update turnout for period %d voter %s with %d stake (add to %d voters, %d stake)",
			// 	vote.VotingPeriod, acc, stake, vote.TurnoutVoters, vote.TurnoutStake)
			vote.TurnoutStake += stake
			vote.TurnoutVoters++
		}
	}

	// update eligible stake when zero (happens when vote opens on genesis)
	if vote.EligibleStake == 0 {
		// not 100% correct but ok
		vote.EligibleStake = block.Supply.ActiveStake - block.Supply.ActiveStake%block.Params.MinimalStake
		vote.EligibleVoters = block.Chain.EligibleBakers
		vote.QuorumPct, _, _ = idx.quorumByHeight(ctx, block.Height, block.Params)
	}

	// finalize vote for this round and safe; stake is used in Jakarta+
	vote.TurnoutPct = vote.TurnoutStake * 10000 / vote.EligibleStake
	if err := idx.tables[model.VoteTableKey].Update(ctx, vote); err != nil {
		return err
	}

	// finalize proposals, reuse slice
	insProposals = insProposals[:0]
	for _, v := range proposalMap {
		insProposals = append(insProposals, v)
	}
	if err := idx.tables[model.ProposalTableKey].Update(ctx, insProposals); err != nil {
		return err
	}
	return idx.tables[model.BallotTableKey].Insert(ctx, insBallots)
}

func (idx *GovIndex) processBallots(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// skip blocks without ballots
	if !block.HasBallots {
		return nil
	}

	// load current vote
	vote, err := idx.voteByHeight(ctx, block.Height, block.Params)
	if err != nil {
		return err
	}

	insBallots := make([]pack.Item, 0)
	for _, op := range block.Ops {
		if op.Type != model.OpTypeBallot {
			continue
		}
		bop, ok := op.Raw.(*rpc.Ballot)
		if !ok {
			return fmt.Errorf("ballot op [%d:%d]: unexpected type %T ", 1, op.OpP, op.Raw)
		}
		bkr, aok := builder.BakerByAddress(bop.Source)
		if !aok {
			return fmt.Errorf("missing account %s in proposal op [%d:%d]", bop.Source, 1, op.OpP)
		}
		// load account stake at snapshot block
		stake, err := idx.snapshotByHeight(ctx, bkr.AccountId, vote.StartHeight)
		if err != nil {
			return fmt.Errorf("missing stake snapshot for %s in vote period %d (%s) start %d",
				bkr, vote.VotingPeriod, vote.VotingPeriodKind, vote.StartHeight-1)
		}
		// fix for missing pre-genesis snapshot
		if block.Cycle == 0 && stake == 0 {
			stake = bkr.StakingBalance()
		}

		// update vote
		vote.TurnoutStake += stake
		vote.TurnoutVoters++
		switch bop.Ballot {
		case tezos.BallotVoteYay:
			vote.YayStake += stake
			vote.YayVoters++
		case tezos.BallotVoteNay:
			vote.NayStake += stake
			vote.NayVoters++
		case tezos.BallotVotePass:
			vote.PassStake += stake
			vote.PassVoters++
		}

		b := &model.Ballot{
			ElectionId:       vote.ElectionId,
			ProposalId:       vote.ProposalId,
			VotingPeriod:     vote.VotingPeriod,
			VotingPeriodKind: vote.VotingPeriodKind,
			Height:           block.Height,
			Time:             block.Timestamp,
			SourceId:         bkr.AccountId,
			OpId:             op.RowId,
			Stake:            stake,
			Ballot:           bop.Ballot,
		}
		insBallots = append(insBallots, b)
	}

	// update eligible stake when zero (happens when vote opens on genesis)
	if vote.EligibleStake == 0 {
		vote.EligibleStake = block.Supply.ActiveStake
		vote.EligibleVoters = block.Chain.EligibleBakers
		vote.QuorumPct, _, _ = idx.quorumByHeight(ctx, block.Height, block.Params)
	}

	// finalize vote for this round and safe; stake is used in Jakarta+
	vote.TurnoutPct = vote.TurnoutStake * 10000 / vote.EligibleStake
	if err := idx.tables[model.VoteTableKey].Update(ctx, vote); err != nil {
		return err
	}
	return idx.tables[model.BallotTableKey].Insert(ctx, insBallots)
}

func (idx *GovIndex) electionByHeight(ctx context.Context, height int64, _ *rpc.Params) (*model.Election, error) {
	election := &model.Election{}
	err := pack.NewQuery("etl.find_election").
		WithTable(idx.tables[model.ElectionTableKey]).
		WithoutCache().
		WithLimit(1).
		WithDesc().
		AndLte("start_height", height). // start height; first cycle starts at 2!
		Execute(ctx, election)
	if err != nil {
		return nil, err
	}
	if election.RowId == 0 {
		return nil, model.ErrNoElection
	}
	return election, nil
}

func (idx *GovIndex) voteByHeight(ctx context.Context, height int64, _ *rpc.Params) (*model.Vote, error) {
	vote := &model.Vote{}
	err := pack.NewQuery("etl.find_vote").
		WithTable(idx.tables[model.VoteTableKey]).
		WithoutCache().
		WithLimit(1).
		WithDesc().
		AndLte("period_start_height", height). // start height; first cycle starts at 2!
		Execute(ctx, vote)
	if err != nil {
		return nil, err
	}
	if vote.RowId == 0 {
		return nil, model.ErrNoVote
	}
	return vote, nil
}

func (idx *GovIndex) proposalsByElection(ctx context.Context, id model.ElectionID) ([]*model.Proposal, error) {
	proposals := make([]*model.Proposal, 0)
	err := pack.NewQuery("etl.list_proposals").
		WithTable(idx.tables[model.ProposalTableKey]).
		WithoutCache().
		AndEqual("election_id", id).
		Execute(ctx, &proposals)
	if err != nil {
		return nil, err
	}
	return proposals, nil
}

// func (idx *GovIndex) ballotsByVote(ctx context.Context, period int64) ([]*model.Ballot, error) {
// 	ballots := make([]*model.Ballot, 0)
// 	err := pack.NewQuery("list_ballots").
// 		WithTable(idx.tables[model.BallotTableKey]).
// 		WithoutCache().
// 		AndEqual("voting_period", period).
// 		Execute(ctx, &ballots)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return ballots, nil
// }

// MUST call at vote end block
func (idx *GovIndex) makeStakeSnapshot(ctx context.Context, block *model.Block, builder model.BlockBuilder) error {
	// use params from vote snapshot block (this is the previous vote end block)
	// using params that were active at that block is essential (esp. when
	// a protocol upgrade changes minimum stake size like Athens did)
	p := builder.Params(block.Height)
	log.Debugf("gov: make stake snapshot at height %d", block.Height)

	// skip deactivated bakers (we don't mark bakers deactivated until
	// next block / first in cycle) so we have to use the block receipt
	deactivated := tezos.NewAddressSet(block.TZ.Block.Metadata.Deactivated...)

	// snapshot all active bakers with at least minium stake (deactivation happens at
	// start of the next cycle, so here bakers are still active!)
	ins := make([]pack.Item, 0, int(block.Chain.EligibleBakers)) // hint
	for _, v := range builder.Bakers() {
		// check for deactivation
		if !v.IsActive || deactivated.Contains(v.Address) {
			continue
		}

		// use stake truncated to full coins
		stake := v.StakingBalance()
		if stake < p.MinimalStake {
			continue
		}

		// round to full rolls for protocols before Jakarta
		if block.Params.Version < 13 {
			stake -= stake % p.MinimalStake
		}

		snap := &model.Stake{
			Height:    block.Height,
			AccountId: v.AccountId,
			Stake:     stake,
		}
		ins = append(ins, snap)
	}
	return idx.tables[model.StakeTableKey].Insert(ctx, ins)
}

func (idx *GovIndex) snapshotByHeight(ctx context.Context, aid model.AccountID, height int64) (int64, error) {
	var snap model.Stake
	err := pack.NewQuery("etl.gov_find_stake").
		WithTable(idx.tables[model.StakeTableKey]).
		WithoutCache().
		WithDesc().
		AndEqual("account_id", aid).
		// need -1 offset, last Florence snap needs -2 as offset
		AndLte("height", height).
		Execute(ctx, &snap)
	if err != nil {
		return 0, err
	}
	if snap.Stake == 0 {
		log.Warnf("govindex: stake snapshot for account %d at height %d is zero", aid, height)
	}
	return snap.Stake, nil
}

func (idx *GovIndex) sumStake(ctx context.Context, height int64) (int64, error) {
	var foundHeight, sum int64
	var snap model.Stake
	err := pack.NewQuery("etl.gov_sum_stake").
		WithTable(idx.tables[model.StakeTableKey]).
		WithoutCache().
		WithDesc().
		// need -1 offset, last Florence snap needs -2 as offset
		AndLte("height", height).
		Stream(ctx, func(r pack.Row) error {
			if err := r.Decode(&snap); err != nil {
				return err
			}
			if foundHeight == 0 {
				foundHeight = snap.Height
			} else if foundHeight != snap.Height {
				return io.EOF
			}
			sum += snap.Stake
			return nil
		})
	if err != nil && err != io.EOF {
		return 0, err
	}
	if sum == 0 {
		log.Warnf("govindex: stake snapshot at height %d is zero", foundHeight)
	}
	return sum, nil
}

// quorums adjust at the end of each exploration & promotion voting period
// starting in v005 the algorithm changes to track participation as EMA (80/20)
func (idx *GovIndex) quorumByHeight(ctx context.Context, height int64, params *rpc.Params) (int64, int64, error) {
	// find most recent exploration or promotion period
	var lastQuorum, lastTurnout, lastTurnoutEma, nextQuorum, nextEma int64
	vote := &model.Vote{}
	err := pack.NewQuery("etl.find_quorum_vote").
		WithTable(idx.tables[model.VoteTableKey]).
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
	switch {
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

func (idx *GovIndex) Flush(ctx context.Context) error {
	for _, v := range idx.Tables() {
		if err := v.Flush(ctx); err != nil {
			log.Errorf("Flushing %s table: %v", v.Name(), err)
		}
	}
	return nil
}

func (idx *GovIndex) OnTaskComplete(_ context.Context, _ *task.TaskResult) error {
	// unused
	return nil
}
