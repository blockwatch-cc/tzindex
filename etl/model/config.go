// Copyright (c) 2022-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"strings"

	"blockwatch.cc/packdb/pack"
	"github.com/echa/config"
)

func ReadConfigOpts(keys ...string) pack.Options {
	key := strings.Join(append([]string{"db"}, keys...), ".")
	return pack.Options{
		PackSizeLog2:    config.GetInt(key + ".pack_size_log2"),
		JournalSizeLog2: config.GetInt(key + ".journal_size_log2"),
		CacheSize:       config.GetInt(key + ".cache_size"),
		FillLevel:       config.GetInt(key + ".fill_level"),
	}
}

func init() {
	// database cache defaults
	config.SetDefault("db.account.cache_size", 512)
	config.SetDefault("db.baker.cache_size", 4)
	config.SetDefault("db.balance.cache_size", 256)
	config.SetDefault("db.bigmaps.cache_size", 128)
	config.SetDefault("db.bigmap_updates.cache_size", 128)
	config.SetDefault("db.bigmap_values.cache_size", 1024)
	config.SetDefault("db.block.cache_size", 128)
	config.SetDefault("db.chain.cache_size", 2)
	config.SetDefault("db.constant.cache_size", 2)
	config.SetDefault("db.contract.cache_size", 512)
	config.SetDefault("db.cycle.cache_size", 2)
	config.SetDefault("db.event.cache_size", 2)
	config.SetDefault("db.flow.cache_size", 2)
	config.SetDefault("db.gov_election.cache_size", 2)
	config.SetDefault("db.gov_proposal.cache_size", 2)
	config.SetDefault("db.gov_vote.cache_size", 2)
	config.SetDefault("db.gov_ballot.cache_size", 2)
	config.SetDefault("db.gov_stake.cache_size", 2)
	config.SetDefault("db.income.cache_size", 32)
	config.SetDefault("db.metadata.cache_size", 16)
	config.SetDefault("db.op.cache_size", 512)
	config.SetDefault("db.endorsement.cache_size", 2)
	config.SetDefault("db.rights.cache_size", 32)
	config.SetDefault("db.snapshot.cache_size", 128)
	config.SetDefault("db.storage.cache_size", 32)
	config.SetDefault("db.supply.cache_size", 16)
	config.SetDefault("db.ticket_types.cache_size", 16)
	config.SetDefault("db.ticket_updates.cache_size", 16)
	config.SetDefault("db.ticket_events.cache_size", 16)
	config.SetDefault("db.ticket_supply.cache_size", 16)
	config.SetDefault("db.ticket_balance.cache_size", 16)
	config.SetDefault("db.ticket_history.cache_size", 16)
	config.SetDefault("db.tasks.cache_size", 16)
	config.SetDefault("db.token.cache_size", 128)
	config.SetDefault("db.token_metadata.cache_size", 64)
	config.SetDefault("db.token_events.cache_size", 16)
	config.SetDefault("db.token_owners.cache_size", 256)
}
