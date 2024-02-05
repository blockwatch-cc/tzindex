// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
	"blockwatch.cc/tzindex/server"
)

var _ server.Resource = (*Supply)(nil)

// configurable marshalling helper
type Supply struct {
	model.Supply
	params *rpc.Params // blockchain amount conversion
}

func (s *Supply) MarshalJSON() ([]byte, error) {
	supply := struct {
		RowId               uint64    `json:"row_id"`
		Height              int64     `json:"height"`
		Cycle               int64     `json:"cycle"`
		Timestamp           time.Time `json:"time"`
		Total               float64   `json:"total"`
		Activated           float64   `json:"activated"`
		Unclaimed           float64   `json:"unclaimed"`
		Circulating         float64   `json:"circulating"`
		Liquid              float64   `json:"liquid"`
		Delegated           float64   `json:"delegated"`
		Staking             float64   `json:"staking"`
		Unstaking           float64   `json:"unstaking"`
		Shielded            float64   `json:"shielded"`
		ActiveStake         float64   `json:"active_stake"`
		ActiveDelegated     float64   `json:"active_delegated"`
		ActiveStaking       float64   `json:"active_staking"`
		InactiveDelegated   float64   `json:"inactive_delegated"`
		InactiveStaking     float64   `json:"inactive_staking"`
		Minted              float64   `json:"minted"`
		MintedBaking        float64   `json:"minted_baking"`
		MintedEndorsing     float64   `json:"minted_endorsing"`
		MintedSeeding       float64   `json:"minted_seeding"`
		MintedAirdrop       float64   `json:"minted_airdrop"`
		MintedSubsidy       float64   `json:"minted_subsidy"`
		Burned              float64   `json:"burned"`
		BurnedDoubleBaking  float64   `json:"burned_double_baking"`
		BurnedDoubleEndorse float64   `json:"burned_double_endorse"`
		BurnedOrigination   float64   `json:"burned_origination"`
		BurnedAllocation    float64   `json:"burned_allocation"`
		BurnedStorage       float64   `json:"burned_storage"`
		BurnedExplicit      float64   `json:"burned_explicit"`
		BurnedSeedMiss      float64   `json:"burned_seed_miss"`
		BurnedOffline       float64   `json:"burned_offline"`
		BurnedRollup        float64   `json:"burned_rollup"`
		Frozen              float64   `json:"frozen"`
		FrozenDeposits      float64   `json:"frozen_deposits"`
		FrozenRewards       float64   `json:"frozen_rewards"`
		FrozenFees          float64   `json:"frozen_fees"`
		FrozenBonds         float64   `json:"frozen_bonds"`
		FrozenStake         float64   `json:"frozen_stake"`
		FrozenBakerStake    float64   `json:"frozen_baker_stake"`
		FrozenStakerStake   float64   `json:"frozen_staker_stake"`
	}{
		RowId:               s.RowId,
		Height:              s.Height,
		Cycle:               s.Cycle,
		Timestamp:           s.Timestamp,
		Total:               s.params.ConvertValue(s.Total),
		Activated:           s.params.ConvertValue(s.Activated),
		Unclaimed:           s.params.ConvertValue(s.Unclaimed),
		Circulating:         s.params.ConvertValue(s.Circulating),
		Liquid:              s.params.ConvertValue(s.Liquid),
		Delegated:           s.params.ConvertValue(s.Delegated),
		Staking:             s.params.ConvertValue(s.Staking),
		Unstaking:           s.params.ConvertValue(s.Unstaking),
		Shielded:            s.params.ConvertValue(s.Shielded),
		ActiveStake:         s.params.ConvertValue(s.ActiveStake),
		ActiveDelegated:     s.params.ConvertValue(s.ActiveDelegated),
		ActiveStaking:       s.params.ConvertValue(s.ActiveStaking),
		InactiveDelegated:   s.params.ConvertValue(s.InactiveDelegated),
		InactiveStaking:     s.params.ConvertValue(s.InactiveStaking),
		Minted:              s.params.ConvertValue(s.Minted),
		MintedBaking:        s.params.ConvertValue(s.MintedBaking),
		MintedEndorsing:     s.params.ConvertValue(s.MintedEndorsing),
		MintedSeeding:       s.params.ConvertValue(s.MintedSeeding),
		MintedAirdrop:       s.params.ConvertValue(s.MintedAirdrop),
		MintedSubsidy:       s.params.ConvertValue(s.MintedSubsidy),
		Burned:              s.params.ConvertValue(s.Burned),
		BurnedDoubleBaking:  s.params.ConvertValue(s.BurnedDoubleBaking),
		BurnedDoubleEndorse: s.params.ConvertValue(s.BurnedDoubleEndorse),
		BurnedOrigination:   s.params.ConvertValue(s.BurnedOrigination),
		BurnedAllocation:    s.params.ConvertValue(s.BurnedAllocation),
		BurnedStorage:       s.params.ConvertValue(s.BurnedStorage),
		BurnedExplicit:      s.params.ConvertValue(s.BurnedExplicit),
		BurnedSeedMiss:      s.params.ConvertValue(s.BurnedSeedMiss),
		BurnedOffline:       s.params.ConvertValue(s.BurnedOffline),
		BurnedRollup:        s.params.ConvertValue(s.BurnedRollup),
		Frozen:              s.params.ConvertValue(s.Frozen),
		FrozenDeposits:      s.params.ConvertValue(s.FrozenDeposits),
		FrozenRewards:       s.params.ConvertValue(s.FrozenRewards),
		FrozenFees:          s.params.ConvertValue(s.FrozenFees),
		FrozenBonds:         s.params.ConvertValue(s.FrozenBonds),
		FrozenStake:         s.params.ConvertValue(s.FrozenStake),
		FrozenBakerStake:    s.params.ConvertValue(s.FrozenBakerStake),
		FrozenStakerStake:   s.params.ConvertValue(s.FrozenStakerStake),
	}
	return json.Marshal(supply)
}

func (s Supply) LastModified() time.Time {
	return s.Timestamp
}

func (s Supply) Expires() time.Time {
	return time.Time{}
}

func ReadSupply(ctx *server.Context) (interface{}, int) {
	id, ok := mux.Vars(ctx.Request)["ident"]
	if !ok || id == "" {
		panic(server.EBadRequest(server.EC_RESOURCE_ID_MISSING, "missing block identifier", nil))
	}
	switch id {
	case "head":
		s, err := ctx.Indexer.SupplyByHeight(ctx, ctx.Crawler.Height())
		if err != nil {
			panic(server.EInternal(server.EC_DATABASE, "cannot read chain data", err))
		}
		return Supply{*s, ctx.Crawler.Params()}, http.StatusOK
	default:
		val, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			panic(server.EBadRequest(server.EC_RESOURCE_ID_MALFORMED, "invalid block height", err))
		}
		s, err := ctx.Indexer.SupplyByHeight(ctx, val)
		if err != nil {
			panic(server.EInternal(server.EC_DATABASE, "cannot read chain data", err))
		}
		return Supply{*s, ctx.Crawler.Params()}, http.StatusOK
	}
}
