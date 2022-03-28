// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package model

import (
	"time"

	"blockwatch.cc/tzgo/tezos"
)

// ChainTip reflects the blockchain state at the currently indexed height.
type ChainTip struct {
	Name          string            `json:"name"`         // chain name, e.g. Bitcoin
	Symbol        string            `json:"symbol"`       // chain symbol, e.g. BTC
	ChainId       tezos.ChainIdHash `json:"chain_id"`     // chain identifier (same for all blocks)
	BestHash      tezos.BlockHash   `json:"last_block"`   // The hash of the chain tip block.
	BestId        uint64            `json:"last_id"`      // The internal blockindex id of the tip block;
	BestHeight    int64             `json:"height"`       // The height of the tip block.
	BestTime      time.Time         `json:"timestamp"`    // The timestamp of the tip block.
	GenesisTime   time.Time         `json:"genesis_time"` // cache of first block generation time
	NYEveBlocks   []int64           `json:"nye_blocks"`   // first block heights per year for annual statistics
	QuarterBlocks []int64           `json:"qtr_blocks"`   // first block heights per quarter for annual statistics
	Deployments   []Deployment      `json:"deployments"`  // protocol deployments
}

type Deployment struct {
	Protocol    tezos.ProtocolHash `json:"protocol"`
	Version     int                `json:"version"`      // protocol version
	Deployment  int                `json:"deployment"`   // protocol sequence id on indexed chain
	StartHeight int64              `json:"start_height"` // first block on indexed chain
	EndHeight   int64              `json:"end_height"`   // last block on indexed chain or -1
}

func (t *ChainTip) AddDeployment(p *tezos.Params) {
	// set end height for previous deployment
	if l := len(t.Deployments); l > 0 {
		t.Deployments[l-1].EndHeight = p.StartHeight - 1
	}
	t.Deployments = append(t.Deployments, Deployment{
		Protocol:    p.Protocol,
		Version:     p.Version,
		Deployment:  p.Deployment,
		StartHeight: p.StartHeight,
		EndHeight:   p.EndHeight,
	})
}

func (t *ChainTip) Clone() *ChainTip {
	tip := &ChainTip{
		Name:          t.Name,
		Symbol:        t.Symbol,
		ChainId:       t.ChainId.Clone(),
		BestHash:      t.BestHash.Clone(),
		BestId:        t.BestId,
		BestHeight:    t.BestHeight,
		BestTime:      t.BestTime,
		GenesisTime:   t.GenesisTime,
		NYEveBlocks:   make([]int64, len(t.NYEveBlocks)),
		QuarterBlocks: make([]int64, len(t.QuarterBlocks)),
		Deployments:   make([]Deployment, len(t.Deployments)),
	}
	copy(tip.NYEveBlocks, t.NYEveBlocks)
	copy(tip.QuarterBlocks, t.QuarterBlocks)
	copy(tip.Deployments, t.Deployments)
	return tip
}
