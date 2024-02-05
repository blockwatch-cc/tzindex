// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package explorer

import (
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/server"
)

func init() {
	server.Register(Explorer{})
}

var _ server.RESTful = (*Explorer)(nil)

func PurgeCaches() {
	purgeCycleStore()
	purgeTipStore()
	purgeMetadataStore()
}

type Explorer struct{}

func (e Explorer) LastModified() time.Time {
	return time.Now().UTC()
}

func (e Explorer) Expires() time.Time {
	return time.Time{}
}

func (e Explorer) RESTPrefix() string {
	return "/explorer"
}

func (e Explorer) RESTPath(r *mux.Router) string {
	return e.RESTPrefix()
}

func (e Explorer) RegisterDirectRoutes(r *mux.Router) error {
	return nil
}

func (b Explorer) RegisterRoutes(r *mux.Router) error {
	r.HandleFunc("/tip", server.C(GetBlockchainTip)).Methods("GET")
	r.HandleFunc("/protocols", server.C(GetBlockchainProtocols)).Methods("GET")
	r.HandleFunc("/config/{ident}", server.C(GetBlockchainConfig)).Methods("GET")
	r.HandleFunc("/chain/{ident}", server.C(ReadChain)).Methods("GET")
	r.HandleFunc("/supply/{ident}", server.C(ReadChain)).Methods("GET")
	r.HandleFunc("/status", server.C(GetStatus)).Methods("GET")
	return nil
}

func GetStatus(ctx *server.Context) (interface{}, int) {
	return ctx.Crawler.Status(), http.StatusOK
}

func GetBlockchainProtocols(ctx *server.Context) (interface{}, int) {
	ct := ctx.Crawler.Tip()
	return ct.Deployments, http.StatusOK
}

type BlockchainConfig struct {
	// chain identity
	Name        string             `json:"name"`
	Network     string             `json:"network"`
	Symbol      string             `json:"symbol"`
	ChainId     tezos.ChainIdHash  `json:"chain_id"`
	Deployment  int                `json:"deployment"`
	Version     int                `json:"version"`
	Protocol    tezos.ProtocolHash `json:"protocol"`
	StartHeight int64              `json:"start_height"`
	EndHeight   int64              `json:"end_height"`
	Decimals    int                `json:"decimals"`

	MinimalStake      float64 `json:"minimal_stake"`
	PreservedCycles   int64   `json:"preserved_cycles"`
	BlocksPerCycle    int64   `json:"blocks_per_cycle"`
	MinimalBlockDelay int     `json:"minimal_block_delay"`

	lastmod time.Time `json:"-"`
	expires time.Time `json:"-"`
}

func (c BlockchainConfig) LastModified() time.Time {
	return c.lastmod
}

func (c BlockchainConfig) Expires() time.Time {
	return c.expires
}

func GetBlockchainConfig(ctx *server.Context) (interface{}, int) {
	p := ctx.Crawler.Params()
	cfg := BlockchainConfig{
		Name:              "Tezos",
		Network:           p.Network,
		Symbol:            p.Symbol,
		ChainId:           p.ChainId,
		Deployment:        p.Deployment,
		Version:           p.Version,
		Protocol:          p.Protocol,
		StartHeight:       p.StartHeight,
		EndHeight:         p.EndHeight,
		Decimals:          p.Decimals,
		MinimalStake:      p.ConvertValue(p.MinimalStake),
		PreservedCycles:   p.PreservedCycles,
		BlocksPerCycle:    p.BlocksPerCycle,
		MinimalBlockDelay: int(p.MinimalBlockDelay / time.Second),
		lastmod:           ctx.Crawler.Time(),
		expires:           ctx.Expires,
	}

	return cfg, http.StatusOK
}
