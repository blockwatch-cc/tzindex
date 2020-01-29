// Copyright (c) 2018 ECAD Labs Inc. MIT License
// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"encoding/json"
	"fmt"

	"blockwatch.cc/tzindex/chain"
)

// TestChainStatus is a variable structure depending on the Status field
type TestChainStatus interface {
	TestChainStatus() string
}

// GenericTestChainStatus holds the common values among all TestChainStatus variants
type GenericTestChainStatus struct {
	Status string `json:"status"`
}

// TestChainStatus gets the TestChainStatus's Status field
func (t *GenericTestChainStatus) TestChainStatus() string {
	return t.Status
}

// NotRunningTestChainStatus is a TestChainStatus variant for Status=not_running
type NotRunningTestChainStatus struct {
	GenericTestChainStatus
}

// ForkingTestChainStatus is a TestChainStatus variant for Status=forking
type ForkingTestChainStatus struct {
	GenericTestChainStatus
	Protocol   chain.ProtocolHash `json:"protocol"`
	Expiration string             `json:"expiration"`
}

// RunningTestChainStatus is a TestChainStatus variant for Status=running
type RunningTestChainStatus struct {
	GenericTestChainStatus
	ChainID    chain.ChainIdHash  `json:"chain_id"`
	Genesis    chain.BlockHash    `json:"genesis"`
	Protocol   chain.ProtocolHash `json:"protocol"`
	Expiration string             `json:"expiration"`
}

func unmarshalTestChainStatus(data []byte) (TestChainStatus, error) {
	var v TestChainStatus
	var tmp GenericTestChainStatus
	if err := json.Unmarshal(data, &tmp); err != nil {
		return nil, err
	}

	switch tmp.Status {
	case "not_running":
		v = &NotRunningTestChainStatus{}
	case "forking":
		v = &ForkingTestChainStatus{}
	case "running":
		v = &RunningTestChainStatus{}

	default:
		return nil, fmt.Errorf("Unknown TestChainStatus.Status: %v", tmp.Status)
	}

	if err := json.Unmarshal(data, v); err != nil {
		return nil, err
	}

	return v, nil
}
