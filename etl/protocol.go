// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"fmt"

	"blockwatch.cc/tzindex/chain"
)

type Registry struct {
	byProtocol   map[string]*chain.Params
	byDeployment map[int]*chain.Params
	inOrder      []*chain.Params
}

func NewRegistry() *Registry {
	return &Registry{
		byProtocol:   make(map[string]*chain.Params),
		byDeployment: make(map[int]*chain.Params),
		inOrder:      make([]*chain.Params, 0),
	}
}

// Register registers network parameters for a Tezos network.
func (r *Registry) Register(p *chain.Params) error {
	if !p.Protocol.IsValid() {
		return fmt.Errorf("invalid protocol hash %s", p.Protocol)
	}
	_, isUpdate := r.byProtocol[p.Protocol.String()]
	r.byProtocol[p.Protocol.String()] = p
	r.byDeployment[p.Deployment] = p
	if !isUpdate {
		r.inOrder = append(r.inOrder, p)
	} else {
		r.inOrder[len(r.inOrder)-1] = p
	}
	return nil
}

func (r *Registry) GetParams(h chain.ProtocolHash) (*chain.Params, error) {
	if p, ok := r.byProtocol[h.String()]; !ok {
		return nil, fmt.Errorf("unknown protocol %s", h)
	} else {
		return p, nil
	}
}

func (r *Registry) GetParamsByHeight(height int64) *chain.Params {
	for _, v := range r.byDeployment {
		if height >= v.StartHeight && (v.EndHeight < 0 || height <= v.EndHeight) {
			return v
		}
	}
	return r.GetParamsLatest()
}

func (r *Registry) GetParamsByDeployment(v int) (*chain.Params, error) {
	if p, ok := r.byDeployment[v]; !ok {
		return nil, fmt.Errorf("unknown protocol deployment %d", v)
	} else {
		return p, nil
	}
}

func (r *Registry) GetAllParams() []*chain.Params {
	return r.inOrder
}

func (r *Registry) GetParamsLatest() *chain.Params {
	l := len(r.inOrder)
	if l == 0 {
		return nil
	}
	return r.inOrder[l-1]
}
