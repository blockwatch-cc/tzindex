// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"fmt"
	"sync"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/rpc"
)

type Registry struct {
	sync.RWMutex
	byProtocol   map[tezos.ProtocolHash]*rpc.Params
	byDeployment map[int]*rpc.Params
	inOrder      []*rpc.Params
}

func NewRegistry() *Registry {
	return &Registry{
		byProtocol:   make(map[tezos.ProtocolHash]*rpc.Params),
		byDeployment: make(map[int]*rpc.Params),
		inOrder:      make([]*rpc.Params, 0),
	}
}

// Register registers network parameters for a Tezos network.
func (r *Registry) Register(p *rpc.Params) {
	if p == nil {
		return
	}
	r.Lock()
	defer r.Unlock()
	_, isUpdate := r.byProtocol[p.Protocol]
	if isUpdate {
		for i := range r.inOrder {
			if r.inOrder[i].Protocol == p.Protocol {
				r.inOrder[i] = p
			}
		}
	} else {
		r.inOrder = append(r.inOrder, p)
		r.byProtocol[p.Protocol] = p
		r.byDeployment[p.Deployment] = p
	}
}

func (r *Registry) GetParams(h tezos.ProtocolHash) (*rpc.Params, error) {
	r.RLock()
	defer r.RUnlock()
	if p, ok := r.byProtocol[h]; !ok {
		return nil, fmt.Errorf("unknown protocol %q", h)
	} else {
		return p, nil
	}
}

func (r *Registry) GetParamsByHeight(height int64) *rpc.Params {
	r.RLock()
	for _, v := range r.inOrder {
		if height >= v.StartHeight && (v.EndHeight < 0 || height <= v.EndHeight) {
			r.RUnlock()
			return v
		}
	}
	r.RUnlock()
	return r.GetParamsLatest()
}

func (r *Registry) GetParamsByCycle(cycle int64) *rpc.Params {
	r.RLock()
	for i := len(r.inOrder) - 1; i >= 0; i-- {
		p := r.inOrder[i]
		if cycle >= p.StartCycle {
			r.RUnlock()
			return p
		}
	}
	r.RUnlock()
	return r.GetParamsLatest()
}

func (r *Registry) GetParamsByDeployment(v int) (*rpc.Params, error) {
	r.RLock()
	defer r.RUnlock()
	if p, ok := r.byDeployment[v]; !ok {
		return nil, fmt.Errorf("unknown protocol deployment %d", v)
	} else {
		return p, nil
	}
}

func (r *Registry) GetAllParams() []*rpc.Params {
	r.RLock()
	defer r.RUnlock()
	ret := make([]*rpc.Params, len(r.inOrder))
	copy(ret, r.inOrder)
	return ret
}

func (r *Registry) GetParamsLatest() *rpc.Params {
	r.RLock()
	defer r.RUnlock()
	l := len(r.inOrder)
	if l == 0 {
		return nil
	}
	return r.inOrder[l-1]
}
