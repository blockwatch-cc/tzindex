// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"fmt"
	"sync"

	"blockwatch.cc/tzgo/tezos"
)

type Registry struct {
	sync.RWMutex
	byProtocol   map[string]*tezos.Params
	byDeployment map[int]*tezos.Params
	inOrder      []*tezos.Params
}

func NewRegistry() *Registry {
	return &Registry{
		byProtocol:   make(map[string]*tezos.Params),
		byDeployment: make(map[int]*tezos.Params),
		inOrder:      make([]*tezos.Params, 0),
	}
}

// Register registers network parameters for a Tezos network.
func (r *Registry) Register(p *tezos.Params) error {
	if !p.Protocol.IsValid() {
		return fmt.Errorf("invalid protocol hash %s", p.Protocol)
	}
	r.Lock()
	defer r.Unlock()
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

func (r *Registry) GetParams(h tezos.ProtocolHash) (*tezos.Params, error) {
	r.RLock()
	defer r.RUnlock()
	if p, ok := r.byProtocol[h.String()]; !ok {
		return nil, fmt.Errorf("unknown protocol %s", h)
	} else {
		return p, nil
	}
}

func (r *Registry) GetParamsByHeight(height int64) *tezos.Params {
	r.RLock()
	for _, v := range r.byDeployment {
		if height >= v.StartHeight && (v.EndHeight < 0 || height <= v.EndHeight) {
			r.RUnlock()
			return v
		}
	}
	r.RUnlock()
	return r.GetParamsLatest()
}

func (r *Registry) GetParamsByDeployment(v int) (*tezos.Params, error) {
	r.RLock()
	defer r.RUnlock()
	if p, ok := r.byDeployment[v]; !ok {
		return nil, fmt.Errorf("unknown protocol deployment %d", v)
	} else {
		return p, nil
	}
}

func (r *Registry) GetAllParams() []*tezos.Params {
	r.RLock()
	defer r.RUnlock()
	ret := make([]*tezos.Params, len(r.inOrder))
	copy(ret, r.inOrder)
	return ret
}

func (r *Registry) GetParamsLatest() *tezos.Params {
	r.RLock()
	defer r.RUnlock()
	l := len(r.inOrder)
	if l == 0 {
		return nil
	}
	return r.inOrder[l-1]
}
