// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package domain

import (
	"context"
	"encoding/json"
	"fmt"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/metadata"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/etl/task"
)

// Tezos Domains reverse records operations
//
// - add reverse record to address
// - remove reverse record from address
// - change reverse record on address
//
// Bigmap Type
// address => struct { name: bytes, owner: address, internal_data: map{string=>bytes} }

const CodeHash uint64 = 0x5befb75267e3402b

func init() {
	metadata.RegisterDecoder(CodeHash, NewDecoder)
}

type Decoder struct {
	reverse int64
	typ     micheline.Type
}

func NewDecoder(_ uint64) metadata.Decoder {
	return &Decoder{}
}

func (d *Decoder) Namespace() string {
	return metadata.NsDomain
}

func (d *Decoder) OnOperation(ctx context.Context, op *model.Op) ([]*metadata.Event, error) {
	if err := d.init(op); err != nil {
		return nil, err
	}
	events := make([]*metadata.Event, 0)
	for _, ev := range op.BigmapEvents {
		// only handle related bigmap updates
		if ev.Id != d.reverse {
			continue
		}

		// skip copy/alloc
		switch ev.Action {
		case micheline.DiffActionCopy, micheline.DiffActionAlloc:
			continue
		}

		// read address the record points to from bigmap key
		var e metadata.Event
		if err := e.Owner.Decode(ev.Key.Bytes); err != nil {
			return nil, err
		}

		switch ev.Action {
		case micheline.DiffActionUpdate:
			var rec DomainReverseRecord
			val := micheline.NewValue(d.typ, ev.Value)
			if err := val.Unmarshal(&rec); err != nil {
				return nil, fmt.Errorf("domain: unmarshal reverse record: %w", err)
			}
			if rec.Name == "" {
				e.Type = metadata.EventTypeRemove
				events = append(events, &e)
			} else {
				e.Type = metadata.EventTypeUpdate
				e.Data = &metadata.TezosDomains{
					Name: rec.Name,
				}
				events = append(events, &e)
			}

		case micheline.DiffActionRemove:
			e.Type = metadata.EventTypeRemove
			events = append(events, &e)
		}
	}
	return events, nil
}

func (d *Decoder) OnTaskComplete(_ context.Context, _ *model.Metadata, _ *task.TaskResult) ([]*metadata.Event, error) {
	// unused
	return nil, nil
}

func (d *Decoder) init(op *model.Op) error {
	if d.reverse > 0 {
		return nil
	}
	script, err := op.Contract.LoadScript()
	if err != nil {
		return fmt.Errorf("domain: cannot load %s script: %v", op.Contract, err)
	}
	maps := script.Bigmaps()
	if maps == nil {
		return fmt.Errorf("domain: cannot detect bigmaps")
	}
	var ok bool
	d.reverse, ok = maps["reverse_records"]
	if !ok {
		return fmt.Errorf("domain: missing reverse_records bigmap in %s: %#v", op.Contract, maps)
	}

	typs := script.BigmapTypes()
	if typs == nil {
		return fmt.Errorf("domain: cannot detect bigmap types")
	}
	typ, ok := typs["reverse_records"]
	if !ok {
		return fmt.Errorf("domain: missing reverse_records type %s: %#v", op.Contract, typs)
	}
	d.typ = typ.Right()

	return nil
}

type DomainReverseRecord struct {
	Name  string        `json:"name"`
	Owner tezos.Address `json:"owner"`
	// InternalData map[string]string `json:"internal_data"`
}

func (r *DomainReverseRecord) UnmarshalJSON(buf []byte) error {
	type alias DomainReverseRecord
	if err := json.Unmarshal(buf, (*alias)(r)); err != nil {
		return err
	}
	r.Name, _ = model.UnpackTnsString(r.Name)
	// r.InternalData = model.UnpackTnsMap(r.InternalData)
	return nil
}
