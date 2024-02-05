// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

import (
	"context"
	"fmt"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/etl/task"
)

type NewDecoderFunc func(uint64) Decoder

var decoderRegistry = make(map[uint64]NewDecoderFunc)

func RegisterDecoder(h uint64, fn NewDecoderFunc) {
	decoderRegistry[h] = fn
}

func LookupDecoder(h uint64) (dec Decoder, ok bool) {
	var fn NewDecoderFunc
	fn, ok = decoderRegistry[h]
	if ok {
		dec = fn(h)
	}
	return
}

type Decoder interface {
	OnOperation(context.Context, *model.Op) ([]*Event, error)
	OnTaskComplete(context.Context, *model.Metadata, *task.TaskResult) ([]*Event, error)
	Namespace() string
}

type EventType byte

const (
	EventTypeInvalid = iota
	EventTypeUpdate
	EventTypeMerge
	EventTypeRemove
	EventTypeResolve
)

var (
	eventTypeString         = "invalid_update_merge_remove_resolve"
	eventTypeIdx            = [5][2]int{{0, 7}, {8, 14}, {15, 20}, {21, 27}, {28, 35}}
	eventTypeReverseStrings = map[string]EventType{}
)

func init() {
	for i, v := range eventTypeIdx {
		eventTypeReverseStrings[eventTypeString[v[0]:v[1]]] = EventType(i)
	}
}

func (t EventType) IsValid() bool {
	return t > EventTypeInvalid
}

func (e EventType) String() string {
	idx := eventTypeIdx[e]
	return eventTypeString[idx[0]:idx[1]]
}

func ParseEventType(s string) EventType {
	return eventTypeReverseStrings[s]
}

func (t *EventType) UnmarshalText(data []byte) error {
	v := ParseEventType(string(data))
	if !v.IsValid() {
		return fmt.Errorf("invalid metadata event type %q", string(data))
	}
	*t = v
	return nil
}

func (t EventType) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

type Event struct {
	Owner tezos.Address
	Type  EventType
	Flags int64
	Data  any
}
