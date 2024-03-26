// Copyright (c) 2023-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package task

import (
	"errors"
	"fmt"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/tzgo/tezos"
)

var (
	ErrAgain        = errors.New("try again later")
	ErrShuttingDown = errors.New("scheduler is shutting down")
	ErrShutdown     = errors.New("scheduler is shut down")
)

type TaskStatus byte

const (
	TaskStatusInvalid = iota
	TaskStatusIdle
	TaskStatusRunning
	TaskStatusSuccess
	TaskStatusFailed
	TaskStatusTimeout
)

var (
	taskStatusString         = "invalid_idle_running_success_failed_timeout"
	taskStatusIdx            = [6][2]int{{0, 7}, {8, 12}, {13, 20}, {21, 28}, {29, 35}, {36, 43}}
	taskStatusReverseStrings = map[string]TaskStatus{}
)

func init() {
	for i, v := range taskStatusIdx {
		taskStatusReverseStrings[taskStatusString[v[0]:v[1]]] = TaskStatus(i)
	}
}

func (t TaskStatus) IsValid() bool {
	return t > TaskStatusInvalid
}

func (s TaskStatus) String() string {
	idx := taskStatusIdx[s]
	return taskStatusString[idx[0]:idx[1]]
}

func ParseTaskStatus(s string) TaskStatus {
	return taskStatusReverseStrings[s]
}

func (t *TaskStatus) UnmarshalText(data []byte) error {
	v := ParseTaskStatus(string(data))
	if !v.IsValid() {
		return fmt.Errorf("invalid task status type %q", string(data))
	}
	*t = v
	return nil
}

func (t TaskStatus) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

// Ensure TaskRequest implements the pack.Item interface.
var _ pack.Item = (*TaskRequest)(nil)

const TaskTableKey = "task"

type TaskRequest struct {
	Id      uint64        `pack:"I,pk"      json:"id"`
	Index   string        `pack:"J"         json:"index"`
	Decoder uint64        `pack:"D"         json:"decoder"`
	Owner   tezos.Address `pack:"o,bloom=2" json:"owner"`
	Account uint64        `pack:"a,bloom=2" json:"account"`
	Ordered bool          `pack:"O"         json:"ordered"`
	Flags   uint64        `pack:"f,snappy"  json:"flags"`
	Url     string        `pack:"u,snappy"  json:"url"`
	Status  TaskStatus    `pack:"s,snappy"  json:"status"`
	Data    []byte        `pack:"d,snappy"  json:"data"`
}

func (r *TaskRequest) SetID(i uint64) {
	r.Id = i
}

func (r TaskRequest) ID() uint64 {
	return r.Id
}

func (r TaskRequest) TableKey() string {
	return TaskTableKey
}

func (r TaskRequest) TableOpts() pack.Options {
	return pack.Options{
		PackSizeLog2:    10,
		JournalSizeLog2: 10,
		CacheSize:       4,
		FillLevel:       100,
	}
}

func (r TaskRequest) IndexOpts(key string) pack.Options {
	return pack.NoOptions
}

type TaskResult struct {
	Id      uint64
	Index   string
	Decoder uint64
	Owner   tezos.Address
	Account uint64
	Flags   uint64
	Status  TaskStatus
	Data    []byte
	Url     string
}
