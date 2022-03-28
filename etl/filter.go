// Copyright (c) 2020-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
    "context"
    "errors"

    "blockwatch.cc/tzindex/rpc"
)

var (
    ErrGapDetected   = errors.New("block gap detected")
    ErrReorgDetected = errors.New("block reorg detected")
)

type ReorgDelayFilter struct {
    queue []*rpc.Bundle
    out   chan<- *rpc.Bundle
    head  int
    tail  int
}

func NewReorgDelayFilter(depth int, out chan<- *rpc.Bundle) *ReorgDelayFilter {
    return &ReorgDelayFilter{
        queue: make([]*rpc.Bundle, depth),
        out:   out,
    }
}

func (f ReorgDelayFilter) Depth() int {
    return len(f.queue)
}

func (f ReorgDelayFilter) Head() *rpc.Bundle {
    if len(f.queue) == 0 {
        return nil
    }
    return f.queue[f.head]
}

func (f ReorgDelayFilter) Tail() *rpc.Bundle {
    if len(f.queue) == 0 {
        return nil
    }
    return f.queue[f.tail]
}

func (f *ReorgDelayFilter) Reset() {
    for i := range f.queue {
        f.queue[i] = nil
    }
    f.head, f.tail = 0, 0
}

func (f *ReorgDelayFilter) Push(ctx context.Context, b *rpc.Bundle, quit <-chan struct{}) error {
    // when disabled, forward directly
    if len(f.queue) == 0 {
        select {
        case f.out <- b: // may block
            return nil
        case <-quit: // unblock on shutdown
            return errInterruptRequested
        case <-ctx.Done(): // unblock on forced shutdown
            return ctx.Err()
        }
    }

    // check pre-cond
    head := f.queue[f.head]
    tail := f.queue[f.tail]

    // detect gaps: the new block is further than 1 block apart from queue tail
    if tail != nil && b.Height() > tail.Height()+1 {
        return ErrGapDetected
    }

    // detect reorg: the new block is smaller than queue head
    if head != nil && b.Height() < head.Height() {
        return ErrReorgDetected
    }

    // detect reorg: the new block's parent does not math the tail's hash
    if tail != nil && !b.ParentHash().Equal(tail.Hash()) {
        return ErrReorgDetected
    }

    // replace when inside reorg-safe window
    if head != nil && tail != nil && b.Height() <= tail.Height() {
        off := int(b.Height() - head.Height())
        pos := (f.head + off) % len(f.queue)
        f.queue[pos] = b
        return nil
    }

    // advance tail only after first wrap-around
    if tail != nil {
        f.tail = (f.tail + 1) % len(f.queue)
    }

    // output when queue is full
    if f.tail == f.head && head != nil {
        select {
        case f.out <- head: // may block
        case <-quit: // unblock on shutdown
            return errInterruptRequested
        case <-ctx.Done(): // unblock on forced shutdown
            return ctx.Err()
        }
        f.head = (f.head + 1) % len(f.queue)
    }

    // replace tail
    f.queue[f.tail] = b
    return nil
}
