// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"sync"
	"time"

	. "blockwatch.cc/tzindex/etl/model"
	logpkg "github.com/echa/log"
)

var log logpkg.Logger = logpkg.Log

func init() {
	DisableLog()
}

func DisableLog() {
	log = logpkg.Disabled
}

func UseLogger(logger logpkg.Logger) {
	log = logger
}

type logClosure func() string

func (c logClosure) String() string {
	return c()
}

func newLogClosure(c func() string) logClosure {
	return logClosure(c)
}

type BlockProgressLogger struct {
	sync.Mutex
	nBlocks  int64
	nTx      int64
	lastTime time.Time
	action   string
}

func NewBlockProgressLogger(msg string) *BlockProgressLogger {
	return &BlockProgressLogger{
		lastTime: time.Now(),
		action:   msg,
	}
}

func (b *BlockProgressLogger) LogBlockHeight(block *Block, qlen int, state State, d time.Duration) {
	b.Lock()
	defer b.Unlock()
	b.nBlocks++
	b.nTx += int64(len(block.Ops))
	now := time.Now()
	duration := now.Sub(b.lastTime)
	if duration < time.Second*10 {
		return
	}

	blockStr := "blocks"
	if b.nBlocks == 1 {
		blockStr = "block"
	}
	txStr := "transactions"
	if b.nTx == 1 {
		txStr = "transaction"
	}

	log.Infof("%s %d %s in %s (%d %s, height %d, %s, q=%d, t=%s, s=%s)",
		b.action, b.nBlocks, blockStr, duration.Truncate(10*time.Millisecond),
		b.nTx, txStr, block.Height,
		block.Timestamp,
		qlen, d, state)

	b.nBlocks = 0
	b.nTx = 0
	b.lastTime = now
}
