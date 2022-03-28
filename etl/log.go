// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"sync"
	"time"

	"blockwatch.cc/tzindex/etl/model"
	logpkg "github.com/echa/log"
)

// log is a logger that is initialized with no output filters.  This
// means the package will not perform any logging by default until the caller
// requests it.
var log logpkg.Logger = logpkg.Log

// The default amount of logging is none.
func init() {
	DisableLog()
}

// DisableLog disables all library log output.  Logging output is disabled
// by default until either UseLogger or SetLogWriter are called.
func DisableLog() {
	log = logpkg.Disabled
}

// UseLogger uses a specified Logger to output package logging info.
// This should be used in preference to SetLogWriter if the caller is also
// using logpkg.
func UseLogger(logger logpkg.Logger) {
	log = logger
}

// LogClosure is a closure that can be printed with %v to be used to
// generate expensive-to-create data for a detailed log level and avoid doing
// the work if the data isn't printed.
type logClosure func() string

// String invokes the log closure and returns the results string.
func (c logClosure) String() string {
	return c()
}

// newLogClosure returns a new closure over the passed function which allows
// it to be used as a parameter in a logging function that is only invoked when
// the logging level is such that the message will actually be logged.
func newLogClosure(c func() string) logClosure {
	return logClosure(c)
}

// BlockProgressLogger provides periodic logging for other services in order
// to show users progress of certain "actions" involving some or all current
// blocks. Ex: syncing to best chain, indexing all blocks, etc.
type BlockProgressLogger struct {
	sync.Mutex
	nBlocks  int64
	nTx      int64
	lastTime time.Time
	action   string
}

// NewBlockProgressLogger returns a new block progress logger.
func NewBlockProgressLogger(msg string) *BlockProgressLogger {
	return &BlockProgressLogger{
		lastTime: time.Now(),
		action:   msg,
	}
}

// LogBlockHeight logs a new block height as an information message to show
// progress to the user. In order to prevent spam, it limits logging to one
// message every 10 seconds with duration and totals included.
func (b *BlockProgressLogger) LogBlockHeight(block *model.Block, qlen int, state State, d time.Duration) {
	b.Lock()
	defer b.Unlock()
	b.nBlocks++
	b.nTx += int64(len(block.Ops))
	now := time.Now()
	duration := now.Sub(b.lastTime)
	if duration < time.Second*10 {
		return
	}

	// Log information about new block height.
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
