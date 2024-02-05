// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"encoding/json"
	"fmt"
	"time"

	"blockwatch.cc/packdb/store"
	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
)

var (
	// tipBucketName is the name of the db bucket used to house the
	// block chain tip.
	tipBucketName = []byte("chaintip")

	// tipKey is the key of the chain tip serialized data in the db.
	tipKey = []byte("tip")

	// tipsBucketName is the name of the bucket holding indexer tips.
	tipsBucketName = []byte("tips")

	// deploymentsBucketName is the name of the bucket holding protocol deployment parameters.
	deploymentsBucketName = []byte("deployments")
)

func dbLoadChainTip(dbTx store.Tx) (*model.ChainTip, error) {
	tip := &model.ChainTip{}
	bucket := dbTx.Bucket(tipBucketName)
	if bucket == nil {
		return nil, ErrNoChainTip
	}
	buf := bucket.Get(tipKey)
	if buf == nil {
		return nil, ErrNoChainTip
	}
	err := json.Unmarshal(buf, &tip)
	if err != nil {
		return nil, err
	}
	return tip, nil
}

func dbStoreChainTip(dbTx store.Tx, tip *model.ChainTip) error {
	buf, err := json.Marshal(tip)
	if err != nil {
		return err
	}
	bucket := dbTx.Bucket(tipBucketName)
	bucket.FillPercent(1.0)
	return bucket.Put(tipKey, buf)
}

type IndexTip struct {
	Hash   *tezos.BlockHash `json:"hash,omitempty"`
	Height int64            `json:"height"`
}

func dbStoreIndexTip(dbTx store.Tx, key string, tip *IndexTip) error {
	buf, err := json.Marshal(tip)
	if err != nil {
		return err
	}
	b := dbTx.Bucket(tipsBucketName)
	b.FillPercent(1.0)
	return b.Put([]byte(key), buf)
}

func dbLoadIndexTip(dbTx store.Tx, key string) (*IndexTip, error) {
	b := dbTx.Bucket(tipsBucketName)
	if b == nil {
		return nil, ErrNoTable
	}
	buf := b.Get([]byte(key))
	if buf == nil {
		return nil, ErrNoTable
	}
	tip := &IndexTip{}
	err := json.Unmarshal(buf, tip)
	if err != nil {
		return nil, err
	}
	return tip, nil
}

type ReportTip struct {
	LastReportTime time.Time `json:"last_time"` // day of last report generation
}

func dbLoadDeployments(dbTx store.Tx, tip *model.ChainTip) ([]*rpc.Params, error) {
	plist := make([]*rpc.Params, 0, len(tip.Deployments))
	bucket := dbTx.Bucket(deploymentsBucketName)
	for _, v := range tip.Deployments {
		buf := bucket.Get(v.Protocol[:])
		if len(buf) == 0 {
			return nil, fmt.Errorf("missing deployment data for protocol %s", v.Protocol)
		}
		p := rpc.NewParams()
		if err := json.Unmarshal(buf, p); err != nil {
			return nil, err
		}
		plist = append(plist, p)
	}
	return plist, nil
}

func dbStoreDeployment(dbTx store.Tx, p *rpc.Params) error {
	buf, err := json.Marshal(p)
	if err != nil {
		return err
	}
	bucket := dbTx.Bucket(deploymentsBucketName)
	bucket.FillPercent(1.0)
	return bucket.Put(p.Protocol[:], buf)
}
