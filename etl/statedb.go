// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package etl

import (
	"encoding/json"
	"fmt"

	"blockwatch.cc/packdb/store"
	"blockwatch.cc/tzindex/chain"
	. "blockwatch.cc/tzindex/etl/model"
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

func dbLoadChainTip(dbTx store.Tx) (*ChainTip, error) {
	tip := &ChainTip{}
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

func dbStoreChainTip(dbTx store.Tx, tip *ChainTip) error {
	buf, err := json.Marshal(tip)
	if err != nil {
		return err
	}
	bucket := dbTx.Bucket(tipBucketName)
	bucket.FillPercent(1.0)
	return bucket.Put(tipKey, buf)
}

type IndexTip struct {
	Hash   *chain.BlockHash `json:"hash,omitempty"`
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

func dbLoadDeployments(dbTx store.Tx, tip *ChainTip) ([]*chain.Params, error) {
	plist := make([]*chain.Params, 0, len(tip.Deployments))
	bucket := dbTx.Bucket([]byte(deploymentsBucketName))
	for _, v := range tip.Deployments {
		buf := bucket.Get(v.Protocol.Hash.Hash)
		if len(buf) == 0 {
			return nil, fmt.Errorf("missing deployment data for protocol %s", v.Protocol)
		}
		p := &chain.Params{}
		if err := json.Unmarshal(buf, p); err != nil {
			return nil, err
		}
		plist = append(plist, p)
	}
	return plist, nil
}

func dbStoreDeployment(dbTx store.Tx, p *chain.Params) error {
	buf, err := json.Marshal(p)
	if err != nil {
		return err
	}
	bucket := dbTx.Bucket([]byte(deploymentsBucketName))
	bucket.FillPercent(1.0)
	return bucket.Put(p.Protocol.Hash.Hash, buf)
}
