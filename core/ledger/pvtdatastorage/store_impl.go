/*
Copyright IBM Corp. 2017 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pvtdatastorage

import (
	"fmt"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
)

var logger = flogging.MustGetLogger("pvtrwstorage")

type provider struct {
	dbProvider *leveldbhelper.Provider
}

type store struct {
	db                 *leveldbhelper.DBHandle
	ledgerid           string
	isEmpty            bool
	lastCommittedBlock uint64
	batchPending       bool
}

type blkTranNumKey []byte

// NewProvider instantiates a StoreProvider
func NewProvider() Provider {
	dbPath := ledgerconfig.GetPvtWritesetStorePath()
	dbProvider := leveldbhelper.NewProvider(&leveldbhelper.Conf{DBPath: dbPath})
	return &provider{dbProvider: dbProvider}
}

// OpenStore returns a handle to a store
func (p *provider) OpenStore(ledgerid string) (Store, error) {
	dbHandle := p.dbProvider.GetDBHandle(ledgerid)
	s := &store{db: dbHandle, ledgerid: ledgerid}
	if err := s.initState(); err != nil {
		return nil, err
	}
	return s, nil
}

// Close closes the store
func (p *provider) Close() {
	p.dbProvider.Close()
}

func (s *store) initState() error {
	var err error
	if s.isEmpty, s.lastCommittedBlock, err = s.getLastCommittedBlockNum(); err != nil {
		return err
	}
	if s.batchPending, err = s.hasPendingCommit(); err != nil {
		return err
	}
	return nil
}

func (s *store) Prepare(blockNum uint64, pvtData []*ledger.TxPvtData) error {
	if s.batchPending {
		return &ErrIllegalCall{"First invoke commit/rollback on the pending batch"}
	}
	expectedBlockNum := s.nextBlockNum()
	if expectedBlockNum != blockNum {
		return &ErrIllegalArgs{fmt.Sprintf("Expected block number=%d, recived block number=%d", expectedBlockNum, blockNum)}
	}

	batch := leveldbhelper.NewUpdateBatch()
	var key, value []byte
	var err error
	for _, pvtDatum := range pvtData {
		key = encodePK(blockNum, pvtDatum.SeqInBlock)
		if value, err = encodePvtRwSet(pvtDatum.WriteSet); err != nil {
			return err
		}
		logger.Debugf("Adding to batch blockNum=%d, tranNum=%d", blockNum, pvtDatum.SeqInBlock)
		batch.Put(key, value)
	}
	batch.Put(pendingCommitKey, emptyValue)
	if err := s.db.WriteBatch(batch, true); err != nil {
		return err
	}
	s.batchPending = true
	return nil
}

func (s *store) Commit() error {
	if !s.batchPending {
		return &ErrIllegalCall{"No pending batch to commit"}
	}
	committingBlockNum := s.nextBlockNum()
	logger.Debugf("Committing pvt data for block = %d", committingBlockNum)
	batch := leveldbhelper.NewUpdateBatch()
	batch.Delete(pendingCommitKey)
	batch.Put(lastCommittedBlkkey, encodeBlockNum(committingBlockNum))
	if err := s.db.WriteBatch(batch, true); err != nil {
		return err
	}
	s.batchPending = false
	s.isEmpty = false
	s.lastCommittedBlock = committingBlockNum
	logger.Debugf("Committed pvt data for block = %d", committingBlockNum)
	return nil
}

func (s *store) Rollback() error {
	var pendingBatchKeys []blkTranNumKey
	var err error
	if !s.batchPending {
		return &ErrIllegalCall{"No pending batch to rollback"}
	}
	rollingbackBlockNum := s.nextBlockNum()
	logger.Debugf("Rolling back pvt data for block = %d", rollingbackBlockNum)

	if pendingBatchKeys, err = s.retrievePendingBatchKeys(); err != nil {
		return err
	}
	batch := leveldbhelper.NewUpdateBatch()
	for _, key := range pendingBatchKeys {
		batch.Delete(key)
	}
	if err := s.db.WriteBatch(batch, true); err != nil {
		return err
	}
	s.batchPending = false
	logger.Debugf("Rolled back pvt data for block = %d", rollingbackBlockNum)
	return nil
}

func (s *store) GetPvtDataByBlockNum(blockNum uint64, filter ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error) {
	logger.Debugf("GetPvtDataByBlockNum(): blockNum=%d, filter=%#v", blockNum, filter)
	if s.isEmpty {
		return nil, &ErrOutOfRange{fmt.Sprintf("The store is empty")}
	}

	if blockNum > s.lastCommittedBlock {
		return nil, &ErrOutOfRange{fmt.Sprintf("Last committed block=%d, block requested=%d", s.lastCommittedBlock, blockNum)}
	}
	var pvtData []*ledger.TxPvtData
	startKey, endKey := getKeysForRangeScanByBlockNum(blockNum)
	logger.Debugf("GetPvtDataByBlockNum(): startKey=%#v, endKey=%#v", startKey, endKey)
	itr := s.db.GetIterator(startKey, endKey)
	defer itr.Release()

	var pvtWSert *rwset.TxPvtReadWriteSet
	var err error
	for itr.Next() {
		bNum, tNum := decodePK(itr.Key())
		if pvtWSert, err = decodePvtRwSet(itr.Value()); err != nil {
			return nil, err
		}
		logger.Debugf("stored pvtWSert for bNum=%d, tNum=%d: %#v", bNum, tNum, pvtWSert)
		filteredWSet := trimPvtWSet(pvtWSert, filter)
		logger.Debugf("filtered pvtWSert for bNum=%d, tNum=%d: %#v", bNum, tNum, filteredWSet)
		pvtData = append(pvtData, &ledger.TxPvtData{SeqInBlock: tNum, WriteSet: filteredWSet})
	}
	return pvtData, nil
}

func (s *store) LastCommittedBlock() (uint64, error) {
	return s.lastCommittedBlock, nil
}

func (s *store) HasPendingBatch() (bool, error) {
	return s.batchPending, nil
}

func (s *store) IsEmpty() (bool, error) {
	return s.isEmpty, nil
}

func (s *store) Shutdown() {
	// do nothing
}

func (s *store) nextBlockNum() uint64 {
	if s.isEmpty {
		return 0
	}
	return s.lastCommittedBlock + 1
}

func (s *store) retrievePendingBatchKeys() ([]blkTranNumKey, error) {
	var pendingBatchKeys []blkTranNumKey
	itr := s.db.GetIterator(encodePK(s.nextBlockNum(), 0), nil)
	for itr.Next() {
		pendingBatchKeys = append(pendingBatchKeys, itr.Key())
	}
	return pendingBatchKeys, nil
}

func (s *store) hasPendingCommit() (bool, error) {
	var v []byte
	var err error
	if v, err = s.db.Get(pendingCommitKey); err != nil {
		return false, err
	}
	return v != nil, nil
}

func (s *store) getLastCommittedBlockNum() (bool, uint64, error) {
	var v []byte
	var err error
	if v, err = s.db.Get(lastCommittedBlkkey); v == nil || err != nil {
		return true, 0, err
	}
	return false, decodeBlockNum(v), nil
}

func trimPvtWSet(pvtWSet *rwset.TxPvtReadWriteSet, filter ledger.PvtNsCollFilter) *rwset.TxPvtReadWriteSet {
	if filter == nil {
		return pvtWSet
	}

	var filteredNsRwSet []*rwset.NsPvtReadWriteSet
	for _, ns := range pvtWSet.NsPvtRwset {
		var filteredCollRwSet []*rwset.CollectionPvtReadWriteSet
		for _, coll := range ns.CollectionPvtRwset {
			if filter.Has(ns.Namespace, coll.CollectionName) {
				filteredCollRwSet = append(filteredCollRwSet, coll)
			}
		}
		if filteredCollRwSet != nil {
			filteredNsRwSet = append(filteredNsRwSet,
				&rwset.NsPvtReadWriteSet{
					Namespace:          ns.Namespace,
					CollectionPvtRwset: filteredCollRwSet,
				},
			)
		}
	}
	var filteredTxPvtRwSet *rwset.TxPvtReadWriteSet
	if filteredNsRwSet != nil {
		filteredTxPvtRwSet = &rwset.TxPvtReadWriteSet{
			DataModel:  pvtWSet.GetDataModel(),
			NsPvtRwset: filteredNsRwSet,
		}
	}
	return filteredTxPvtRwSet
}
