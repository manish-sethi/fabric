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

package ledgerstorage

import (
	"fmt"
	"sync"

	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/fsblkstorage"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/hyperledger/fabric/protos/common"
)

// Provider encapusaltes two providers 1) block store provider and 2) and pvt data store provider
type Provider struct {
	blkStoreProvider     blkstorage.BlockStoreProvider
	pvtdataStoreProvider pvtdatastorage.Provider
}

// Store encapsulates two stores 1) block store and pvt data store
type Store struct {
	blkstorage.BlockStore
	pvtdataStore pvtdatastorage.Store
	rwlock       *sync.RWMutex
}

// NewProvider returns the handle to the provider
func NewProvider() *Provider {
	// Initialize the block storage
	attrsToIndex := []blkstorage.IndexableAttr{
		blkstorage.IndexableAttrBlockHash,
		blkstorage.IndexableAttrBlockNum,
		blkstorage.IndexableAttrTxID,
		blkstorage.IndexableAttrBlockNumTranNum,
		blkstorage.IndexableAttrBlockTxID,
		blkstorage.IndexableAttrTxValidationCode,
	}
	indexConfig := &blkstorage.IndexConfig{AttrsToIndex: attrsToIndex}
	blockStoreProvider := fsblkstorage.NewProvider(
		fsblkstorage.NewConf(ledgerconfig.GetBlockStorePath(), ledgerconfig.GetMaxBlockfileSize()),
		indexConfig)

	pvtStoreProvider := pvtdatastorage.NewProvider()
	return &Provider{blockStoreProvider, pvtStoreProvider}
}

// Open opens the store
func (p *Provider) Open(ledgerid string) (*Store, error) {
	var blockStore blkstorage.BlockStore
	var pvtdataStore pvtdatastorage.Store
	var err error
	if blockStore, err = p.blkStoreProvider.OpenBlockStore(ledgerid); err != nil {
		return nil, err
	}
	if pvtdataStore, err = p.pvtdataStoreProvider.OpenStore(ledgerid); err != nil {
		return nil, err
	}
	store := &Store{blockStore, pvtdataStore, &sync.RWMutex{}}
	if err := store.init(); err != nil {
		return nil, err
	}
	return store, nil
}

// Close closes the provider
func (p *Provider) Close() {
	p.blkStoreProvider.Close()
	p.pvtdataStoreProvider.Close()
}

// CommitWithPvtData commits the block and the corresponding pvt data in an atomic operation
func (s *Store) CommitWithPvtData(pvtdataAndBlock *ledger.PvtDataAndBlock) error {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	var pvtdata []*ledger.TxPvtData
	for _, v := range pvtdataAndBlock.BlockPvtData {
		pvtdata = append(pvtdata, v)
	}
	if err := s.pvtdataStore.Prepare(pvtdataAndBlock.Block.Header.Number, pvtdata); err != nil {
		return err
	}
	if err := s.AddBlock(pvtdataAndBlock.Block); err != nil {
		s.pvtdataStore.Rollback()
		return err
	}
	return s.pvtdataStore.Commit()
}

// GetPvtDataAndBlockByNum returns the block and the corresponding pvt data.
// The pvt data is filtered by the list of 'collections' supplied
func (s *Store) GetPvtDataAndBlockByNum(blockNum uint64, filter ledger.PvtNsCollFilter) (*ledger.PvtDataAndBlock, error) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()

	var block *common.Block
	var pvtdata []*ledger.TxPvtData
	var err error
	if block, err = s.RetrieveBlockByNumber(blockNum); err != nil {
		return nil, err
	}
	if pvtdata, err = s.GetPvtDataByNum(blockNum, filter); err != nil {
		return nil, err
	}
	return &ledger.PvtDataAndBlock{Block: block, BlockPvtData: constructPvtdataMap(pvtdata)}, nil
}

// GetPvtDataByNum returns only the pvt data  corresponding to the given block number
// The pvt data is filtered by the list of 'ns/collections' supplied in the filter
// A nil filter does not filter any results
func (s *Store) GetPvtDataByNum(blockNum uint64, filter ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()

	var pvtdata []*ledger.TxPvtData
	var err error
	if pvtdata, err = s.pvtdataStore.GetPvtDataByBlockNum(blockNum, filter); err != nil {
		return nil, err
	}
	return pvtdata, nil
}

func (s *Store) init() error {
	if pendingPvtbatch, err := s.pvtdataStore.HasPendingBatch(); err != nil || !pendingPvtbatch {
		return err
	}
	var bcInfo *common.BlockchainInfo
	var pvtdataStoreHt uint64
	var err error

	if bcInfo, err = s.GetBlockchainInfo(); err != nil {
		return err
	}
	if pvtdataStoreHt, err = s.pvtdataStore.LastCommittedBlock(); err != nil {
		return err
	}

	if bcInfo.Height == pvtdataStoreHt {
		return s.pvtdataStore.Rollback()
	}

	if bcInfo.Height == pvtdataStoreHt+1 {
		return s.pvtdataStore.Commit()
	}

	return fmt.Errorf("This is not expected. blockStoreHeight=%d, pvtdataStoreHeight=%d", bcInfo.Height, pvtdataStoreHt)
}

func constructPvtdataMap(pvtdata []*ledger.TxPvtData) map[uint64]*ledger.TxPvtData {
	if pvtdata == nil {
		return nil
	}
	m := make(map[uint64]*ledger.TxPvtData)
	for _, pvtdatum := range pvtdata {
		m[pvtdatum.SeqInBlock] = pvtdatum
	}
	return m
}
