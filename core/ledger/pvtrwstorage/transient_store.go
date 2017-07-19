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

package pvtrwstorage

import (
	"errors"

	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

var emptyValue = []byte{}

// ErrTransientStoreEmpty is used to indicate that there are no entries in transient store
var ErrTransientStoreEmpty = errors.New("Transient store is empty")

// TransientStoreProvider encapsulates a leveldb provider which is used to store
// private read-write set of simulated transactions, and implements TransientStoreProvider
// interface.
type transientStoreProvider struct {
	dbProvider *leveldbhelper.Provider
}

// TransientStoreProvider provides an instance of a TransientStore
type TransientStoreProvider interface {
	OpenStore(ledgerID string) (TransientStore, error)
	Close()
}

// TransientStore holds an instance of a levelDB.
type transientStore struct {
	db       *leveldbhelper.DBHandle
	ledgerID string
}

type rwsetScanner struct {
	txid  string
	dbItr iterator.Iterator
}

// TransientStore manages the storage of private read-write sets for a ledgerId.
// Ideally, a ledger can remove the data from this storage when it is committed to
// the permanent storage or the pruning of some data items is enforced by the policy
type TransientStore interface {
	// Persist stores the private read-write set of a transaction in the transient store
	Persist(txid string, endorserid string, endorsementBlkHt uint64, privateSimulationResults []byte) error
	// GetTxPrivateRWSetByTxid returns private read-write set of a given transaction
	GetTxPrivateRWSetByTxid(txid string) (commonledger.ResultsIterator, error)
	// GetSelfSimulatedTxPvtRWSet returns the pvt read-write set from the simulation performed by the peer itself
	GetSelfSimulatedTxPvtRWSet(txid string) (*ledger.EndorserPrivateSimulationResults, error)
	// Purge removes private read-writes set generated by endorsers at block height lesser than
	// a given maxBlockNumToRetain. In other words, Purge only retains private read-write sets
	// that were generated at block height of maxBlockNumToRetain or higher.
	Purge(maxBlockNumToRetain uint64) error
	// GetMinEndorsementBlkHt returns the lowest retained endorsement block height
	GetMinEndorsementBlkHt() (uint64, error)
	Shutdown()
}

// NewTransientStoreProvider instantiates TransientStoreProvider
func NewTransientStoreProvider() TransientStoreProvider {
	dbPath := ledgerconfig.GetTransientStorePath()
	dbProvider := leveldbhelper.NewProvider(&leveldbhelper.Conf{DBPath: dbPath})
	return &transientStoreProvider{dbProvider: dbProvider}
}

// OpenStore returns a handle to a ledgerId in TransientStore
func (provider *transientStoreProvider) OpenStore(ledgerID string) (TransientStore, error) {
	dbHandle := provider.dbProvider.GetDBHandle(ledgerID)
	return &transientStore{db: dbHandle, ledgerID: ledgerID}, nil
}

// Close closes the TransientStoreProvider
func (provider *transientStoreProvider) Close() {
	provider.dbProvider.Close()
}

// Persist stores the private read-write set of a transaction in the transient store
func (store *transientStore) Persist(txid string, endorserid string,
	endorsementBlkHt uint64, privateSimulationResults []byte) error {
	dbBatch := leveldbhelper.NewUpdateBatch()

	// Create compositeKey with appropriate prefix, txid, endorserid and endorsementBlkHt
	compositeKey := createCompositeKeyForPRWSet(txid, endorserid, endorsementBlkHt)
	dbBatch.Put(compositeKey, privateSimulationResults)

	// Create compositeKey with appropriate prefix, endorsementBlkHt, txid, endorserid & Store
	// the compositeKey (purge index) a null byte as value.
	compositeKey = createCompositeKeyForPurgeIndex(endorsementBlkHt, txid, endorserid)
	dbBatch.Put(compositeKey, emptyValue)

	return store.db.WriteBatch(dbBatch, true)
}

// GetTxPrivateRWSetByTxid returns private read-write set of a given transaction
func (store *transientStore) GetTxPrivateRWSetByTxid(txid string) (commonledger.ResultsIterator, error) {
	// Construct startKey and endKey to do an range query
	startKey := createTxidRangeStartKey(txid)
	endKey := createTxidRangeEndKey(txid)

	iter := store.db.GetIterator(startKey, endKey)
	return &rwsetScanner{txid, iter}, nil
}

// GetSelfSimulatedTxPvtRWSet returns the pvt read-write set from the simulation performed by the peer itself
func (store *transientStore) GetSelfSimulatedTxPvtRWSet(txid string) (*ledger.EndorserPrivateSimulationResults, error) {
	var err error
	var itr commonledger.ResultsIterator
	if itr, err = store.GetTxPrivateRWSetByTxid(txid); err != nil {
		return nil, err
	}
	defer itr.Close()
	var temp commonledger.QueryResult
	var res *ledger.EndorserPrivateSimulationResults
	for {
		if temp, err = itr.Next(); err != nil {
			return nil, err
		}
		if temp == nil {
			return nil, nil
		}
		res = temp.(*ledger.EndorserPrivateSimulationResults)
		if selfSimulated(res) {
			return res, nil
		}
	}
}

// Purge removes private read-writes set generated by endorsers at block height lesser than
// a given maxBlockNumToRetain. In other words, Purge only retains private read-write sets
// that were generated at block height of maxBlockNumToRetain or higher.
func (store *transientStore) Purge(maxBlockNumToRetain uint64) error {
	// Do a range query with 0 as startKey and maxBlockNumToRetain-1 as endKey
	startKey := createEndorsementBlkHtRangeStartKey(0)
	endKey := createEndorsementBlkHtRangeEndKey(maxBlockNumToRetain - 1)
	iter := store.db.GetIterator(startKey, endKey)

	dbBatch := leveldbhelper.NewUpdateBatch()

	// Get all txid and endorserid from above result and remove it from transient store (both
	// read/write set and the corresponding index.
	for iter.Next() {
		dbKey := iter.Key()
		txid, endorserid, endorsementBlkHt := splitCompositeKeyOfPurgeIndex(dbKey)
		compositeKey := createCompositeKeyForPRWSet(txid, endorserid, endorsementBlkHt)
		dbBatch.Delete(compositeKey)
		dbBatch.Delete(dbKey)
	}
	return store.db.WriteBatch(dbBatch, true)
}

// GetMinEndorsementBlkHt returns the lowest retained endorsement block height
func (store *transientStore) GetMinEndorsementBlkHt() (uint64, error) {
	// Either maintain the minEndorsementBlkHt separately in the DB or
	// do a range query on the compositeKey namespace (endorsement
	startKey := createEndorsementBlkHtRangeStartKey(0)
	iter := store.db.GetIterator(startKey, nil)
	// Fetch the minimum endorsement block height
	if iter.Next() {
		dbKey := iter.Key()
		_, _, endorsementBlkHt := splitCompositeKeyOfPurgeIndex(dbKey)
		return endorsementBlkHt, nil
	}
	// Returning an error may not be the right thing to do here. May be
	// return a bool. -1 is not possible due to unsigned int as first
	// return value
	return 0, ErrTransientStoreEmpty
}

func (store *transientStore) Shutdown() {
	// do nothing because shared db is used
}

// Next moves the iterator to the next key/value pair.
// It returns whether the iterator is exhausted.
func (scanner *rwsetScanner) Next() (commonledger.QueryResult, error) {
	if !scanner.dbItr.Next() {
		return nil, nil
	}
	dbKey := scanner.dbItr.Key()
	dbVal := scanner.dbItr.Value()
	endorserid, endorsementBlkHt := splitCompositeKeyOfPRWSet(dbKey)
	return &ledger.EndorserPrivateSimulationResults{
		EndorserID:               endorserid,
		EndorsementBlockHeight:   endorsementBlkHt,
		PrivateSimulationResults: dbVal,
	}, nil
}

// Close releases resource held by the iterator
func (scanner *rwsetScanner) Close() {
	scanner.dbItr.Release()
}

func selfSimulated(pvtSimRes *ledger.EndorserPrivateSimulationResults) bool {
	return pvtSimRes.EndorserID == ""
}
