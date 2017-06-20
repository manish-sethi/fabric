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
	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

const (
	PRWSET_NS     string = "P" // namespace for storing private read-write set in transient store.
	PURGEINDEX_NS string = "I" // namespace for storing index on private read-write set using endorsement block height.
)

var emptyValue = []byte{}

// TransientStoreProvider encapsulates a leveldb provider which is used to store
// private read-write set of simulated transactions, and implements TransientStoreProvider
// interface.
type transientStoreProvider struct {
	dbProvider *leveldbhelper.Provider
}

// TransientStoreProvider provides an instance of a TransientStore
type TransientStoreProvider interface {
	OpenStore(dbName string) (TransientStore, error)
	Close()
}

// TransientStore holds an instance of a levelDB.
type transientStore struct {
	db     *leveldbhelper.DBHandle
	dbName string
}

type rwsetScanner struct {
	txId  string
	dbItr iterator.Iterator
}

// TransientStore manages the storage of private read-write sets for a namespace for sometime.
// Ideally, a ledger can remove the data from this storage when it is committed to the permanent storage or
// the pruning of some data items is enforced by the policy
type TransientStore interface {
	Persist(txid string, endorserid string, endorsementBlkHt uint64, privateSimulationResults []byte) error
	GetTxPrivateRWSetByTxId(txId string) (commonledger.ResultsIterator, error)
	Purge(maxBlockNumToRetain uint64) error
	GetMinEndorsementBlkHt() (uint64, error)
	Shutdown()
}

// NewTransientStoreProvider instantiates TransientStoreProvider
func NewTransientStoreProvider() TransientStoreProvider {
	dbPath := ledgerconfig.GetTransientStorePath()
	dbProvider := leveldbhelper.NewProvider(&leveldbhelper.Conf{DBPath: dbPath})
	return &transientStoreProvider{dbProvider: dbProvider}
}

// OpenStore returns a handle to a name database in TransientStore
func (provider *transientStoreProvider) OpenStore(dbName string) (TransientStore, error) {
	dbHandle := provider.dbProvider.GetDBHandle(dbName)
	return &transientStore{db: dbHandle, dbName: dbName}, nil
}

// Close closes the TransientStoreProvider
func (provider *transientStoreProvider) Close() {
	provider.dbProvider.Close()
}

// Persist stores the private read-write set of a transaction in the transientStore
func (store *transientStore) Persist(txId string, endorserId string,
	endorsementBlkHt uint64, privateSimulationResults []byte) error {

	// Create compositeKey with the namespace of private read-write set, txId and endorserId
	compositeKey := CreateCompositeKeyForPRWSet(PRWSET_NS, txId, endorserId, endorsementBlkHt)
	if err := store.db.Put(compositeKey, privateSimulationResults, true); err != nil {
		return err
	}

	// Create compositeKey with endorsementBlkHt, txId, endorserId & Store the compositeKey (purge index)
	// in a separate namespace with a null byte as value.
	compositeKey = CreateCompositeKeyForPurgeIndex(PURGEINDEX_NS, endorsementBlkHt, txId, endorserId)
	if err := store.db.Put(compositeKey, emptyValue, true); err != nil {
		return err
	}
	return nil
	// TODO: Node failure can result in inconsistent state between private read-write set namespace
	// and purge index. Need to recover from failure.
}

// GetTxPrivateRWSetbyTxId returns private read-write set of a given transaction
func (store *transientStore) GetTxPrivateRWSetByTxId(txId string) (commonledger.ResultsIterator, error) {
	startKey := CreatePartialCompositeKeyForPRWSet(PRWSET_NS, txId, false)
	endKey := CreatePartialCompositeKeyForPRWSet(PRWSET_NS, txId, true)
	iter := store.db.GetIterator(startKey, endKey)
	return &rwsetScanner{txId, iter}, nil
}

// Purge removes private read-writes set generated by endorsers at block height lesser than
// a given maxBlockNumToRetain. In other words, Purge only retains private read-write sets
// that were generated at block height of maxBlockNumToRetain or higher.
func (store *transientStore) Purge(maxBlockNumToRetain uint64) error {
	// Do a range query with 0 as startKey and maxBlockNumToRetain
	startKey := CreatePartialCompositeKeyForPurgeIndex(PURGEINDEX_NS, 0, false)
	endKey := CreatePartialCompositeKeyForPurgeIndex(PURGEINDEX_NS, maxBlockNumToRetain, true)
	iter := store.db.GetIterator(startKey, endKey)

	// get all txId and endorserId from above result and remove it from the db
	for iter.Next() {
		dbKey := iter.Key()
		txId, endorserId, endorsementBlkHt := SplitCompositeKeyOfPurgeIndex(dbKey)
		compositeKey := CreateCompositeKeyForPRWSet(PRWSET_NS, txId, endorserId, endorsementBlkHt)
		if err := store.db.Delete(compositeKey, true); err != nil {
			return err
		}
		if err := store.db.Delete(dbKey, true); err != nil {
			return err
		}
		// TODO: Node failure can result in inconsistent state between private read-write set namespace
		// and purge index. Need to recover from failure.
	}
	return nil
}

// GetMinEndorsementBlkHt returns the lowest retained endorsement block height
func (store *transientStore) GetMinEndorsementBlkHt() (uint64, error) {
	// either maintain the minEndorsementBlkHt separately in the DB or
	// do a range query on the compositeKey namespace (endorsement
	return 0, nil
}

func (store *transientStore) Shutdown() {
	// do nothing because shared db is used
}

func (scanner *rwsetScanner) Next() (commonledger.QueryResult, error) {
	if !scanner.dbItr.Next() {
		return nil, nil
	}
	dbKey := scanner.dbItr.Key()
	dbVal := scanner.dbItr.Value()
	endorserId, endorsementBlkHt := SplitCompositeKeyOfPRWSet(dbKey)
	return &ledger.EndorserPrivateSimulationResults{
		EndorserId:               endorserId,
		EndorsementBlockHeight:   endorsementBlkHt,
		PrivateSimulationResults: dbVal,
	}, nil
}

func (scanner *rwsetScanner) Close() {
	scanner.dbItr.Release()
}
