/*
Copyright IBM Corp. 2016 All Rights Reserved.

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

package privacyenabledstate

import (
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
)

// DBProvider provides handle to a PvtVersionedDB
type DBProvider interface {
	// GetDBHandle returns a handle to a PvtVersionedDB
	GetDBHandle(id string) (DB, error)
	// Close closes all the PvtVersionedDB instances and releases any resources held by VersionedDBProvider
	Close()
}

// DB extends VersionedDB interface. This interface provides additional functions for managing private data state
type DB interface {
	statedb.VersionedDB
	GetPrivateState(namespace, collection, key string) (*statedb.VersionedValue, error)
	GetValueHash(namespace, collection string, keyHash []byte) (*statedb.VersionedValue, error)
	GetPrivateStateMultipleKeys(namespace, collection string, keys []string) ([]*statedb.VersionedValue, error)
	GetPrivateStateRangeScanIterator(namespace, collection, startKey, endKey string) (statedb.ResultsIterator, error)
	ApplyPubPvtAndHashUpdates(pubDataBatch *statedb.UpdateBatch, pvtDataBatch PvtDataBatch, hashedDataBatch PvtDataBatch, height *version.Height) error
}

// PvtDataBatch contains either pvt data or hashes of the public data
type PvtDataBatch map[string]nsBatch

type nsBatch struct {
	*statedb.UpdateBatch
}

// NewPvtDataBatch creates an empty PvtDataBatch
func NewPvtDataBatch() PvtDataBatch {
	return make(map[string]nsBatch)
}

// Put sets the value in the batch for a given combination of namespace and collection name
func (b PvtDataBatch) Put(ns, coll, key string, value []byte, version *version.Height) {
	b.getOrCreateNsBatch(ns).Put(coll, key, value, version)
}

// Delete removes the entry from the batch for a given combination of namespace and collection name
func (b PvtDataBatch) Delete(ns, coll, key string, version *version.Height) {
	b.getOrCreateNsBatch(ns).Delete(coll, key, version)
}

// Get retrieves the value from the bacth for a given combination of namespace and collection name
func (b PvtDataBatch) Get(ns, coll, key string) *statedb.VersionedValue {
	nsPvtBatch, ok := b[ns]
	if !ok {
		return nil
	}
	return nsPvtBatch.Get(coll, key)
}

func (nsb nsBatch) GetCollectionNames() []string {
	return nsb.GetUpdatedNamespaces()
}

func (b PvtDataBatch) getOrCreateNsBatch(ns string) nsBatch {
	batch, ok := b[ns]
	if !ok {
		batch = nsBatch{statedb.NewUpdateBatch()}
		b[ns] = batch
	}
	return batch
}
