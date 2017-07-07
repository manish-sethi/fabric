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
	"encoding/base64"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/statecouchdb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/stateleveldb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
)

const (
	nsJoiner       = "/"
	pvtDataPrefix  = "p"
	hashDataPrefix = "h"
)

// CommonStorageDBProvider implements interface DBProvider
type CommonStorageDBProvider struct {
	statedb.VersionedDBProvider
}

// NewCommonStorageDBProvider constructs an instance of DBProvider
func NewCommonStorageDBProvider() (DBProvider, error) {
	var vdbProvider statedb.VersionedDBProvider
	var err error
	if ledgerconfig.IsCouchDBEnabled() {
		if vdbProvider, err = statecouchdb.NewVersionedDBProvider(); err != nil {
			return nil, err
		}
	} else {
		vdbProvider = stateleveldb.NewVersionedDBProvider()
	}
	return &CommonStorageDBProvider{vdbProvider}, nil
}

// GetDBHandle implements function from interface DBProvider
func (p *CommonStorageDBProvider) GetDBHandle(id string) (DB, error) {
	vdb, err := p.VersionedDBProvider.GetDBHandle(id)
	if err != nil {
		return nil, err
	}
	return NewCommonStorageDB(vdb, id)
}

// Close implements function from interface DBProvider
func (p *CommonStorageDBProvider) Close() {
	p.VersionedDBProvider.Close()
}

// CommonStorageDB implements interface DB. This implementation uses a single database to maintain
// both the public and private data
type CommonStorageDB struct {
	statedb.VersionedDB
}

// NewCommonStorageDB wraps a VersionedDB instance. The public data is managed directly by the wrapped versionedDB.
// For managing the hashed data and private data, this implementation creates separate namespaces in the wrapped db
func NewCommonStorageDB(vdb statedb.VersionedDB, ledgerid string) (DB, error) {
	return &CommonStorageDB{VersionedDB: vdb}, nil
}

// GetPrivateData implements corresponding function in interface PrivacyAwareVersionedDB
func (s *CommonStorageDB) GetPrivateData(namespace, collection, key string) (*statedb.VersionedValue, error) {
	return s.GetState(derivePvtDataNs(namespace, collection), key)
}

// GetValueHash implements corresponding function in interface PrivacyAwareVersionedDB
func (s *CommonStorageDB) GetValueHash(namespace, collection string, keyHash []byte) (*statedb.VersionedValue, error) {
	keyHashStr := string(keyHash)
	if !s.BytesKeySuppoted() {
		keyHashStr = base64.StdEncoding.EncodeToString(keyHash)
	}
	return s.GetState(deriveHashedDataNs(namespace, collection), keyHashStr)
}

// GetPrivateDataMultipleKeys implements corresponding function in interface PrivacyAwareVersionedDB
func (s *CommonStorageDB) GetPrivateDataMultipleKeys(namespace, collection string, keys []string) ([]*statedb.VersionedValue, error) {
	return s.GetStateMultipleKeys(derivePvtDataNs(namespace, collection), keys)
}

// GetPrivateDataRangeScanIterator implements corresponding function in interface PrivacyAwareVersionedDB
func (s *CommonStorageDB) GetPrivateDataRangeScanIterator(namespace, collection, startKey, endKey string) (statedb.ResultsIterator, error) {
	return s.GetStateRangeScanIterator(derivePvtDataNs(namespace, collection), startKey, endKey)
}

// ExecuteQueryOnPrivateData implements corresponding function in interface PrivacyAwareVersionedDB
func (s CommonStorageDB) ExecuteQueryOnPrivateData(namespace, collection, query string) (statedb.ResultsIterator, error) {
	return s.ExecuteQuery(derivePvtDataNs(namespace, collection), query)
}

// ApplyUpdates overrides the funciton in statedb.VersionedDB and throws appropriate message
// TODO uncomment the following when integration of new code is done
// func (s *CommonStorageDB) ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error {
// 	return fmt.Errorf("This function should not be invoked. Please invoke function 'ApplyPubPvtAndHashUpdates'")
// }

// ApplyPubPvtAndHashUpdates implements corresponding function in interface PrivacyAwareVersionedDB
func (s *CommonStorageDB) ApplyPubPvtAndHashUpdates(
	pubDataBatch *statedb.UpdateBatch, pvtDataBatch PvtDataBatch, hashedDataBatch PvtDataBatch, height *version.Height) error {
	updateBatchWithPvtData(pubDataBatch, pvtDataBatch)
	updateBatchWithHashedData(pubDataBatch, hashedDataBatch, !s.BytesKeySuppoted())
	return s.ApplyUpdates(pubDataBatch, height)
}

func derivePvtDataNs(namespace, collection string) string {
	return namespace + nsJoiner + pvtDataPrefix + collection
}

func deriveHashedDataNs(namespace, collection string) string {
	return namespace + nsJoiner + hashDataPrefix + collection
}

func updateBatchWithPvtData(masterBatch *statedb.UpdateBatch, pvtDataBatch PvtDataBatch) {
	for ns, nsBatch := range pvtDataBatch {
		for _, coll := range nsBatch.GetCollectionNames() {
			for key, vv := range nsBatch.GetUpdates(coll) {
				masterBatch.Update(derivePvtDataNs(ns, coll), key, vv)
			}
		}
	}
}

func updateBatchWithHashedData(masterBatch *statedb.UpdateBatch, pvtDataBatch PvtDataBatch, base64Key bool) {
	for ns, nsBatch := range pvtDataBatch {
		for _, coll := range nsBatch.GetCollectionNames() {
			for key, vv := range nsBatch.GetUpdates(coll) {
				if base64Key {
					key = base64.StdEncoding.EncodeToString([]byte(key))
				}
				masterBatch.Update(deriveHashedDataNs(ns, coll), key, vv)
			}
		}
	}
}
