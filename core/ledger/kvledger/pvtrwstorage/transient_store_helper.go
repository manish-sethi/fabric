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
	"bytes"

	"github.com/hyperledger/fabric/common/ledger/util"
)

var compositeKeySep = []byte{0x00}

// createCompositeKeyForPRWSet creates a key for storing private read-write set
// in the transient store. The structure of the key is namespace~transactionId~endorserId.
func createCompositeKeyForPRWSet(ns string, txId string, endorserId string, endorsementBlkHt uint64) []byte {
	var compositeKey []byte

	compositeKey = append(compositeKey, []byte(ns)...)
	compositeKey = append(compositeKey, compositeKeySep...)
	compositeKey = append(compositeKey, []byte(txId)...)
	compositeKey = append(compositeKey, compositeKeySep...)
	compositeKey = append(compositeKey, []byte(endorserId)...)
	compositeKey = append(compositeKey, compositeKeySep...)
	compositeKey = append(compositeKey, util.EncodeOrderPreservingVarUint64(endorsementBlkHt)...)

	return compositeKey
}

// createCompositeKeyForPurgeIndex creates a key to index private read-write set based on
// endorsement block height such that purge based on block height can be achieved. The structure
// of the key is namespace~endorsementBlkHt~txId~endorserId.
func createCompositeKeyForPurgeIndex(ns string, endorsementBlkHt uint64, txId string, endorserId string) []byte {
	var compositeKey []byte

	compositeKey = append(compositeKey, []byte(ns)...)
	compositeKey = append(compositeKey, compositeKeySep...)
	compositeKey = append(compositeKey, util.EncodeOrderPreservingVarUint64(endorsementBlkHt)...)
	compositeKey = append(compositeKey, compositeKeySep...)
	compositeKey = append(compositeKey, []byte(txId)...)
	compositeKey = append(compositeKey, compositeKeySep...)
	compositeKey = append(compositeKey, []byte(endorserId)...)

	return compositeKey
}

// splitCompositeKeyOfPRWSet splits the compositeKey into endorserId and endorsementBlkHt.
func splitCompositeKeyOfPRWSet(compositeKey []byte) (endorserId string, endorsementBlkHt uint64) {
	splits := bytes.Split(compositeKey, compositeKeySep)
	// splits[0] is the namesapce & splits[1] is the txId: Ignored as they are not required currently.
	endorserId = string(splits[2])
	endorsementBlkHt, _ = util.DecodeOrderPreservingVarUint64(splits[3])
	return endorserId, endorsementBlkHt
}

// splitCompositeKeyOfPurgeIndex splits the compositeKey into txId, endorserId and endorsementBlkHt.
func splitCompositeKeyOfPurgeIndex(compositeKey []byte) (txId string, endorserId string, endorsementBlkHt uint64) {
	splits := bytes.Split(compositeKey, compositeKeySep)
	// splits[0] is the namesapce: Ignored as it is not required currently.
	endorsementBlkHt, _ = util.DecodeOrderPreservingVarUint64(splits[1])
	txId = string(splits[2])
	endorserId = string(splits[3])
	return txId, endorserId, endorsementBlkHt
}

// createTxIdRangeStartKey returns a startKey to do a range query on transient store using txId
func createTxIdRangeStartKey(prefix string, txId string) []byte {
	var startKey []byte
	startKey = append(startKey, []byte(prefix)...)
	startKey = append(startKey, compositeKeySep...)
	startKey = append(startKey, []byte(txId)...)
	startKey = append(startKey, compositeKeySep...)
	return startKey
}

// createTxIdRangeStartKey returns a endKey to do a range query on transient store using txId
func createTxIdRangeEndKey(prefix string, txId string) []byte {
	var endKey []byte
	endKey = append(endKey, []byte(prefix)...)
	endKey = append(endKey, compositeKeySep...)
	endKey = append(endKey, []byte(txId)...)
	endKey = append(endKey, []byte{0xff}...)
	return endKey
}

// createEndorsementBlkHtRangeStartKey returns a startKey to do a range query on index stored in transient store
// using endorsementBlkHt
func createEndorsementBlkHtRangeStartKey(prefix string, endorsementBlkHt uint64) []byte {
	var startKey []byte
	startKey = append(startKey, []byte(prefix)...)
	startKey = append(startKey, compositeKeySep...)
	startKey = append(startKey, util.EncodeOrderPreservingVarUint64(endorsementBlkHt)...)
	return startKey
}

// createEndorsementBlkHtRangeStartKey returns a endKey to do a range query on index stored in transient store
// using endorsementBlkHt
func createEndorsementBlkHtRangeEndKey(prefix string, endorsementBlkHt uint64) []byte {
	var endKey []byte
	endKey = append(endKey, []byte(prefix)...)
	endKey = append(endKey, compositeKeySep...)
	endKey = append(endKey, util.EncodeOrderPreservingVarUint64(endorsementBlkHt)...)
	endKey = append(endKey, []byte{0xff}...)
	return endKey
}
