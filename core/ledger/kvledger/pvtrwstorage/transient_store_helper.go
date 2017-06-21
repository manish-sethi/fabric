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

type count uint8

const (
	NUM_ATTRIBUTES_IN_PRWSET_KEY      count = 4 //namespace~txId~endorserId~endorsementBlkHt
	NUM_ATTRIBUTES_IN_PURGE_INDEX_KEY count = 4 //namespace~endorsementBlkHt~txId~endorserId
)

var compositeKeySep = []byte{0x00}

// CreateCompositeKeyForPRWSet creates a key for storing private read-write set
// in the transient store. The structure of the key is namespace~transactionId~endorserId.
func CreateCompositeKeyForPRWSet(ns string, txId string, endorserId string, endorsementBlkHt uint64) []byte {
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

// CreateCompositeKeyForPurgeIndex creates a key to index private read-write set based on
// endorsement block height such that purge based on block height can be achieved. The structure
// of the key is namespace~endorsementBlkHt~txId~endorserId.
func CreateCompositeKeyForPurgeIndex(ns string, endorsementBlkHt uint64, txId string, endorserId string) []byte {
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

// SplitCompositeKey splits the compositeKey into splitCount number of attributes.
func SplitCompositeKeyOfPRWSet(compositeKey []byte) (endorserId string, endorsementBlkHt uint64) {
	splits := bytes.Split(compositeKey, compositeKeySep)
	// splits[0] is the namesapce & splits[1] is the txId: Ignored as they are not required currently.
	endorserId = string(splits[2])
	endorsementBlkHt, _ = util.DecodeOrderPreservingVarUint64(splits[3])
	return endorserId, endorsementBlkHt
}

func SplitCompositeKeyOfPurgeIndex(compositeKey []byte) (txId string, endorserId string, endorsementBlkHt uint64) {
	splits := bytes.Split(compositeKey, compositeKeySep)
	// splits[0] is the namesapce: Ignored as it is not required currently.
	endorsementBlkHt, _ = util.DecodeOrderPreservingVarUint64(splits[1])
	txId = string(splits[2])
	endorserId = string(splits[3])
	return txId, endorserId, endorsementBlkHt
}

func CreatePartialCompositeKeyForPRWSet(ns string, txId string, endKey bool) []byte {
	var partialCompositeKey []byte

	partialCompositeKey = append(partialCompositeKey, []byte(ns)...)
	partialCompositeKey = append(partialCompositeKey, compositeKeySep...)
	partialCompositeKey = append(partialCompositeKey, []byte(txId)...)

	if endKey {
		partialCompositeKey = append(partialCompositeKey, []byte{0xff}...)
	}
	return partialCompositeKey
}

func CreatePartialCompositeKeyForPurgeIndex(ns string, endorsementBlkHt uint64, endKey bool) []byte {
	var partialCompositeKey []byte

	partialCompositeKey = append(partialCompositeKey, []byte(ns)...)
	partialCompositeKey = append(partialCompositeKey, compositeKeySep...)
	partialCompositeKey = append(partialCompositeKey, util.EncodeOrderPreservingVarUint64(endorsementBlkHt)...)

	if endKey {
		partialCompositeKey = append(partialCompositeKey, []byte{0xff}...)
	}
	return partialCompositeKey

}
