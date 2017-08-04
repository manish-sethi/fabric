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
	"math"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
)

var (
	pendingCommitKey    = []byte{'b'}
	lastCommittedBlkkey = []byte{'s'}
	pvtDataKeyPrefix    = []byte{('p')}

	emptyValue = []byte{}
)

func encodePK(blockNum uint64, tranNum uint64) blkTranNumKey {
	return append(pvtDataKeyPrefix, version.NewHeight(blockNum, tranNum).ToBytes()...)
}

func decodePK(key blkTranNumKey) (blockNum uint64, tranNum uint64) {
	height, _ := version.NewHeightFromBytes(key[1:])
	return height.BlockNum, height.TxNum
}

func getKeysForRangeScanByBlockNum(blockNum uint64) (startKey []byte, endKey []byte) {
	startKey = encodePK(blockNum, 0)
	endKey = encodePK(blockNum, math.MaxUint64)
	return
}

func encodePvtRwSet(txPvtRwSet *rwset.TxPvtReadWriteSet) ([]byte, error) {
	return proto.Marshal(txPvtRwSet)
}

func decodePvtRwSet(encodedBytes []byte) (*rwset.TxPvtReadWriteSet, error) {
	writeset := &rwset.TxPvtReadWriteSet{}
	return writeset, proto.Unmarshal(encodedBytes, writeset)
}

func encodeBlockNum(blockNum uint64) []byte {
	return proto.EncodeVarint(blockNum)
}

func decodeBlockNum(blockNumBytes []byte) uint64 {
	s, _ := proto.DecodeVarint(blockNumBytes)
	return s
}
