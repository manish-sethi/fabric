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
	"os"
	"testing"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	flogging.SetModuleLevel("pvtdatastorage", "debug")
	viper.Set("peer.fileSystemPath", "/tmp/fabric/core/ledger/pvtdatastorage")
	os.Exit(m.Run())
}

func TestEmptyStore(t *testing.T) {
	env := NewTestStoreEnv(t)
	defer env.Cleanup()
	assert := assert.New(t)
	store := env.TestStore
	testEmpty(true, assert, store)
	testPendingBatch(false, assert, store)
}

func TestStoreBasicCommitAndRetrieval(t *testing.T) {
	env := NewTestStoreEnv(t)
	defer env.Cleanup()
	assert := assert.New(t)
	store := env.TestStore
	testData := samplePvtData(t, []uint64{2, 4})

	assert.NoError(store.Prepare(0, testData))
	assert.NoError(store.Commit())

	assert.NoError(store.Prepare(1, testData))
	assert.NoError(store.Commit())

	assert.NoError(store.Prepare(2, testData))
	assert.NoError(store.Commit())

	retrievedData, err := store.GetPvtDataByBlockNum(1, nil)
	assert.NoError(err)
	assert.Equal(testData, retrievedData)

	filter := ledger.NewPvtNsCollFilter()
	filter.Add("ns-1", "coll-1")
	filter.Add("ns-2", "coll-2")
	retrievedData, err = store.GetPvtDataByBlockNum(1, filter)
	assert.Equal(1, len(retrievedData[0].WriteSet.NsPvtRwset[0].CollectionPvtRwset))
	assert.Equal(1, len(retrievedData[0].WriteSet.NsPvtRwset[1].CollectionPvtRwset))
	assert.True(retrievedData[0].Has("ns-1", "coll-1"))
	assert.True(retrievedData[0].Has("ns-2", "coll-2"))
}

func testEmpty(expectedEmpty bool, assert *assert.Assertions, store Store) {
	isEmpty, err := store.IsEmpty()
	assert.NoError(err)
	assert.Equal(expectedEmpty, isEmpty)
}

func testPendingBatch(expectedPending bool, assert *assert.Assertions, store Store) {
	hasPendingBatch, err := store.HasPendingBatch()
	assert.NoError(err)
	assert.Equal(expectedPending, hasPendingBatch)
}

func samplePvtData(t *testing.T, txNums []uint64) []*ledger.TxPvtData {
	rwsetBuilder := rwsetutil.NewRWSetBuilder()
	rwsetBuilder.AddToPvtAndHashedWriteSet("ns-1", "coll-1", "key1", []byte("value1"))
	rwsetBuilder.AddToPvtAndHashedWriteSet("ns-1", "coll-2", "key2", []byte("value2"))
	rwsetBuilder.AddToPvtAndHashedWriteSet("ns-2", "coll-1", "key3", []byte("value3"))
	rwsetBuilder.AddToPvtAndHashedWriteSet("ns-2", "coll-2", "key4", []byte("value4"))
	simRes, err := rwsetBuilder.GetTxSimulationResults()
	assert.NoError(t, err)
	var pvtData []*ledger.TxPvtData
	for _, txNum := range txNums {
		pvtData = append(pvtData, &ledger.TxPvtData{SeqInBlock: txNum, WriteSet: simRes.PvtSimulationResults})
	}
	return pvtData
}
