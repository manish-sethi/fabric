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
	"os"
	"testing"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	flogging.SetModuleLevel("ledgerstorage", "debug")
	flogging.SetModuleLevel("pvtdatastorage", "debug")
	viper.Set("peer.fileSystemPath", "/tmp/fabric/core/ledger/ledgerstorage")
	os.Exit(m.Run())
}

func TestStore(t *testing.T) {
	testEnv := newTestEnv(t)
	defer testEnv.cleanup()
	provider := NewProvider()
	defer provider.Close()
	store, err := provider.Open("testLedger")
	defer store.Shutdown()

	assert.NoError(t, err)
	sampleData := sampleData(t)
	for _, sampleDatum := range sampleData {
		assert.NoError(t, store.CommitWithPvtData(sampleDatum))
	}

	// block 1 has no pvt data
	pvtdata, err := store.GetPvtDataByNum(1, nil)
	assert.NoError(t, err)
	assert.Nil(t, pvtdata)

	// block 4 has no pvt data
	pvtdata, err = store.GetPvtDataByNum(1, nil)
	assert.NoError(t, err)
	assert.Nil(t, pvtdata)

	// block 2 has pvt data for tx 3 and 5 only
	pvtdata, err = store.GetPvtDataByNum(2, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(pvtdata))
	assert.Equal(t, uint64(3), pvtdata[0].SeqInBlock)
	assert.Equal(t, uint64(5), pvtdata[1].SeqInBlock)

	// block 3 has pvt data for tx 4 and 6 only
	pvtdata, err = store.GetPvtDataByNum(3, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(pvtdata))
	assert.Equal(t, uint64(4), pvtdata[0].SeqInBlock)
	assert.Equal(t, uint64(6), pvtdata[1].SeqInBlock)
}

func sampleData(t *testing.T) []*ledger.PvtDataAndBlock {
	var pvtdataAndBlocks []*ledger.PvtDataAndBlock
	blocks := testutil.ConstructTestBlocks(t, 10)
	for i := 0; i < 10; i++ {
		pvtdataAndBlocks = append(pvtdataAndBlocks, &ledger.PvtDataAndBlock{Block: blocks[i]})
	}
	// txNum 3, 5 in block 2 has pvtdata
	pvtdataAndBlocks[2].BlockPvtData = samplePvtData(t, []uint64{3, 5})
	// txNum 4, 6 in block 3 has pvtdata
	pvtdataAndBlocks[3].BlockPvtData = samplePvtData(t, []uint64{4, 6})

	return pvtdataAndBlocks
}

func samplePvtData(t *testing.T, txNums []uint64) map[uint64]*ledger.TxPvtData {
	rwsetBuilder := rwsetutil.NewRWSetBuilder()
	rwsetBuilder.AddToPvtAndHashedWriteSet("ns-1", "coll-1", "key1", []byte("value1"))
	rwsetBuilder.AddToPvtAndHashedWriteSet("ns-1", "coll-2", "key2", []byte("value2"))
	simRes, err := rwsetBuilder.GetTxSimulationResults()
	assert.NoError(t, err)
	var pvtData []*ledger.TxPvtData
	for _, txNum := range txNums {
		pvtData = append(pvtData, &ledger.TxPvtData{SeqInBlock: txNum, WriteSet: simRes.PvtSimulationResults})
	}
	return constructPvtdataMap(pvtData)
}
