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

package transienthandlertxmgr

import (
	"os"
	"testing"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/pvtrwstorage"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

var (
	dbTestEnvs = []privacyenabledstate.TestEnv{
		&privacyenabledstate.LevelDBCommonStorageTestEnv{},
		&privacyenabledstate.CouchDBCommonStorageTestEnv{},
	}
)

func TestMain(m *testing.M) {
	flogging.SetModuleLevel("transienthandlertxmgr", "debug")
	viper.Set("peer.fileSystemPath", "/tmp/fabric/ledgertests/kvledger/txmgmt/txmgr/transienthandlertxmgr")
	os.Exit(m.Run())
}

func TestTransientHandlerTxmgr(t *testing.T) {
	for _, dbEnv := range dbTestEnvs {
		dbEnv.Init(t)
		defer dbEnv.Cleanup()
		tStoreTestEnv := pvtrwstorage.NewTestTransientStoreEnv(t)
		defer tStoreTestEnv.Cleanup()
		testTransientHandlerTxmgr(t, dbEnv.GetName(), dbEnv.GetDBHandle("test"), tStoreTestEnv.TestTransientStore)
	}
}

func testTransientHandlerTxmgr(t *testing.T, testcase string, db privacyenabledstate.DB, tStore pvtrwstorage.TransientStore) {
	t.Run(testcase, func(t *testing.T) {
		// initially the transient store is empty
		txid := "test-tx-id"
		initialEntries := retrieveTestEntriesFromTStore(t, tStore, txid)
		t.Logf("len(initialEntries)=%d", len(initialEntries))
		assert.Nil(t, initialEntries)
		txmgr := NewLockbasedTxMgr(db, tStore)

		// run a simulation with only public data and the transient store should be empty at the end
		sim1, err := txmgr.NewTxSimulator(txid)
		assert.NoError(t, err)
		sim1.GetState("ns1", "key1")
		sim1.SetState("ns1", "key1", []byte("value1"))
		_, err = sim1.GetTxSimulationResults()
		assert.NoError(t, err)
		entriesAfterPubSimulation := retrieveTestEntriesFromTStore(t, tStore, txid)
		assert.Nil(t, entriesAfterPubSimulation)

		// run a read-only private simulation and the transient store should be empty at the end
		sim2, err := txmgr.NewTxSimulator(txid)
		assert.NoError(t, err)
		sim2.GetState("ns1", "key1")
		sim2.SetState("ns1", "key1", []byte("value1"))
		sim2.GetPrivateData("ns1", "key1", "coll1")
		_, err = sim2.GetTxSimulationResults()
		assert.NoError(t, err)
		entriesAfterReadOnlyPvtSimulation := retrieveTestEntriesFromTStore(t, tStore, txid)
		assert.Nil(t, entriesAfterReadOnlyPvtSimulation)

		// run a private simulation that inlovles writes and the transient store should have a corresponding entry at the end
		sim3, err := txmgr.NewTxSimulator(txid)
		assert.NoError(t, err)
		sim3.GetState("ns1", "key1")
		sim3.SetState("ns1", "key1", []byte("value1"))
		sim3.GetPrivateData("ns1", "key1", "coll1")
		sim3.SetPrivateData("ns1", "key1", "coll1", []byte("value1"))
		sim3Res, err := sim3.GetTxSimulationResults()
		assert.NoError(t, err)
		sim3ResBytes, err := sim3Res.GetPvtSimulationBytes()
		assert.NoError(t, err)
		entriesAfterWritePvtSimulation := retrieveTestEntriesFromTStore(t, tStore, txid)
		assert.Equal(t, 1, len(entriesAfterWritePvtSimulation))
		assert.Equal(t, sim3ResBytes, entriesAfterWritePvtSimulation[0].PrivateSimulationResults)
	})
}

func retrieveTestEntriesFromTStore(t *testing.T, tStore pvtrwstorage.TransientStore, txid string) []*ledger.EndorserPrivateSimulationResults {
	itr, err := tStore.GetTxPrivateRWSetByTxid(txid)
	assert.NoError(t, err)
	var results []*ledger.EndorserPrivateSimulationResults
	for {
		result, err := itr.Next()
		assert.NoError(t, err)
		if result == nil {
			break
		}
		results = append(results, result.(*ledger.EndorserPrivateSimulationResults))
	}
	return results
}
