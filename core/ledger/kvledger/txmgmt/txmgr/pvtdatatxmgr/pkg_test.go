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

package pvtdatatxmgr

import (
	"testing"

	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/txmgr"
	"github.com/hyperledger/fabric/core/ledger/pvtrwstorage"
)

var (
	// TestEnvs lists all the txmgr environments (e.g. with leveldb and with couchdb) for testing
	TestEnvs = []*TestEnv{
		createEnv(&privacyenabledstate.LevelDBCommonStorageTestEnv{}),
		createEnv(&privacyenabledstate.CouchDBCommonStorageTestEnv{}),
	}
)

// TestEnv reprsents a test txmgr test environment for testing
type TestEnv struct {
	t         *testing.T
	Name      string
	LedgerID  string
	DBEnv     privacyenabledstate.TestEnv
	TStoreEnv *pvtrwstorage.TransientStoreEnv

	DB     privacyenabledstate.DB
	TStore pvtrwstorage.TransientStore
	Txmgr  txmgr.TxMgr
}

func createEnv(statedbEnv privacyenabledstate.TestEnv) *TestEnv {
	return &TestEnv{Name: statedbEnv.GetName(), DBEnv: statedbEnv}
}

// Init initializes the test environment
func (env *TestEnv) Init(t *testing.T, testLedgerID string) {
	env.t = t
	env.DBEnv.Init(t)
	env.DB = env.DBEnv.GetDBHandle(testLedgerID)
	env.TStoreEnv = pvtrwstorage.NewTestTransientStoreEnv(t)
	var err error
	env.TStore, err = env.TStoreEnv.TestTransientStoreProvider.OpenStore(testLedgerID)
	testutil.AssertNoError(t, err, "")
	env.Txmgr = NewLockbasedTxMgr(env.DB, env.TStore)
}

// Cleanup cleansup the test environment
func (env *TestEnv) Cleanup() {
	env.Txmgr.Shutdown()
	env.DBEnv.Cleanup()
	env.TStoreEnv.Cleanup()
}

type TestTx struct {
	ID          string
	PubSimBytes []byte
}
