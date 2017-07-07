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
	"os"
	"testing"
	"time"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/statecouchdb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/util"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

const (
	testFilesystemPath = "/tmp/fabric/ledgertests/kvledger/txmgmt/privacyenabledstate"
)

type testEnv interface {
	init(t testing.TB)
	getDBHandle(id string) DB
	getName() string
	cleanup()
}

// Tests will be run against each environment in this array
// For example, to skip CouchDB tests, remove &couchDBLockBasedEnv{}
var testEnvs = []testEnv{&levelDBCommonStorageTestEnv{}, &couchDBCommonStorageTestEnv{}}

///////////// LevelDB Environment //////////////
type levelDBCommonStorageTestEnv struct {
	t        testing.TB
	provider DBProvider
}

func (env *levelDBCommonStorageTestEnv) init(t testing.TB) {
	viper.Set("peer.fileSystemPath", testFilesystemPath)
	removeDBPath(t)
	dbProvider, err := NewCommonStorageDBProvider()
	assert.NoError(t, err)
	env.t = t
	env.provider = dbProvider
}

func (env *levelDBCommonStorageTestEnv) getDBHandle(id string) DB {
	db, err := env.provider.GetDBHandle(id)
	assert.NoError(env.t, err)
	return db
}

func (env *levelDBCommonStorageTestEnv) getName() string {
	return "levelDBCommonStorageTestEnv"
}

func (env *levelDBCommonStorageTestEnv) cleanup() {
	env.provider.Close()
	removeDBPath(env.t)
}

///////////// CouchDB Environment //////////////
type couchDBCommonStorageTestEnv struct {
	t         testing.TB
	provider  DBProvider
	openDbIds map[string]bool
}

func (env *couchDBCommonStorageTestEnv) init(t testing.TB) {
	viper.Set("peer.fileSystemPath", testFilesystemPath)
	viper.Set("ledger.state.stateDatabase", "CouchDB")
	// both vagrant and CI have couchdb configured at host "couchdb"
	viper.Set("ledger.state.couchDBConfig.couchDBAddress", "couchdb:5984")
	// Replace with correct username/password such as
	// admin/admin if user security is enabled on couchdb.
	viper.Set("ledger.state.couchDBConfig.username", "")
	viper.Set("ledger.state.couchDBConfig.password", "")
	viper.Set("ledger.state.couchDBConfig.maxRetries", 3)
	viper.Set("ledger.state.couchDBConfig.maxRetriesOnStartup", 10)
	viper.Set("ledger.state.couchDBConfig.requestTimeout", time.Second*35)
	dbProvider, err := NewCommonStorageDBProvider()
	assert.NoError(t, err)
	env.t = t
	env.provider = dbProvider
	env.openDbIds = make(map[string]bool)
}

func (env *couchDBCommonStorageTestEnv) getDBHandle(id string) DB {
	db, err := env.provider.GetDBHandle(id)
	assert.NoError(env.t, err)
	env.openDbIds[id] = true
	return db
}

func (env *couchDBCommonStorageTestEnv) getName() string {
	return "couchDBCommonStorageTestEnv"
}

func (env *couchDBCommonStorageTestEnv) cleanup() {
	for id := range env.openDbIds {
		statecouchdb.CleanupDB(id)
	}
	env.provider.Close()
}

func removeDBPath(t testing.TB) {
	dbPath := ledgerconfig.GetStateLevelDBPath()
	if err := os.RemoveAll(dbPath); err != nil {
		t.Fatalf("Err: %s", err)
		t.FailNow()
	}
}

func testComputeStringHash(t *testing.T, str string) []byte {
	hash, err := util.ComputeStringHash(str)
	assert.NoError(t, err)
	return hash
}

func testComputeHash(t *testing.T, b []byte) []byte {
	hash, err := util.ComputeHash(b)
	assert.NoError(t, err)
	return hash
}

func populateBatches(t *testing.T, pvtDataBatch PvtDataBatch, hashesBatch PvtDataBatch, ns, coll, key string, value []byte, ver *version.Height) {
	pvtDataBatch.Put(ns, coll, key, value, ver)
	hashesBatch.Put(ns, coll, string(testComputeStringHash(t, key)), testComputeHash(t, value), ver)
}
