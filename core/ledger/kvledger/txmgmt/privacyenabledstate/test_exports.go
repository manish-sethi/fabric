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
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

type TestEnv interface {
	Init(t testing.TB)
	GetDBHandle(id string) DB
	GetName() string
	Cleanup()
}

// Tests will be run against each environment in this array
// For example, to skip CouchDB tests, remove &couchDBLockBasedEnv{}
//var testEnvs = []testEnv{&levelDBCommonStorageTestEnv{}, &couchDBCommonStorageTestEnv{}}
var testEnvs = []TestEnv{&LevelDBCommonStorageTestEnv{}, &CouchDBCommonStorageTestEnv{}}

///////////// LevelDB Environment //////////////
type LevelDBCommonStorageTestEnv struct {
	t        testing.TB
	provider DBProvider
}

func (env *LevelDBCommonStorageTestEnv) Init(t testing.TB) {
	viper.Set("ledger.state.stateDatabase", "")
	removeDBPath(t)
	dbProvider, err := NewCommonStorageDBProvider()
	assert.NoError(t, err)
	env.t = t
	env.provider = dbProvider
}

func (env *LevelDBCommonStorageTestEnv) GetDBHandle(id string) DB {
	db, err := env.provider.GetDBHandle(id)
	assert.NoError(env.t, err)
	return db
}

func (env *LevelDBCommonStorageTestEnv) GetName() string {
	return "levelDBCommonStorageTestEnv"
}

func (env *LevelDBCommonStorageTestEnv) Cleanup() {
	env.provider.Close()
	removeDBPath(env.t)
}

///////////// CouchDB Environment //////////////
type CouchDBCommonStorageTestEnv struct {
	t         testing.TB
	provider  DBProvider
	openDbIds map[string]bool
}

func (env *CouchDBCommonStorageTestEnv) Init(t testing.TB) {
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

func (env *CouchDBCommonStorageTestEnv) GetDBHandle(id string) DB {
	db, err := env.provider.GetDBHandle(id)
	assert.NoError(env.t, err)
	env.openDbIds[id] = true
	return db
}

func (env *CouchDBCommonStorageTestEnv) GetName() string {
	return "couchDBCommonStorageTestEnv"
}

func (env *CouchDBCommonStorageTestEnv) Cleanup() {
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
