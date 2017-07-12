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
	"fmt"
	"os"
	"testing"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/util"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	viper.Set("peer.fileSystemPath", "/tmp/fabric/ledgertests/kvledger/txmgmt/privacyenabledstate")
	os.Exit(m.Run())
}

func TestPvtDataBatch(t *testing.T) {
	batch := NewPvtDataBatch()
	v := version.NewHeight(1, 1)
	for i := 0; i < 5; i++ {
		for j := 0; j < 5; j++ {
			for k := 0; k < 5; k++ {
				batch.Put(fmt.Sprintf("ns-%d", i), fmt.Sprintf("collection-%d", j), fmt.Sprintf("key-%d", k),
					[]byte(fmt.Sprintf("value-%d-%d-%d", i, j, k)), v)
			}
		}
	}
	for i := 0; i < 5; i++ {
		for j := 0; j < 5; j++ {
			for k := 0; k < 5; k++ {
				vv := batch.Get(fmt.Sprintf("ns-%d", i), fmt.Sprintf("collection-%d", j), fmt.Sprintf("key-%d", k))
				assert.NotNil(t, vv)
				assert.Equal(t,
					&statedb.VersionedValue{Value: []byte(fmt.Sprintf("value-%d-%d-%d", i, j, k)), Version: v},
					vv)
			}
		}
	}
	assert.Nil(t, batch.Get("ns-1", "collection-1", "key-5"))
	assert.Nil(t, batch.Get("ns-1", "collection-5", "key-1"))
	assert.Nil(t, batch.Get("ns-5", "collection-1", "key-1"))
}

func TestDB(t *testing.T) {
	for _, env := range testEnvs {
		t.Run(env.GetName(), func(t *testing.T) {
			testDB(t, env)
		})
	}
}

func testDB(t *testing.T, env TestEnv) {
	env.Init(t)
	defer env.Cleanup()
	db := env.GetDBHandle("test-ledger-id")

	pubDataBatch := statedb.NewUpdateBatch()
	pvtDataBatch := NewPvtDataBatch()
	hashesBatch := NewPvtDataBatch()

	pubDataBatch.Put("ns1", "key1", []byte("value1"), version.NewHeight(1, 1))
	pubDataBatch.Put("ns1", "key2", []byte("value2"), version.NewHeight(1, 2))
	pubDataBatch.Put("ns2", "key3", []byte("value3"), version.NewHeight(1, 3))

	putToBatches(t, pvtDataBatch, hashesBatch, "ns1", "coll1", "key1", []byte("pvt_value1"), version.NewHeight(1, 4))
	putToBatches(t, pvtDataBatch, hashesBatch, "ns1", "coll1", "key2", []byte("pvt_value2"), version.NewHeight(1, 5))
	putToBatches(t, pvtDataBatch, hashesBatch, "ns2", "coll1", "key3", []byte("pvt_value3"), version.NewHeight(1, 6))
	db.ApplyPubPvtAndHashUpdates(pubDataBatch, pvtDataBatch, hashesBatch, version.NewHeight(2, 6))

	vv, err := db.GetState("ns1", "key1")
	assert.NoError(t, err)
	assert.Equal(t, &statedb.VersionedValue{Value: []byte("value1"), Version: version.NewHeight(1, 1)}, vv)

	vv, err = db.GetPrivateData("ns1", "coll1", "key1")
	assert.NoError(t, err)
	assert.Equal(t, &statedb.VersionedValue{Value: []byte("pvt_value1"), Version: version.NewHeight(1, 4)}, vv)

	vv, err = db.GetValueHash("ns1", "coll1", testComputeStringHash(t, "key1"))
	assert.NoError(t, err)
	assert.Equal(t, &statedb.VersionedValue{Value: testComputeHash(t, []byte("pvt_value1")), Version: version.NewHeight(1, 4)}, vv)

	pubDataBatch = statedb.NewUpdateBatch()
	pvtDataBatch = NewPvtDataBatch()
	hashesBatch = NewPvtDataBatch()
	pubDataBatch.Delete("ns1", "key1", version.NewHeight(2, 7))
	deleteToBatches(t, pvtDataBatch, hashesBatch, "ns1", "coll1", "key1", version.NewHeight(2, 7))
	db.ApplyPubPvtAndHashUpdates(pubDataBatch, pvtDataBatch, hashesBatch, version.NewHeight(2, 7))

	vv, err = db.GetState("ns1", "key1")
	assert.NoError(t, err)
	assert.Nil(t, vv)

	vv, err = db.GetPrivateData("ns1", "coll1", "key1")
	assert.NoError(t, err)
	assert.Nil(t, vv)

	vv, err = db.GetValueHash("ns1", "coll1", testComputeStringHash(t, "key1"))
	assert.NoError(t, err)
	assert.Nil(t, vv)

	//TODO add tests for functions GetPrivateStateMultipleKeys and GetPrivateStateRangeScanIterator
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

func putToBatches(t *testing.T, pvtDataBatch PvtDataBatch, hashesBatch PvtDataBatch, ns, coll, key string, value []byte, ver *version.Height) {
	pvtDataBatch.Put(ns, coll, key, value, ver)
	hashesBatch.Put(ns, coll, string(testComputeStringHash(t, key)), testComputeHash(t, value), ver)
}

func deleteToBatches(t *testing.T, pvtDataBatch PvtDataBatch, hashesBatch PvtDataBatch, ns, coll, key string, ver *version.Height) {
	pvtDataBatch.Delete(ns, coll, key, ver)
	hashesBatch.Delete(ns, coll, string(testComputeStringHash(t, key)), ver)
}
