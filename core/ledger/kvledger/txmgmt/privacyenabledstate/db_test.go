package privacyenabledstate

import (
	"testing"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/stretchr/testify/assert"
)

func TestDB(t *testing.T) {
	for _, env := range testEnvs {
		t.Run(env.getName(), func(t *testing.T) {
			testDB(t, env)
		})
	}
}

func testDB(t *testing.T, env testEnv) {
	env.init(t)
	defer env.cleanup()
	db := env.getDBHandle("test-ledger-id")

	pubDataBatch := statedb.NewUpdateBatch()
	pubDataBatch.Put("ns1", "key1", []byte("value1"), version.NewHeight(1, 1))
	pubDataBatch.Put("ns1", "key2", []byte("value2"), version.NewHeight(1, 2))
	pubDataBatch.Put("ns2", "key3", []byte("value3"), version.NewHeight(1, 3))

	pvtDataBatch := NewPvtDataBatch()
	hashesBatch := NewPvtDataBatch()
	populateBatches(t, pvtDataBatch, hashesBatch, "ns1", "coll1", "key1", []byte("pvt_value1"), version.NewHeight(1, 4))
	populateBatches(t, pvtDataBatch, hashesBatch, "ns1", "coll1", "key2", []byte("pvt_value2"), version.NewHeight(1, 5))
	populateBatches(t, pvtDataBatch, hashesBatch, "ns2", "coll1", "key3", []byte("pvt_value3"), version.NewHeight(1, 6))
	db.ApplyPubPvtAndHashUpdates(pubDataBatch, pvtDataBatch, hashesBatch, version.NewHeight(2, 6))

	vv, err := db.GetState("ns1", "key1")
	assert.NoError(t, err)
	assert.Equal(t, &statedb.VersionedValue{Value: []byte("value1"), Version: version.NewHeight(1, 1)}, vv)

	vv, err = db.GetPrivateState("ns1", "coll1", "key1")
	assert.NoError(t, err)
	assert.Equal(t, &statedb.VersionedValue{Value: []byte("pvt_value1"), Version: version.NewHeight(1, 4)}, vv)

	vv, err = db.GetValueHash("ns1", "coll1", testComputeStringHash(t, "key1"))
	assert.NoError(t, err)
	assert.Equal(t, &statedb.VersionedValue{Value: testComputeHash(t, []byte("pvt_value1")), Version: version.NewHeight(1, 4)}, vv)
}
