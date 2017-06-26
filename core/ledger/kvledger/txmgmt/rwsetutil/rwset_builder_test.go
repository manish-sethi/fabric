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

package rwsetutil

import (
	"os"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/kr/pretty"
)

func TestMain(m *testing.M) {
	flogging.SetModuleLevel("rwsetutil", "debug")
	os.Exit(m.Run())
}

func TestRWSetHolder(t *testing.T) {
	rwSetBuilder := NewRWSetBuilder()

	rwSetBuilder.AddToReadSet("ns1", "key2", version.NewHeight(1, 2))
	rwSetBuilder.AddToReadSet("ns1", "key1", version.NewHeight(1, 1))
	rwSetBuilder.AddToWriteSet("ns1", "key2", []byte("value2"))

	rqi1 := &kvrwset.RangeQueryInfo{StartKey: "bKey", EndKey: "", ItrExhausted: false, ReadsInfo: nil}
	rqi1.EndKey = "eKey"
	rqi1.SetRawReads([]*kvrwset.KVRead{NewKVRead("bKey1", version.NewHeight(2, 3)), NewKVRead("bKey2", version.NewHeight(2, 4))})
	rqi1.ItrExhausted = true
	rwSetBuilder.AddToRangeQuerySet("ns1", rqi1)

	rqi2 := &kvrwset.RangeQueryInfo{StartKey: "bKey", EndKey: "", ItrExhausted: false, ReadsInfo: nil}
	rqi2.EndKey = "eKey"
	rqi2.SetRawReads([]*kvrwset.KVRead{NewKVRead("bKey1", version.NewHeight(2, 3)), NewKVRead("bKey2", version.NewHeight(2, 4))})
	rqi2.ItrExhausted = true
	rwSetBuilder.AddToRangeQuerySet("ns1", rqi2)

	rqi3 := &kvrwset.RangeQueryInfo{StartKey: "bKey", EndKey: "", ItrExhausted: true, ReadsInfo: nil}
	rqi3.EndKey = "eKey1"
	rqi3.SetRawReads([]*kvrwset.KVRead{NewKVRead("bKey1", version.NewHeight(2, 3)), NewKVRead("bKey2", version.NewHeight(2, 4))})
	rwSetBuilder.AddToRangeQuerySet("ns1", rqi3)

	rwSetBuilder.AddToReadSet("ns2", "key2", version.NewHeight(1, 2))
	rwSetBuilder.AddToWriteSet("ns2", "key3", []byte("value3"))

	txRWSet := rwSetBuilder.GetTxReadWriteSet()

	ns1RWSet := &NsRwSet{
		NameSpace: "ns1",
		KvRwSet: &kvrwset.KVRWSet{
			Reads:            []*kvrwset.KVRead{NewKVRead("key1", version.NewHeight(1, 1)), NewKVRead("key2", version.NewHeight(1, 2))},
			RangeQueriesInfo: []*kvrwset.RangeQueryInfo{rqi1, rqi3},
			Writes:           []*kvrwset.KVWrite{newKVWrite("key2", []byte("value2"))}}}

	ns2RWSet := &NsRwSet{
		NameSpace: "ns2",
		KvRwSet: &kvrwset.KVRWSet{
			Reads:            []*kvrwset.KVRead{NewKVRead("key2", version.NewHeight(1, 2))},
			RangeQueriesInfo: nil,
			Writes:           []*kvrwset.KVWrite{newKVWrite("key3", []byte("value3"))}}}

	expectedTxRWSet := &TxRwSet{NsRwSets: []*NsRwSet{ns1RWSet, ns2RWSet}}
	t.Logf("Actual=%s\n Expected=%s, Diff=%s", spew.Sdump(txRWSet), spew.Sdump(expectedTxRWSet), pretty.Diff(txRWSet, expectedTxRWSet))
	testutil.AssertEquals(t, txRWSet, expectedTxRWSet)
}

func TestRWSetHolderWithPrivateData(t *testing.T) {
	rwSetBuilder := NewRWSetBuilder()

	rwSetBuilder.AddToReadSet("ns1", "key2", version.NewHeight(1, 2))
	rwSetBuilder.AddToReadSet("ns1", "key1", version.NewHeight(1, 1))
	rwSetBuilder.AddToWriteSet("ns1", "key2", []byte("value2"))

	rwSetBuilder.AddToHashedReadSet("ns1", "coll1", "key1", version.NewHeight(1, 1))
	rwSetBuilder.AddToHashedReadSet("ns1", "coll1", "key2", version.NewHeight(1, 2))
	rwSetBuilder.AddToPvtAndHashedWriteSet("ns1", "coll1", "key2", []byte("pvt_value2"))

	rwSetBuilder.AddToHashedReadSet("ns1", "coll2", "key1", version.NewHeight(1, 2))
	rwSetBuilder.AddToPvtAndHashedWriteSet("ns1", "coll2", "key1", []byte("pvt_value1"))
	rwSetBuilder.AddToPvtAndHashedWriteSet("ns1", "coll2", "key2", nil)

	ns1RWSet := &NsRwSet{
		NameSpace: "ns1",
		KvRwSet: &kvrwset.KVRWSet{
			Reads:            []*kvrwset.KVRead{NewKVRead("key1", version.NewHeight(1, 1)), NewKVRead("key2", version.NewHeight(1, 2))},
			RangeQueriesInfo: nil,
			Writes:           []*kvrwset.KVWrite{newKVWrite("key2", []byte("value2"))},
		},
		CollHashedRwSet: []*CollHashedRwSet{
			&CollHashedRwSet{CollectionName: "coll1",
				HashedRwSet: &kvrwset.HashedRWSet{
					HashedReads: []*kvrwset.KVReadHash{
						constructTestPvtKVReadHash(t, "key1", version.NewHeight(1, 1)),
						constructTestPvtKVReadHash(t, "key2", version.NewHeight(1, 2)),
					},
					HashedWrites: []*kvrwset.KVWriteHash{
						constructTestPvtKVWriteHash(t, "key2", []byte("pvt_value2")),
					},
				},
			},
			&CollHashedRwSet{CollectionName: "coll2",
				HashedRwSet: &kvrwset.HashedRWSet{
					HashedReads: []*kvrwset.KVReadHash{
						constructTestPvtKVReadHash(t, "key1", version.NewHeight(1, 2)),
					},
					HashedWrites: []*kvrwset.KVWriteHash{
						constructTestPvtKVWriteHash(t, "key1", []byte("pvt_value1")),
						constructTestPvtKVWriteHash(t, "key2", nil),
					},
				},
			},
		},
	}

	txRWSet := rwSetBuilder.GetTxReadWriteSet()
	expectedTxRWSet := &TxRwSet{NsRwSets: []*NsRwSet{ns1RWSet}}
	t.Logf("Actual=%s\n Expected=%s, Diff=%s", spew.Sdump(txRWSet), spew.Sdump(expectedTxRWSet), pretty.Diff(txRWSet, expectedTxRWSet))
	testutil.AssertEquals(t, txRWSet, expectedTxRWSet)

	ns1PvtRWSet := &NsPvtRwSet{
		NameSpace: "ns1",
		CollPvtRwSet: []*CollPvtRwSet{
			&CollPvtRwSet{CollectionName: "coll1",
				KvRwSet: &kvrwset.KVRWSet{
					Writes: []*kvrwset.KVWrite{newKVWrite("key2", []byte("pvt_value2"))},
				},
			},
			&CollPvtRwSet{CollectionName: "coll2",
				KvRwSet: &kvrwset.KVRWSet{
					Writes: []*kvrwset.KVWrite{newKVWrite("key1", []byte("pvt_value1")), newKVWrite("key2", nil)},
				},
			},
		},
	}
	expectedPvtRWSet := &TxPvtRwSet{NsPvtRwSet: []*NsPvtRwSet{ns1PvtRWSet}}
	actualPvtTxRWSet := rwSetBuilder.GetTxPvtReadWriteSet()
	t.Logf("ActualPvt=%s\n ExpectedPvt=%s, DiffPvt=%s", spew.Sdump(actualPvtTxRWSet), spew.Sdump(expectedPvtRWSet), pretty.Diff(actualPvtTxRWSet, expectedTxRWSet))
	testutil.AssertEquals(t, actualPvtTxRWSet, expectedPvtRWSet)
}

func constructTestPvtKVReadHash(t *testing.T, key string, version *version.Height) *kvrwset.KVReadHash {
	kvReadHash, err := newPvtKVReadHash(key, version)
	testutil.AssertNoError(t, err, "")
	return kvReadHash
}

func constructTestPvtKVWriteHash(t *testing.T, key string, value []byte) *kvrwset.KVWriteHash {
	_, kvWriteHash, err := newPvtKVWriteAndHash(key, value)
	testutil.AssertNoError(t, err, "")
	return kvWriteHash
}
