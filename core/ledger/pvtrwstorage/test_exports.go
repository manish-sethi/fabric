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

package pvtrwstorage

import (
	"os"
	"testing"

	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/stretchr/testify/assert"
)

// TransientStoreEnv provides the transient store env for testing
type TransientStoreEnv struct {
	t                          testing.TB
	TestTransientStoreProvider TransientStoreProvider
	TestTransientStore         TransientStore
}

// NewTestTransientStoreEnv construct a TransientStoreEnv for testing
func NewTestTransientStoreEnv(t *testing.T) *TransientStoreEnv {
	removeTransientStorePath(t)
	assert := assert.New(t)
	testTransientStoreProvider := NewTransientStoreProvider()
	testTransientStore, err := testTransientStoreProvider.OpenStore("TestTransientStore")
	assert.NoError(err)
	return &TransientStoreEnv{t, testTransientStoreProvider, testTransientStore}
}

// Cleanup cleansup the transient store env after testing
func (env *TransientStoreEnv) Cleanup() {
	env.TestTransientStoreProvider.Close()
	removeTransientStorePath(env.t)
}

func removeTransientStorePath(t testing.TB) {
	dbPath := ledgerconfig.GetTransientStorePath()
	if err := os.RemoveAll(dbPath); err != nil {
		t.Fatalf("Err: %s", err)
		t.FailNow()
	}
}
