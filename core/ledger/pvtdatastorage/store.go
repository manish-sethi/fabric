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
	"github.com/hyperledger/fabric/core/ledger"
)

// Provider provides handle to specific 'Store' that in turn manages
// private write sets for a ledger
type Provider interface {
	OpenStore(id string) (Store, error)
	Close()
}

// Store manages the permanent storage of private write sets for a ledger
type Store interface {
	// GetPvtDataByBlockNum returns only the pvt data  corresponding to the given block number
	// The pvt data is filtered by the list of 'ns/collections' supplied in the filter
	// A nil filter does not filter any results
	GetPvtDataByBlockNum(blockNum uint64, filter ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error)
	// Prepare prepares the Store for commiting the pvt data. This call does not commit the pvt data.
	// However, this ensures that the enough preparation is done such that `Commit` function invoked afterwards commits
	// this and the store is capable of surviving a crash between this function call and the next invoke to the `Commit`
	Prepare(blockNum uint64, pvtData []*ledger.TxPvtData) error
	// Commit commits the pvt data passed in the previous invoke to the `PrepareForCommitPvtData` function
	Commit() error
	// Rollback rolls back the pvt data passed in the previous invoke to the `PrepareForCommitPvtData` function
	Rollback() error
	// IsEmpty returns true if the store does not have any block committed yet
	IsEmpty() (bool, error)
	// LastCommittedBlock returns the last committed blocknum
	LastCommittedBlock() (uint64, error)
	// HasPendingBatch returns if the store has a pending batch
	HasPendingBatch() (bool, error)
	// Shutdown stops the store
	Shutdown()
}

// ErrIllegalCall is to be thrown by a store impl if the store does not expect a call to Prepare/Commit/Rollback
type ErrIllegalCall struct {
	msg string
}

func (err *ErrIllegalCall) Error() string {
	return err.msg
}

// ErrIllegalArgs is to be thrown by a store impl if the args passed are not allowed
type ErrIllegalArgs struct {
	msg string
}

func (err *ErrIllegalArgs) Error() string {
	return err.msg
}

// ErrOutOfRange is to be thrown for the request for the data that is not yet committed
type ErrOutOfRange struct {
	msg string
}

func (err *ErrOutOfRange) Error() string {
	return err.msg
}
