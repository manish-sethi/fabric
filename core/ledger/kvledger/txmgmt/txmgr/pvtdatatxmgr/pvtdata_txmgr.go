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
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/txmgr"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/txmgr/lockbasedtxmgr"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/pvtrwstorage"
)

var logger = flogging.MustGetLogger("pvtdatatxmgr")

// TransisentHandlerTxMgr wraps a specific txmgr implementation (such as lockbasedtxmgr)
// and adds the additional functionality of persisting the private writesets into transient store
type TransisentHandlerTxMgr struct {
	txmgr.TxMgr
	tStore pvtrwstorage.TransientStore
}

// NewLockbasedTxMgr constructs a new instance of TransisentHandlerTxMgr
func NewLockbasedTxMgr(db privacyenabledstate.DB, tStore pvtrwstorage.TransientStore) *TransisentHandlerTxMgr {
	return &TransisentHandlerTxMgr{lockbasedtxmgr.NewLockBasedTxMgr(db, tStore), tStore}
}

// NewTxSimulator extends the implementation of this function in the wrapped txmgr.
func (w *TransisentHandlerTxMgr) NewTxSimulator(txid string) (ledger.TxSimulator, error) {
	var ht *version.Height
	var err error
	var actualTxSim ledger.TxSimulator
	var simBlkHt uint64

	if ht, err = w.TxMgr.GetLastSavepoint(); err != nil {
		return nil, err
	}

	if ht != nil {
		simBlkHt = ht.BlockNum
	}

	if actualTxSim, err = w.TxMgr.NewTxSimulator(txid); err != nil {
		return nil, err
	}
	return newSimulatorWrapper(actualTxSim, w.tStore, txid, simBlkHt), nil
}

// transisentHandlerTxSimulator wraps a txsimulator and adds the additional functionality of persisting
// the private writesets into transient store
type transisentHandlerTxSimulator struct {
	ledger.TxSimulator
	tStore   pvtrwstorage.TransientStore
	txid     string
	simBlkHt uint64
}

func newSimulatorWrapper(actualSim ledger.TxSimulator, tStore pvtrwstorage.TransientStore, txid string, simBlkHt uint64) *transisentHandlerTxSimulator {
	return &transisentHandlerTxSimulator{actualSim, tStore, txid, simBlkHt}
}

func (w *transisentHandlerTxSimulator) GetTxSimulationResults() (*ledger.TxSimulationResults, error) {
	var txSimRes *ledger.TxSimulationResults
	var pvtSimBytes []byte
	var err error

	if txSimRes, err = w.TxSimulator.GetTxSimulationResults(); err != nil || !txSimRes.ContainsPvtWrites() {
		logger.Debugf("Not adding private simulation results into transient store for txid=[%s]. Results available=%t, err=%#v",
			w.txid, txSimRes.ContainsPvtWrites(), err)
		return txSimRes, err
	}
	if pvtSimBytes, err = txSimRes.GetPvtSimulationBytes(); err != nil {
		return nil, err
	}
	logger.Debugf("Adding private simulation results into transient store for txid = [%s]", w.txid)
	if err = w.tStore.Persist(w.txid, "", w.simBlkHt, pvtSimBytes); err != nil {
		return nil, err
	}
	return txSimRes, nil
}
