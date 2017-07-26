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

package valimpl

import (
	"fmt"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator/statebasedval"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator/valinternal"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/pvtrwstorage"
	"github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
)

var logger = flogging.MustGetLogger("valimpl")

// DefaultImpl implements the interface validator.Validator
// This performs the common tasks that are independent of a particular scheme of validation
// and for actual validation of the public rwset, it encloses an internal validator (that implements interface
// valinternal.InternalValidator) such as statebased validator
type DefaultImpl struct {
	valinternal.InternalValidator
	tStore pvtrwstorage.TransientStore
}

// NewStatebasedValidator constructs a validator that internally manages statebased validator and in addition
// handles the tasks that are agnostic to a particular validation scheme such as parsing the block and handling the pvt data
func NewStatebasedValidator(db privacyenabledstate.DB, tStore pvtrwstorage.TransientStore) validator.Validator {
	return &DefaultImpl{statebasedval.NewValidator(db), tStore}
}

// ValidateAndPrepareBatch implements the function in interface validator.Validator
func (impl *DefaultImpl) ValidateAndPrepareBatch(block *common.Block, doMVCCValidation bool) (*privacyenabledstate.UpdateBatch, error) {
	logger.Debugf("ValidateAndPrepareBatch() for block number = [%d]", block.Header.Number)
	var internalBlock *valinternal.Block
	var pubAndHashUpdates *valinternal.PubAndHashUpdates
	var pvtUpdates *privacyenabledstate.PvtUpdateBatch
	var err error

	if internalBlock, err = preprocessProtoBlock(block); err != nil {
		return nil, err
	}
	logger.Debugf("preprocessing ProtoBlock...")
	if pubAndHashUpdates, err = impl.InternalValidator.ValidateAndPrepareBatch(internalBlock, doMVCCValidation); err != nil {
		return nil, err
	}
	logger.Debugf("validated rwset...")
	if pvtUpdates, err = impl.validatePvtWriteSet(internalBlock); err != nil {
		return nil, err
	}
	logger.Debugf("validated pvtrwset...")
	postprocessProtoBlock(block, internalBlock)
	logger.Debugf("postprocessing ProtoBlock...")
	return &privacyenabledstate.UpdateBatch{
		PubUpdates:  pubAndHashUpdates.PubUpdates,
		HashUpdates: pubAndHashUpdates.HashUpdates,
		PvtUpdates:  pvtUpdates,
	}, nil
}

// validatePvtWriteSet pulls out the private write-set from transient store for the transactions that are marked as valid
// by the internal public data validator. Finally, it validates (if not already self-endorsed) the pvt rwset against the
// corresponding hash present in the public rwset
func (impl *DefaultImpl) validatePvtWriteSet(block *valinternal.Block) (*privacyenabledstate.PvtUpdateBatch, error) {
	pvtUpdates := privacyenabledstate.NewPvtUpdateBatch()
	for _, tx := range block.Txs {
		if tx.ValidationCode != peer.TxValidationCode_VALID {
			continue
		}
		if !tx.InvolvesPvtData() {
			continue
		}
		var pvtSimRes *ledger.EndorserPrivateSimulationResults
		var err error
		if selfEndorsed(tx) {
			if pvtSimRes, err = impl.tStore.GetSelfSimulatedTxPvtRWSet(tx.ID); err != nil {
				return nil, err
			}
			if pvtSimRes == nil {
				return nil, fmt.Errorf(
					"The transaction tx-id [%s], is endorsed by the peer but the pvt results missing from the transient store", tx.ID)
			}
		} else {
			// TODO iterate over the results (available from other endorsers) in the transient store and
			// pick the one for which the hashes for the <Ns,Coll> tuple matches with the hash present in
			// corresponding tx.RWSet.NsRwSets[x].CollHashedRwSets[x]
			return nil, fmt.Errorf(
				"Not a self endorsed transaction - tx-id [%s], not yet implemented", tx.ID)
		}
		pvtRWSet := &rwsetutil.TxPvtRwSet{}
		if err = pvtRWSet.FromProtoBytes(pvtSimRes.PrivateSimulationResults); err != nil {
			return nil, err
		}
		addPvtRWSetToPvtUpdateBatch(pvtRWSet, pvtUpdates, version.NewHeight(block.Num, uint64(tx.IndexInBlock)))
	}
	return pvtUpdates, nil
}

// preprocessProtoBlock parses the proto instance of block into 'Block' structure.
// The retuned 'Block' structure contains only transactions that are endorser transactions and are not alredy marked as invalid
func preprocessProtoBlock(block *common.Block) (*valinternal.Block, error) {
	b := &valinternal.Block{Num: block.Header.Number}
	// Committer validator has already set validation flags based on well formed tran checks
	txsFilter := util.TxValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	// Precaution in case committer validator has not added validation flags yet
	if len(txsFilter) == 0 {
		txsFilter = util.NewTxValidationFlags(len(block.Data.Data))
		block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER] = txsFilter
	}
	for txIndex, envBytes := range block.Data.Data {
		if txsFilter.IsInvalid(txIndex) {
			// Skiping invalid transaction
			logger.Warningf("Block [%d] Transaction index [%d] marked as invalid by committer. Reason code [%d]",
				block.Header.Number, txIndex, txsFilter.Flag(txIndex))
			continue
		}
		env, err := utils.GetEnvelopeFromBlock(envBytes)
		if err != nil {
			return nil, err
		}
		payload, err := utils.GetPayload(env)
		if err != nil {
			return nil, err
		}
		chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
		if err != nil {
			return nil, err
		}
		txType := common.HeaderType(chdr.Type)
		if txType != common.HeaderType_ENDORSER_TRANSACTION {
			logger.Debugf("Skipping mvcc validation for Block [%d] Transaction index [%d] because, the transaction type is [%s]",
				block.Header.Number, txIndex, txType)
			continue
		}
		// extract actions from the envelope message
		respPayload, err := utils.GetActionFromEnvelope(envBytes)
		if err != nil {
			txsFilter.SetFlag(txIndex, peer.TxValidationCode_NIL_TXACTION)
			continue
		}
		//preparation for extracting RWSet from transaction
		txRWSet := &rwsetutil.TxRwSet{}
		// Get the Result from the Action
		// and then Unmarshal it into a TxReadWriteSet using custom unmarshalling
		if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
			txsFilter.SetFlag(txIndex, peer.TxValidationCode_INVALID_OTHER_REASON)
			continue
		}
		b.Txs = append(b.Txs, &valinternal.Transaction{IndexInBlock: txIndex, ID: chdr.TxId, RWSet: txRWSet})
	}
	return b, nil
}

// postprocessProtoBlock updates the proto block's validation flags (in metadata) by the results of validation process
func postprocessProtoBlock(block *common.Block, validatedBlock *valinternal.Block) {
	txsFilter := util.TxValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	for _, tx := range validatedBlock.Txs {
		txsFilter.SetFlag(tx.IndexInBlock, tx.ValidationCode)
	}
	block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER] = txsFilter
}

// selfEndorsed returns true if the transaction's simulation is endorsed by this peer
func selfEndorsed(tx *valinternal.Transaction) bool {
	// TODO - derive based on singnatures. For first cut, it is assumed that the pvt tran will be endorsed by all the endorsers
	return true
}

func addPvtRWSetToPvtUpdateBatch(pvtRWSet *rwsetutil.TxPvtRwSet, pvtUpdateBatch *privacyenabledstate.PvtUpdateBatch, ver *version.Height) {
	for _, ns := range pvtRWSet.NsPvtRwSet {
		for _, coll := range ns.CollPvtRwSets {
			for _, kvwrite := range coll.KvRwSet.Writes {
				if !kvwrite.IsDelete {
					pvtUpdateBatch.Put(ns.NameSpace, coll.CollectionName, kvwrite.Key, kvwrite.Value, ver)
				} else {
					pvtUpdateBatch.Delete(ns.NameSpace, coll.CollectionName, kvwrite.Key, ver)
				}
			}
		}
	}
}
