/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package commit2

import (
	"sync"

	"github.com/hyperledger/fabric/core/ledger"
)

type txCommitWaitMgr struct {
	lock          sync.Mutex
	waitersByTxID map[string]map[*txIDCommitWaiter]struct{}
	closed        bool
}

func newTxCommitWaitMgr() *txCommitWaitMgr {
	return &txCommitWaitMgr{
		waitersByTxID: make(map[string]map[*txIDCommitWaiter]struct{}),
	}
}

func (m *txCommitWaitMgr) notifyAndCloseEligibleWaiters(blockEvent *ledger.CommitNotification) {
	m.removeCanceledWaiters()

	for _, txInfo := range blockEvent.TxsInfo {
		statusEvent := &Status{
			BlockNumber:   blockEvent.BlockNumber,
			TransactionID: txInfo.TxID,
			Code:          txInfo.ValidationCode,
		}

		m.lock.Lock()
		defer m.lock.Unlock()

		for w := range m.waitersByTxID[statusEvent.TransactionID] {
			w.notifyChannel <- statusEvent
			m.removeWaiter(statusEvent.TransactionID, w)
		}
	}
}

func (m *txCommitWaitMgr) removeCanceledWaiters() {
	m.lock.Lock()
	defer m.lock.Unlock()

	for transactionID, waiters := range m.waitersByTxID {
		for w := range waiters {
			if w.isCanceled() {
				m.removeWaiter(transactionID, w)
			}
		}
	}
}

func (m *txCommitWaitMgr) newWaitChanFor(transactionID string, cancel <-chan struct{}) <-chan *Status {
	notifyChannel := make(chan *Status, 1) // Avoid blocking and only expect one notification per channel

	m.lock.Lock()
	defer m.lock.Unlock()

	if m.closed {
		close(notifyChannel)
	} else {
		waiter := &txIDCommitWaiter{
			cancelChan:    cancel,
			notifyChannel: notifyChannel,
		}
		m.waitersForTxID(transactionID)[waiter] = struct{}{}
	}

	return notifyChannel
}

func (m *txCommitWaitMgr) waitersForTxID(transactionID string) map[*txIDCommitWaiter]struct{} {
	waiters, exists := m.waitersByTxID[transactionID]
	if !exists {
		waiters = make(map[*txIDCommitWaiter]struct{})
		m.waitersByTxID[transactionID] = waiters
	}

	return waiters
}

func (m *txCommitWaitMgr) removeWaiter(transactionID string, waiter *txIDCommitWaiter) {
	close(waiter.notifyChannel)

	waiters := m.waitersByTxID[transactionID]
	delete(waiters, waiter)

	if len(waiters) == 0 {
		delete(m.waitersByTxID, transactionID)
	}
}

func (m *txCommitWaitMgr) close() {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, waiters := range m.waitersByTxID {
		for w := range waiters {
			close(w.notifyChannel)
		}
	}

	m.waitersByTxID = nil
	m.closed = true
}

type txIDCommitWaiter struct {
	cancelChan    <-chan struct{}
	notifyChannel chan<- *Status
}

func (l *txIDCommitWaiter) isCanceled() bool {
	select {
	case <-l.cancelChan:
		return true
	default:
		return false
	}
}
