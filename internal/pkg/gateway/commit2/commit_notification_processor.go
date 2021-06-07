/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package commit2

import (
	"sync"

	"github.com/hyperledger/fabric/core/ledger"
)

type blockCommitNotificationsProcessor struct {
	commitNotificationChannel <-chan *ledger.CommitNotification
	done                      <-chan struct{}

	txCommitListernersMgr     *txCommitWaitMgr
	chaincodeEventsStreamsMgr *chaincodeEventsStreamsMgr

	lock   sync.Mutex
	closed bool
}

func newBlockCommitNotificationProcessor(done <-chan struct{}, commitNotificationChannel <-chan *ledger.CommitNotification) *blockCommitNotificationsProcessor {
	p := &blockCommitNotificationsProcessor{
		commitNotificationChannel: commitNotificationChannel,
		txCommitListernersMgr:     newTxCommitWaitMgr(),
		chaincodeEventsStreamsMgr: newChaincodeEventsStreamsMgr(),
		done:                      done,
	}
	go p.run()
	return p
}

func (p *blockCommitNotificationsProcessor) run() {
	for !p.isClosed() {
		select {
		case blockCommit, ok := <-p.commitNotificationChannel:
			if !ok {
				p.close()
				return
			}
			p.process(blockCommit)
		case <-p.done:
			p.close()
			return
		}
	}
}

func (p *blockCommitNotificationsProcessor) process(commitNotification *ledger.CommitNotification) {
	p.txCommitListernersMgr.notifyAndCloseEligibleWaiters(commitNotification)
	p.chaincodeEventsStreamsMgr.deliverToEligibleStreams(commitNotification)
}

func (p *blockCommitNotificationsProcessor) close() {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.closed {
		return
	}
	p.txCommitListernersMgr.close()
	p.chaincodeEventsStreamsMgr.close()
	p.closed = true
}

func (p *blockCommitNotificationsProcessor) isClosed() bool {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.closed
}
