/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package commit2

import (
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/ledger"
)

type chaincodeEventsStreamsMgr struct {
	lock                   sync.Mutex
	streamsByChaincodeName map[string]map[*chaincodeEventStream]struct{}
	closed                 bool
}

func newChaincodeEventsStreamsMgr() *chaincodeEventsStreamsMgr {
	return &chaincodeEventsStreamsMgr{
		streamsByChaincodeName: make(map[string]map[*chaincodeEventStream]struct{}),
	}
}

func (m *chaincodeEventsStreamsMgr) deliverToEligibleStreams(blockEvent *ledger.CommitNotification) {
	m.removeCanceledStreams()

	chaincodeEvents := toEventsByChaincodeName(blockEvent)
	m.lock.Lock()
	defer m.lock.Unlock()

	for chaincodeName, events := range chaincodeEvents {
		for s := range m.streamsByChaincodeName[chaincodeName] {
			s.notifyChannel <- events
		}
	}
}

func (m *chaincodeEventsStreamsMgr) removeCanceledStreams() {
	m.lock.Lock()
	defer m.lock.Unlock()

	for chaincodeName, streams := range m.streamsByChaincodeName {
		for s := range streams {
			if s.isCanceled() {
				m.removeStream(chaincodeName, s)
			}
		}
	}
}

func (m *chaincodeEventsStreamsMgr) removeStream(chaincodeName string, s *chaincodeEventStream) {
	close(s.notifyChannel)

	streams := m.streamsByChaincodeName[chaincodeName]
	delete(streams, s)

	if len(streams) == 0 {
		delete(m.streamsByChaincodeName, chaincodeName)
	}
}

func toEventsByChaincodeName(blockEvent *ledger.CommitNotification) map[string]*BlockChaincodeEvents {
	results := make(map[string]*BlockChaincodeEvents)

	for _, txInfo := range blockEvent.TxsInfo {
		if txInfo.ChaincodeEventData != nil {
			event := &peer.ChaincodeEvent{}
			if err := proto.Unmarshal(txInfo.ChaincodeEventData, event); err != nil {
				continue
			}

			events := results[event.ChaincodeId]
			if events == nil {
				events = &BlockChaincodeEvents{
					BlockNumber: blockEvent.BlockNumber,
				}
				results[event.ChaincodeId] = events
			}

			events.Events = append(events.Events, event)
		}
	}

	return results
}

func (m *chaincodeEventsStreamsMgr) newStream(done <-chan struct{}, chaincodeName string) <-chan *BlockChaincodeEvents {
	notifyChannel := make(chan *BlockChaincodeEvents, 100) // Avoid blocking by buffering a number of blocks

	m.lock.Lock()
	defer m.lock.Unlock()

	if m.closed {
		close(notifyChannel)
	} else {
		stream := &chaincodeEventStream{
			cancelChan:    done,
			notifyChannel: notifyChannel,
		}
		m.streamsFor(chaincodeName)[stream] = struct{}{}
	}

	return notifyChannel
}

func (m *chaincodeEventsStreamsMgr) streamsFor(chaincodeName string) map[*chaincodeEventStream]struct{} {
	streams, exists := m.streamsByChaincodeName[chaincodeName]
	if !exists {
		streams = make(map[*chaincodeEventStream]struct{})
		m.streamsByChaincodeName[chaincodeName] = streams
	}

	return streams
}

func (m *chaincodeEventsStreamsMgr) close() {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, streams := range m.streamsByChaincodeName {
		for s := range streams {
			close(s.notifyChannel)
		}
	}

	m.streamsByChaincodeName = nil
	m.closed = true
}

type chaincodeEventStream struct {
	cancelChan    <-chan struct{}
	notifyChannel chan<- *BlockChaincodeEvents
}

func (l *chaincodeEventStream) isCanceled() bool {
	select {
	case <-l.cancelChan:
		return true
	default:
		return false
	}
}
