package commit2

import (
	"context"
	"sync"

	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/pkg/errors"
)

// LocalPeer is dependency that provides a mean to query the status of already committed transactions
// and a golang channel for receiving `ledger.CommitNotification` when a new block is committed
type LocalPeer interface {
	FetchCommittedTxInfo(channelName string, transactionID string) (peer.TxValidationCode, uint64, error)
	StartBlockCommitNotifications(done <-chan struct{}, channelName string) (<-chan *ledger.CommitNotification, error)
}

type Status struct {
	BlockNumber   uint64
	TransactionID string
	Code          peer.TxValidationCode
}

type BlockChaincodeEvents struct {
	BlockNumber uint64
	Events      []*peer.ChaincodeEvent
}

// InfoProvider provides the transaction commit status and blockchain events
type InfoProvider struct {
	localPeer LocalPeer

	processorsByChannel map[string]*blockCommitNotificationsProcessor
	once                sync.Once
	cancel              chan struct{}
}

// NewInfoProvider provides an instance of `InfoProvider`
func NewInfoProvider(ledger LocalPeer) *InfoProvider {
	return &InfoProvider{
		localPeer:           ledger,
		processorsByChannel: map[string]*blockCommitNotificationsProcessor{},
		cancel:              make(chan struct{}),
	}
}

// RetrieveOrWaitForTxCommit provides status of a specified transaction on a given channel. If the transaction has already
// committed, the status is returned immediately; otherwise this call blocks waiting for the transaction to be
// committed or the context to be cancelled.
func (p *InfoProvider) RetrieveOrWaitForTxCommit(ctx context.Context, channelName string, transactionID string) (*Status, error) {
	// Set up notifier first to ensure no commit missed after completing query
	cancelChan := make(chan struct{})
	defer close(cancelChan)
	commitNotificationsProcessor, err := p.getOrCreateCommitNotificationProcessor(channelName)
	if err != nil {
		return nil, err
	}
	txIDCommitWaitChan := commitNotificationsProcessor.txCommitListernersMgr.newWaitChanFor(transactionID, cancelChan)
	if err != nil {
		return nil, err
	}

	if code, blockNumber, err := p.localPeer.FetchCommittedTxInfo(channelName, transactionID); err == nil {
		status := &Status{
			BlockNumber:   blockNumber,
			TransactionID: transactionID,
			Code:          code,
		}
		return status, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case status, ok := <-txIDCommitWaitChan:
		if !ok {
			return nil, errors.New("unexpected close of commit notification channel")
		}
		return status, nil
	}

}

// NewChaincodeEventsStream procides a channel that can be read for blockchain events. Upon commit of each block, one instance of `BlockChaincodeEvents`
// is written to the channel if one or more events are present in the committed block.
func (p *InfoProvider) NewChaincodeEventsStream(ctx context.Context, channelName string, chaincodeName string) (<-chan *BlockChaincodeEvents, error) {
	commitNotificationsProcessor, err := p.getOrCreateCommitNotificationProcessor(channelName)
	if err != nil {
		return nil, err
	}

	return commitNotificationsProcessor.chaincodeEventsStreamsMgr.newStream(ctx.Done(), chaincodeName), nil
}

func (p *InfoProvider) getOrCreateCommitNotificationProcessor(channelName string) (*blockCommitNotificationsProcessor, error) {
	processor := p.processorsByChannel[channelName]
	if processor != nil && !processor.isClosed() {
		return processor, nil
	}

	commitChannel, err := p.localPeer.StartBlockCommitNotifications(p.cancel, channelName)
	if err != nil {
		return nil, err
	}

	processor = newBlockCommitNotificationProcessor(p.cancel, commitChannel)
	p.processorsByChannel[channelName] = processor

	return processor, nil
}

func (p *InfoProvider) close() {
	p.once.Do(func() {
		close(p.cancel)
	})
}
