package mqtt

import (
	"github.com/kaspanet/kaspad/rpcmodel"
	"github.com/kaspanet/kasparov/apimodels"
)

// SelectedParentChainTopic is an MQTT topic for changes in the
// selected parent chain
const SelectedParentChainTopic = "selected-parent-chain"

// PublishSelectedParentChainNotifications publishes notifications for changes in the selected parent chain
func PublishSelectedParentChainNotifications(removedChainHashes []string, addedChainBlocks []rpcmodel.ChainBlock) error {
	if !isConnected() {
		return nil
	}

	notificationData := &apimodels.SelectedParentChainNotification{
		AddedBlockHashes:   make([]string, len(addedChainBlocks)),
		RemovedBlockHashes: make([]string, len(removedChainHashes)),
	}

	for i, block := range addedChainBlocks {
		notificationData.AddedBlockHashes[i] = block.Hash
	}

	for i, hash := range removedChainHashes {
		notificationData.RemovedBlockHashes[i] = hash
	}

	return publish(SelectedParentChainTopic, notificationData)
}
