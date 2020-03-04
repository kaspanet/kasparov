package apimodels

// SelectedParentChainNotification is a json representation of
// selected-parent-chain MQTT notification data.
type SelectedParentChainNotification struct {
	AddedChainBlocks   []*AddedChainBlock `json:"addedChainBlocks"`
	RemovedBlockHashes []string           `json:"removedBlockHashes"`
}

// AddedChainBlock is a json representation of
// an added chain block and its accepted blocks.
type AddedChainBlock struct {
	Hash                string   `json:"hash"`
	AcceptedBlockHashes []string `json:"acceptedBlockHashes"`
}
