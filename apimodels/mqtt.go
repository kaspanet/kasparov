package apimodels

// SelectedParentChainNotification is a json representation of
// selected-parent-chain MQTT notification data.
type SelectedParentChainNotification struct {
	AddedBlockHashes, RemovedBlockHashes []string
}
