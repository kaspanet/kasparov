package mqtt

import (
	"github.com/kaspanet/kasparov/apimodels"
	"github.com/kaspanet/kasparov/dbaccess"
)

const (
	// SelectedTipTopic is an MQTT topic for DAG selected tips
	SelectedTipTopic = "dag/selected-tip"
)

// PublishSelectedTipNotification publishes notification for a new selected tip
func PublishSelectedTipNotification(selectedTipHash string) error {
	if !isConnected() {
		return nil
	}
	dbBlock, err := dbaccess.BlockByHash(dbaccess.NoTx(), selectedTipHash)
	if err != nil {
		return err
	}

	block := apimodels.ConvertBlockModelToBlockResponse(dbBlock)
	return publish(SelectedTipTopic, block)
}
