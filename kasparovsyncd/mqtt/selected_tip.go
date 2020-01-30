package mqtt

import "github.com/kaspanet/kasparov/kasparovd/controllers"

const (
	SelectedTipTopic = "dag/selected-tip"
)

// PublishSelectedTipNotification publishes notification for a new selected tip
func PublishSelectedTipNotification(selectedTipHash string) error {
	if !isConnected() {
		return nil
	}
	block, err := controllers.GetBlockByHashHandler(selectedTipHash)
	if err != nil {
		return err
	}
	return publish(SelectedTipTopic, block)
}
