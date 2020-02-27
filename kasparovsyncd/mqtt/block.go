package mqtt

import (
	"github.com/kaspanet/kasparov/apimodels"
	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"
)

// BlockAddedTopic is an MQTT topic for new blocks
const BlockAddedTopic = "block-added"

// PublishBlockAddedNotifications publishes notifications for the block
// that was added, and notifications for its transactions.
func PublishBlockAddedNotifications(hash string) error {
	if !isConnected() {
		return nil
	}

	preloadedFields := make([]dbmodels.FieldName, len(dbmodels.TransactionRecommendedPreloadedFields)+1)

	for i, fieldName := range dbmodels.TransactionRecommendedPreloadedFields {
		preloadedFields[i] = dbmodels.BlockFieldNames.Transactions + dbmodels.FieldName(".") + fieldName
	}

	preloadedFields[len(dbmodels.TransactionRecommendedPreloadedFields)] = dbmodels.BlockFieldNames.ParentBlocks

	dbBlock, err := dbaccess.BlockByHash(dbaccess.NoTx(), hash, preloadedFields...)
	if err != nil {
		return err
	}

	err = publish(BlockAddedTopic, apimodels.ConvertBlockModelToBlockResponse(dbBlock))
	if err != nil {
		return err
	}

	return publishTransactionsNotifications(TransactionsTopic, dbBlock.Transactions)
}
