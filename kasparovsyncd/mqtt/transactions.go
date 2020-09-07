package mqtt

import (
	"github.com/kaspanet/kaspad/app/appmessage"
	"github.com/kaspanet/kasparov/database"
	"path"

	"github.com/kaspanet/kasparov/apimodels"
	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"
)

const (
	// TransactionsTopic is an MQTT topic for transactions
	TransactionsTopic = "transactions"

	// AcceptedTransactionsTopic is an MQTT topic for accepted transactions
	AcceptedTransactionsTopic = "transactions/accepted"

	// UnacceptedTransactionsTopic is an MQTT topic for unaccepted transactions
	UnacceptedTransactionsTopic = "transactions/unaccepted"
)

// publishTransactionsNotifications publishes notifications for each transaction of the given transactions
func publishTransactionsNotifications(topic string, dbTransactions []*dbmodels.Transaction, selectedTipBlueScore uint64) error {
	for _, dbTransaction := range dbTransactions {
		transaction := apimodels.ConvertTxModelToTxResponse(dbTransaction, selectedTipBlueScore)
		addresses := uniqueAddressesForTransaction(transaction)
		for _, address := range addresses {
			err := publishTransactionNotificationForAddress(transaction, address, topic)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func uniqueAddressesForTransaction(transaction *apimodels.TransactionResponse) []string {
	addressesMap := make(map[string]struct{})
	addresses := []string{}
	for _, output := range transaction.Outputs {
		if _, exists := addressesMap[output.Address]; !exists {
			addresses = append(addresses, output.Address)
			addressesMap[output.Address] = struct{}{}
		}
	}
	for _, input := range transaction.Inputs {
		if _, exists := addressesMap[input.Address]; !exists {
			addresses = append(addresses, input.Address)
			addressesMap[input.Address] = struct{}{}
		}
	}
	return addresses
}

func publishTransactionNotificationForAddress(transaction *apimodels.TransactionResponse, address string, topic string) error {
	return publish(path.Join(topic, address), transaction)
}

// PublishAcceptedTransactionsNotifications publishes notification for each accepted transaction of the given chain-block
func PublishAcceptedTransactionsNotifications(addedChainBlocks []*appmessage.ChainBlock) error {
	if !isConnected() {
		return nil
	}

	selectedTipBlueScore, err := dbaccess.SelectedTipBlueScore(database.NoTx())
	if err != nil {
		return err
	}

	for _, addedChainBlock := range addedChainBlocks {
		for _, acceptedBlock := range addedChainBlock.AcceptedBlocks {
			dbTransactions, err := dbaccess.TransactionsByIDsAndBlockHash(database.NoTx(), acceptedBlock.AcceptedTxIDs, acceptedBlock.Hash,
				dbmodels.TransactionRecommendedPreloadedFields...)
			if err != nil {
				return err
			}

			err = publishTransactionsNotifications(AcceptedTransactionsTopic, dbTransactions, selectedTipBlueScore)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// PublishUnacceptedTransactionsNotifications publishes notification for each unaccepted transaction of the given chain-block
func PublishUnacceptedTransactionsNotifications(unacceptedTransactions []*dbmodels.Transaction) error {
	if !isConnected() {
		return nil
	}

	selectedTipBlueScore, err := dbaccess.SelectedTipBlueScore(database.NoTx())
	if err != nil {
		return err
	}

	err = publishTransactionsNotifications(UnacceptedTransactionsTopic, unacceptedTransactions, selectedTipBlueScore)
	if err != nil {
		return err
	}
	return nil
}
