package mqtt

import (
	"path"

	"github.com/kaspanet/kaspad/rpcmodel"
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

// PublishTransactionsNotifications publishes notification for each transaction of the given block
func PublishTransactionsNotifications(rawTransactions []rpcmodel.TxRawResult) error {
	if !isConnected() {
		return nil
	}

	transactionIDs := make([]string, len(rawTransactions))
	for i, tx := range rawTransactions {
		transactionIDs[i] = tx.TxID
	}

	dbTransactions, err := dbaccess.TransactionsByIDs(dbaccess.NoTx(), transactionIDs,
		dbmodels.TransactionRecommendedPreloadedFields...)
	if err != nil {
		return err
	}

	selectedTipBlueScore, err := dbaccess.SelectedTipBlueScore(dbaccess.NoTx())
	if err != nil {
		return err
	}

	for _, dbTransaction := range dbTransactions {
		transaction := apimodels.ConvertTxModelToTxResponse(dbTransaction, selectedTipBlueScore)
		err = publishTransactionNotifications(transaction, TransactionsTopic)
		if err != nil {
			return err
		}
	}
	return nil
}

func publishTransactionNotifications(transaction *apimodels.TransactionResponse, topic string) error {
	addresses := uniqueAddressesForTransaction(transaction)
	for _, address := range addresses {
		err := publishTransactionNotificationForAddress(transaction, address, topic)
		if err != nil {
			return err
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
func PublishAcceptedTransactionsNotifications(addedChainBlocks []rpcmodel.ChainBlock) error {
	if !isConnected() {
		return nil
	}

	selectedTipBlueScore, err := dbaccess.SelectedTipBlueScore(dbaccess.NoTx())
	if err != nil {
		return err
	}

	for _, addedChainBlock := range addedChainBlocks {
		for _, acceptedBlock := range addedChainBlock.AcceptedBlocks {
			dbTransactions, err := dbaccess.TransactionsByIDs(dbaccess.NoTx(), acceptedBlock.AcceptedTxIDs,
				dbmodels.TransactionRecommendedPreloadedFields...)
			if err != nil {
				return err
			}

			for _, dbTransaction := range dbTransactions {
				transaction := apimodels.ConvertTxModelToTxResponse(dbTransaction, selectedTipBlueScore)
				err = publishTransactionNotifications(transaction, AcceptedTransactionsTopic)
				if err != nil {
					return err
				}
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

	selectedTipBlueScore, err := dbaccess.SelectedTipBlueScore(dbaccess.NoTx())
	if err != nil {
		return err
	}

	for _, dbTransaction := range unacceptedTransactions {
		transaction := apimodels.ConvertTxModelToTxResponse(dbTransaction, selectedTipBlueScore)
		err := publishTransactionNotifications(transaction, UnacceptedTransactionsTopic)
		if err != nil {
			return err
		}
	}
	return nil
}
