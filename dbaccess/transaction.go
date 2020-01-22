package dbaccess

import (
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
)

// GetAcceptedTransactionIDsByBlockHash retrieves a list of transaction IDs that were accepted
// by block with provided hash
func GetAcceptedTransactionIDsByBlockHash(blockHash string) ([]string, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	var transactionIDs []string
	dbResult := db.Model(&dbmodels.Transaction{}).
		Joins("LEFT JOIN `blocks` ON `blocks`.`id` = `transactions`.`accepting_block_id`").
		Where("`blocks`.`block_hash` = ?", blockHash).
		Pluck("`transaction_id`", &transactionIDs)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Failed to find transactions: ", dbErrors)
	}

	return transactionIDs, nil
}
