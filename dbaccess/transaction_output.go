package dbaccess

import (
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
)

// TransactionOutputsByAddress retrieves all transaction outputs incoming to `address`.
// If preloadedColumns was provided - preloads the requested columns
func TransactionOutputsByAddress(address string, preloadedColumns ...string) ([]*dbmodels.TransactionOutput, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	query := db.
		Joins("LEFT JOIN `addresses` ON `addresses`.`id` = `transaction_outputs`.`address_id`").
		Joins("INNER JOIN `transactions` ON `transaction_outputs`.`transaction_id` = `transactions`.`id`").
		Where("`addresses`.`address` = ? AND `transaction_outputs`.`is_spent` = 0", address)
	query = preloadColumns(query, preloadedColumns)

	var transactionOutputs []*dbmodels.TransactionOutput
	dbResult := query.Find(&transactionOutputs)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading UTXOs from the database:", dbErrors)
	}

	return transactionOutputs, nil
}
