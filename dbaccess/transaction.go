package dbaccess

import (
	"fmt"

	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
)

// TransactionByID retrieves a transaction from the database that has the provided ID
// If preloadedColumns was provided - preloads the requested columns
func TransactionByID(transactionID string, preloadedColumns ...string) (*dbmodels.Transaction, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	query := db.Where(&dbmodels.Transaction{TransactionID: transactionID})
	query = preloadColumns(query, preloadedColumns)

	tx := &dbmodels.Transaction{}
	dbResult := query.First(&tx)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.IsDBRecordNotFoundError(dbErrors) {
		return nil, nil
	}
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading transaction from the database:", dbErrors)
	}

	return tx, nil
}

// TransactionByHash retrieves a transaction from the database that has the provided hash
// If preloadedColumns was provided - preloads the requested columns
func TransactionByHash(transactionHash string, preloadedColumns ...string) (*dbmodels.Transaction, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	query := db.Where(&dbmodels.Transaction{TransactionHash: transactionHash})
	query = preloadColumns(query, preloadedColumns)

	tx := &dbmodels.Transaction{}
	dbResult := query.First(&tx)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.IsDBRecordNotFoundError(dbErrors) {
		return nil, nil
	}
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading transaction from the database:", dbErrors)
	}

	return tx, nil
}

// TransactionsByAddress retrieves up to `limit` transactions sent to or from `address`,
// in the requested `order`, skipping the first `skip` blocks
// If preloadedColumns was provided - preloads the requested columns
func TransactionsByAddress(address string, order Order, skip uint64, limit uint64, preloadedColumns ...string) (
	[]*dbmodels.Transaction, error) {

	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	query := joinTxInputsTxOutputsAndAddresses(db).
		Where("`out_addresses`.`address` = ?", address).
		Or("`in_addresses`.`address` = ?", address).
		Limit(limit).
		Offset(skip)

	if order != OrderUnknown {
		query = query.Order(fmt.Sprintf("`transactions`.`id` %s", order))
	}
	query = preloadColumns(query, preloadedColumns)

	txs := []*dbmodels.Transaction{}
	dbResult := query.Find(&txs)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading transactions from the database:", dbErrors)
	}

	return txs, nil
}

// AcceptedTransactionIDsByBlockHash retrieves a list of transaction IDs that were accepted
// by block with provided hash
func AcceptedTransactionIDsByBlockHash(blockHash string) ([]string, error) {
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

// TransactionsByIDs retrieves all transactions by their `transactionIDs`.
// If preloadedColumns was provided - preloads the requested columns
func TransactionsByIDs(transactionIDs []string, preloadedColumns ...string) ([]*dbmodels.Transaction, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	query := joinTxInputsTxOutputsAndAddresses(db).
		Where("`transactions`.`transaction_id` IN (?)", transactionIDs)
	query = preloadColumns(query, preloadedColumns)

	var txs []*dbmodels.Transaction
	dbResult := query.Find(&txs)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading transactions from the database:", dbErrors)
	}

	return txs, nil
}

// TransactionsInBlock checks for every transactionID in transactionIDs if it's in given block.
// Returns a slice of all transactionIDs that are in this block.
// Note: this function works with database-ids, not the actual transactionIDs.
func TransactionsInBlock(transactionIDs []uint64, blockID uint64) ([]uint64, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	transactionIDsInBlock := []uint64{}
	dbResult := db.Model(&dbmodels.TransactionBlock{}).
		Where(&dbmodels.TransactionBlock{BlockID: blockID}).
		Where("`transaction_id` in (?)", transactionIDs).
		Pluck("`transaction_id`", &transactionIDsInBlock)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading UTXOs from the database:", dbErrors)
	}

	return transactionIDsInBlock, nil
}

func joinTxInputsTxOutputsAndAddresses(query *gorm.DB) *gorm.DB {
	return query.
		Joins("LEFT JOIN `transaction_outputs` ON `transaction_outputs`.`transaction_id` = `transactions`.`id`").
		Joins("LEFT JOIN `addresses` AS `out_addresses` ON `out_addresses`.`id` = `transaction_outputs`.`address_id`").
		Joins("LEFT JOIN `transaction_inputs` ON `transaction_inputs`.`transaction_id` = `transactions`.`id`").
		Joins("LEFT JOIN `transaction_outputs` AS `inputs_outs` ON `inputs_outs`.`id` = `transaction_inputs`.`previous_transaction_output_id`").
		Joins("LEFT JOIN `addresses` AS `in_addresses` ON `in_addresses`.`id` = `inputs_outs`.`address_id`")
}
