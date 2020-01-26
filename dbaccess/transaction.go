package dbaccess

import (
	"fmt"

	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
)

// TransactionByID retrieves a transaction from the database that has the provided ID
// If preloadedColumns was provided - preloads the requested columns
func TransactionByID(ctx Context, transactionID string, preloadedColumns ...string) (*dbmodels.Transaction, error) {
	db, err := ctx.db()
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
func TransactionByHash(ctx Context, transactionHash string, preloadedColumns ...string) (*dbmodels.Transaction, error) {
	db, err := ctx.db()
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
func TransactionsByAddress(ctx Context, address string, order Order, skip uint64, limit uint64, preloadedColumns ...string) (
	[]*dbmodels.Transaction, error) {

	db, err := ctx.db()
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
// by block with hash equal to `blockHash`
func AcceptedTransactionIDsByBlockHash(ctx Context, blockHash string) ([]string, error) {
	db, err := ctx.db()
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

// AcceptedTransactionsByBlockID retrieves a list of transactions that were accepted
// by block with ID equal to `blockID`
// If preloadedColumns was provided - preloads the requested columns
func AcceptedTransactionsByBlockID(ctx Context, blockID uint64, preloadedColumns ...string) ([]*dbmodels.Transaction, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	query := db.Model(&dbmodels.Transaction{}).
		Where("`transactions`.`accepting_block_id` = ?", blockID)
	query = preloadColumns(query, preloadedColumns)

	var transactions []*dbmodels.Transaction
	dbResult := query.Find(&transactions)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Failed to find transactions: ", dbErrors)
	}

	return transactions, nil
}

// TransactionsByIDs retrieves all transactions by their `transactionIDs`.
// If preloadedColumns was provided - preloads the requested columns
func TransactionsByIDs(ctx Context, transactionIDs []string, preloadedColumns ...string) ([]*dbmodels.Transaction, error) {
	db, err := ctx.db()
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

// UpdateTransactionAcceptingBlockID updates the transaction with given `transactionID` to have given `acceptingBlockID`
func UpdateTransactionAcceptingBlockID(ctx Context, transactionID uint64, acceptingBlockID *uint64) error {
	db, err := ctx.db()
	if err != nil {
		return err
	}

	dbResult := db.
		Model(&dbmodels.Transaction{}).
		Where("id = ?", transactionID).
		Update("accepting_block_id", acceptingBlockID)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("Failed to update transaction acceptingBlockID: ", dbErrors)
	}

	return nil
}

func joinTxInputsTxOutputsAndAddresses(query *gorm.DB) *gorm.DB {
	return query.
		Joins("LEFT JOIN `transaction_outputs` ON `transaction_outputs`.`transaction_id` = `transactions`.`id`").
		Joins("LEFT JOIN `addresses` AS `out_addresses` ON `out_addresses`.`id` = `transaction_outputs`.`address_id`").
		Joins("LEFT JOIN `transaction_inputs` ON `transaction_inputs`.`transaction_id` = `transactions`.`id`").
		Joins("LEFT JOIN `transaction_outputs` AS `inputs_outs` ON `inputs_outs`.`id` = `transaction_inputs`.`previous_transaction_output_id`").
		Joins("LEFT JOIN `addresses` AS `in_addresses` ON `in_addresses`.`id` = `inputs_outs`.`address_id`")
}
