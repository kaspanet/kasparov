package dbaccess

import (
	"fmt"

	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
)

// TransactionByID retrieves a transaction from the database that has the provided ID
// If preloadedFields was provided - preloads the requested fields
func TransactionByID(ctx Context, transactionID string, preloadedFields ...dbmodels.FieldName) (*dbmodels.Transaction, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	query := db.Where(&dbmodels.Transaction{TransactionID: transactionID})
	query = preloadFields(query, preloadedFields)

	tx := &dbmodels.Transaction{}
	dbResult := query.First(&tx)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.IsDBRecordNotFoundError(dbErrors) {
		return nil, nil
	}
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("some errors were encountered when loading transaction from the database:", dbErrors)
	}

	return tx, nil
}

// TransactionByHash retrieves a transaction from the database that has the provided hash
// If preloadedFields was provided - preloads the requested fields
func TransactionByHash(ctx Context, transactionHash string, preloadedFields ...dbmodels.FieldName) (*dbmodels.Transaction, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	query := db.Where(&dbmodels.Transaction{TransactionHash: transactionHash})
	query = preloadFields(query, preloadedFields)

	tx := &dbmodels.Transaction{}
	dbResult := query.First(&tx)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.IsDBRecordNotFoundError(dbErrors) {
		return nil, nil
	}
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("some errors were encountered when loading transaction from the database:", dbErrors)
	}

	return tx, nil
}

// TransactionsByAddress retrieves up to `limit` transactions sent to or from `address`,
// in the requested `order`, skipping the first `skip` blocks
// If preloadedFields was provided - preloads the requested fields
func TransactionsByAddress(ctx Context, address string, order Order, skip uint64, limit uint64, preloadedFields ...dbmodels.FieldName) (
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
	query = preloadFields(query, preloadedFields)

	txs := []*dbmodels.Transaction{}
	dbResult := query.Find(&txs)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("some errors were encountered when loading transactions from the database:", dbErrors)
	}

	return txs, nil
}

// TransactionsByAddressCount returns the total number of transactions sent to or from `address`
func TransactionsByAddressCount(ctx Context, address string) (uint64, error) {
	db, err := ctx.db()
	if err != nil {
		return 0, err
	}

	var count uint64
	query := db.Model(&dbmodels.Transaction{})
	dbResult := joinTxInputsTxOutputsAndAddresses(query).
		Where("`out_addresses`.`address` = ?", address).
		Or("`in_addresses`.`address` = ?", address).
		Count(&count)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return 0, httpserverutils.NewErrorFromDBErrors("some errors were encountered when counting transactions in the database:", dbErrors)
	}

	return count, nil
}

// AcceptedTransactionsByBlockHashes retrieves a list of transactions that were accepted
// by blocks with the given `blockHashes`
func AcceptedTransactionsByBlockHashes(ctx Context, blockHashes []string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Transaction, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	query := db.
		Select("DISTINCT(transactions.id)").
		Joins("LEFT JOIN `blocks` ON `blocks`.`id` = `transactions`.`accepting_block_id`").
		Where("`blocks`.`block_hash` in (?)", blockHashes)
	query = preloadFields(query, preloadedFields)

	var transactions []*dbmodels.Transaction
	dbResult := query.Find(&transactions)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find transactions: ", dbErrors)
	}

	return transactions, nil
}

// AcceptedTransactionsByBlockID retrieves a list of transactions that were accepted
// by block with ID equal to `blockID`
// If preloadedFields was provided - preloads the requested fields
func AcceptedTransactionsByBlockID(ctx Context, blockID uint64, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Transaction, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	query := db.Model(&dbmodels.Transaction{}).
		Where("`transactions`.`accepting_block_id` = ?", blockID)
	query = preloadFields(query, preloadedFields)

	var transactions []*dbmodels.Transaction
	dbResult := query.Find(&transactions)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find transactions: ", dbErrors)
	}

	return transactions, nil
}

// TransactionsByHashes retrieves all transactions by their `transactionHashes`.
// If preloadedFields was provided - preloads the requested fields
func TransactionsByHashes(ctx Context, transactionHashes []string,
	preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Transaction, error) {

	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	query := db.Where("`transactions`.`transaction_hash` IN (?)", transactionHashes)
	query = preloadFields(query, preloadedFields)

	var txs []*dbmodels.Transaction
	dbResult := query.Find(&txs)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("some errors were encountered when loading transactions from the database:", dbErrors)
	}

	return txs, nil
}

// TransactionsByIDsAndBlockID retrieves all transactions in a
// block with the given ID by their `transactionIDs`.
// If preloadedFields was provided - preloads the requested fields
func TransactionsByIDsAndBlockID(ctx Context, transactionIDs []string, blockID uint64,
	preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Transaction, error) {

	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	query := db.
		Joins("INNER JOIN `transactions_to_blocks` ON `transactions`.`id` = `transactions_to_blocks`.`transaction_id`").
		Where("`transactions`.`transaction_id` IN (?)", transactionIDs).
		Where("`transactions_to_blocks`.`block_id` = ?", blockID)
	query = preloadFields(query, preloadedFields)

	var txs []*dbmodels.Transaction
	dbResult := query.Find(&txs)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("some errors were encountered when loading transactions from the database:", dbErrors)
	}

	return txs, nil
}

// TransactionsByIDsAndBlockHash retrieves all transactions in a
// block with the given hash by their `transactionIDs`.
// If preloadedFields was provided - preloads the requested fields
func TransactionsByIDsAndBlockHash(ctx Context, transactionIDs []string, blockHash string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Transaction, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	query := db.
		Joins("INNER JOIN `transactions_to_blocks` ON `transactions`.`id` = `transactions_to_blocks`.`transaction_id`").
		Joins("INNER JOIN `blocks` ON `blocks`.`id` = `transactions_to_blocks`.`block_id`").
		Where("`transactions`.`transaction_id` IN (?)", transactionIDs).
		Where("`blocks`.`block_hash` = ?", blockHash)
	query = preloadFields(query, preloadedFields)

	var txs []*dbmodels.Transaction
	dbResult := query.Find(&txs)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("some errors were encountered when loading transactions from the database:", dbErrors)
	}

	return txs, nil
}

// TransactionsByIDs retrieves all transactions by their `transactionIDs`.
// If preloadedFields was provided - preloads the requested fields
func TransactionsByIDs(ctx Context, transactionIDs []string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Transaction, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	query := db.Where("`transactions`.`transaction_id` IN (?)", transactionIDs)
	query = preloadFields(query, preloadedFields)

	var txs []*dbmodels.Transaction
	dbResult := query.Find(&txs)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("some errors were encountered when loading transactions from the database:", dbErrors)
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
		return httpserverutils.NewErrorFromDBErrors("failed to update transaction acceptingBlockID: ", dbErrors)
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
