package dbaccess

import (
	"fmt"
	"github.com/go-pg/pg/v9"
	"github.com/go-pg/pg/v9/orm"
	"github.com/kaspanet/kasparov/dbmodels"
)

// TransactionByID retrieves a transaction from the database that has the provided ID
// If preloadedFields was provided - preloads the requested fields
func TransactionByID(ctx Context, transactionID string, preloadedFields ...dbmodels.FieldName) (*dbmodels.Transaction, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	tx := &dbmodels.Transaction{TransactionID: transactionID}
	query := db.Model(tx)
	query = preloadFields(query, preloadedFields)
	err = query.First()

	if err != nil {
		// TODO CHECK IF NOT FOUND ERROR, AS WE DON'T WANT TO RETURN ERROR IN THAT CASE
		return nil, err
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

	tx := &dbmodels.Transaction{TransactionHash: transactionHash}
	query := db.Model(tx)
	query = preloadFields(query, preloadedFields)
	err = query.First()

	if err != nil {
		// TODO CHECK IF NOT FOUND ERROR, AS WE DON'T WANT TO RETURN ERROR IN THAT CASE
		return nil, err
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

	txs := []*dbmodels.Transaction{}
	query := db.Model(&txs)
	query = joinTxInputsTxOutputsAndAddresses(query).
		Where("out_addresses.address = ?", address).
		WhereOr("in_addresses.address = ?", address).
		Limit(int(limit)).
		Offset(int(skip))

	if order != OrderUnknown {
		query = query.Order(fmt.Sprintf("id %s", order))
	}
	query = preloadFields(query, preloadedFields)
	err = query.Select()

	if err != nil {
		return nil, err
	}

	return txs, nil
}

// TransactionsByAddressCount returns the total number of transactions sent to or from `address`
func TransactionsByAddressCount(ctx Context, address string) (uint64, error) {
	db, err := ctx.db()
	if err != nil {
		return 0, err
	}

	query := db.Model(&dbmodels.Transaction{})
	count, err := joinTxInputsTxOutputsAndAddresses(query).
		Where("out_addresses.address = ?", address).
		WhereOr("in_addresses.address = ?", address).
		Count()

	if err != nil {
		return 0, err
	}

	// TODO CHANGE TO INT
	return uint64(count), nil
}

// AcceptedTransactionsByBlockHashes retrieves a list of transactions that were accepted
// by blocks with the given `blockHashes`
func AcceptedTransactionsByBlockHashes(ctx Context, blockHashes []string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Transaction, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}
	if len(blockHashes) == 0 { //TODO REMOVE
		return nil, nil
	}
	var transactions []*dbmodels.Transaction
	query := db.Model(&transactions).
		ColumnExpr("DISTINCT(transaction.id)").
		Join("LEFT JOIN blocks").
		JoinOn("blocks.id = transaction.accepting_block_id").
		Where("blocks.block_hash in (?)", pg.In(blockHashes))

	query = preloadFields(query, preloadedFields)
	err = query.Select()
	if err != nil {
		return nil, err
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

	var transactions []*dbmodels.Transaction
	query := db.Model(&transactions).
		Where("transaction.accepting_block_id = ?", blockID)
	query = preloadFields(query, preloadedFields)
	err = query.Select()
	if err != nil {
		return nil, err
	}

	return transactions, nil
}

// TransactionsByIDs retrieves all transactions by their `transactionIDs`.
// If preloadedFields was provided - preloads the requested fields
func TransactionsByIDs(ctx Context, transactionIDs []string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Transaction, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}
	if len(transactionIDs) == 0 { // TODO REMOVE
		return nil, nil
	}

	var transactions []*dbmodels.Transaction
	query := db.Model(&transactions).Where("transaction.transaction_id IN (?)", pg.In(transactionIDs))
	query = preloadFields(query, preloadedFields)
	err = query.Select()
	if err != nil {
		return nil, err
	}

	return transactions, nil
}

// UpdateTransactionAcceptingBlockID updates the transaction with given `transactionID` to have given `acceptingBlockID`
func UpdateTransactionAcceptingBlockID(ctx Context, transactionID uint64, acceptingBlockID *uint64) error {
	db, err := ctx.db()
	if err != nil {
		return err
	}
	_, err = db.
		Model(&dbmodels.Transaction{}).
		Where("id = ?", transactionID).
		Set("accepting_block_id = ?", acceptingBlockID).
		Update()
	if err != nil {
		return err
	}

	return nil
}

func joinTxInputsTxOutputsAndAddresses(query *orm.Query) *orm.Query {
	return query.
		Join("LEFT JOIN transaction_outputs ON transaction_outputs.transaction_id = transaction.id").
		Join("LEFT JOIN addresses AS out_addresses ON out_addresses.id = transaction_outputs.address_id").
		Join("LEFT JOIN transaction_inputs ON transaction_inputs.transaction_id = transaction.id").
		Join("LEFT JOIN transaction_outputs AS inputs_outs ON inputs_outs.id = transaction_inputs.previous_transaction_output_id").
		Join("LEFT JOIN addresses AS in_addresses ON in_addresses.id = inputs_outs.address_id")
}
