package dbaccess

import (
	"fmt"

	"github.com/go-pg/pg/v9"
	"github.com/go-pg/pg/v9/orm"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/dbmodels"
)

// TransactionByID retrieves a transaction from the database that has the provided ID
// If preloadedFields was provided - preloads the requested fields
func TransactionByID(ctx database.Context, transactionID string, preloadedFields ...dbmodels.FieldName) (*dbmodels.Transaction, error) {
	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}

	tx := &dbmodels.Transaction{}
	query := db.Model(tx).Where("transaction.transaction_id = ?", transactionID)
	query = preloadFields(query, preloadedFields)
	err = query.First()

	if err == pg.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return tx, nil
}

// TransactionByHash retrieves a transaction from the database that has the provided hash
// If preloadedFields was provided - preloads the requested fields
func TransactionByHash(ctx database.Context, transactionHash string, preloadedFields ...dbmodels.FieldName) (*dbmodels.Transaction, error) {
	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}

	tx := &dbmodels.Transaction{}
	query := db.Model(tx).
		Where("transaction_hash = ?", transactionHash)
	query = preloadFields(query, preloadedFields)
	err = query.First()

	if err == pg.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return tx, nil
}

// TransactionsByAddress retrieves up to `limit` transactions sent to or from `address`,
// in the requested `order`, skipping the first `skip` blocks
// If preloadedFields was provided - preloads the requested fields
func TransactionsByAddress(ctx database.Context, address string, order Order, skip uint64, limit uint64, preloadedFields ...dbmodels.FieldName) (
	[]*dbmodels.Transaction, error) {

	if limit == 0 {
		return []*dbmodels.Transaction{}, nil
	}

	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}

	var txs []*dbmodels.Transaction
	query := db.Model(&txs)
	query = joinTxInputsTxOutputsAndAddresses(query).
		DistinctOn("transaction.id").
		Where("out_addresses.address = ?", address).
		WhereOr("in_addresses.address = ?", address).
		Limit(int(limit)).
		Offset(int(skip))

	if order != OrderUnknown {
		query = query.Order(fmt.Sprintf("transaction.id %s", order))
	}
	query = preloadFields(query, preloadedFields)
	err = query.Select()

	if err != nil {
		return nil, err
	}

	return txs, nil
}

// TransactionsByAddressCount returns the total number of transactions sent to or from `address`
func TransactionsByAddressCount(ctx database.Context, address string) (uint64, error) {
	db, err := ctx.DB()
	if err != nil {
		return 0, err
	}

	var result struct {
		TransactionCount uint64
	}
	_, err = db.QueryOne(&result, `
SELECT count(*) as transaction_count FROM (
	SELECT
		DISTINCT transactions.id
	FROM
		transactions
	where
	EXISTS
	(
		SELECT transaction_id FROM transaction_outputs
		INNER JOIN addresses ON transaction_outputs.address_id = addresses.id
		WHERE addresses.address = ?
	)
	OR
	EXISTS
	(
		SELECT transaction_inputs.transaction_id FROM transaction_inputs
		INNER JOIN transaction_outputs ON transaction_inputs.previous_transaction_output_id = transaction_outputs.id
		INNER JOIN addresses ON transaction_outputs.address_id = addresses.id
		WHERE addresses.address = ?
	)
) as transaction_ids
`, address, address)

	if err != nil {
		return 0, err
	}

	return result.TransactionCount, nil
}

// AcceptedTransactionsByBlockHashes retrieves a list of transactions that were accepted
// by blocks with the given `blockHashes`
func AcceptedTransactionsByBlockHashes(ctx database.Context, blockHashes []string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Transaction, error) {
	if len(blockHashes) == 0 {
		return nil, nil
	}

	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}
	var transactions []*dbmodels.Transaction
	query := db.Model(&transactions).
		ColumnExpr("DISTINCT transaction.*").
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
func AcceptedTransactionsByBlockID(ctx database.Context, blockID uint64, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Transaction, error) {
	db, err := ctx.DB()
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

// TransactionDoubleSpends retrieves transactions, that have at least one input which is the same as in transaction that has the provided txID
func TransactionDoubleSpends(ctx database.Context, txID string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Transaction, error) {
	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}

	originalInputs := db.Model(&dbmodels.TransactionInput{}).
		Column("transaction_input.transaction_id", "transaction_input.index").
		Join("INNER JOIN transactions").
		JoinOn("transactions.id = transaction_input.transaction_id").
		Where("transactions.transaction_id = ?", txID)
	txIDs := db.Model(&dbmodels.TransactionInput{}).
		With("original_inputs", originalInputs).
		Column("transaction_input.transaction_id").
		Join("INNER JOIN original_inputs").
		JoinOn("original_inputs.index = transaction_input.index").
		Where("transaction_input.transaction_id != original_inputs.transaction_id")

	var txs []*dbmodels.Transaction
	query := db.Model(&txs).
		With("txids", txIDs).
		Join("INNER JOIN txids").
		JoinOn("txids.transaction_id = transaction.id")
	query = preloadFields(query, preloadedFields)
	err = query.Select()
	if err != nil {
		return nil, err
	}

	return txs, nil
}

// TransactionsByHashes retrieves all transactions by their `transactionHashes`.
// If preloadedFields was provided - preloads the requested fields
func TransactionsByHashes(ctx database.Context, transactionHashes []string,
	preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Transaction, error) {

	if len(transactionHashes) == 0 {
		return nil, nil
	}

	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}

	var txs []*dbmodels.Transaction
	query := db.Model(&txs).
		Where("transaction_hash IN (?)", pg.In(transactionHashes))
	query = preloadFields(query, preloadedFields)
	err = query.Select()
	if err != nil {
		return nil, err
	}

	return txs, nil
}

// TransactionsByIDsAndBlockID retrieves all transactions in a
// block with the given ID by their `transactionIDs`.
// If preloadedFields was provided - preloads the requested fields
func TransactionsByIDsAndBlockID(ctx database.Context, transactionIDs []string, blockID uint64,
	preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Transaction, error) {

	if len(transactionIDs) == 0 {
		return nil, nil
	}

	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}

	var txs []*dbmodels.Transaction
	query := db.
		Model(&txs).
		Join("INNER JOIN transactions_to_blocks").
		JoinOn("transaction.id = transactions_to_blocks.transaction_id").
		Where("transaction.transaction_id IN (?)", pg.In(transactionIDs)).
		Where("transactions_to_blocks.block_id = ?", blockID)
	query = preloadFields(query, preloadedFields)

	err = query.Select()
	if err != nil {
		return nil, err
	}

	return txs, nil
}

// TransactionsByIDsAndBlockHash retrieves all transactions in a
// block with the given hash by their `transactionIDs`.
// If preloadedFields was provided - preloads the requested fields
func TransactionsByIDsAndBlockHash(ctx database.Context, transactionIDs []string, blockHash string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Transaction, error) {
	if len(transactionIDs) == 0 {
		return nil, nil
	}

	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}

	var txs []*dbmodels.Transaction
	query := db.
		Model(&txs).
		Join("INNER JOIN transactions_to_blocks").
		JoinOn("transaction.id = transactions_to_blocks.transaction_id").
		Join("INNER JOIN blocks").
		JoinOn("blocks.id = transactions_to_blocks.block_id").
		Where("transaction.transaction_id IN (?)", pg.In(transactionIDs)).
		Where("blocks.block_hash = ?", blockHash)
	query = preloadFields(query, preloadedFields)
	err = query.Select()
	if err != nil {
		return nil, err
	}

	return txs, nil
}

// TransactionsByIDs retrieves all transactions by their `transactionIDs`.
// If preloadedFields was provided - preloads the requested fields
func TransactionsByIDs(ctx database.Context, transactionIDs []string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Transaction, error) {
	if len(transactionIDs) == 0 {
		return nil, nil
	}

	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}

	var transactions []*dbmodels.Transaction
	query := db.Model(&transactions).
		Where("transaction.transaction_id IN (?)", pg.In(transactionIDs))
	query = preloadFields(query, preloadedFields)
	err = query.Select()
	if err != nil {
		return nil, err
	}

	return transactions, nil
}

// TransactionsByBlockHash retrieves a list of transactions included by the block
// with the given blockHash
func TransactionsByBlockHash(ctx database.Context, blockHash string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Transaction, error) {
	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}

	var transactions []*dbmodels.Transaction
	query := db.Model(&transactions).
		Join("INNER JOIN transactions_to_blocks ON transactions_to_blocks.transaction_id = transaction.id").
		Join("INNER JOIN blocks ON blocks.id = transactions_to_blocks.block_id").
		Where("blocks.block_hash = ?", blockHash).
		Order("transactions_to_blocks.index ASC")
	query = preloadFields(query, preloadedFields)

	err = query.Select()
	if err != nil {
		return nil, err
	}

	return transactions, nil
}

// UpdateTransactionAcceptingBlockID updates the transaction with given `transactionID` to have given `acceptingBlockID`
func UpdateTransactionAcceptingBlockID(ctx database.Context, transactionID uint64, acceptingBlockID *uint64) error {
	db, err := ctx.DB()
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
		Join("LEFT JOIN transaction_outputs").
		JoinOn("transaction_outputs.transaction_id = transaction.id").
		Join("LEFT JOIN addresses AS out_addresses").
		JoinOn("out_addresses.id = transaction_outputs.address_id").
		Join("LEFT JOIN transaction_inputs").
		JoinOn("transaction_inputs.transaction_id = transaction.id").
		Join("LEFT JOIN transaction_outputs AS inputs_outs").
		JoinOn("inputs_outs.id = transaction_inputs.previous_transaction_output_id").
		Join("LEFT JOIN addresses AS in_addresses").
		JoinOn("in_addresses.id = inputs_outs.address_id")
}
