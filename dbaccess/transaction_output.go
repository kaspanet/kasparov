package dbaccess

import (
	"github.com/go-pg/pg/v9"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/dbmodels"
)

// Outpoint represent an outpoint in a transaction input.
type Outpoint struct {
	TransactionID string
	Index         uint32
}

// UTXOsByAddress retrieves all transaction outputs incoming to `address`.
// If preloadedFields was provided - preloads the requested fields
func UTXOsByAddress(ctx database.Context, address string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.TransactionOutput, error) {
	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}
	var transactionOutputs []*dbmodels.TransactionOutput
	query := db.Model(&transactionOutputs).
		Join("LEFT JOIN addresses").
		JoinOn("addresses.id = transaction_output.address_id").
		Join("INNER JOIN transactions").
		JoinOn("transaction_output.transaction_id = transactions.id").
		Where("addresses.address = ?", address).
		Where("transaction_output.is_spent = ?", false).
		Where("transactions.accepting_block_id IS NOT NULL")
	query = preloadFields(query, preloadedFields)
	err = query.Select()
	if err != nil {
		return nil, err
	}

	return transactionOutputs, nil
}

// TransactionOutputsByOutpoints retrieves all transaction outputs referenced by `outpoints`.
// If preloadedFields was provided - preloads the requested fields
func TransactionOutputsByOutpoints(ctx database.Context, outpoints []*Outpoint) ([]*dbmodels.TransactionOutput, error) {
	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}
	outpointTuples := outpointsToSQLTuples(outpoints)

	var dbPreviousTransactionsOutputs []*dbmodels.TransactionOutput
	// fetch previous transaction outputs in chunks to prevent too-large SQL queries
	for offset := 0; offset < len(outpointTuples); {
		var chunk [][]interface{}
		chunk, offset = outpointsChunk(outpointTuples, offset)
		var dbPreviousTransactionsOutputsChunk []*dbmodels.TransactionOutput
		err = db.Model(&dbPreviousTransactionsOutputsChunk).
			Join("LEFT JOIN transactions").
			JoinOn("transactions.id = transaction_output.transaction_id").
			Where("(transactions.transaction_id, transaction_output.index) in (?)", pg.In(chunk)).
			Relation(string(dbmodels.TransactionOutputFieldNames.Transaction)).
			Select()

		if err != nil {
			return nil, err
		}

		dbPreviousTransactionsOutputs = append(dbPreviousTransactionsOutputs, dbPreviousTransactionsOutputsChunk...)
	}

	return dbPreviousTransactionsOutputs, nil
}

// UpdateTransactionOutputIsSpent updates transaction-output `txOutID` by setting its IsSpent field to `isSpent`
func UpdateTransactionOutputIsSpent(ctx database.Context, txOutID uint64, isSpent bool) error {
	db, err := ctx.DB()
	if err != nil {
		return err
	}

	_, err = db.
		Model(&dbmodels.TransactionOutput{}).
		Where("id = ?", txOutID).
		Set("is_spent = ?", isSpent).
		Update()
	if err != nil {
		return err
	}

	return nil
}

func outpointsToSQLTuples(outpoints []*Outpoint) [][]interface{} {
	tuples := make([][]interface{}, len(outpoints))
	i := 0
	for _, o := range outpoints {
		tuples[i] = []interface{}{o.TransactionID, o.Index}
		i++
	}
	return tuples
}

func outpointsChunk(outpointTuples [][]interface{}, offset int) (chunk [][]interface{}, nextOffset int) {
	nextOffset = offset + chunkSize

	if nextOffset > len(outpointTuples) {
		nextOffset = len(outpointTuples)
	}

	return outpointTuples[offset:nextOffset], nextOffset
}
