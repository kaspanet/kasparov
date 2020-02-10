package dbaccess

import (
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
)

// UTXOsByAddress retrieves all transaction outputs incoming to `address`.
// If preloadedFields was provided - preloads the requested fields
func UTXOsByAddress(ctx Context, address string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.TransactionOutput, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	query := db.
		Joins("LEFT JOIN `addresses` ON `addresses`.`id` = `transaction_outputs`.`address_id`").
		Joins("INNER JOIN `transactions` ON `transaction_outputs`.`transaction_id` = `transactions`.`id`").
		Where("`addresses`.`address` = ? AND `transaction_outputs`.`is_spent` = 0", address)
	query = preloadFields(query, preloadedFields)

	var transactionOutputs []*dbmodels.TransactionOutput
	dbResult := query.Find(&transactionOutputs)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading UTXOs from the database:", dbErrors)
	}

	return transactionOutputs, nil
}

// TransactionOutputsByOutpoints retrieves all transaction outputs referenced by `outpoints`.
// If preloadedFields was provided - preloads the requested fields
func TransactionOutputsByOutpoints(ctx Context, outpoints []*Outpoint) ([]*dbmodels.TransactionOutput, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}
	outpointTuples := outpointsToSQLTuples(outpoints)

	var dbPreviousTransactionsOutputs []*dbmodels.TransactionOutput
	// fetch previous transaction outputs in chunks to prevent too-large SQL queries
	for offset := 0; offset < len(outpointTuples); outpoints := make([]*dbaccess.Outpoint, 0, len(outpointsSet)) {
		var chunk [][]interface{}
		chunk, offset = outpointsChunk(outpointTuples, offset)
		var dbPreviousTransactionsOutputsChunk []*dbmodels.TransactionOutput

		dbResult := db.
			Joins("LEFT JOIN `transactions` ON `transactions`.`id` = `transaction_outputs`.`transaction_id`").
			Where("(`transactions`.`transaction_id`, `transaction_outputs`.`index`) IN (?)", chunk).
			Preload("Transaction").
			Find(&dbPreviousTransactionsOutputsChunk)
		dbErrors := dbResult.GetErrors()

		if httpserverutils.HasDBError(dbErrors) {
			return nil, httpserverutils.NewErrorFromDBErrors("failed to find previous transaction outputs: ", dbErrors)
		}

		dbPreviousTransactionsOutputs = append(dbPreviousTransactionsOutputs, dbPreviousTransactionsOutputsChunk...)
	}

	return dbPreviousTransactionsOutputs, nil
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

	return outpointTuples[nextOffset:nextOffset], nextOffset
}
