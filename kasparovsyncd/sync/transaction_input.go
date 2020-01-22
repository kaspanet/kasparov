package sync

import (
	"encoding/hex"

	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kaspad/rpcmodel"
	"github.com/kaspanet/kaspad/util/subnetworkid"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
	"github.com/pkg/errors"
)

func outpointsChunk(outpoints [][]interface{}, i int) [][]interface{} {
	chunkStart := i * chunkSize
	chunkEnd := chunkStart + chunkSize
	if chunkEnd > len(outpoints) {
		chunkEnd = len(outpoints)
	}
	return outpoints[chunkStart:chunkEnd]
}

func insertTransactionInputs(dbTx *gorm.DB, transactionIDsToTxsWithMetadata map[string]*txWithMetadata) error {
	outpointsSet := make(map[outpoint]struct{})
	newNonCoinbaseTransactions := make(map[string]*txWithMetadata)
	inputsCount := 0
	for transactionID, transaction := range transactionIDsToTxsWithMetadata {
		if !transaction.isNew {
			continue
		}
		isCoinbase, err := isTransactionCoinbase(transaction.verboseTx)
		if err != nil {
			return err
		}
		if isCoinbase {
			continue
		}

		newNonCoinbaseTransactions[transactionID] = transaction
		inputsCount += len(transaction.verboseTx.Vin)
		for _, txIn := range transaction.verboseTx.Vin {
			outpointsSet[outpoint{
				transactionID: txIn.TxID,
				index:         txIn.Vout,
			}] = struct{}{}
		}
	}

	if inputsCount == 0 {
		return nil
	}

	outpoints := outpointSetToSQLTuples(outpointsSet)

	var dbPreviousTransactionsOutputs []*dbmodels.TransactionOutput
	// fetch previous transaction outputs in chunks to prevent too-large SQL queries
	for i := 0; i < len(outpoints)/chunkSize+1; i++ {
		var dbPreviousTransactionsOutputsChunk []*dbmodels.TransactionOutput

		dbResult := dbTx.
			Joins("LEFT JOIN `transactions` ON `transactions`.`id` = `transaction_outputs`.`transaction_id`").
			Where("(`transactions`.`transaction_id`, `transaction_outputs`.`index`) IN (?)", outpointsChunk(outpoints, i)).
			Preload("Transaction").
			Find(&dbPreviousTransactionsOutputsChunk)
		dbErrors := dbResult.GetErrors()
		if httpserverutils.HasDBError(dbErrors) {
			return httpserverutils.NewErrorFromDBErrors("failed to find previous transaction outputs: ", dbErrors)
		}

		dbPreviousTransactionsOutputs = append(dbPreviousTransactionsOutputs, dbPreviousTransactionsOutputsChunk...)
	}

	if len(dbPreviousTransactionsOutputs) != len(outpoints) {
		return errors.New("couldn't fetch all of the requested outpoints")
	}

	outpointsToIDs := make(map[outpoint]uint64)
	for _, dbTransactionOutput := range dbPreviousTransactionsOutputs {
		outpointsToIDs[outpoint{
			transactionID: dbTransactionOutput.Transaction.TransactionID,
			index:         dbTransactionOutput.Index,
		}] = dbTransactionOutput.ID
	}

	inputsToAdd := make([]interface{}, inputsCount)
	inputIndex := 0
	for _, transaction := range newNonCoinbaseTransactions {
		for i, txIn := range transaction.verboseTx.Vin {
			scriptSig, err := hex.DecodeString(txIn.ScriptSig.Hex)
			if err != nil {
				return nil
			}
			prevOutputID, ok := outpointsToIDs[outpoint{
				transactionID: txIn.TxID,
				index:         txIn.Vout,
			}]
			if !ok || prevOutputID == 0 {
				return errors.Errorf("couldn't find ID for outpoint (%s:%d)", txIn.TxID, txIn.Vout)
			}
			inputsToAdd[inputIndex] = &dbmodels.TransactionInput{
				TransactionID:               transaction.id,
				PreviousTransactionOutputID: prevOutputID,
				Index:                       uint32(i),
				SignatureScript:             scriptSig,
				Sequence:                    txIn.Sequence,
			}
			inputIndex++
		}
	}
	return bulkInsert(dbTx, inputsToAdd)
}

func isTransactionCoinbase(transaction *rpcmodel.TxRawResult) (bool, error) {
	subnetwork, err := subnetworkid.NewFromStr(transaction.Subnetwork)
	if err != nil {
		return false, err
	}
	return subnetwork.IsEqual(subnetworkid.SubnetworkIDCoinbase), nil
}

type outpoint struct {
	transactionID string
	index         uint32
}

func outpointSetToSQLTuples(outpointsToIDs map[outpoint]struct{}) [][]interface{} {
	outpoints := make([][]interface{}, len(outpointsToIDs))
	i := 0
	for o := range outpointsToIDs {
		outpoints[i] = []interface{}{o.transactionID, o.index}
		i++
	}
	return outpoints
}
