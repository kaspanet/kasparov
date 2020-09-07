package sync

import (
	"encoding/hex"
	"github.com/kaspanet/kaspad/app/appmessage"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/serializer"

	"github.com/kaspanet/kaspad/util/subnetworkid"
	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/pkg/errors"
)

func insertTransactionInputs(dbTx *database.TxContext, transactionHashesToTxsWithMetadata map[string]*txWithMetadata) error {
	outpointsSet := make(map[dbaccess.Outpoint]struct{})
	newNonCoinbaseTransactions := make(map[string]*txWithMetadata)
	inputsCount := 0
	for txHash, transaction := range transactionHashesToTxsWithMetadata {
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

		newNonCoinbaseTransactions[txHash] = transaction
		inputsCount += len(transaction.verboseTx.TransactionVerboseInputs)
		for i := range transaction.verboseTx.TransactionVerboseInputs {
			txIn := transaction.verboseTx.TransactionVerboseInputs[i]
			outpoint := dbaccess.Outpoint{
				TransactionID: txIn.TxID,
				Index:         txIn.OutputIndex,
			}
			outpointsSet[outpoint] = struct{}{}
		}
	}

	if inputsCount == 0 {
		return nil
	}
	outpoints := make([]*dbaccess.Outpoint, len(outpointsSet))
	i := 0
	for outpoint := range outpointsSet {
		outpointCopy := outpoint // since outpoint is a value type - copy it, othewise it would be overwritten
		outpoints[i] = &outpointCopy
		i++
	}

	dbPreviousTransactionsOutputs, err := dbaccess.TransactionOutputsByOutpoints(dbTx, outpoints)
	if err != nil {
		return err
	}

	if len(dbPreviousTransactionsOutputs) != len(outpoints) {
		return errors.New("couldn't fetch all of the requested outpoints")
	}

	outpointsToIDs := make(map[dbaccess.Outpoint]uint64)
	for _, dbTransactionOutput := range dbPreviousTransactionsOutputs {
		outpointsToIDs[dbaccess.Outpoint{
			TransactionID: dbTransactionOutput.Transaction.TransactionID,
			Index:         dbTransactionOutput.Index,
		}] = dbTransactionOutput.ID
	}

	inputsToAdd := make([]interface{}, inputsCount)
	inputIndex := 0
	for _, transaction := range newNonCoinbaseTransactions {
		for i, txIn := range transaction.verboseTx.TransactionVerboseInputs {
			scriptSig, err := hex.DecodeString(txIn.ScriptSig.Hex)
			if err != nil {
				return nil
			}
			prevOutputID, ok := outpointsToIDs[dbaccess.Outpoint{
				TransactionID: txIn.TxID,
				Index:         txIn.OutputIndex,
			}]
			if !ok || prevOutputID == 0 {
				return errors.Errorf("couldn't find ID for outpoint (%s:%d)", txIn.TxID, txIn.OutputIndex)
			}
			inputsToAdd[inputIndex] = &dbmodels.TransactionInput{
				TransactionID:               transaction.id,
				PreviousTransactionOutputID: prevOutputID,
				Index:                       uint32(i),
				SignatureScript:             scriptSig,
				Sequence:                    serializer.Uint64ToBytes(txIn.Sequence),
			}
			inputIndex++
		}
	}
	return dbaccess.BulkInsert(dbTx, inputsToAdd)
}

func isTransactionCoinbase(transaction *appmessage.TransactionVerboseData) (bool, error) {
	subnetwork, err := subnetworkid.NewFromStr(transaction.SubnetworkID)
	if err != nil {
		return false, err
	}
	return subnetwork.IsEqual(subnetworkid.SubnetworkIDCoinbase), nil
}
