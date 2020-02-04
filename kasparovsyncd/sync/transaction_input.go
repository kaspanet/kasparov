package sync

import (
	"encoding/hex"

	"github.com/kaspanet/kaspad/rpcmodel"
	"github.com/kaspanet/kaspad/util/subnetworkid"
	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/pkg/errors"
)

func insertTransactionInputs(dbTx *dbaccess.TxContext, transactionIDsToTxsWithMetadata map[string]*txWithMetadata) error {
	outpointsSet := make(map[dbaccess.Outpoint]struct{})
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
		for i := range transaction.verboseTx.Vin {
			txIn := transaction.verboseTx.Vin[i]
			outpoint := dbaccess.Outpoint{
				TransactionID: txIn.TxID,
				Index:         txIn.Vout,
			}
			outpointsSet[outpoint] = struct{}{}
		}
	}

	if inputsCount == 0 {
		return nil
	}
	outpoints := make([]*dbaccess.Outpoint, 0, len(outpointsSet))
	for outpoint := range outpointsSet {
		outpointCopy := outpoint // since outpoint is a value type - copy it, othewise it would be overwritten
		outpoints = append(outpoints, &outpointCopy)
	}

	dbPreviousTransactionsOutputs, err := dbaccess.TransactionOutputsByOutpoints(dbTx, outpoints)
	if err != nil {
		return err
	}

	if len(dbPreviousTransactionsOutputs) != len(outpoints) {
		log.Infof("len(dbPreviousTransactionsOutputs): %d,  len(outpoints): %d", len(dbPreviousTransactionsOutputs), len(outpoints))
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
		for i, txIn := range transaction.verboseTx.Vin {
			scriptSig, err := hex.DecodeString(txIn.ScriptSig.Hex)
			if err != nil {
				return nil
			}
			prevOutputID, ok := outpointsToIDs[dbaccess.Outpoint{
				TransactionID: txIn.TxID,
				Index:         txIn.Vout,
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
	return dbaccess.BulkInsert(dbTx, inputsToAdd)
}

func isTransactionCoinbase(transaction *rpcmodel.TxRawResult) (bool, error) {
	subnetwork, err := subnetworkid.NewFromStr(transaction.Subnetwork)
	if err != nil {
		return false, err
	}
	return subnetwork.IsEqual(subnetworkid.SubnetworkIDCoinbase), nil
}
