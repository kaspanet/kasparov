package sync

import (
	"encoding/hex"
	"github.com/kaspanet/kasparov/serializer"

	"github.com/kaspanet/kaspad/blockdag"
	"github.com/kaspanet/kaspad/rpcmodel"
	"github.com/kaspanet/kaspad/util"
	"github.com/kaspanet/kaspad/util/daghash"
	"github.com/kaspanet/kaspad/util/subnetworkid"
	"github.com/kaspanet/kaspad/wire"
	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"

	"github.com/pkg/errors"
)

type txWithMetadata struct {
	verboseTx *rpcmodel.TxRawResult
	id        uint64
	isNew     bool
	mass      uint64
}

func transactionIDsToTxsWithMetadataToTransactionIDs(transactionIDsToTxsWithMetadata map[string]*txWithMetadata) []string {
	transactionIDs := make([]string, len(transactionIDsToTxsWithMetadata))
	i := 0
	for txID := range transactionIDsToTxsWithMetadata {
		transactionIDs[i] = txID
		i++
	}
	return transactionIDs
}

func insertRawTransactions(dbTx *dbaccess.TxContext, transactionIDsToTxsWithMetadata map[string]*txWithMetadata) error {
	rawTransactionsToAdd := make([]interface{}, 0)
	for _, transaction := range transactionIDsToTxsWithMetadata {
		if !transaction.isNew {
			continue
		}
		verboseTx := transaction.verboseTx
		txData, err := hex.DecodeString(verboseTx.Hex)
		if err != nil {
			return err
		}
		rawTransactionsToAdd = append(rawTransactionsToAdd, dbmodels.RawTransaction{
			TransactionID:   transaction.id,
			Transaction:     dbmodels.Transaction{},
			TransactionData: txData,
		})
	}
	return dbaccess.BulkInsert(dbTx, rawTransactionsToAdd)
}

func insertTransactions(dbTx *dbaccess.TxContext, blocks []*rawAndVerboseBlock, subnetworkIDsToIDs map[string]uint64) (
	map[string]*txWithMetadata, error) {

	transactionIDsToTxsWithMetadata := make(map[string]*txWithMetadata)
	for _, block := range blocks {
		// We do not directly iterate over block.Verbose.RawTx because it is a slice of values, and iterating
		// over such will re-use the same address, making all pointers pointing into it point to the same address
		for i := range block.Verbose.RawTx {
			transaction := &block.Verbose.RawTx[i]
			transactionIDsToTxsWithMetadata[transaction.TxID] = &txWithMetadata{
				verboseTx: transaction,
			}
		}
	}

	transactionIDs := transactionIDsToTxsWithMetadataToTransactionIDs(transactionIDsToTxsWithMetadata)

	dbTransactions, err := dbaccess.TransactionsByIDs(dbTx, transactionIDs)
	if err != nil {
		return nil, err
	}

	for _, dbTransaction := range dbTransactions {
		transactionIDsToTxsWithMetadata[dbTransaction.TransactionID].id = dbTransaction.ID
		transactionIDsToTxsWithMetadata[dbTransaction.TransactionID].mass = dbTransaction.Mass
	}

	newTransactionIDs := make([]string, 0)
	for txID, transaction := range transactionIDsToTxsWithMetadata {
		if transaction.id != 0 {
			continue
		}
		newTransactionIDs = append(newTransactionIDs, txID)
	}

	transactionsToAdd := make([]interface{}, len(newTransactionIDs))
	for i, id := range newTransactionIDs {
		verboseTx := transactionIDsToTxsWithMetadata[id].verboseTx
		mass, err := calcTxMass(dbTx, verboseTx)
		if err != nil {
			return nil, err
		}
		transactionIDsToTxsWithMetadata[id].mass = mass

		payload, err := hex.DecodeString(verboseTx.Payload)
		if err != nil {
			return nil, err
		}

		subnetworkID, ok := subnetworkIDsToIDs[verboseTx.Subnetwork]
		if !ok {
			return nil, errors.Errorf("couldn't find ID for subnetwork %s", verboseTx.Subnetwork)
		}

		transactionsToAdd[i] = dbmodels.Transaction{
			TransactionHash: verboseTx.Hash,
			TransactionID:   verboseTx.TxID,
			LockTime:        serializer.Uint64ToBytes(verboseTx.LockTime),
			SubnetworkID:    subnetworkID,
			Gas:             verboseTx.Gas,
			PayloadHash:     verboseTx.PayloadHash,
			Payload:         payload,
			Mass:            mass,
			Version:         verboseTx.Version,
		}
	}

	err = dbaccess.BulkInsert(dbTx, transactionsToAdd)
	if err != nil {
		return nil, err
	}

	dbNewTransactions, err := dbaccess.TransactionsByIDs(dbTx, newTransactionIDs)
	if err != nil {
		return nil, err
	}

	if len(dbNewTransactions) != len(newTransactionIDs) {
		return nil, errors.New("couldn't add all new transactions")
	}

	for _, dbTransaction := range dbNewTransactions {
		transactionIDsToTxsWithMetadata[dbTransaction.TransactionID].id = dbTransaction.ID
		transactionIDsToTxsWithMetadata[dbTransaction.TransactionID].isNew = true
	}

	return transactionIDsToTxsWithMetadata, nil
}

func calcTxMass(dbTx *dbaccess.TxContext, transaction *rpcmodel.TxRawResult) (uint64, error) {
	msgTx, err := convertTxRawResultToMsgTx(transaction)
	if err != nil {
		return 0, err
	}

	outpoints := make([]*dbaccess.Outpoint, 0, len(transaction.Vin))
	for _, txIn := range transaction.Vin {
		outpoints = append(outpoints, &dbaccess.Outpoint{
			TransactionID: txIn.TxID,
			Index:         txIn.Vout,
		})
	}
	prevDBTransactionsOutputs, err := dbaccess.TransactionOutputsByOutpoints(dbTx, outpoints)
	if err != nil {
		return 0, err
	}

	prevScriptPubKeysMap := make(map[string]map[uint32][]byte)
	for _, prevDBTransactionsOutput := range prevDBTransactionsOutputs {
		txID := prevDBTransactionsOutput.Transaction.TransactionID
		if prevScriptPubKeysMap[txID] == nil {
			prevScriptPubKeysMap[txID] = make(map[uint32][]byte)
		}
		prevScriptPubKeysMap[txID][prevDBTransactionsOutput.Index] = prevDBTransactionsOutput.ScriptPubKey
	}
	orderedPrevScriptPubKeys := make([][]byte, len(transaction.Vin))
	for i, txIn := range transaction.Vin {
		orderedPrevScriptPubKeys[i] = prevScriptPubKeysMap[txIn.TxID][uint32(i)]
	}
	return blockdag.CalcTxMass(util.NewTx(msgTx), orderedPrevScriptPubKeys), nil
}

func convertTxRawResultToMsgTx(tx *rpcmodel.TxRawResult) (*wire.MsgTx, error) {
	txIns := make([]*wire.TxIn, len(tx.Vin))
	for i, txIn := range tx.Vin {
		prevTxID, err := daghash.NewTxIDFromStr(txIn.TxID)
		if err != nil {
			return nil, err
		}
		signatureScript, err := hex.DecodeString(txIn.ScriptSig.Hex)
		if err != nil {
			return nil, err
		}
		txIns[i] = &wire.TxIn{
			PreviousOutpoint: wire.Outpoint{
				TxID:  *prevTxID,
				Index: txIn.Vout,
			},
			SignatureScript: signatureScript,
			Sequence:        txIn.Sequence,
		}
	}
	txOuts := make([]*wire.TxOut, len(tx.Vout))
	for i, txOut := range tx.Vout {
		scriptPubKey, err := hex.DecodeString(txOut.ScriptPubKey.Hex)
		if err != nil {
			return nil, err
		}
		txOuts[i] = &wire.TxOut{
			Value:        txOut.Value,
			ScriptPubKey: scriptPubKey,
		}
	}
	subnetworkID, err := subnetworkid.NewFromStr(tx.Subnetwork)
	if err != nil {
		return nil, err
	}
	if subnetworkID.IsEqual(subnetworkid.SubnetworkIDNative) {
		return wire.NewNativeMsgTx(tx.Version, txIns, txOuts), nil
	}
	payload, err := hex.DecodeString(tx.Payload)
	if err != nil {
		return nil, err
	}
	return wire.NewSubnetworkMsgTx(tx.Version, txIns, txOuts, subnetworkID, tx.Gas, payload), nil
}
