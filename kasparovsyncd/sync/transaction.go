package sync

import (
	"encoding/hex"
	"github.com/kaspanet/kasparov/database"
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

func transactionHashesToTxsWithMetadataToTransactionHashes(transactionHashesToTxsWithMetadata map[string]*txWithMetadata) []string {
	hashes := make([]string, len(transactionHashesToTxsWithMetadata))
	i := 0
	for hash := range transactionHashesToTxsWithMetadata {
		hashes[i] = hash
		i++
	}
	return hashes
}

func insertRawTransactions(dbTx *database.TxContext, transactionHashesToTxsWithMetadata map[string]*txWithMetadata) error {
	rawTransactionsToAdd := make([]interface{}, 0)
	for _, transaction := range transactionHashesToTxsWithMetadata {
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

func insertTransactions(dbTx *database.TxContext, blocks []*rawAndVerboseBlock, subnetworkIDsToIDs map[string]uint64) (
	map[string]*txWithMetadata, error) {

	transactionHashesToTxsWithMetadata := make(map[string]*txWithMetadata)
	for _, block := range blocks {
		// We do not directly iterate over block.Verbose.RawTx because it is a slice of values, and iterating
		// over such will re-use the same address, making all pointers pointing into it point to the same address
		for i := range block.Verbose.RawTx {
			transaction := &block.Verbose.RawTx[i]
			transactionHashesToTxsWithMetadata[transaction.Hash] = &txWithMetadata{
				verboseTx: transaction,
			}
		}
	}

	transactionHashes := transactionHashesToTxsWithMetadataToTransactionHashes(transactionHashesToTxsWithMetadata)

	dbTransactions, err := dbaccess.TransactionsByHashes(dbTx, transactionHashes)
	if err != nil {
		return nil, err
	}

	for _, dbTransaction := range dbTransactions {
		transactionHashesToTxsWithMetadata[dbTransaction.TransactionHash].id = dbTransaction.ID
		transactionHashesToTxsWithMetadata[dbTransaction.TransactionHash].mass = dbTransaction.Mass
	}

	newTransactionHashes := make([]string, 0)
	for hash, transaction := range transactionHashesToTxsWithMetadata {
		if transaction.id != 0 {
			continue
		}
		newTransactionHashes = append(newTransactionHashes, hash)
	}

	transactionsToAdd := make([]interface{}, len(newTransactionHashes))
	for i, hash := range newTransactionHashes {
		verboseTx := transactionHashesToTxsWithMetadata[hash].verboseTx
		mass, err := calcTxMass(dbTx, verboseTx)
		if err != nil {
			return nil, err
		}
		transactionHashesToTxsWithMetadata[hash].mass = mass

		payload, err := hex.DecodeString(verboseTx.Payload)
		if err != nil {
			return nil, err
		}

		subnetworkID, ok := subnetworkIDsToIDs[verboseTx.Subnetwork]
		if !ok {
			return nil, errors.Errorf("couldn't find ID for subnetwork %s", verboseTx.Subnetwork)
		}

		transactionsToAdd[i] = &dbmodels.Transaction{
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

	dbNewTransactions, err := dbaccess.TransactionsByHashes(dbTx, newTransactionHashes)
	if err != nil {
		return nil, err
	}

	if len(dbNewTransactions) != len(newTransactionHashes) {
		return nil, errors.New("couldn't add all new transactions")
	}

	for _, dbTransaction := range dbNewTransactions {
		transactionHashesToTxsWithMetadata[dbTransaction.TransactionHash].id = dbTransaction.ID
		transactionHashesToTxsWithMetadata[dbTransaction.TransactionHash].isNew = true
	}

	return transactionHashesToTxsWithMetadata, nil
}

func calcTxMass(dbTx *database.TxContext, transaction *rpcmodel.TxRawResult) (uint64, error) {
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
