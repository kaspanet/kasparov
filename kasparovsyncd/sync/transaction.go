package sync

import (
	"encoding/hex"
	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kaspad/blockdag"
	"github.com/kaspanet/kaspad/rpcmodel"
	"github.com/kaspanet/kaspad/util"
	"github.com/kaspanet/kaspad/util/daghash"
	"github.com/kaspanet/kaspad/util/subnetworkid"
	"github.com/kaspanet/kaspad/wire"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
	"github.com/kaspanet/kasparov/kasparovsyncd/utils"
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

func insertTransactions(dbTx *gorm.DB, blocks []*utils.RawAndVerboseBlock, subnetworkIDsToIDs map[string]uint64) (map[string]*txWithMetadata, error) {
	transactionIDsToTxsWithMetadata := make(map[string]*txWithMetadata)
	for _, block := range blocks {
		for _, transaction := range block.Verbose.RawTx {
			transactionIDsToTxsWithMetadata[transaction.TxID] = &txWithMetadata{
				verboseTx: &transaction,
			}
		}
	}

	transactionIDs := transactionIDsToTxsWithMetadataToTransactionIDs(transactionIDsToTxsWithMetadata)

	var dbTransactions []*dbmodels.Transaction
	dbResult := dbTx.
		Where("transaction_id in (?)", transactionIDs).
		Find(&dbTransactions)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find transactions: ", dbErrors)
	}

	for _, dbTransaction := range dbTransactions {
		transactionIDsToTxsWithMetadata[dbTransaction.TransactionID].id = dbTransaction.ID
		transactionIDsToTxsWithMetadata[dbTransaction.TransactionID].mass = dbTransaction.Mass
	}

	newTransactions := make([]string, 0)
	for txID, verboseTx := range transactionIDsToTxsWithMetadata {
		if verboseTx.id != 0 {
			continue
		}
		newTransactions = append(newTransactions, txID)
	}

	transactionsToAdd := make([]interface{}, len(newTransactions))
	for i, id := range newTransactions {
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
			LockTime:        verboseTx.LockTime,
			SubnetworkID:    subnetworkID,
			Gas:             verboseTx.Gas,
			PayloadHash:     verboseTx.PayloadHash,
			Payload:         payload,
			Mass:            mass,
		}
	}

	err := bulkInsert(dbTx, transactionsToAdd)
	if err != nil {
		return nil, err
	}

	var dbNewTransactions []*dbmodels.Transaction
	dbResult = dbTx.
		Where("transaction_id in (?)", newTransactions).
		Find(&dbNewTransactions)
	dbErrors = dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find transactions: ", dbErrors)
	}

	if len(dbNewTransactions) != len(newTransactions) {
		return nil, errors.New("couldn't add all new transactions")
	}

	for _, dbTransaction := range dbNewTransactions {
		transactionIDsToTxsWithMetadata[dbTransaction.TransactionID].id = dbTransaction.ID
		transactionIDsToTxsWithMetadata[dbTransaction.TransactionID].isNew = true
	}
	return transactionIDsToTxsWithMetadata, nil
}

func insertTransactionBlocks(dbTx *gorm.DB, blocks []*utils.RawAndVerboseBlock, blockHashesToIDs map[string]uint64, transactionIDsToTxsWithMetadata map[string]*txWithMetadata) error {
	transactionBlocksToAdd := make([]interface{}, 0)
	for _, block := range blocks {
		blockID, ok := blockHashesToIDs[block.Hash()]
		if !ok {
			return errors.Errorf("couldn't find block ID for block %s", block)
		}
		for i, tx := range block.Verbose.RawTx {
			transactionBlocksToAdd = append(transactionBlocksToAdd, &dbmodels.TransactionBlock{
				TransactionID: transactionIDsToTxsWithMetadata[tx.TxID].id,
				BlockID:       blockID,
				Index:         uint32(i),
			})
		}
	}
	return bulkInsert(dbTx, transactionBlocksToAdd)
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
	dbResult := dbTx.
		Joins("LEFT JOIN `transactions` ON `transactions`.`id` = `transaction_outputs`.`transaction_id`").
		Where("(`transactions`.`transaction_id`, `transaction_outputs`.`index`) IN (?)", outpoints).
		Preload("Transaction").
		Find(&dbPreviousTransactionsOutputs)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("failed to find previous transaction outputs: ", dbErrors)
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
	inputIterator := 0
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
			inputsToAdd[inputIterator] = &dbmodels.TransactionInput{
				TransactionID:               transaction.id,
				PreviousTransactionOutputID: prevOutputID,
				Index:                       uint32(i),
				SignatureScript:             scriptSig,
				Sequence:                    txIn.Sequence,
			}
			inputIterator++
		}
	}
	return bulkInsert(dbTx, inputsToAdd)
}

func insertTransactionOutputs(dbTx *gorm.DB, transactionIDsToTxsWithMetadata map[string]*txWithMetadata) error {
	addressesToAddressIDs, err := insertBlocksTransactionAddresses(dbTx, transactionIDsToTxsWithMetadata)
	if err != nil {
		return err
	}

	outputsToAdd := make([]interface{}, 0)
	for _, transaction := range transactionIDsToTxsWithMetadata {
		if !transaction.isNew {
			continue
		}
		for i, txOut := range transaction.verboseTx.Vout {
			scriptPubKey, err := hex.DecodeString(txOut.ScriptPubKey.Hex)
			if err != nil {
				return errors.WithStack(err)
			}
			var addressID *uint64
			if txOut.ScriptPubKey.Address != nil {
				addressID = rpcmodel.Uint64(addressesToAddressIDs[*txOut.ScriptPubKey.Address])
			}
			outputsToAdd = append(outputsToAdd, &dbmodels.TransactionOutput{
				TransactionID: transaction.id,
				Index:         uint32(i),
				Value:         txOut.Value,
				IsSpent:       false, // This must be false for updateSelectedParentChain to work properly
				ScriptPubKey:  scriptPubKey,
				AddressID:     addressID,
			})
		}
	}

	return bulkInsert(dbTx, outputsToAdd)
}

func isTransactionCoinbase(transaction *rpcmodel.TxRawResult) (bool, error) {
	subnetwork, err := subnetworkid.NewFromStr(transaction.Subnetwork)
	if err != nil {
		return false, err
	}
	return subnetwork.IsEqual(subnetworkid.SubnetworkIDCoinbase), nil
}

func calcTxMass(dbTx *gorm.DB, transaction *rpcmodel.TxRawResult) (uint64, error) {
	msgTx, err := convertTxRawResultToMsgTx(transaction)
	if err != nil {
		return 0, err
	}
	prevTxIDs := make([]string, len(transaction.Vin))
	for i, txIn := range transaction.Vin {
		prevTxIDs[i] = txIn.TxID
	}
	var prevDBTransactionsOutputs []dbmodels.TransactionOutput
	dbResult := dbTx.
		Joins("LEFT JOIN `transactions` ON `transactions`.`id` = `transaction_outputs`.`transaction_id`").
		Where("transactions.transaction_id in (?)", prevTxIDs).
		Preload("Transaction").
		Find(&prevDBTransactionsOutputs)
	dbErrors := dbResult.GetErrors()
	if len(dbErrors) > 0 {
		return 0, httpserverutils.NewErrorFromDBErrors("error fetching previous transactions: ", dbErrors)
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
