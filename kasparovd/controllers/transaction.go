package controllers

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/kaspanet/kaspad/util"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/jsonrpc"
	"github.com/kaspanet/kasparov/kasparovd/apimodels"
	"github.com/kaspanet/kasparov/kasparovd/config"

	"github.com/kaspanet/kaspad/blockdag"
	"github.com/kaspanet/kaspad/util/subnetworkid"
	"github.com/kaspanet/kasparov/httpserverutils"
	"github.com/pkg/errors"

	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kaspad/rpcmodel"
	"github.com/kaspanet/kaspad/util/daghash"
	"github.com/kaspanet/kaspad/wire"
)

const maxGetTransactionsLimit = 1000

// GetTransactionByIDHandler returns a transaction by a given transaction ID.
func GetTransactionByIDHandler(txID string) (interface{}, error) {
	if bytes, err := hex.DecodeString(txID); err != nil || len(bytes) != daghash.TxIDSize {
		return nil, httpserverutils.NewHandlerError(http.StatusUnprocessableEntity,
			errors.Errorf("The given txid is not a hex-encoded %d-byte hash.", daghash.TxIDSize))
	}

	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	tx := &dbmodels.Transaction{}
	query := db.Where(&dbmodels.Transaction{TransactionID: txID})
	dbResult := addTxPreloadedFields(query).First(&tx)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.IsDBRecordNotFoundError(dbErrors) {
		return nil, httpserverutils.NewHandlerError(http.StatusNotFound, errors.New("No transaction with the given txid was found"))
	}
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading transaction from the database:", dbErrors)
	}
	return convertTxDBModelToTxResponse(tx), nil
}

// GetTransactionByHashHandler returns a transaction by a given transaction hash.
func GetTransactionByHashHandler(txHash string) (interface{}, error) {
	if bytes, err := hex.DecodeString(txHash); err != nil || len(bytes) != daghash.HashSize {
		return nil, httpserverutils.NewHandlerError(http.StatusUnprocessableEntity,
			errors.Errorf("The given txhash is not a hex-encoded %d-byte hash.", daghash.HashSize))
	}

	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	tx := &dbmodels.Transaction{}
	query := db.Where(&dbmodels.Transaction{TransactionHash: txHash})
	dbResult := addTxPreloadedFields(query).First(&tx)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.IsDBRecordNotFoundError(dbErrors) {
		return nil, httpserverutils.NewHandlerError(http.StatusNotFound, errors.Errorf("No transaction with the given txhash was found."))
	}
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading transaction from the database:", dbErrors)
	}
	return convertTxDBModelToTxResponse(tx), nil
}

// GetTransactionsByAddressHandler searches for all transactions
// where the given address is either an input or an output.
func GetTransactionsByAddressHandler(address string, skip uint64, limit uint64) (interface{}, error) {
	if limit < 1 || limit > maxGetTransactionsLimit {
		return nil, httpserverutils.NewHandlerError(http.StatusBadRequest,
			errors.Errorf("Limit higher than %d or lower than 1 was requested.", maxGetTransactionsLimit))
	}

	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	txs := []*dbmodels.Transaction{}
	query := joinTxInputsTxOutputsAndAddresses(db).
		Where("`out_addresses`.`address` = ?", address).
		Or("`in_addresses`.`address` = ?", address).
		Limit(limit).
		Offset(skip).
		Order("`transactions`.`id` ASC")
	dbResult := addTxPreloadedFields(query).Find(&txs)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading transactions from the database:", dbErrors)
	}
	txResponses := make([]*apimodels.TransactionResponse, len(txs))
	for i, tx := range txs {
		txResponses[i] = convertTxDBModelToTxResponse(tx)
	}
	return txResponses, nil
}

func fetchSelectedTip() (*dbmodels.Block, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}
	block := &dbmodels.Block{}
	dbResult := db.Order("blue_score DESC").
		Where(&dbmodels.Block{IsChainBlock: true}).
		First(block)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading transactions from the database:", dbErrors)
	}
	return block, nil
}

func areTxsInBlock(blockID uint64, txIDs []uint64) (map[uint64]bool, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}
	transactionBlocks := []*dbmodels.TransactionBlock{}
	dbErrors := db.
		Where(&dbmodels.TransactionBlock{BlockID: blockID}).
		Where("transaction_id in (?)", txIDs).
		Find(&transactionBlocks).GetErrors()

	if len(dbErrors) > 0 {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading UTXOs from the database:", dbErrors)
	}

	isInBlock := make(map[uint64]bool)
	for _, transactionBlock := range transactionBlocks {
		isInBlock[transactionBlock.TransactionID] = true
	}
	return isInBlock, nil
}

// GetUTXOsByAddressHandler searches for all UTXOs that belong to a certain address.
func GetUTXOsByAddressHandler(address string) (interface{}, error) {
	_, err := util.DecodeAddress(address, config.ActiveConfig().ActiveNetParams.Prefix)
	if err != nil {
		return nil, httpserverutils.NewHandlerErrorWithCustomClientMessage(http.StatusUnprocessableEntity,
			errors.Wrap(err, "error decoding address"),
			"The given address is not a well-formatted P2PKH or P2SH address.")
	}

	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	var transactionOutputs []*dbmodels.TransactionOutput
	dbErrors := db.
		Joins("LEFT JOIN `addresses` ON `addresses`.`id` = `transaction_outputs`.`address_id`").
		Where("`addresses`.`address` = ? AND `transaction_outputs`.`is_spent` = 0", address).
		Preload("Transaction.AcceptingBlock").
		Preload("Transaction.Subnetwork").
		Find(&transactionOutputs).GetErrors()
	if len(dbErrors) > 0 {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading UTXOs from the database:", dbErrors)
	}

	nonAcceptedTxIds := make([]uint64, len(transactionOutputs))
	for i, txOut := range transactionOutputs {
		if txOut.Transaction.AcceptingBlock == nil {
			nonAcceptedTxIds[i] = txOut.TransactionID
		}
	}

	var selectedTip *dbmodels.Block
	var isTxInSelectedTip map[uint64]bool
	if len(nonAcceptedTxIds) != 0 {
		selectedTip, err = fetchSelectedTip()
		if err != nil {
			return nil, err
		}

		isTxInSelectedTip, err = areTxsInBlock(selectedTip.ID, nonAcceptedTxIds)
		if err != nil {
			return nil, err
		}
	}

	activeNetParams := config.ActiveConfig().NetParams()

	UTXOsResponses := make([]*apimodels.TransactionOutputResponse, len(transactionOutputs))
	for i, transactionOutput := range transactionOutputs {
		subnetworkID := &subnetworkid.SubnetworkID{}
		err := subnetworkid.Decode(subnetworkID, transactionOutput.Transaction.Subnetwork.SubnetworkID)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("Couldn't decode subnetwork id %s", transactionOutput.Transaction.Subnetwork.SubnetworkID))
		}
		var acceptingBlockHash *string
		var confirmations uint64
		acceptingBlockBlueScore := blockdag.UnacceptedBlueScore
		if isTxInSelectedTip[transactionOutput.ID] {
			confirmations = 1
		} else if transactionOutput.Transaction.AcceptingBlock != nil {
			acceptingBlockHash = rpcmodel.String(transactionOutput.Transaction.AcceptingBlock.BlockHash)
			acceptingBlockBlueScore = transactionOutput.Transaction.AcceptingBlock.BlueScore
			confirmations = selectedTip.BlueScore - acceptingBlockBlueScore + 2
		}
		isCoinbase := subnetworkID.IsEqual(subnetworkid.SubnetworkIDCoinbase)
		UTXOsResponses[i] = &apimodels.TransactionOutputResponse{
			TransactionID:           transactionOutput.Transaction.TransactionID,
			Value:                   transactionOutput.Value,
			ScriptPubKey:            hex.EncodeToString(transactionOutput.ScriptPubKey),
			AcceptingBlockHash:      acceptingBlockHash,
			AcceptingBlockBlueScore: acceptingBlockBlueScore,
			Index:                   transactionOutput.Index,
			IsCoinbase:              rpcmodel.Bool(isCoinbase),
			Confirmations:           rpcmodel.Uint64(confirmations),
			IsSpendable:             rpcmodel.Bool(!isCoinbase || confirmations >= activeNetParams.BlockCoinbaseMaturity),
		}
	}
	return UTXOsResponses, nil
}

func joinTxInputsTxOutputsAndAddresses(query *gorm.DB) *gorm.DB {
	return query.
		Joins("LEFT JOIN `transaction_outputs` ON `transaction_outputs`.`transaction_id` = `transactions`.`id`").
		Joins("LEFT JOIN `addresses` AS `out_addresses` ON `out_addresses`.`id` = `transaction_outputs`.`address_id`").
		Joins("LEFT JOIN `transaction_inputs` ON `transaction_inputs`.`transaction_id` = `transactions`.`id`").
		Joins("LEFT JOIN `transaction_outputs` AS `inputs_outs` ON `inputs_outs`.`id` = `transaction_inputs`.`previous_transaction_output_id`").
		Joins("LEFT JOIN `addresses` AS `in_addresses` ON `in_addresses`.`id` = `inputs_outs`.`address_id`")
}

func addTxPreloadedFields(query *gorm.DB) *gorm.DB {
	return query.Preload("AcceptingBlock").
		Preload("Subnetwork").
		Preload("TransactionOutputs").
		Preload("TransactionOutputs.Address").
		Preload("TransactionInputs.PreviousTransactionOutput.Transaction").
		Preload("TransactionInputs.PreviousTransactionOutput.Address")
}

// PostTransaction forwards a raw transaction to the JSON-RPC API server
func PostTransaction(requestBody []byte) error {
	client, err := jsonrpc.GetClient()
	if err != nil {
		return err
	}

	rawTx := &apimodels.RawTransaction{}
	err = json.Unmarshal(requestBody, rawTx)
	if err != nil {
		return httpserverutils.NewHandlerErrorWithCustomClientMessage(http.StatusUnprocessableEntity,
			errors.Wrap(err, "Error unmarshalling request body"),
			"The request body is not json-formatted")
	}

	txBytes, err := hex.DecodeString(rawTx.RawTransaction)
	if err != nil {
		return httpserverutils.NewHandlerErrorWithCustomClientMessage(http.StatusUnprocessableEntity,
			errors.Wrap(err, "Error decoding hex raw transaction"),
			"The raw transaction is not a hex-encoded transaction")
	}

	txReader := bytes.NewReader(txBytes)
	tx := &wire.MsgTx{}
	err = tx.KaspaDecode(txReader, 0)
	if err != nil {
		return httpserverutils.NewHandlerErrorWithCustomClientMessage(http.StatusUnprocessableEntity,
			errors.Wrap(err, "Error decoding raw transaction"),
			"Error decoding raw transaction")
	}

	_, err = client.SendRawTransaction(tx, true)
	if err != nil {
		switch err := errors.Cause(err).(type) {
		case *rpcmodel.RPCError:
			return httpserverutils.NewHandlerError(http.StatusUnprocessableEntity, err)
		default:
			return err
		}
	}
	return nil
}

// GetTransactionsByIDsHandler finds transactions by the given transactionIds.
func GetTransactionsByIDsHandler(transactionIds []string) ([]*apimodels.TransactionResponse, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	var txs []*dbmodels.Transaction
	query := joinTxInputsTxOutputsAndAddresses(db).
		Where("`transactions`.`transaction_id` IN (?)", transactionIds)

	dbResult := addTxPreloadedFields(query).Find(&txs)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading transactions from the database:", dbErrors)
	}

	txResponses := make([]*apimodels.TransactionResponse, len(txs))
	for i, tx := range txs {
		txResponses[i] = convertTxDBModelToTxResponse(tx)
	}
	return txResponses, nil
}
