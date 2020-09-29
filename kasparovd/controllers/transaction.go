package controllers

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/kaspanet/kaspad/app/appmessage"
	"github.com/kaspanet/kasparov/database"

	"github.com/kaspanet/kasparov/apimodels"
	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/kaspadrpc"

	"github.com/kaspanet/kasparov/httpserverutils"
	"github.com/pkg/errors"

	"github.com/kaspanet/kaspad/infrastructure/network/rpcclient"
	"github.com/kaspanet/kaspad/util/daghash"
)

const maxGetTransactionsLimit = 1000

// GetTransactionByIDHandler returns a transaction by a given transaction ID.
func GetTransactionByIDHandler(txID string) (interface{}, error) {
	if bytes, err := hex.DecodeString(txID); err != nil || len(bytes) != daghash.TxIDSize {
		return nil, httpserverutils.NewHandlerError(http.StatusUnprocessableEntity,
			errors.Errorf("The given txid is not a hex-encoded %d-byte hash", daghash.TxIDSize))
	}

	tx, err := dbaccess.TransactionByID(database.NoTx(), txID, dbmodels.TransactionRecommendedPreloadedFields...)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, httpserverutils.NewHandlerError(http.StatusNotFound, errors.New("no transaction with the given txid was found"))
	}

	selectedTipBlueScore, err := dbaccess.SelectedTipBlueScore(database.NoTx())
	if err != nil {
		return nil, err
	}

	txResponse := apimodels.ConvertTxModelToTxResponse(tx, selectedTipBlueScore)
	return txResponse, nil
}

// GetTransactionByHashHandler returns a transaction by a given transaction hash.
func GetTransactionByHashHandler(txHash string) (interface{}, error) {
	if bytes, err := hex.DecodeString(txHash); err != nil || len(bytes) != daghash.HashSize {
		return nil, httpserverutils.NewHandlerError(http.StatusUnprocessableEntity,
			errors.Errorf("The given txhash is not a hex-encoded %d-byte hash", daghash.HashSize))
	}

	tx, err := dbaccess.TransactionByHash(database.NoTx(), txHash, dbmodels.TransactionRecommendedPreloadedFields...)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, httpserverutils.NewHandlerError(http.StatusNotFound, errors.New("no transaction with the given txhash was found"))
	}

	selectedTipBlueScore, err := dbaccess.SelectedTipBlueScore(database.NoTx())
	if err != nil {
		return nil, err
	}

	txResponse := apimodels.ConvertTxModelToTxResponse(tx, selectedTipBlueScore)
	return txResponse, nil
}

// GetTransactionsByAddressHandler searches for all transactions
// where the given address is either an input or an output.
func GetTransactionsByAddressHandler(address string, skip, limit int64) (interface{}, error) {
	if limit > maxGetTransactionsLimit || limit < 1 {
		return nil, httpserverutils.NewHandlerError(http.StatusBadRequest,
			errors.Errorf("limit higher than %d or lower than 1 was requested", maxGetTransactionsLimit))
	}

	if skip < 0 {
		return nil, httpserverutils.NewHandlerError(http.StatusBadRequest,
			errors.New("skip lower than 0 was requested"))
	}

	if err := validateAddress(address); err != nil {
		return nil, err
	}

	txs, err := dbaccess.TransactionsByAddress(database.NoTx(), address, dbaccess.OrderAscending, uint64(skip), uint64(limit),
		dbmodels.TransactionRecommendedPreloadedFields...)
	if err != nil {
		return nil, err
	}

	selectedTipBlueScore, err := dbaccess.SelectedTipBlueScore(database.NoTx())
	if err != nil {
		return nil, err
	}

	txResponses := make([]*apimodels.TransactionResponse, len(txs))
	for i, tx := range txs {
		txResponses[i] = apimodels.ConvertTxModelToTxResponse(tx, selectedTipBlueScore)
	}

	return txResponses, nil
}

// GetTransactionCountByAddressHandler returns the total
// number of transactions by address.
func GetTransactionCountByAddressHandler(address string) (interface{}, error) {
	if err := validateAddress(address); err != nil {
		return nil, err
	}

	return dbaccess.TransactionsByAddressCount(database.NoTx(), address)
}

// GetTransactionsByBlockHashHandler retrieves all transactions
// included by the block with the given blockHash.
func GetTransactionsByBlockHashHandler(blockHash string) (interface{}, error) {
	txs, err := dbaccess.TransactionsByBlockHash(database.NoTx(), blockHash, dbmodels.TransactionRecommendedPreloadedFields...)
	if err != nil {
		return nil, err
	}
	if len(txs) == 0 {
		return nil, httpserverutils.NewHandlerError(http.StatusNotFound, errors.New("no block with the given block hash was found"))
	}

	selectedTipBlueScore, err := dbaccess.SelectedTipBlueScore(database.NoTx())
	if err != nil {
		return nil, err
	}

	txResponses := make([]*apimodels.TransactionResponse, len(txs))
	for i, tx := range txs {
		txResponses[i] = apimodels.ConvertTxModelToTxResponse(tx, selectedTipBlueScore)
	}

	return apimodels.TransactionsResponse{
		Transactions: txResponses,
	}, nil
}

// GetTransactionDoubleSpends returns array of transactions that spend
// at least one of the same inputs as the given transaction
func GetTransactionDoubleSpends(txID string) (interface{}, error) {
	if bytes, err := hex.DecodeString(txID); err != nil || len(bytes) != daghash.TxIDSize {
		return nil, httpserverutils.NewHandlerError(http.StatusUnprocessableEntity,
			errors.Errorf("The given txid is not a hex-encoded %d-byte hash", daghash.TxIDSize))
	}

	txs, err := dbaccess.TransactionDoubleSpends(database.NoTx(), txID)
	if err != nil {
		return nil, err
	}

	selectedTipBlueScore, err := dbaccess.SelectedTipBlueScore(database.NoTx())
	if err != nil {
		return nil, err
	}

	txResponses := make([]*apimodels.TransactionResponse, len(txs))
	for i, tx := range txs {
		txResponses[i] = apimodels.ConvertTxModelToTxResponse(tx, selectedTipBlueScore)
	}

	return apimodels.TransactionDoubleSpendsResponse{
		Transactions: txResponses,
	}, nil
}

// PostTransaction forwards a raw transaction to the JSON-RPC API server
func PostTransaction(requestBody []byte) error {
	client, err := kaspadrpc.GetClient()
	if err != nil {
		return err
	}

	rawTx := &apimodels.RawTransaction{}
	err = json.Unmarshal(requestBody, rawTx)
	if err != nil {
		return httpserverutils.NewHandlerErrorWithCustomClientMessage(http.StatusUnprocessableEntity,
			errors.Wrap(err, "error unmarshalling request body"),
			"the request body is not json-formatted")
	}

	txBytes, err := hex.DecodeString(rawTx.RawTransaction)
	if err != nil {
		return httpserverutils.NewHandlerErrorWithCustomClientMessage(http.StatusUnprocessableEntity,
			errors.Wrap(err, "error decoding hex raw transaction"),
			"the raw transaction is not a hex-encoded transaction")
	}

	txReader := bytes.NewReader(txBytes)
	tx := &appmessage.MsgTx{}
	err = tx.KaspaDecode(txReader, 0)
	if err != nil {
		return httpserverutils.NewHandlerErrorWithCustomClientMessage(http.StatusUnprocessableEntity,
			errors.Wrap(err, "error decoding raw transaction"),
			"error decoding raw transaction")
	}

	_, err = client.SubmitTransaction(tx)
	if err != nil {
		if errors.Is(err, rpcclient.ErrRPC) {
			return httpserverutils.NewHandlerError(http.StatusUnprocessableEntity, err)
		}
		return err
	}
	return nil
}
