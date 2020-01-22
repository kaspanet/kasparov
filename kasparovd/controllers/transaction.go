package controllers

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/jsonrpc"
	"github.com/kaspanet/kasparov/kasparovd/apimodels"

	"github.com/kaspanet/kasparov/httpserverutils"
	"github.com/pkg/errors"

	"github.com/kaspanet/kaspad/rpcmodel"
	"github.com/kaspanet/kaspad/util/daghash"
	"github.com/kaspanet/kaspad/wire"
)

const maxGetTransactionsLimit = 1000

var txPreloadedColumns = []string{
	"AcceptingBlock",
	"Subnetwork",
	"TransactionOutputs",
	"TransactionOutputs.Address",
	"TransactionInputs.PreviousTransactionOutput.Transaction",
	"TransactionInputs.PreviousTransactionOutput.Address",
}

// GetTransactionByIDHandler returns a transaction by a given transaction ID.
func GetTransactionByIDHandler(txID string) (interface{}, error) {
	if bytes, err := hex.DecodeString(txID); err != nil || len(bytes) != daghash.TxIDSize {
		return nil, httpserverutils.NewHandlerError(http.StatusUnprocessableEntity,
			errors.Errorf("The given txid is not a hex-encoded %d-byte hash.", daghash.TxIDSize))
	}

	tx, err := dbaccess.GetTransactionByID(txID, txPreloadedColumns...)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, httpserverutils.NewHandlerError(http.StatusNotFound, errors.New("No transaction with the given id was found"))
	}

	return convertTxDBModelToTxResponse(tx), nil
}

// GetTransactionByHashHandler returns a transaction by a given transaction hash.
func GetTransactionByHashHandler(txHash string) (interface{}, error) {
	if bytes, err := hex.DecodeString(txHash); err != nil || len(bytes) != daghash.HashSize {
		return nil, httpserverutils.NewHandlerError(http.StatusUnprocessableEntity,
			errors.Errorf("The given txhash is not a hex-encoded %d-byte hash.", daghash.HashSize))
	}

	tx, err := dbaccess.GetTransactionByHash(txHash, txPreloadedColumns...)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, httpserverutils.NewHandlerError(http.StatusNotFound, errors.New("No transaction with the given hash was found"))
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

	if err := validateAddress(address); err != nil {
		return nil, err
	}

	txs, err := dbaccess.GetTransactionsByAddress(address, dbaccess.OrderAscending, skip, limit, txPreloadedColumns...)
	if err != nil {
		return nil, err
	}

	txResponses := make([]*apimodels.TransactionResponse, len(txs))
	for i, tx := range txs {
		txResponses[i] = convertTxDBModelToTxResponse(tx)
	}
	return txResponses, nil
}

// GetTransactionsByIDsHandler finds transactions by the given transactionIds.
func GetTransactionsByIDsHandler(transactionIds []string) ([]*apimodels.TransactionResponse, error) {
	txs, err := dbaccess.GetTransactionsByIDs(transactionIds, txPreloadedColumns...)
	if err != nil {
		return nil, err
	}

	txResponses := make([]*apimodels.TransactionResponse, len(txs))
	for i, tx := range txs {
		txResponses[i] = convertTxDBModelToTxResponse(tx)
	}
	return txResponses, nil
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
