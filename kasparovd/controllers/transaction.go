package controllers

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/kaspanet/kasparov/apimodels"
	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/jsonrpc"

	"github.com/kaspanet/kasparov/httpserverutils"
	"github.com/pkg/errors"

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

	tx, err := dbaccess.TransactionByID(dbaccess.NoTx(), txID, dbmodels.TransactionRecommendedPreloadedFields...)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, httpserverutils.NewHandlerError(http.StatusNotFound, errors.New("no transaction with the given txid was found"))
	}

	selectedTipBlueScore, err := dbaccess.SelectedTipBlueScore(dbaccess.NoTx())
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
			errors.Errorf("The given txhash is not a hex-encoded %d-byte hash.", daghash.HashSize))
	}

	tx, err := dbaccess.TransactionByHash(dbaccess.NoTx(), txHash, dbmodels.TransactionRecommendedPreloadedFields...)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, httpserverutils.NewHandlerError(http.StatusNotFound, errors.New("no transaction with the given txhash was found"))
	}

	selectedTipBlueScore, err := dbaccess.SelectedTipBlueScore(dbaccess.NoTx())
	if err != nil {
		return nil, err
	}

	txResponse := apimodels.ConvertTxModelToTxResponse(tx, selectedTipBlueScore)
	return txResponse, nil
}

// GetTransactionsByAddressHandler searches for all transactions
// where the given address is either an input or an output.
func GetTransactionsByAddressHandler(address string, skip uint64, limit uint64) (interface{}, error) {
	if limit > maxGetTransactionsLimit {
		return nil, httpserverutils.NewHandlerError(http.StatusBadRequest,
			errors.Errorf("limit higher than %d was requested.", maxGetTransactionsLimit))
	}

	if err := validateAddress(address); err != nil {
		return nil, err
	}

	txs, err := dbaccess.TransactionsByAddress(dbaccess.NoTx(), address, dbaccess.OrderAscending, skip, limit,
		dbmodels.TransactionRecommendedPreloadedFields...)
	if err != nil {
		return nil, err
	}

	selectedTipBlueScore, err := dbaccess.SelectedTipBlueScore(dbaccess.NoTx())
	if err != nil {
		return nil, err
	}

	txResponses := make([]*apimodels.TransactionResponse, len(txs))
	for i, tx := range txs {
		txResponses[i] = apimodels.ConvertTxModelToTxResponse(tx, selectedTipBlueScore)
	}

	total, err := dbaccess.TransactionsByAddressCount(dbaccess.NoTx(), address)
	if err != nil {
		return nil, err
	}

	return apimodels.PaginatedTransactionsResponse{
		Transactions: txResponses,
		Total:        total,
	}, nil
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
	tx := &wire.MsgTx{}
	err = tx.KaspaDecode(txReader, 0)
	if err != nil {
		return httpserverutils.NewHandlerErrorWithCustomClientMessage(http.StatusUnprocessableEntity,
			errors.Wrap(err, "error decoding raw transaction"),
			"error decoding raw transaction")
	}

	_, err = client.SendRawTransaction(tx, true)
	if err != nil {
		if rpcErr := &(rpcmodel.RPCError{}); errors.As(err, &rpcErr) {
			return httpserverutils.NewHandlerError(http.StatusUnprocessableEntity, err)
		}
		return err
	}
	return nil
}
