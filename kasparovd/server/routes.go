package server

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/httpserverutils"
	"github.com/kaspanet/kasparov/kasparovd/controllers"
	"github.com/pkg/errors"

	"github.com/gorilla/mux"
)

const (
	routeParamTxID      = "txID"
	routeParamTxHash    = "txHash"
	routeParamAddress   = "address"
	routeParamBlockHash = "blockHash"
)

const (
	queryParamSkip  = "skip"
	queryParamLimit = "limit"
	queryParamOrder = "order"
)

const (
	defaultGetTransactionsLimit = 100
	defaultGetBlocksLimit       = 25
	defaultGetBlocksOrder       = string(dbaccess.OrderDescending)
)

func mainHandler(_ *httpserverutils.ServerContext, _ *http.Request, _ map[string]string, _ map[string]string, _ []byte) (interface{}, error) {
	return struct {
		Message string `json:"message"`
	}{
		Message: "Kasparov server is running",
	}, nil
}

func addRoutes(router *mux.Router) {
	router.HandleFunc("/", httpserverutils.MakeHandler(mainHandler))

	router.HandleFunc(
		fmt.Sprintf("/transaction/id/{%s}", routeParamTxID),
		httpserverutils.MakeHandler(getTransactionByIDHandler)).
		Methods("GET")

	router.HandleFunc(
		fmt.Sprintf("/transaction/hash/{%s}", routeParamTxHash),
		httpserverutils.MakeHandler(getTransactionByHashHandler)).
		Methods("GET")

	router.HandleFunc(
		fmt.Sprintf("/transactions/address/{%s}", routeParamAddress),
		httpserverutils.MakeHandler(getTransactionsByAddressHandler)).
		Methods("GET")

	router.HandleFunc(
		fmt.Sprintf("/transactions/address/{%s}/count", routeParamAddress),
		httpserverutils.MakeHandler(getTransactionCountByAddressHandler)).
		Methods("GET")

	router.HandleFunc(
		fmt.Sprintf("/transactions/block/{%s}", routeParamBlockHash),
		httpserverutils.MakeHandler(getTransactionsByBlockHashHandler)).
		Methods("GET")

	router.HandleFunc(
		fmt.Sprintf("/transaction/id/{%s}/doublespends", routeParamTxID),
		httpserverutils.MakeHandler(getTransactionDoubleSpendsHandler)).
		Methods("GET")

	router.HandleFunc(
		fmt.Sprintf("/utxos/address/{%s}", routeParamAddress),
		httpserverutils.MakeHandler(getUTXOsByAddressHandler)).
		Methods("GET")

	router.HandleFunc(
		fmt.Sprintf("/block/{%s}", routeParamBlockHash),
		httpserverutils.MakeHandler(getBlockByHashHandler)).
		Methods("GET")

	router.HandleFunc(
		"/blocks",
		httpserverutils.MakeHandler(getBlocksHandler)).
		Methods("GET")

	router.HandleFunc(
		"/blocks/count",
		httpserverutils.MakeHandler(getBlockCountHandler)).
		Methods("GET")

	router.HandleFunc(
		"/fee-estimates",
		httpserverutils.MakeHandler(getFeeEstimatesHandler)).
		Methods("GET")

	router.HandleFunc(
		"/transaction",
		httpserverutils.MakeHandler(postTransactionHandler)).
		Methods("POST")
}

func convertQueryParamToInt64(queryParams map[string]string, param string, defaultValue int64) (int64, error) {
	if _, ok := queryParams[param]; ok {
		int64Value, err := strconv.ParseInt(queryParams[param], 10, 64)
		if err != nil {
			errorMessage := fmt.Sprintf("Couldn't parse the '%s' query parameter", param)
			return 0, httpserverutils.NewHandlerErrorWithCustomClientMessage(
				http.StatusUnprocessableEntity,
				errors.Wrap(err, errorMessage),
				errorMessage)
		}
		return int64Value, nil
	}
	return defaultValue, nil
}

func getTransactionByIDHandler(_ *httpserverutils.ServerContext, _ *http.Request, routeParams map[string]string, _ map[string]string,
	_ []byte) (interface{}, error) {

	return controllers.GetTransactionByIDHandler(routeParams[routeParamTxID])
}

func getTransactionByHashHandler(_ *httpserverutils.ServerContext, _ *http.Request, routeParams map[string]string, _ map[string]string,
	_ []byte) (interface{}, error) {

	return controllers.GetTransactionByHashHandler(routeParams[routeParamTxHash])
}

func getTransactionsByAddressHandler(_ *httpserverutils.ServerContext, _ *http.Request, routeParams map[string]string, queryParams map[string]string,
	_ []byte) (interface{}, error) {

	skip, err := convertQueryParamToInt64(queryParams, queryParamSkip, 0)
	if err != nil {
		return nil, err
	}
	limit, err := convertQueryParamToInt64(queryParams, queryParamLimit, defaultGetTransactionsLimit)
	if err != nil {
		return nil, err
	}
	return controllers.GetTransactionsByAddressHandler(routeParams[routeParamAddress], skip, limit)
}

func getTransactionCountByAddressHandler(_ *httpserverutils.ServerContext, _ *http.Request, routeParams map[string]string, _ map[string]string,
	_ []byte) (interface{}, error) {
	return controllers.GetTransactionCountByAddressHandler(routeParams[routeParamAddress])
}

func getTransactionsByBlockHashHandler(_ *httpserverutils.ServerContext, _ *http.Request, routeParams map[string]string, _ map[string]string,
	_ []byte) (interface{}, error) {

	return controllers.GetTransactionsByBlockHashHandler(routeParams[routeParamBlockHash])
}

func getTransactionDoubleSpendsHandler(_ *httpserverutils.ServerContext, _ *http.Request, routeParams map[string]string, _ map[string]string,
	_ []byte) (interface{}, error) {

	return controllers.GetTransactionDoubleSpends(routeParams[routeParamTxID])
}

func getUTXOsByAddressHandler(_ *httpserverutils.ServerContext, _ *http.Request, routeParams map[string]string, _ map[string]string,
	_ []byte) (interface{}, error) {

	return controllers.GetUTXOsByAddressHandler(routeParams[routeParamAddress])
}

func getBlockByHashHandler(_ *httpserverutils.ServerContext, _ *http.Request, routeParams map[string]string, _ map[string]string,
	_ []byte) (interface{}, error) {

	return controllers.GetBlockByHashHandler(routeParams[routeParamBlockHash])
}

func getFeeEstimatesHandler(_ *httpserverutils.ServerContext, _ *http.Request, _ map[string]string, _ map[string]string,
	_ []byte) (interface{}, error) {

	return controllers.GetFeeEstimatesHandler()
}

func getBlocksHandler(_ *httpserverutils.ServerContext, _ *http.Request, _ map[string]string, queryParams map[string]string,
	_ []byte) (interface{}, error) {

	skip, err := convertQueryParamToInt64(queryParams, queryParamSkip, 0)
	if err != nil {
		return nil, err
	}
	limit, err := convertQueryParamToInt64(queryParams, queryParamLimit, defaultGetBlocksLimit)
	if err != nil {
		return nil, err
	}
	order := defaultGetBlocksOrder
	if orderParamValue, ok := queryParams[queryParamOrder]; ok {
		order = orderParamValue
	}
	return controllers.GetBlocksHandler(order, skip, limit)
}

func getBlockCountHandler(_ *httpserverutils.ServerContext, _ *http.Request, _ map[string]string, _ map[string]string,
	_ []byte) (interface{}, error) {
	return controllers.GetBlockCountHandler()
}

func postTransactionHandler(_ *httpserverutils.ServerContext, _ *http.Request, _ map[string]string, _ map[string]string,
	requestBody []byte) (interface{}, error) {
	return nil, controllers.PostTransaction(requestBody)
}
