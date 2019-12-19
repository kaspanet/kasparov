package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/kaspanet/kasparov/kasparovd/apimodels"
	"github.com/pkg/errors"
)

func getUTXOs(apiAddress string, address string) ([]*apimodels.TransactionOutputResponse, error) {
	resp, err := http.Get(fmt.Sprintf("%s/utxos/%s", apiAddress, address))
	if err != nil {
		return nil, errors.Wrap(err, "Error getting utxos from API server")
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "Error reading utxos from API server response")
	}

	utxos := []*apimodels.TransactionOutputResponse{}

	err = json.Unmarshal(body, utxos)
	if err != nil {
		return nil, errors.Wrap(err, "Error unmarshalling utxos")
	}

	return utxos, nil
}
