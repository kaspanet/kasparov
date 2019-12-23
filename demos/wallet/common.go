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
	response, err := http.Get(fmt.Sprintf("%s/utxos/address/%s", apiAddress, address))
	if err != nil {
		return nil, errors.Wrap(err, "Error getting utxos from API server")
	}
	body, err := readResponse(response)
	if err != nil {
		return nil, errors.Wrap(err, "Error reading utxos from API server response")
	}

	utxos := []*apimodels.TransactionOutputResponse{}

	err = json.Unmarshal(body, &utxos)
	if err != nil {
		return nil, errors.Wrap(err, "Error unmarshalling utxos")
	}

	return utxos, nil
}

func readResponse(response *http.Response) (body []byte, err error) {
	defer response.Body.Close()

	body, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, errors.Wrap(err, "Error reading response")
	}

	if response.StatusCode != http.StatusOK {
		return nil, errors.Errorf("Response status %s\nResponseBody:\n%s", response.Status, body)
	}

	return body, nil
}
