package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/kaspanet/kasparov/apimodels"
	"github.com/pkg/errors"
)

func getUTXOs(kasparovAddress string, address string) ([]*apimodels.TransactionOutputResponse, error) {
	response, err := http.Get(fmt.Sprintf("%s/utxos/address/%s", kasparovAddress, address))
	if err != nil {
		return nil, errors.Wrap(err, "Error getting UTXOs from Kasparov server")
	}
	body, err := readResponse(response)
	if err != nil {
		return nil, errors.Wrap(err, "Error reading UTXOs from Kasparov server response")
	}

	utxos := []*apimodels.TransactionOutputResponse{}

	err = json.Unmarshal(body, &utxos)
	if err != nil {
		return nil, errors.Wrap(err, "Error unmarshalling UTXOs")
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
