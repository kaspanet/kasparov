package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"

	"github.com/kaspanet/kasparov/apimodels"
	"github.com/pkg/errors"
)

// apiURL returns a full concatenated URL from the base
// kasparov server URL and the given path.
func apiURL(kasparovAddress string, pathElements ...string) (string, error) {
	u, err := url.Parse(kasparovAddress)
	if err != nil {
		return "", errors.WithStack(err)
	}
	newPathElements := make([]string, len(pathElements)+1)
	newPathElements[0] = u.Path
	copy(newPathElements[1:], pathElements)
	u.Path = path.Join(newPathElements...)
	return u.String(), nil
}

func getUTXOs(kasparovAddress string, address string) ([]*apimodels.TransactionOutputResponse, error) {
	requestURL, err := apiURL(kasparovAddress, "utxos/address", address)
	if err != nil {
		return nil, err
	}
	response, err := http.Get(requestURL)
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
