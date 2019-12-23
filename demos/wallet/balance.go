package main

import (
	"fmt"

	"github.com/kaspanet/kaspad/util"
)

func balance(conf *balanceConfig) error {
	utxos, err := getUTXOs(conf.APIAddress, conf.Address)
	if err != nil {
		return err
	}

	var availableBalance, pendingBalance uint64 = 0, 0
	for _, utxo := range utxos {
		if utxo.IsSpendable != nil && *utxo.IsSpendable {
			availableBalance += utxo.Value
		} else {
			pendingBalance += utxo.Value
		}
	}

	fmt.Printf("Available balance is %f\n", float64(availableBalance)/util.SompiPerKaspa)
	if pendingBalance > 0 {
		fmt.Printf("In addition, immature coinbase balance is %f\n", float64(pendingBalance)/util.SompiPerKaspa)
	}

	return nil
}
