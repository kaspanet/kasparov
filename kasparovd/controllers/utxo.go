package controllers

import (
	"github.com/kaspanet/kasparov/apimodels"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/kasparovd/config"
)

// GetUTXOsByAddressHandler searches for all UTXOs that belong to a certain address.
func GetUTXOsByAddressHandler(address string) (interface{}, error) {
	if err := validateAddress(address); err != nil {
		return nil, err
	}

	transactionOutputs, err := dbaccess.UTXOsByAddress(database.NoTx(), address,
		dbmodels.TransactionOutputFieldNames.TransactionAcceptingBlock,
		dbmodels.TransactionOutputFieldNames.TransactionSubnetwork)
	if err != nil {
		return nil, err
	}

	selectedTipBlueScore, err := dbaccess.SelectedTipBlueScore(database.NoTx())
	if err != nil {
		return nil, err
	}
	activeNetParams := config.ActiveConfig().NetParams()

	UTXOsResponses := make([]*apimodels.TransactionOutputResponse, len(transactionOutputs))
	for i, transactionOutput := range transactionOutputs {
		UTXOsResponses[i], err = apimodels.ConvertTransactionOutputModelToTransactionOutputResponse(transactionOutput, selectedTipBlueScore, activeNetParams, false)
		if err != nil {
			return nil, err
		}
	}
	return UTXOsResponses, nil
}
