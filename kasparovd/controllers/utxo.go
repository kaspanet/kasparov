package controllers

import (
	"encoding/hex"
	"fmt"

	"github.com/kaspanet/kaspad/blockdag"
	"github.com/kaspanet/kaspad/rpcmodel"
	"github.com/kaspanet/kaspad/util/subnetworkid"
	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/kasparovd/apimodels"
	"github.com/kaspanet/kasparov/kasparovd/config"
	"github.com/pkg/errors"
)

// GetUTXOsByAddressHandler searches for all UTXOs that belong to a certain address.
func GetUTXOsByAddressHandler(address string) (interface{}, error) {
	if err := validateAddress(address); err != nil {
		return nil, err
	}

	transactionOutputs, err := dbaccess.TransactionOutputsByAddress(address, "Transaction.AcceptingBlock", "Transaction.Subnetwork")
	if err != nil {
		return nil, err
	}

	nonAcceptedTxIds := make([]uint64, len(transactionOutputs))
	for i, txOut := range transactionOutputs {
		if txOut.Transaction.AcceptingBlock == nil {
			nonAcceptedTxIds[i] = txOut.TransactionID
		}
	}

	var selectedTip *dbmodels.Block
	var isTxInSelectedTip map[uint64]bool
	if len(nonAcceptedTxIds) != 0 {
		selectedTip, err = dbaccess.GetSelectedTip()
		if err != nil {
			return nil, err
		}

		txIDsInSelectedTip, err := dbaccess.TransactionsInBlock(nonAcceptedTxIds, selectedTip.ID)
		if err != nil {
			return nil, err
		}

		for _, txID := range txIDsInSelectedTip {
			isTxInSelectedTip[txID] = true
		}
	}

	activeNetParams := config.ActiveConfig().NetParams()

	UTXOsResponses := make([]*apimodels.TransactionOutputResponse, len(transactionOutputs))
	for i, transactionOutput := range transactionOutputs {
		subnetworkID := &subnetworkid.SubnetworkID{}
		err := subnetworkid.Decode(subnetworkID, transactionOutput.Transaction.Subnetwork.SubnetworkID)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("Couldn't decode subnetwork id %s", transactionOutput.Transaction.Subnetwork.SubnetworkID))
		}
		var acceptingBlockHash *string
		var confirmations uint64
		acceptingBlockBlueScore := blockdag.UnacceptedBlueScore
		if isTxInSelectedTip[transactionOutput.ID] {
			confirmations = 1
		} else if transactionOutput.Transaction.AcceptingBlock != nil {
			acceptingBlockHash = rpcmodel.String(transactionOutput.Transaction.AcceptingBlock.BlockHash)
			acceptingBlockBlueScore = transactionOutput.Transaction.AcceptingBlock.BlueScore
			confirmations = selectedTip.BlueScore - acceptingBlockBlueScore + 2
		}
		isCoinbase := subnetworkID.IsEqual(subnetworkid.SubnetworkIDCoinbase)
		isSpendable := confirmations > 0 && (!isCoinbase || confirmations >= activeNetParams.BlockCoinbaseMaturity)
		UTXOsResponses[i] = &apimodels.TransactionOutputResponse{
			TransactionID:           transactionOutput.Transaction.TransactionID,
			Value:                   transactionOutput.Value,
			ScriptPubKey:            hex.EncodeToString(transactionOutput.ScriptPubKey),
			AcceptingBlockHash:      acceptingBlockHash,
			AcceptingBlockBlueScore: acceptingBlockBlueScore,
			Index:                   transactionOutput.Index,
			IsCoinbase:              rpcmodel.Bool(isCoinbase),
			Confirmations:           rpcmodel.Uint64(confirmations),
			IsSpendable:             rpcmodel.Bool(isSpendable),
		}
	}
	return UTXOsResponses, nil
}
