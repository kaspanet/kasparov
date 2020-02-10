package sync

import (
	"encoding/hex"

	"github.com/kaspanet/kaspad/rpcmodel"
	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/pkg/errors"
)

func insertTransactionOutputs(dbTx *dbaccess.TxContext, transactionIDsToTxsWithMetadata map[string]*txWithMetadata) error {
	addressesToAddressIDs, err := insertAddresses(dbTx, transactionIDsToTxsWithMetadata)
	if err != nil {
		return err
	}

	outputsToAdd := make([]interface{}, 0)
	for _, transaction := range transactionIDsToTxsWithMetadata {
		if !transaction.isNew {
			continue
		}
		for i, txOut := range transaction.verboseTx.Vout {
			scriptPubKey, err := hex.DecodeString(txOut.ScriptPubKey.Hex)
			if err != nil {
				return errors.WithStack(err)
			}
			var addressID *uint64
			if txOut.ScriptPubKey.Address != nil {
				addressID = rpcmodel.Uint64(addressesToAddressIDs[*txOut.ScriptPubKey.Address])
			}
			outputsToAdd = append(outputsToAdd, &dbmodels.TransactionOutput{
				TransactionID: transaction.id,
				Index:         uint32(i),
				Value:         txOut.Value,
				IsSpent:       false, // This must be false for updateSelectedParentChain to work properly
				ScriptPubKey:  scriptPubKey,
				AddressID:     addressID,
			})
		}
	}

	return dbaccess.BulkInsert(dbTx, outputsToAdd)
}
