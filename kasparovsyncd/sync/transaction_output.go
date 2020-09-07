package sync

import (
	"encoding/hex"
	"github.com/kaspanet/kaspad/util/pointers"
	"github.com/kaspanet/kasparov/database"

	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/pkg/errors"
)

func insertTransactionOutputs(dbTx *database.TxContext, transactionHashesToTxsWithMetadata map[string]*txWithMetadata) error {
	addressesToAddressIDs, err := insertAddresses(dbTx, transactionHashesToTxsWithMetadata)
	if err != nil {
		return err
	}

	outputsToAdd := make([]interface{}, 0)
	for _, transaction := range transactionHashesToTxsWithMetadata {
		if !transaction.isNew {
			continue
		}
		for i, txOut := range transaction.verboseTx.TransactionVerboseOutputs {
			scriptPubKey, err := hex.DecodeString(txOut.ScriptPubKey.Hex)
			if err != nil {
				return errors.WithStack(err)
			}
			var addressID *uint64
			if txOut.ScriptPubKey.Address != "" {
				addressID = pointers.Uint64(addressesToAddressIDs[txOut.ScriptPubKey.Address])
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
