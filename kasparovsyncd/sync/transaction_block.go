package sync

import (
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"

	"github.com/pkg/errors"
)

func insertTransactionBlocks(dbTx *database.TxContext, blocks []*rawAndVerboseBlock,
	blockHashesToIDs map[string]uint64, transactionHashesToTxsWithMetadata map[string]*txWithMetadata) error {

	transactionBlocksToAdd := make([]interface{}, 0)
	for _, block := range blocks {
		blockID, ok := blockHashesToIDs[block.hash()]
		if !ok {
			return errors.Errorf("couldn't find block ID for block %s", block)
		}
		for i, tx := range block.Verbose.TransactionVerboseData {
			transactionBlocksToAdd = append(transactionBlocksToAdd, &dbmodels.TransactionBlock{
				TransactionID: transactionHashesToTxsWithMetadata[tx.Hash].id,
				BlockID:       blockID,
				Index:         uint32(i),
			})
		}
	}
	return dbaccess.BulkInsert(dbTx, transactionBlocksToAdd)
}
