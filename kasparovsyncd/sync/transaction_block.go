package sync

import (
	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/kasparovsyncd/utils"
	"github.com/pkg/errors"
)

func insertTransactionBlocks(dbTx *gorm.DB, blocks []*utils.RawAndVerboseBlock, blockHashesToIDs map[string]uint64, transactionIDsToTxsWithMetadata map[string]*txWithMetadata) error {
	transactionBlocksToAdd := make([]interface{}, 0)
	for _, block := range blocks {
		blockID, ok := blockHashesToIDs[block.Hash()]
		if !ok {
			return errors.Errorf("couldn't find block ID for block %s", block)
		}
		for i, tx := range block.Verbose.RawTx {
			transactionBlocksToAdd = append(transactionBlocksToAdd, &dbmodels.TransactionBlock{
				TransactionID: transactionIDsToTxsWithMetadata[tx.TxID].id,
				BlockID:       blockID,
				Index:         uint32(i),
			})
		}
	}
	return bulkInsert(dbTx, transactionBlocksToAdd)
}
