package sync

import (
	"encoding/hex"
	"github.com/kaspanet/kasparov/database"

	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"

	"github.com/pkg/errors"
)

func insertRawBlocks(dbTx *database.TxContext, blocks []*rawAndVerboseBlock, blockHashesToIDs map[string]uint64) error {
	rawBlocksToAdd := make([]interface{}, len(blocks))
	for i, block := range blocks {
		blockID, ok := blockHashesToIDs[block.hash()]
		if !ok {
			return errors.Errorf("couldn't find block ID for block %s", block)
		}
		dbRawBlock, err := dbRawBlockFromBlockData(block.Raw, blockID)
		if err != nil {
			return err
		}
		rawBlocksToAdd[i] = dbRawBlock
	}
	err := dbaccess.BulkInsert(dbTx, rawBlocksToAdd)
	if err != nil {
		return err
	}
	return nil
}

func dbRawBlockFromBlockData(blockDataHex string, blockID uint64) (*dbmodels.RawBlock, error) {
	blockData, err := hex.DecodeString(blockDataHex)
	if err != nil {
		return nil, err
	}
	return &dbmodels.RawBlock{
		BlockID:   blockID,
		BlockData: blockData,
	}, nil
}
