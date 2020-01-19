package sync

import (
	"encoding/hex"
	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kasparov/dbmodels"

	"github.com/pkg/errors"
)

func insertRawBlocks(dbTx *gorm.DB, blocks []*rawAndVerboseBlock, blockHashesToIDs map[string]uint64) error {
	rawBlocksToAdd := make([]interface{}, len(blocks))
	for i, block := range blocks {
		blockID, ok := blockHashesToIDs[block.hash()]
		if !ok {
			return errors.Errorf("couldn't find block ID for block %s", block)
		}
		dbRawBlock, err := makeDBRawBlock(block.Raw, blockID)
		if err != nil {
			return err
		}
		rawBlocksToAdd[i] = dbRawBlock
	}
	err := bulkInsert(dbTx, rawBlocksToAdd)
	if err != nil {
		return err
	}
	return nil
}

func makeDBRawBlock(rawBlock string, blockID uint64) (*dbmodels.RawBlock, error) {
	blockData, err := hex.DecodeString(rawBlock)
	if err != nil {
		return nil, err
	}
	return &dbmodels.RawBlock{
		BlockID:   blockID,
		BlockData: blockData,
	}, nil
}
