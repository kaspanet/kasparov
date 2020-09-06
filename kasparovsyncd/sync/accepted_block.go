package sync

import (
	"github.com/kaspanet/kaspad/app/appmessage"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"

	"github.com/pkg/errors"
)

func insertAcceptedBlocks(dbTx *database.TxContext, blocks []*rawAndVerboseBlock, blockHashesToIDs map[string]uint64) error {
	acceptedBlocksToAdd := make([]interface{}, 0)
	for _, block := range blocks {
		dbAcceptedBlocks, err := dbAcceptedBlocksFromVerboseBlock(blockHashesToIDs, block.Verbose)
		if err != nil {
			return err
		}
		for _, dbAcceptedBlock := range dbAcceptedBlocks {
			acceptedBlocksToAdd = append(acceptedBlocksToAdd, dbAcceptedBlock)
		}
	}
	err := dbaccess.BulkInsert(dbTx, acceptedBlocksToAdd)
	if err != nil {
		return err
	}
	return nil
}

func dbAcceptedBlocksFromVerboseBlock(blockHashesToIDs map[string]uint64, verboseBlock *appmessage.BlockVerboseData) ([]*dbmodels.AcceptedBlock, error) {
	// Exit early if this is the genesis block
	if len(verboseBlock.AcceptedBlockHashes) == 0 {
		return []*dbmodels.AcceptedBlock{}, nil
	}

	blockID, ok := blockHashesToIDs[verboseBlock.Hash]
	if !ok {
		return nil, errors.Errorf("couldn't find block ID for block %s", verboseBlock.Hash)
	}
	dbAcceptedBlocks := make([]*dbmodels.AcceptedBlock, len(verboseBlock.AcceptedBlockHashes))
	for i, acceptedBlockHash := range verboseBlock.AcceptedBlockHashes {
		acceptedBlockID, ok := blockHashesToIDs[acceptedBlockHash]
		if !ok {
			return nil, errors.Errorf("missing accepted block hash %s for block %s", acceptedBlockHash, verboseBlock.Hash)
		}
		dbAcceptedBlocks[i] = &dbmodels.AcceptedBlock{
			BlockID:         blockID,
			AcceptedBlockID: acceptedBlockID,
		}
	}
	return dbAcceptedBlocks, nil
}
