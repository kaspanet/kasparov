package sync

import (
	"encoding/hex"
	"github.com/kaspanet/kasparov/dbmodels"
)

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
