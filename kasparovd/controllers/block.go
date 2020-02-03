package controllers

import (
	"encoding/hex"
	"net/http"

	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/kasparovd/apimodels"

	"github.com/pkg/errors"

	"github.com/kaspanet/kaspad/util/daghash"
	"github.com/kaspanet/kasparov/httpserverutils"
)

const maxGetBlocksLimit = 100

// GetBlockByHashHandler returns a block by a given hash.
func GetBlockByHashHandler(blockHash string) (interface{}, error) {
	if bytes, err := hex.DecodeString(blockHash); err != nil || len(bytes) != daghash.HashSize {
		return nil, httpserverutils.NewHandlerError(http.StatusUnprocessableEntity,
			errors.Errorf("The given block hash is not a hex-encoded %d-byte hash.", daghash.HashSize))
	}

	block, err := dbaccess.BlockByHash(dbaccess.NoTx(), blockHash, "AcceptingBlock")
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, httpserverutils.NewHandlerError(http.StatusNotFound, errors.New("No block with the given block hash was found"))
	}

	return convertBlockModelToBlockResponse(block), nil
}

// GetBlocksHandler searches for all blocks
func GetBlocksHandler(orderString string, skip uint64, limit uint64) (interface{}, error) {
	if limit < 1 || limit > maxGetBlocksLimit {
		return nil, httpserverutils.NewHandlerError(http.StatusBadRequest,
			errors.Errorf("Limit higher than %d was requested.", maxGetBlocksLimit))
	}

	order, err := dbaccess.StringToOrder(orderString)
	if err != nil {
		return nil, httpserverutils.NewHandlerError(http.StatusUnprocessableEntity, err)
	}

	blocks, err := dbaccess.Blocks(dbaccess.NoTx(), order, skip, limit, "AcceptingBlock")
	if err != nil {
		return nil, err
	}

	blockResponses := make([]*apimodels.BlockResponse, len(blocks))
	for i, block := range blocks {
		blockResponses[i] = convertBlockModelToBlockResponse(block)
	}

	total, err := dbaccess.BlocksCount()
	if err != nil {
		return nil, err
	}

	return apimodels.PaginatedBlocksResponse{
		Blocks: blockResponses,
		Total:  total,
	}, nil
}
