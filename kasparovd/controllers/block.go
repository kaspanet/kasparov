package controllers

import (
	"encoding/hex"
	"net/http"

	"github.com/kaspanet/kasparov/apimodels"
	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"

	"github.com/pkg/errors"

	"github.com/kaspanet/kaspad/util/daghash"
	"github.com/kaspanet/kasparov/httpserverutils"
)

const maxGetBlocksLimit = 100

// GetBlockByHashHandler returns a block by a given hash.
func GetBlockByHashHandler(blockHash string) (interface{}, error) {
	if bytes, err := hex.DecodeString(blockHash); err != nil || len(bytes) != daghash.HashSize {
		return nil, httpserverutils.NewHandlerError(http.StatusUnprocessableEntity,
			errors.Errorf("the given block hash is not a hex-encoded %d-byte hash.", daghash.HashSize))
	}

	block, err := dbaccess.BlockByHash(dbaccess.NoTx(), blockHash, dbmodels.BlockRecommendedPreloadedFields...)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, httpserverutils.NewHandlerError(http.StatusNotFound, errors.New("no block with the given block hash was found"))
	}

	return apimodels.ConvertBlockModelToBlockResponse(block), nil
}

// GetBlocksHandler searches for all blocks
func GetBlocksHandler(orderString string, skip int, limit int) (interface{}, error) {
	if limit > maxGetBlocksLimit {
		return nil, httpserverutils.NewHandlerError(http.StatusBadRequest,
			errors.Errorf("limit higher than %d was requested.", maxGetBlocksLimit))
	}

	order, err := dbaccess.StringToOrder(orderString)
	if err != nil {
		return nil, httpserverutils.NewHandlerError(http.StatusUnprocessableEntity, err)
	}

	blocks, err := dbaccess.Blocks(dbaccess.NoTx(), order, skip, limit, dbmodels.BlockRecommendedPreloadedFields...)
	if err != nil {
		return nil, err
	}

	blockResponses := make([]*apimodels.BlockResponse, len(blocks))
	for i, block := range blocks {
		blockResponses[i] = apimodels.ConvertBlockModelToBlockResponse(block)
	}

	total, err := dbaccess.BlocksCount(dbaccess.NoTx())
	if err != nil {
		return nil, err
	}

	return apimodels.PaginatedBlocksResponse{
		Blocks: blockResponses,
		Total:  total,
	}, nil
}
