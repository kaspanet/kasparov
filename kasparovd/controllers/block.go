package controllers

import (
	"encoding/hex"
	"net/http"

	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/kasparovd/apimodels"

	"github.com/pkg/errors"

	"github.com/kaspanet/kaspad/util/daghash"
	"github.com/kaspanet/kasparov/httpserverutils"
)

const (
	// OrderAscending is parameter that can be used
	// in a get list handler to get a list ordered
	// in an ascending order.
	OrderAscending = "asc"

	// OrderDescending is parameter that can be used
	// in a get list handler to get a list ordered
	// in an ascending order.
	OrderDescending = "desc"
)

const maxGetBlocksLimit = 100

// GetBlockByHashHandler returns a block by a given hash.
func GetBlockByHashHandler(blockHash string) (interface{}, error) {
	if bytes, err := hex.DecodeString(blockHash); err != nil || len(bytes) != daghash.HashSize {
		return nil, httpserverutils.NewHandlerError(http.StatusUnprocessableEntity,
			errors.Errorf("The given block hash is not a hex-encoded %d-byte hash.", daghash.HashSize))
	}

	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	block := &dbmodels.Block{}
	dbResult := db.Where(&dbmodels.Block{BlockHash: blockHash}).Preload("AcceptingBlock").First(block)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.IsDBRecordNotFoundError(dbErrors) {
		return nil, httpserverutils.NewHandlerError(http.StatusNotFound, errors.New("No block with the given block hash was found"))
	}
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading transactions from the database:",
			dbResult.GetErrors())
	}
	return convertBlockModelToBlockResponse(block), nil
}

// GetBlocksHandler searches for all blocks
func GetBlocksHandler(order string, skip uint64, limit uint64) (interface{}, error) {
	if limit > maxGetBlocksLimit {
		return nil, httpserverutils.NewHandlerError(http.StatusBadRequest,
			errors.Errorf("Limit higher than %d was requested.", maxGetBlocksLimit))
	}
	db, err := database.DB()
	if err != nil {
		return nil, err
	}
	var total uint64
	dbResult := db.Model(dbmodels.Block{}).Count(&total)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when counting blocks:", dbErrors)
	}

	var blockResponses []*apimodels.BlockResponse
	// limit can be set to 0, if the user is interested
	// only on the `total` field.
	if limit > 0 {
		blocks := []*dbmodels.Block{}
		query := db.
			Limit(limit).
			Offset(skip).
			Preload("AcceptingBlock")
		if order == OrderAscending {
			query = query.Order("`id` ASC")
		} else if order == OrderDescending {
			query = query.Order("`id` DESC")
		} else {
			return nil, httpserverutils.NewHandlerError(http.StatusUnprocessableEntity, errors.Errorf("'%s' is not a valid order", order))
		}
		dbResult = query.Find(&blocks)
		dbErrors = dbResult.GetErrors()
		if httpserverutils.HasDBError(dbErrors) {
			return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading blocks from the database:", dbErrors)
		}

		blockResponses = make([]*apimodels.BlockResponse, len(blocks))
		for i, block := range blocks {
			blockResponses[i] = convertBlockModelToBlockResponse(block)
		}
	}

	return apimodels.PaginatedBlocksResponse{
		Blocks: blockResponses,
		Total:  total,
	}, nil
}

// GetAcceptedTransactionIDsByBlockHashHandler returns an array of transaction IDs for a given block hash
func GetAcceptedTransactionIDsByBlockHashHandler(blockHash string) ([]string, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	var transactions []dbmodels.Transaction
	dbResult := db.
		Joins("LEFT JOIN `blocks` ON `blocks`.`id` = `transactions`.`accepting_block_id`").
		Where("`blocks`.`block_hash` = ?", blockHash).
		Find(&transactions)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Failed to find transactions: ", dbErrors)
	}

	result := make([]string, len(transactions))
	for _, transaction := range transactions {
		result = append(result, transaction.TransactionID)
	}

	return result, nil
}
