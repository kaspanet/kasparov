package dbaccess

import (
	"fmt"

	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
)

// GetBlockByHash retrieves a block from the database according to it's hash
// If preloadedColumns was provided - preloads the requested columns
func GetBlockByHash(blockHash string, preloadedColumns ...string) (*dbmodels.Block, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	query := db.Where(&dbmodels.Block{BlockHash: blockHash})
	query = preloadColumns(query, preloadedColumns)

	block := &dbmodels.Block{}
	dbResult := query.First(block)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.IsDBRecordNotFoundError(dbErrors) {
		return nil, nil
	}
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading block from database:",
			dbResult.GetErrors())
	}

	return block, nil
}

// GetBlocks retrieves from the database up to `limit` blocks in the requested `order`, skipping the first `skip` blocks
// If preloadedColumns was provided - preloads the requested columns
func GetBlocks(order Order, skip uint64, limit uint64, preloadedColumns ...string) ([]*dbmodels.Block, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	query := db.
		Limit(limit).
		Offset(skip)

	if order != OrderUnknown {
		query = query.Order(fmt.Sprintf("`id` %s", order))
	}

	query = preloadColumns(query, preloadedColumns)

	blocks := []*dbmodels.Block{}
	dbResult := query.Find(&blocks)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading blocks from the database:",
			dbResult.GetErrors())
	}

	return blocks, nil
}

// GetSelectedTip fetches the selected tip from the database
func GetSelectedTip() (*dbmodels.Block, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	block := &dbmodels.Block{}
	dbResult := db.Order("blue_score DESC").
		Where(&dbmodels.Block{IsChainBlock: true}).
		First(block)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading selected tip from the database:", dbErrors)
	}

	return block, nil
}
