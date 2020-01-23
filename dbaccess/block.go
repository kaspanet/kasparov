package dbaccess

import (
	"fmt"

	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
)

// BlockByHash retrieves a block from the database according to it's hash
// If preloadedColumns was provided - preloads the requested columns
func BlockByHash(ctx Context, blockHash string, preloadedColumns ...string) (*dbmodels.Block, error) {
	db, err := ctx.db()
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

// Blocks retrieves from the database up to `limit` blocks in the requested `order`, skipping the first `skip` blocks
// If preloadedColumns was provided - preloads the requested columns
func Blocks(ctx Context, order Order, skip uint64, limit uint64, preloadedColumns ...string) ([]*dbmodels.Block, error) {
	db, err := ctx.db()
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

// SelectedTip fetches the selected tip from the database
func SelectedTip(ctx Context) (*dbmodels.Block, error) {
	db, err := ctx.db()
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

// BluestBlock fetches the block with the highest blue score from the database
// Note this is not necessarily the same as SelectedTip(), since SelectedTip requires
// the selected-parent-chain to be fully synced
func BluestBlock(ctx Context) (*dbmodels.Block, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	block := &dbmodels.Block{}
	dbResult := db.Order("blue_score DESC").
		First(block)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading selected tip from the database:", dbErrors)
	}

	return block, nil
}
