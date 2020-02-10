package dbaccess

import (
	"fmt"

	"github.com/kaspanet/kaspad/rpcmodel"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
)

// BlockByHash retrieves a block from the database according to its hash
// If preloadedFields was provided - preloads the requested fields
func BlockByHash(ctx Context, blockHash string, preloadedFields ...dbmodels.FieldName) (*dbmodels.Block, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	query := db.Where(&dbmodels.Block{BlockHash: blockHash})
	query = preloadFields(query, preloadedFields)

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

// BlocksByHashes retreives a list of blocks with the corresponding `hashes`
func BlocksByHashes(ctx Context, hashes []string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Block, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	query := db.Where("block_hash in (?)", hashes)
	query = preloadFields(query, preloadedFields)

	blocks := []*dbmodels.Block{}
	dbResult := query.Find(&blocks)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading blocks from database:",
			dbResult.GetErrors())
	}

	return blocks, nil
}

// Blocks retrieves from the database up to `limit` blocks in the requested `order`, skipping the first `skip` blocks
// If preloadedFields was provided - preloads the requested fields
func Blocks(ctx Context, order Order, skip uint64, limit uint64, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Block, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	query := db.
		Offset(skip).
		Limit(limit)

	if order != OrderUnknown {
		query = query.Order(fmt.Sprintf("`id` %s", order))
	}

	query = preloadFields(query, preloadedFields)

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
	if httpserverutils.IsDBRecordNotFoundError(dbErrors) {
		return nil, nil
	}
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
	if httpserverutils.IsDBRecordNotFoundError(dbErrors) {
		return nil, nil
	}
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading bluest block from the database:", dbErrors)
	}

	return block, nil
}

// UpdateBlocksAcceptedByAcceptingBlock updates all blocks which are currently accepted by `currentAcceptingBlockID`
// to be accepted by `newAcceptingBlockID`.
// `newAcceptingBlockID` can be set nil.
func UpdateBlocksAcceptedByAcceptingBlock(ctx Context, currentAcceptingBlockID uint64, newAcceptingBlockID *uint64) error {
	db, err := ctx.db()
	if err != nil {
		return err
	}

	dbResult := db.
		Model(&dbmodels.Block{}).
		Where(&dbmodels.Block{AcceptingBlockID: rpcmodel.Uint64(currentAcceptingBlockID)}).
		Update("accepting_block_id", newAcceptingBlockID)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("failed to update blocks: ", dbErrors)
	}

	return nil
}

// UpdateBlockAcceptingBlockID updates blocks with `blockID` to be accepted by `acceptingBlockID `.
// `acceptingBlockID` can be set nil.
func UpdateBlockAcceptingBlockID(ctx Context, blockID uint64, acceptingBlockID *uint64) error {
	db, err := ctx.db()
	if err != nil {
		return err
	}

	dbResult := db.
		Model(&dbmodels.Block{}).
		Where(&dbmodels.Block{ID: blockID}).
		Update("accepting_block_id", acceptingBlockID)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("failed to update blocks: ", dbErrors)
	}

	return nil
}

// UpdateBlockIsChainBlock updates the block `blockID` by setting its isChainBlock field to `isChainBlock`
func UpdateBlockIsChainBlock(ctx Context, blockID uint64, isChainBlock bool) error {
	db, err := ctx.db()
	if err != nil {
		return err
	}

	dbResult := db.
		Model(&dbmodels.Block{}).
		Where("id = ?", blockID).
		Update("is_chain_block", isChainBlock)
	dbErrors := dbResult.GetErrors()

	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("failed to update block: ", dbErrors)
	}

	return nil
}

// DoesBlockExist checks in the database whether a block with `blockHash` exists.
func DoesBlockExist(ctx Context, blockHash string) (bool, error) {
	db, err := ctx.db()
	if err != nil {
		return false, err
	}

	var blocksCount uint64
	dbResult := db.
		Model(&dbmodels.Block{}).
		Where(&dbmodels.Block{BlockHash: blockHash}).
		Count(&blocksCount)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return false, httpserverutils.NewErrorFromDBErrors("Some errors were encountered while checking if block exists: ", dbErrors)
	}
	return blocksCount > 0, nil
}

// BlocksCount returns the total number of blocks stored in the database
func BlocksCount(ctx Context) (uint64, error) {
	db, err := ctx.db()
	if err != nil {
		return 0, err
	}

	var count uint64
	dbResult := db.Model(dbmodels.Block{}).Count(&count)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return 0, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when counting blocks:", dbErrors)
	}

	return count, nil
}

// SelectedTipBlueScore returns the blue score of the selected tip
func SelectedTipBlueScore(ctx Context) (uint64, error) {
	db, err := ctx.db()
	if err != nil {
		return 0, err
	}

	var blueScore []uint64
	dbResult := db.Model(&dbmodels.Block{}).
		Order("blue_score DESC").
		Where(&dbmodels.Block{IsChainBlock: true}).
		Limit(1).
		Pluck("blue_score", &blueScore)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return 0, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading selected tip blue score from the database:", dbErrors)
	}

	if len(blueScore) == 0 {
		return 0, nil
	}
	return blueScore[0], nil
}
