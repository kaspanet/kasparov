package dbaccess

import (
	"fmt"
	"github.com/go-pg/pg/v9"
	"github.com/kaspanet/kasparov/database"

	"github.com/kaspanet/kasparov/dbmodels"
)

// BlockByHash retrieves a block from the database according to its hash
// If preloadedFields was provided - preloads the requested fields
func BlockByHash(ctx database.Context, blockHash string, preloadedFields ...dbmodels.FieldName) (*dbmodels.Block, error) {
	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}

	block := &dbmodels.Block{}
	query := db.Model(block).
		Where("block.block_hash = ?", blockHash)
	query = preloadFields(query, preloadedFields)
	err = query.First()
	if err == pg.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return block, nil
}

// BlocksByHashes retreives a list of blocks with the corresponding `hashes`
func BlocksByHashes(ctx database.Context, hashes []string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Block, error) {
	if len(hashes) == 0 {
		return nil, nil
	}

	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}

	blocks := []*dbmodels.Block{}
	query := db.Model(&blocks).Where("block.block_hash in (?)", pg.In(hashes))
	query = preloadFields(query, preloadedFields)
	err = query.Select()
	if err != nil {
		return nil, err
	}

	return blocks, nil
}

// Blocks retrieves from the database up to `limit` blocks in the requested `order`, skipping the first `skip` blocks
// If preloadedFields was provided - preloads the requested fields
func Blocks(ctx database.Context, order Order, skip uint64, limit uint64,
	preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Block, error) {

	if limit == 0 {
		return []*dbmodels.Block{}, nil
	}

	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}

	var blocks []*dbmodels.Block
	query := db.Model(&blocks).
		Offset(int(skip)).
		Limit(int(limit))

	if order != OrderUnknown {
		query = query.Order(fmt.Sprintf("id %s", order))
	}

	query = preloadFields(query, preloadedFields)
	err = query.Select()

	if err != nil {
		return nil, err
	}

	return blocks, nil
}

// SelectedTip fetches the selected tip from the database
func SelectedTip(ctx database.Context) (*dbmodels.Block, error) {
	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}

	block := &dbmodels.Block{}
	err = db.Model(block).
		Where("is_chain_block = ?", true).
		Order("blue_score DESC").
		First()

	if err == pg.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return block, nil
}

// SelectedTipBlueScore returns the blue score of the selected tip
func SelectedTipBlueScore(ctx database.Context) (uint64, error) {
	db, err := ctx.DB()
	if err != nil {
		return 0, err
	}

	var blueScore uint64
	err = db.Model(&dbmodels.Block{}).
		Where("is_chain_block = ?", true).
		ColumnExpr("MAX(blue_score) as blue_score").
		Select(&blueScore)
	if err != nil {
		return 0, err
	}

	return blueScore, nil
}

// BluestBlock fetches the block with the highest blue score from the database
// Note: this is not necessarily the same as SelectedTip(): In a non-fully synced
// Kasparov - chain is still partial, and therefore SelectedTip() returns a lower
// block.
func BluestBlock(ctx database.Context) (*dbmodels.Block, error) {
	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}

	block := &dbmodels.Block{}
	err = db.Model(block).Order("blue_score DESC").First()
	if err == pg.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return block, nil
}

// UpdateBlocksAcceptedByAcceptingBlock updates all blocks which are currently accepted by `currentAcceptingBlockID`
// to be accepted by `newAcceptingBlockID`.
// `newAcceptingBlockID` can be set nil.
func UpdateBlocksAcceptedByAcceptingBlock(ctx database.Context, currentAcceptingBlockID uint64, newAcceptingBlockID *uint64) error {
	db, err := ctx.DB()
	if err != nil {
		return err
	}

	_, err = db.
		Model(&dbmodels.Block{}).
		Where("accepting_block_id = ?", currentAcceptingBlockID).
		Set("accepting_block_id = ?", newAcceptingBlockID).
		Update()
	if err != nil {
		return err
	}

	return nil
}

// UpdateBlockAcceptingBlockID updates blocks with `blockID` to be accepted by `acceptingBlockID `.
// `acceptingBlockID` can be set nil.
func UpdateBlockAcceptingBlockID(ctx database.Context, blockID uint64, acceptingBlockID *uint64) error {
	db, err := ctx.DB()
	if err != nil {
		return err
	}

	_, err = db.
		Model(&dbmodels.Block{}).
		Where("id = ?", blockID).
		Set("accepting_block_id = ?", acceptingBlockID).
		Update()
	if err != nil {
		return err
	}

	return nil
}

// UpdateBlockIsChainBlock updates the block `blockID` by setting its isChainBlock field to `isChainBlock`
func UpdateBlockIsChainBlock(ctx database.Context, blockID uint64, isChainBlock bool) error {
	db, err := ctx.DB()
	if err != nil {
		return err
	}

	_, err = db.
		Model(&dbmodels.Block{}).
		Where("id = ?", blockID).
		Set("is_chain_block = ?", isChainBlock).
		Update()
	if err != nil {
		return err
	}

	return nil
}

// DoesBlockExist checks in the database whether a block with `blockHash` exists.
func DoesBlockExist(ctx database.Context, blockHash string) (bool, error) {
	db, err := ctx.DB()
	if err != nil {
		return false, err
	}

	count, err := db.
		Model(&dbmodels.Block{}).
		Where("block_hash = ?", blockHash).
		Count()
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

// BlocksCount returns the total number of blocks stored in the database
func BlocksCount(ctx database.Context) (uint64, error) {
	db, err := ctx.DB()
	if err != nil {
		return 0, err
	}

	count, err := db.Model(&dbmodels.Block{}).Count()
	if err != nil {
		return 0, err
	}

	return uint64(count), nil
}
