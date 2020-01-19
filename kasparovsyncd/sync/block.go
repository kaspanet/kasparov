package sync

import (
	"encoding/hex"
	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kaspad/rpcmodel"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
	"github.com/kaspanet/kasparov/jsonrpc"
	"github.com/kaspanet/kasparov/kasparovsyncd/utils"
	"github.com/pkg/errors"
	"strconv"
	"time"
)

// bulkInsertBlocksData inserts the given blocks and their data (transactions
// and new subnetworks data) to the database in chunks.
func bulkInsertBlocksData(client *jsonrpc.Client, blocks []*utils.RawAndVerboseBlock) error {
	db, err := database.DB()
	if err != nil {
		return err
	}
	dbTx := db.Begin()
	defer dbTx.RollbackUnlessCommitted()

	subnetworkIDToID, err := insertBlocksSubnetworks(dbTx, client, blocks)
	if err != nil {
		return err
	}

	transactionIDsToTxsWithMetaData, err := insertTransactions(dbTx, blocks, subnetworkIDToID)
	if err != nil {
		return err
	}

	err = insertTransactionOutputs(dbTx, transactionIDsToTxsWithMetaData)
	if err != nil {
		return err
	}

	err = insertTransactionInputs(dbTx, transactionIDsToTxsWithMetaData)
	if err != nil {
		return err
	}

	blockHashesToIDs, err := insertBlocks(dbTx, blocks, transactionIDsToTxsWithMetaData)
	if err != nil {
		return err
	}

	err = insertTransactionBlocks(dbTx, blocks, blockHashesToIDs, transactionIDsToTxsWithMetaData)
	if err != nil {
		return err
	}

	dbTx.Commit()
	log.Infof("Added %d blocks", len(blocks))
	return nil
}

func insertBlocks(dbTx *gorm.DB, blocks []*utils.RawAndVerboseBlock, transactionIDsToTxsWithMetaData map[string]*txWithMetaData) (blockHashesToIDs map[string]uint64, err error) {
	blocksToAdd := make([]interface{}, len(blocks))
	for i, block := range blocks {
		blockMass := uint64(0)
		for _, tx := range block.Verbose.RawTx {
			blockMass += transactionIDsToTxsWithMetaData[tx.TxID].mass
		}
		var err error
		blocksToAdd[i], err = makeDBBlock(block.Verbose, blockMass)
		if err != nil {
			return nil, err
		}
	}
	err = bulkInsert(dbTx, blocksToAdd)
	if err != nil {
		return nil, err
	}

	blockHashesToIDs, err = getBlocksAndParentIDs(dbTx, blocks)
	if err != nil {
		return nil, err
	}

	parentsToAdd := make([]interface{}, 0)
	rawBlocksToAdd := make([]interface{}, len(blocks))
	for i, block := range blocks {
		blockID, ok := blockHashesToIDs[block.Hash()]
		if !ok {
			return nil, errors.Errorf("couldn't find block ID for block %s", block)
		}
		dbBlockParents, err := makeBlockParents(blockHashesToIDs, block.Verbose)
		if err != nil {
			return nil, err
		}
		dbRawBlock, err := makeDBRawBlock(block.Raw, blockID)
		if err != nil {
			return nil, err
		}
		for _, dbBlockParent := range dbBlockParents {
			parentsToAdd = append(parentsToAdd, dbBlockParent)
		}
		rawBlocksToAdd[i] = dbRawBlock
	}
	err = bulkInsert(dbTx, parentsToAdd)
	if err != nil {
		return nil, err
	}
	err = bulkInsert(dbTx, rawBlocksToAdd)
	if err != nil {
		return nil, err
	}
	return blockHashesToIDs, nil
}

func getBlocksAndParentIDs(dbTx *gorm.DB, blocks []*utils.RawAndVerboseBlock) (map[string]uint64, error) {
	blockSet := make(map[string]struct{})
	for _, block := range blocks {
		blockSet[block.Hash()] = struct{}{}
		for _, parentHash := range block.Verbose.ParentHashes {
			blockSet[parentHash] = struct{}{}
		}
	}

	blockHashes := stringsSetToSlice(blockSet)
	var dbBlocks []*dbmodels.Block
	dbResult := dbTx.
		Where("block_hash in (?)", blockHashes).
		Find(&dbBlocks)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find blocks: ", dbErrors)
	}

	if len(dbBlocks) != len(blockSet) {
		return nil, errors.Errorf("couldn't retrieve all block IDs")
	}

	blockHashesToIDs := make(map[string]uint64)
	for _, dbBlock := range dbBlocks {
		blockHashesToIDs[dbBlock.BlockHash] = dbBlock.ID
	}
	return blockHashesToIDs, nil
}

func insertBlocksTransactionAddresses(dbTx *gorm.DB, transactionIDsToTxsWithMetaData map[string]*txWithMetaData) (map[string]uint64, error) {
	addressSet := make(map[string]struct{})
	for _, transaction := range transactionIDsToTxsWithMetaData {
		if !transaction.isNew {
			continue
		}
		for _, txOut := range transaction.verboseTx.Vout {
			if txOut.ScriptPubKey.Address == nil {
				continue
			}
			addressSet[*txOut.ScriptPubKey.Address] = struct{}{}
		}
	}
	addresses := stringsSetToSlice(addressSet)

	var dbAddresses []*dbmodels.Address
	dbResult := dbTx.
		Where("address in (?)", addresses).
		Find(&dbAddresses)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find addresses: ", dbErrors)
	}

	addressesToAddressIDs := make(map[string]uint64)
	for _, dbAddress := range dbAddresses {
		addressesToAddressIDs[dbAddress.Address] = dbAddress.ID
	}

	newAddresses := make([]string, 0)
	for address := range addressSet {
		if _, exists := addressesToAddressIDs[address]; exists {
			continue
		}
		newAddresses = append(newAddresses, address)
	}

	addressesToAdd := make([]interface{}, len(newAddresses))
	for i, address := range newAddresses {
		addressesToAdd[i] = &dbmodels.Address{
			Address: address,
		}
	}

	err := bulkInsert(dbTx, addressesToAdd)
	if err != nil {
		return nil, err
	}

	var dbNewAddresses []*dbmodels.Address
	dbResult = dbTx.
		Where("address in (?)", newAddresses).
		Find(&dbNewAddresses)
	dbErrors = dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find addresses: ", dbErrors)
	}

	if len(dbNewAddresses) != len(newAddresses) {
		return nil, errors.New("couldn't add all new addresses")
	}

	for _, dbNewAddress := range dbNewAddresses {
		addressesToAddressIDs[dbNewAddress.Address] = dbNewAddress.ID
	}
	return addressesToAddressIDs, nil
}

func stringsSetToSlice(set map[string]struct{}) []string {
	ids := make([]string, len(set))
	i := 0
	for id := range set {
		ids[i] = id
		i++
	}
	return ids
}

func makeDBBlock(verboseBlock *rpcmodel.GetBlockVerboseResult, mass uint64) (*dbmodels.Block, error) {
	bits, err := strconv.ParseUint(verboseBlock.Bits, 16, 32)
	if err != nil {
		return nil, err
	}
	dbBlock := dbmodels.Block{
		BlockHash:            verboseBlock.Hash,
		Version:              verboseBlock.Version,
		HashMerkleRoot:       verboseBlock.HashMerkleRoot,
		AcceptedIDMerkleRoot: verboseBlock.AcceptedIDMerkleRoot,
		UTXOCommitment:       verboseBlock.UTXOCommitment,
		Timestamp:            time.Unix(verboseBlock.Time, 0),
		Bits:                 uint32(bits),
		Nonce:                verboseBlock.Nonce,
		BlueScore:            verboseBlock.BlueScore,
		IsChainBlock:         false, // This must be false for updateSelectedParentChain to work properly
		Mass:                 mass,
	}

	// Set genesis block as the initial chain block
	if len(verboseBlock.ParentHashes) == 0 {
		dbBlock.IsChainBlock = true
	}
	return &dbBlock, nil
}

func makeBlockParents(blockHashesToIDs map[string]uint64, verboseBlock *rpcmodel.GetBlockVerboseResult) ([]*dbmodels.ParentBlock, error) {
	// Exit early if this is the genesis block
	if len(verboseBlock.ParentHashes) == 0 {
		return nil, nil
	}

	blockID, ok := blockHashesToIDs[verboseBlock.Hash]
	if !ok {
		return nil, errors.Errorf("couldn't find block ID for block %s", verboseBlock.Hash)
	}
	dbParentBlocks := make([]*dbmodels.ParentBlock, len(verboseBlock.ParentHashes))
	for i, parentHash := range verboseBlock.ParentHashes {
		parentID, ok := blockHashesToIDs[parentHash]
		if !ok {
			return nil, errors.Errorf("missing parent %s for block %s", parentHash, verboseBlock.Hash)
		}
		dbParentBlocks[i] = &dbmodels.ParentBlock{
			BlockID:       blockID,
			ParentBlockID: parentID,
		}
	}
	return dbParentBlocks, nil
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
