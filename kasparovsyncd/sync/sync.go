package sync

import (
	"bytes"
	"encoding/hex"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
	"github.com/kaspanet/kasparov/jsonrpc"
	"github.com/kaspanet/kasparov/kasparovsyncd/mqtt"
	"github.com/kaspanet/kasparov/kasparovsyncd/utils"

	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kaspad/rpcmodel"
	"github.com/kaspanet/kaspad/util/daghash"
	"github.com/pkg/errors"
)

// pendingChainChangedMsgs holds chainChangedMsgs in order of arrival
var pendingChainChangedMsgs []*jsonrpc.ChainChangedMsg

// StartSync keeps the node and the database in sync. On start, it downloads
// all data that's missing from the dabase, and once it's done it keeps
// sync with the node via notifications.
func StartSync(doneChan chan struct{}) error {
	client, err := jsonrpc.GetClient()
	if err != nil {
		return err
	}

	// Mass download missing data
	err = fetchInitialData(client)
	if err != nil {
		return err
	}

	// Keep the node and the database in sync
	return sync(client, doneChan)
}

// fetchInitialData downloads all data that's currently missing from
// the database.
func fetchInitialData(client *jsonrpc.Client) error {
	log.Infof("Syncing past blocks")
	err := syncBlocks(client)
	if err != nil {
		return err
	}
	log.Infof("Syncing past selected parent chain")
	err = syncSelectedParentChain(client)
	if err != nil {
		return err
	}
	log.Infof("Finished syncing past data")
	return nil
}

// sync keeps the database in sync with the node via notifications
func sync(client *jsonrpc.Client, doneChan chan struct{}) error {
	// Handle client notifications until we're told to stop
	for {
		select {
		case blockAdded := <-client.OnBlockAdded:
			err := handleBlockAddedMsg(client, blockAdded)
			if err != nil {
				return err
			}
		case chainChanged := <-client.OnChainChanged:
			enqueueChainChangedMsg(chainChanged)
			err := processChainChangedMsgs()
			if err != nil {
				return err
			}
		case <-doneChan:
			log.Infof("StartSync stopped")
			return nil
		}
	}
}

func stringPointerToString(str *string) string {
	if str == nil {
		return "<nil>"
	}
	return *str
}

// syncBlocks attempts to download all DAG blocks starting with
// the bluest block, and then inserts them into the database.
func syncBlocks(client *jsonrpc.Client) error {
	// Start syncing from the bluest block hash. We use blue score to
	// simulate the "last" block we have because blue-block order is
	// the order that the node uses in the various JSONRPC calls.
	startHash, err := findHashOfBluestBlock(false)
	if err != nil {
		return err
	}

	for {
		log.Debugf("Calling getBlocks with start hash %v", stringPointerToString(startHash))
		blocksResult, err := client.GetBlocks(true, true, startHash)
		if err != nil {
			return err
		}
		if len(blocksResult.Hashes) == 0 {
			break
		}

		startHash = &blocksResult.Hashes[len(blocksResult.Hashes)-1]
		err = addBlocks(client, blocksResult.RawBlocks, blocksResult.VerboseBlocks)
		if err != nil {
			return err
		}
	}

	return nil
}

// syncSelectedParentChain attempts to download the selected parent
// chain starting with the bluest chain-block, and then updates the
// database accordingly.
func syncSelectedParentChain(client *jsonrpc.Client) error {
	// Start syncing from the selected tip hash
	startHash, err := findHashOfBluestBlock(true)
	if err != nil {
		return err
	}

	for {
		log.Debugf("Calling getChainFromBlock with start hash %s", stringPointerToString(startHash))
		chainFromBlockResult, err := client.GetChainFromBlock(false, startHash)
		if err != nil {
			return err
		}
		if len(chainFromBlockResult.AddedChainBlocks) == 0 {
			break
		}

		startHash = &chainFromBlockResult.AddedChainBlocks[len(chainFromBlockResult.AddedChainBlocks)-1].Hash
		err = updateSelectedParentChain(chainFromBlockResult.RemovedChainBlockHashes,
			chainFromBlockResult.AddedChainBlocks)
		if err != nil {
			return err
		}
	}
	return nil
}

// findHashOfBluestBlock finds the block with the highest
// blue score in the database. If the database is empty,
// return nil.
func findHashOfBluestBlock(mustBeChainBlock bool) (*string, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	var blockHashes []string
	dbQuery := db.Model(&dbmodels.Block{}).
		Order("blue_score DESC").
		Limit(1)
	if mustBeChainBlock {
		dbQuery = dbQuery.Where(&dbmodels.Block{IsChainBlock: true})
	}
	dbResult := dbQuery.Pluck("block_hash", &blockHashes)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find hash of bluest block: ", dbErrors)
	}
	if len(blockHashes) == 0 {
		return nil, nil
	}
	return &blockHashes[0], nil
}

// fetchBlock downloads the serialized block and raw block data of
// the block with hash blockHash.
func fetchBlock(client *jsonrpc.Client, blockHash *daghash.Hash) (
	*utils.RawAndVerboseBlock, error) {
	log.Debugf("Getting block %s from the RPC server", blockHash)
	msgBlock, err := client.GetBlock(blockHash, nil)
	if err != nil {
		return nil, err
	}
	writer := bytes.NewBuffer(make([]byte, 0, msgBlock.SerializeSize()))
	err = msgBlock.Serialize(writer)
	if err != nil {
		return nil, err
	}
	rawBlock := hex.EncodeToString(writer.Bytes())

	verboseBlock, err := client.GetBlockVerboseTx(blockHash, nil)
	if err != nil {
		return nil, err
	}
	return &utils.RawAndVerboseBlock{
		Raw:     rawBlock,
		Verbose: verboseBlock,
	}, nil
}

// updateSelectedParentChain updates the database to reflect the current selected
// parent chain. First it "unaccepts" all removedChainHashes and then it "accepts"
// all addChainBlocks.
// Note that if this function may take a nil dbTx, in which case it would start
// a database transaction by itself and commit it before returning.
func updateSelectedParentChain(removedChainHashes []string, addedChainBlocks []rpcmodel.ChainBlock) error {
	db, err := database.DB()
	if err != nil {
		return err
	}
	dbTx := db.Begin()
	defer dbTx.RollbackUnlessCommitted()

	for _, removedHash := range removedChainHashes {
		err := updateRemovedChainHashes(dbTx, removedHash)
		if err != nil {
			return err
		}
	}
	for _, addedBlock := range addedChainBlocks {
		err := updateAddedChainBlocks(dbTx, &addedBlock)
		if err != nil {
			return err
		}
	}

	dbTx.Commit()
	return nil
}

// updateRemovedChainHashes "unaccepts" the block of the given removedHash.
// That is to say, it marks it as not in the selected parent chain in the
// following ways:
// * All its TransactionInputs.PreviousTransactionOutputs are set IsSpent = false
// * All its Transactions are set AcceptingBlockID = nil
// * The block is set IsChainBlock = false
// This function will return an error if any of the above are in an unexpected state
func updateRemovedChainHashes(dbTx *gorm.DB, removedHash string) error {
	var dbBlock dbmodels.Block
	dbResult := dbTx.
		Where(&dbmodels.Block{BlockHash: removedHash}).
		First(&dbBlock)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("failed to find block: ", dbErrors)
	}
	if httpserverutils.IsDBRecordNotFoundError(dbErrors) {
		return errors.Errorf("missing block for hash: %s", removedHash)
	}
	if !dbBlock.IsChainBlock {
		return errors.Errorf("block erroneously marked as not a chain block: %s", removedHash)
	}

	var dbTransactions []dbmodels.Transaction
	dbResult = dbTx.
		Where(&dbmodels.Transaction{AcceptingBlockID: &dbBlock.ID}).
		Preload("TransactionInputs.PreviousTransactionOutput").
		Find(&dbTransactions)
	dbErrors = dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("failed to find transactions: ", dbErrors)
	}
	for _, dbTransaction := range dbTransactions {
		for _, dbTransactionInput := range dbTransaction.TransactionInputs {
			dbPreviousTransactionOutput := dbTransactionInput.PreviousTransactionOutput

			if !dbPreviousTransactionOutput.IsSpent {
				return errors.Errorf("cannot de-spend an unspent transaction output: %s index: %d",
					dbTransaction.TransactionID, dbTransactionInput.Index)
			}
			dbPreviousTransactionOutput.IsSpent = false
			dbResult = dbTx.Save(&dbPreviousTransactionOutput)
			dbErrors = dbResult.GetErrors()
			if httpserverutils.HasDBError(dbErrors) {
				return httpserverutils.NewErrorFromDBErrors("failed to update transactionOutput: ", dbErrors)
			}
		}

		// Don't use Save() here--it updates all fields in dbTransaction
		dbTransaction.AcceptingBlockID = nil
		dbResult = dbTx.
			Model(&dbmodels.Transaction{}).
			Where("id = ?", dbTransaction.ID).
			Update("accepting_block_id", nil)
		dbErrors := dbResult.GetErrors()
		if httpserverutils.HasDBError(dbErrors) {
			return httpserverutils.NewErrorFromDBErrors("failed to update transaction: ", dbErrors)
		}
	}

	dbResult = dbTx.
		Model(&dbmodels.Block{}).
		Where(&dbmodels.Block{AcceptingBlockID: rpcmodel.Uint64(dbBlock.ID)}).
		Updates(map[string]interface{}{"AcceptingBlockID": nil})
	dbErrors = dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("failed to update blocks: ", dbErrors)
	}

	// Don't use Save() here--it updates all fields in dbBlock
	dbBlock.IsChainBlock = false
	dbResult = dbTx.
		Model(&dbmodels.Block{}).
		Where("id = ?", dbBlock.ID).
		Update("is_chain_block", false)
	dbErrors = dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("failed to update block: ", dbErrors)
	}

	return nil
}

// updateAddedChainBlocks "accepts" the given addedBlock. That is to say,
// it marks it as in the selected parent chain in the following ways:
// * All its TransactionInputs.PreviousTransactionOutputs are set IsSpent = true
// * All its Transactions are set AcceptingBlockID = addedBlock
// * The block is set IsChainBlock = true
// This function will return an error if any of the above are in an unexpected state
func updateAddedChainBlocks(dbTx *gorm.DB, addedBlock *rpcmodel.ChainBlock) error {
	var dbAddedBlock dbmodels.Block
	dbResult := dbTx.
		Where(&dbmodels.Block{BlockHash: addedBlock.Hash}).
		First(&dbAddedBlock)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("failed to find block: ", dbErrors)
	}
	if httpserverutils.IsDBRecordNotFoundError(dbErrors) {
		return errors.Errorf("missing block for hash: %s", addedBlock.Hash)
	}
	if dbAddedBlock.IsChainBlock {
		return errors.Errorf("block erroneously marked as a chain block: %s", addedBlock.Hash)
	}

	for _, acceptedBlock := range addedBlock.AcceptedBlocks {
		var dbAccepedBlock dbmodels.Block
		dbResult := dbTx.
			Where(&dbmodels.Block{BlockHash: acceptedBlock.Hash}).
			First(&dbAccepedBlock)
		dbErrors := dbResult.GetErrors()
		if httpserverutils.HasDBError(dbErrors) {
			return httpserverutils.NewErrorFromDBErrors("failed to find block: ", dbErrors)
		}
		if httpserverutils.IsDBRecordNotFoundError(dbErrors) {
			return errors.Errorf("missing block for hash: %s", acceptedBlock.Hash)
		}
		if dbAccepedBlock.AcceptingBlockID != nil && *dbAccepedBlock.AcceptingBlockID == dbAddedBlock.ID {
			return errors.Errorf("block %s erroneously marked as accepted by %s", acceptedBlock.Hash, addedBlock.Hash)
		}

		transactionIDsIn := make([]string, len(acceptedBlock.AcceptedTxIDs))
		for i, acceptedTxID := range acceptedBlock.AcceptedTxIDs {
			transactionIDsIn[i] = acceptedTxID
		}
		var dbAcceptedTransactions []dbmodels.Transaction
		dbResult = dbTx.
			Where("transaction_id in (?)", transactionIDsIn).
			Preload("TransactionInputs.PreviousTransactionOutput").
			Find(&dbAcceptedTransactions)
		dbErrors = dbResult.GetErrors()
		if httpserverutils.HasDBError(dbErrors) {
			return httpserverutils.NewErrorFromDBErrors("failed to find transactions: ", dbErrors)
		}
		if len(dbAcceptedTransactions) != len(acceptedBlock.AcceptedTxIDs) {
			return errors.Errorf("some transaction are missing for block: %s", acceptedBlock.Hash)
		}

		for _, dbAcceptedTransaction := range dbAcceptedTransactions {
			for _, dbTransactionInput := range dbAcceptedTransaction.TransactionInputs {
				dbPreviousTransactionOutput := dbTransactionInput.PreviousTransactionOutput

				if dbPreviousTransactionOutput.IsSpent {
					return errors.Errorf("cannot spend an already spent transaction output: %s index: %d",
						dbAcceptedTransaction.TransactionID, dbTransactionInput.Index)
				}
				dbPreviousTransactionOutput.IsSpent = true
				dbResult = dbTx.Save(&dbPreviousTransactionOutput)
				dbErrors = dbResult.GetErrors()
				if httpserverutils.HasDBError(dbErrors) {
					return httpserverutils.NewErrorFromDBErrors("failed to update transactionOutput: ", dbErrors)
				}
			}

			// Don't use Save() here--it updates all fields in dbAcceptedTransaction
			dbAcceptedTransaction.AcceptingBlockID = rpcmodel.Uint64(dbAddedBlock.ID)
			dbResult = dbTx.
				Model(&dbmodels.Transaction{}).
				Where("id = ?", dbAcceptedTransaction.ID).
				Update("accepting_block_id", dbAddedBlock.ID)
			dbErrors = dbResult.GetErrors()
			if httpserverutils.HasDBError(dbErrors) {
				return httpserverutils.NewErrorFromDBErrors("failed to update transaction: ", dbErrors)
			}
		}

		// Don't use Save() here--it updates all fields in dbAcceptedBlock
		dbAccepedBlock.AcceptingBlockID = rpcmodel.Uint64(dbAddedBlock.ID)
		dbResult = dbTx.
			Model(&dbmodels.Block{}).
			Where("id = ?", dbAccepedBlock.ID).
			Update("accepting_block_id", dbAddedBlock.ID)
		dbErrors = dbResult.GetErrors()
		if httpserverutils.HasDBError(dbErrors) {
			return httpserverutils.NewErrorFromDBErrors("failed to update block: ", dbErrors)
		}
	}

	// Don't use Save() here--it updates all fields in dbAddedBlock
	dbAddedBlock.IsChainBlock = true
	dbResult = dbTx.
		Model(&dbmodels.Block{}).
		Where("id = ?", dbAddedBlock.ID).
		Update("is_chain_block", true)
	dbErrors = dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("failed to update block: ", dbErrors)
	}

	return nil
}

func doesBlockExist(dbTx *gorm.DB, blockHash string) (bool, error) {
	var dbBlock dbmodels.Block
	dbResult := dbTx.
		Where(&dbmodels.Block{BlockHash: blockHash}).
		First(&dbBlock)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return false, httpserverutils.NewErrorFromDBErrors("failed to find block: ", dbErrors)
	}
	return !httpserverutils.IsDBRecordNotFoundError(dbErrors), nil
}

func handleBlockAddedMsg(client *jsonrpc.Client, blockAdded *jsonrpc.BlockAddedMsg) error {
	db, err := database.DB()
	if err != nil {
		return err
	}
	blockExists, err := doesBlockExist(db, blockAdded.Header.BlockHash().String())
	if err != nil {
		return err
	}
	if blockExists {
		return nil
	}

	block, err := fetchBlock(client, blockAdded.Header.BlockHash())
	if err != nil {
		return err
	}

	missingAncestors, err := fetchMissingAncestors(client, block, nil)
	if err != nil {
		return err
	}

	blocks := append([]*utils.RawAndVerboseBlock{block}, missingAncestors...)
	err = bulkInsertBlocksData(client, blocks)
	if err != nil {
		return err
	}

	for _, block := range blocks {
		err := mqtt.PublishTransactionsNotifications(block.Verbose.RawTx)
		if err != nil {
			return err
		}
	}
	return nil
}

func fetchMissingAncestors(client *jsonrpc.Client, block *utils.RawAndVerboseBlock, blockExistingInMemory map[string]*utils.RawAndVerboseBlock) ([]*utils.RawAndVerboseBlock, error) {
	pendingBlocks := []*utils.RawAndVerboseBlock{block}
	missingAncestors := make([]*utils.RawAndVerboseBlock, 0)
	missingAncestorsSet := make(map[string]struct{})
	for len(pendingBlocks) > 0 {
		var currentBlock *utils.RawAndVerboseBlock
		currentBlock, pendingBlocks = pendingBlocks[0], pendingBlocks[1:]
		missingParentHashes, err := missingBlockHashes(currentBlock.Verbose.ParentHashes, blockExistingInMemory)
		if err != nil {
			return nil, err
		}
		blocksToPrependToPending := make([]*utils.RawAndVerboseBlock, 0, len(missingParentHashes))
		for _, missingHash := range missingParentHashes {
			if _, ok := missingAncestorsSet[missingHash]; ok {
				continue
			}
			hash, err := daghash.NewHashFromStr(missingHash)
			if err != nil {
				return nil, err
			}
			block, err := fetchBlock(client, hash)
			if err != nil {
				return nil, err
			}
			blocksToPrependToPending = append(blocksToPrependToPending, block)
		}
		if len(blocksToPrependToPending) == 0 {
			if currentBlock != block {
				missingAncestorsSet[currentBlock.Hash()] = struct{}{}
				missingAncestors = append(missingAncestors, currentBlock)
			}
			continue
		}
		log.Debugf("Found %s missing parents for block %s and fetched them", blocksToPrependToPending, currentBlock)
		blocksToPrependToPending = append(blocksToPrependToPending, currentBlock)
		pendingBlocks = append(blocksToPrependToPending, pendingBlocks...)
	}
	return missingAncestors, nil
}

// missingBlockHashes takes a slice of block hashes and returns
// a slice that contains all the block hashes that do not exist
// in the database or in the given blocksExistingInMemory map.
func missingBlockHashes(blockHashes []string, blocksExistingInMemory map[string]*utils.RawAndVerboseBlock) ([]string, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	// filter out all the hashes that exist in blocksExistingInMemory
	hashesNotInMemory := make([]string, 0)
	for _, hash := range blockHashes {
		if _, ok := blocksExistingInMemory[hash]; !ok {
			hashesNotInMemory = append(hashesNotInMemory, hash)
		}
	}

	// Check which of the hashes in hashesNotInMemory do
	// not exist in the database.
	var dbBlocks []dbmodels.Block
	dbResult := db.
		Model(&dbmodels.Block{}).
		Where("block_hash in (?)", hashesNotInMemory).
		Find(&dbBlocks)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find parent blocks: ", dbErrors)
	}
	if len(hashesNotInMemory) != len(dbBlocks) {
		// Some hashes are missing. Collect and return them
		var missingHashes []string
	outerLoop:
		for _, hash := range hashesNotInMemory {
			for _, dbBlock := range dbBlocks {
				if dbBlock.BlockHash == hash {
					continue outerLoop
				}
			}
			missingHashes = append(missingHashes, hash)
		}
		return missingHashes, nil
	}

	return nil, nil
}

// enqueueChainChangedMsg enqueues onChainChanged messages to be handled later
func enqueueChainChangedMsg(chainChanged *jsonrpc.ChainChangedMsg) {
	pendingChainChangedMsgs = append(pendingChainChangedMsgs, chainChanged)
}

// processChainChangedMsgs processes all pending onChainChanged messages.
// Messages that cannot yet be processed are re-enqueued.
func processChainChangedMsgs() error {
	var unprocessedChainChangedMessages []*jsonrpc.ChainChangedMsg
	for _, chainChanged := range pendingChainChangedMsgs {
		canHandle, err := canHandleChainChangedMsg(chainChanged)
		if err != nil {
			return errors.Wrap(err, "Could not resolve if can handle ChainChangedMsg")
		}
		if !canHandle {
			unprocessedChainChangedMessages = append(unprocessedChainChangedMessages, chainChanged)
			continue
		}

		err = mqtt.PublishUnacceptedTransactionsNotifications(chainChanged.RemovedChainBlockHashes)
		if err != nil {
			panic(errors.Errorf("Error while publishing unaccepted transactions notifications %s", err))
		}

		err = handleChainChangedMsg(chainChanged)
		if err != nil {
			return err
		}
	}
	pendingChainChangedMsgs = unprocessedChainChangedMessages
	return nil
}

func handleChainChangedMsg(chainChanged *jsonrpc.ChainChangedMsg) error {
	// Convert the data in chainChanged to something we can feed into
	// updateSelectedParentChain
	removedHashes, addedBlocks := convertChainChangedMsg(chainChanged)

	err := updateSelectedParentChain(removedHashes, addedBlocks)
	if err != nil {
		return errors.Wrap(err, "Could not update selected parent chain")
	}
	log.Infof("Chain changed: removed %d blocks and added %d block",
		len(removedHashes), len(addedBlocks))

	err = mqtt.PublishAcceptedTransactionsNotifications(chainChanged.AddedChainBlocks)
	if err != nil {
		return errors.Wrap(err, "Error while publishing accepted transactions notifications")
	}
	return mqtt.PublishSelectedTipNotification(addedBlocks[len(addedBlocks)-1].Hash)
}

// canHandleChainChangedMsg checks whether we have all the necessary data
// to successfully handle a ChainChangedMsg.
func canHandleChainChangedMsg(chainChanged *jsonrpc.ChainChangedMsg) (bool, error) {
	db, err := database.DB()
	if err != nil {
		return false, err
	}

	// Collect all referenced block hashes
	hashesIn := make([]string, 0, len(chainChanged.AddedChainBlocks)+len(chainChanged.RemovedChainBlockHashes))
	for _, hash := range chainChanged.RemovedChainBlockHashes {
		hashesIn = append(hashesIn, hash.String())
	}
	for _, block := range chainChanged.AddedChainBlocks {
		hashesIn = append(hashesIn, block.Hash.String())
	}

	// Make sure that all the hashes exist in the database
	var dbBlocks []dbmodels.Block
	dbResult := db.
		Model(&dbmodels.Block{}).
		Where("block_hash in (?)", hashesIn).
		Find(&dbBlocks)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return false, httpserverutils.NewErrorFromDBErrors("failed to find blocks: ", dbErrors)
	}
	if len(hashesIn) != len(dbBlocks) {
		return false, nil
	}

	// Make sure that chain changes are valid for this message
	hashesToIsChainBlocks := make(map[string]bool)
	for _, dbBlock := range dbBlocks {
		hashesToIsChainBlocks[dbBlock.BlockHash] = dbBlock.IsChainBlock
	}
	for _, hash := range chainChanged.RemovedChainBlockHashes {
		isDBBlockChainBlock := hashesToIsChainBlocks[hash.String()]
		if !isDBBlockChainBlock {
			return false, nil
		}
		hashesToIsChainBlocks[hash.String()] = false
	}
	for _, block := range chainChanged.AddedChainBlocks {
		isDBBlockChainBlock := hashesToIsChainBlocks[block.Hash.String()]
		if isDBBlockChainBlock {
			return false, nil
		}
		hashesToIsChainBlocks[block.Hash.String()] = true
	}

	return true, nil
}

func convertChainChangedMsg(chainChanged *jsonrpc.ChainChangedMsg) (
	removedHashes []string, addedBlocks []rpcmodel.ChainBlock) {

	removedHashes = make([]string, len(chainChanged.RemovedChainBlockHashes))
	for i, hash := range chainChanged.RemovedChainBlockHashes {
		removedHashes[i] = hash.String()
	}

	addedBlocks = make([]rpcmodel.ChainBlock, len(chainChanged.AddedChainBlocks))
	for i, addedBlock := range chainChanged.AddedChainBlocks {
		acceptedBlocks := make([]rpcmodel.AcceptedBlock, len(addedBlock.AcceptedBlocks))
		for j, acceptedBlock := range addedBlock.AcceptedBlocks {
			acceptedTxIDs := make([]string, len(acceptedBlock.AcceptedTxIDs))
			for k, acceptedTxID := range acceptedBlock.AcceptedTxIDs {
				acceptedTxIDs[k] = acceptedTxID.String()
			}
			acceptedBlocks[j] = rpcmodel.AcceptedBlock{
				Hash:          acceptedBlock.Hash.String(),
				AcceptedTxIDs: acceptedTxIDs,
			}
		}
		addedBlocks[i] = rpcmodel.ChainBlock{
			Hash:           addedBlock.Hash.String(),
			AcceptedBlocks: acceptedBlocks,
		}
	}

	return removedHashes, addedBlocks
}

// addBlocks inserts data in the given rawBlocks and verboseBlocks pairwise
// into the database.
func addBlocks(client *jsonrpc.Client, rawBlocks []string, verboseBlocks []rpcmodel.GetBlockVerboseResult) error {
	db, err := database.DB()
	if err != nil {
		return err
	}

	blocks := make([]*utils.RawAndVerboseBlock, 0)
	blockHashesToRawAndVerboseBlock := make(map[string]*utils.RawAndVerboseBlock)
	for i, rawBlock := range rawBlocks {
		blockExists, err := doesBlockExist(db, verboseBlocks[i].Hash)
		if err != nil {
			return err
		}
		if blockExists {
			continue
		}

		block := &utils.RawAndVerboseBlock{
			Raw:     rawBlock,
			Verbose: &verboseBlocks[i],
		}
		missingAncestors, err := fetchMissingAncestors(client, &utils.RawAndVerboseBlock{
			Raw:     rawBlock,
			Verbose: &verboseBlocks[i],
		}, blockHashesToRawAndVerboseBlock)
		if err != nil {
			return err
		}

		blocks = append(blocks, block)
		blockHashesToRawAndVerboseBlock[block.Hash()] = block

		blocks = append(blocks, missingAncestors...)
		for _, block := range missingAncestors {
			blockHashesToRawAndVerboseBlock[block.Hash()] = block
		}
	}
	return bulkInsertBlocksData(client, blocks)
}

// bulkInsertBlocksData inserts the given blocks and their data (transactions
// and new subnetworks data) to the database in chunks.
func bulkInsertBlocksData(client *jsonrpc.Client, blocks []*utils.RawAndVerboseBlock) error {
	db, err := database.DB()
	if err != nil {
		return err
	}
	dbTx := db.Begin()
	defer dbTx.RollbackUnlessCommitted()

	subnetworkIDToID, err := insertSubnetworks(dbTx, client, blocks)
	if err != nil {
		return err
	}

	transactionIDsToTxsWithMetadata, err := insertTransactions(dbTx, blocks, subnetworkIDToID)
	if err != nil {
		return err
	}

	err = insertTransactionOutputs(dbTx, transactionIDsToTxsWithMetadata)
	if err != nil {
		return err
	}

	err = insertTransactionInputs(dbTx, transactionIDsToTxsWithMetadata)
	if err != nil {
		return err
	}

	err = insertBlocks(dbTx, blocks, transactionIDsToTxsWithMetadata)
	if err != nil {
		return err
	}

	blockHashesToIDs, err := getBlocksAndParentIDs(dbTx, blocks)
	if err != nil {
		return err
	}

	err = insertBlockParents(dbTx, blocks, blockHashesToIDs)
	if err != nil {
		return err
	}

	err = insertRawBlocks(dbTx, blocks, blockHashesToIDs)
	if err != nil {
		return err
	}

	err = insertTransactionBlocks(dbTx, blocks, blockHashesToIDs, transactionIDsToTxsWithMetadata)
	if err != nil {
		return err
	}

	dbTx.Commit()
	log.Infof("Added %d blocks", len(blocks))
	return nil
}
