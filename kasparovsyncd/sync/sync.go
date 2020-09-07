package sync

import (
	"github.com/kaspanet/kaspad/app/appmessage"
	"github.com/kaspanet/kasparov/database"

	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/kaspadrpc"
	"github.com/kaspanet/kasparov/kasparovsyncd/mqtt"

	"github.com/kaspanet/kaspad/util/daghash"
	"github.com/pkg/errors"
)

// pendingChainChangedMsgs holds chainChangedMsgs in order of arrival
var pendingChainChangedMsgs []*appmessage.ChainChangedNotificationMessage

// StartSync keeps the node and the database in sync. On start, it downloads
// all data that's missing from the database, and once it's done it keeps
// sync with the node via notifications.
func StartSync(doneChan chan struct{}) error {
	client, err := kaspadrpc.GetClient()
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
func fetchInitialData(client *kaspadrpc.Client) error {
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
func sync(client *kaspadrpc.Client, doneChan chan struct{}) error {
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
			err := processChainChangedMsgs(client)
			if err != nil {
				return err
			}
		case <-doneChan:
			log.Infof("StartSync stopped")
			return nil
		}
	}
}

// syncBlocks attempts to download all DAG blocks starting with
// the bluest block, and then inserts them into the database.
func syncBlocks(client *kaspadrpc.Client) error {
	// Start syncing from the bluest block hash. We use blue score to
	// simulate the "last" block we have because blue-block order is
	// the order that the node uses in the various JSONRPC calls.
	startBlock, err := dbaccess.BluestBlock(database.NoTx())
	if err != nil {
		return err
	}
	var startHash string
	if startBlock != nil {
		startHash = startBlock.BlockHash
	}

	for {
		if startHash != "" {
			log.Debugf("Calling getBlocks with start hash %s", startHash)
		} else {
			log.Debugf("Calling getBlocks with no start hash")
		}
		blocksResult, err := client.GetBlocks(startHash, true, true)
		if err != nil {
			return err
		}
		if len(blocksResult.BlockHashes) == 0 {
			break
		}
		log.Debugf("Got %d blocks", len(blocksResult.BlockHashes))

		startHash = blocksResult.BlockHashes[len(blocksResult.BlockHashes)-1]
		err = addBlocks(client, blocksResult.BlockHexes, blocksResult.BlockVerboseData)
		if err != nil {
			return err
		}
	}

	return nil
}

// syncSelectedParentChain attempts to download the selected parent
// chain starting with the bluest chain-block, and then updates the
// database accordingly.
func syncSelectedParentChain(client *kaspadrpc.Client) error {
	// Start syncing from the selected tip hash
	startBlock, err := dbaccess.SelectedTip(database.NoTx())
	if err != nil {
		return err
	}
	startHash := startBlock.BlockHash

	for {
		log.Debugf("Calling getChainFromBlock with start hash %s", startHash)
		chainFromBlockResult, err := client.GetChainFromBlock(startHash, false)
		if err != nil {
			return err
		}
		if len(chainFromBlockResult.AddedChainBlocks) == 0 {
			break
		}

		startHash = chainFromBlockResult.AddedChainBlocks[len(chainFromBlockResult.AddedChainBlocks)-1].Hash
		err = updateSelectedParentChain(client, chainFromBlockResult.RemovedChainBlockHashes,
			chainFromBlockResult.AddedChainBlocks)
		if err != nil {
			return err
		}
	}
	return nil
}

// fetchBlock downloads the serialized block and raw block data of
// the block with hash blockHash.
func fetchBlock(client *kaspadrpc.Client, blockHash *daghash.Hash) (
	*rawAndVerboseBlock, error) {
	log.Debugf("Getting block %s from the RPC server", blockHash)
	blockHexResponse, err := client.GetBlock(blockHash.String(), "", true, true, true)
	if err != nil {
		return nil, err
	}
	return &rawAndVerboseBlock{
		Raw:     blockHexResponse.BlockHex,
		Verbose: blockHexResponse.BlockVerboseData,
	}, nil
}

// updateSelectedParentChain updates the database to reflect the current selected
// parent chain. First it "unaccepts" all removedChainHashes and then it "accepts"
// all addChainBlocks.
// Note that if this function may take a nil dbTx, in which case it would start
// a database transaction by itself and commit it before returning.
func updateSelectedParentChain(client *kaspadrpc.Client, removedChainHashes []string, addedChainBlocks []*appmessage.ChainBlock) error {
	unacceptedTransactions, err := dbaccess.AcceptedTransactionsByBlockHashes(database.NoTx(), removedChainHashes, dbmodels.TransactionRecommendedPreloadedFields...)
	if err != nil {
		return err
	}

	dbTx, err := database.NewTx()
	if err != nil {
		return err
	}

	defer dbTx.RollbackUnlessCommitted()

	for _, removedHash := range removedChainHashes {
		err := updateRemovedChainHashes(dbTx, removedHash)
		if err != nil {
			return err
		}
	}

	missingBlockHashes, err := fetchAndAddMissingAddedChainBlocks(client, dbTx, addedChainBlocks)
	if err != nil {
		return err
	}

	for _, addedBlock := range addedChainBlocks {
		err := updateAddedChainBlocks(dbTx, addedBlock)
		if err != nil {
			return err
		}
	}

	err = dbTx.Commit()
	if err != nil {
		return err
	}

	err = mqtt.PublishUnacceptedTransactionsNotifications(unacceptedTransactions)
	if err != nil {
		return errors.Wrap(err, "Error while publishing unaccepted transactions notifications")
	}

	err = mqtt.PublishAcceptedTransactionsNotifications(addedChainBlocks)
	if err != nil {
		return errors.Wrap(err, "Error while publishing accepted transactions notifications")

	}

	err = mqtt.PublishSelectedParentChainNotifications(removedChainHashes, addedChainBlocks)
	if err != nil {
		return errors.Wrap(err, "Error while publishing chain changes notifications")
	}

	for _, hash := range missingBlockHashes {
		err := mqtt.PublishBlockAddedNotifications(hash)
		if err != nil {
			return err
		}
	}

	return nil
}

// fetchAndAddMissingAddedChainBlocks takes cares of cases where a block referenced in a selectedParent-chain
// have not yet been added to the database. In that case - it fetches it and its missing ancestors and add them
// to the database.
func fetchAndAddMissingAddedChainBlocks(client *kaspadrpc.Client, dbTx *database.TxContext, addedChainBlocks []*appmessage.ChainBlock) (missingBlockHashes []string, err error) {
	missingBlockHashes = make([]string, 0)
	for _, block := range addedChainBlocks {
		dbBlock, err := dbaccess.BlockByHash(dbTx, block.Hash)
		if err != nil {
			return nil, err
		}

		if dbBlock != nil {
			continue
		}

		log.Debugf("Block %s not found in the database - fetching from node", block.Hash)
		blockHash, err := daghash.NewHashFromStr(block.Hash)
		if err != nil {
			return nil, err
		}

		addedBlockHashes, err := fetchAndAddBlock(client, dbTx, blockHash)
		if err != nil {
			return nil, err
		}
		missingBlockHashes = append(missingBlockHashes, addedBlockHashes...)
	}
	return missingBlockHashes, nil
}

// updateRemovedChainHashes "unaccepts" the block of the given removedHash.
// That is to say, it marks it as not in the selected parent chain in the
// following ways:
// * All its TransactionInputs.PreviousTransactionOutputs are set IsSpent = false
// * All its Transactions are set AcceptingBlockID = nil
// * The block is set IsChainBlock = false
// This function will return an error if any of the above are in an unexpected state
func updateRemovedChainHashes(dbTx *database.TxContext, removedHash string) error {
	dbBlock, err := dbaccess.BlockByHash(dbTx, removedHash)
	if err != nil {
		return err
	}
	if dbBlock == nil {
		return errors.Errorf("missing block for hash: %s", removedHash)
	}
	if !dbBlock.IsChainBlock {
		return errors.Errorf("block erroneously marked as not a chain block: %s", removedHash)
	}

	dbTransactions, err := dbaccess.AcceptedTransactionsByBlockID(dbTx, dbBlock.ID,
		dbmodels.TransactionFieldNames.InputsPreviousTransactionOutputs)
	if err != nil {
		return err
	}

	for _, dbTransaction := range dbTransactions {
		for _, dbTransactionInput := range dbTransaction.TransactionInputs {
			dbPreviousTransactionOutput := dbTransactionInput.PreviousTransactionOutput

			if !dbPreviousTransactionOutput.IsSpent {
				return errors.Errorf("cannot de-spend an unspent transaction output: %s index: %d",
					dbTransaction.TransactionID, dbTransactionInput.Index)
			}
			err := dbaccess.UpdateTransactionOutputIsSpent(dbTx, dbPreviousTransactionOutput.ID, false)
			if err != nil {
				return err
			}
		}

		dbTransaction.AcceptingBlockID = nil
		err := dbaccess.UpdateTransactionAcceptingBlockID(dbTx, dbTransaction.ID, nil)
		if err != nil {
			return err
		}
	}

	err = dbaccess.UpdateBlocksAcceptedByAcceptingBlock(dbTx, dbBlock.ID, nil)
	if err != nil {
		return err
	}

	err = dbaccess.UpdateBlockIsChainBlock(dbTx, dbBlock.ID, false)
	if err != nil {
		return err
	}

	return nil
}

// updateAddedChainBlocks "accepts" the given addedBlock. That is to say,
// it marks it as in the selected parent chain in the following ways:
// * All its TransactionInputs.PreviousTransactionOutputs are set IsSpent = true
// * All its Transactions are set AcceptingBlockID = addedBlock
// * The block is set IsChainBlock = true
// This function will return an error if any of the above are in an unexpected state
func updateAddedChainBlocks(dbTx *database.TxContext, addedBlock *appmessage.ChainBlock) error {
	dbAddedBlock, err := dbaccess.BlockByHash(dbTx, addedBlock.Hash)
	if err != nil {
		return err
	}
	if dbAddedBlock == nil {
		return errors.Errorf("missing block for hash %s", addedBlock.Hash)
	}
	if dbAddedBlock.IsChainBlock {
		return errors.Errorf("block erroneously marked as a chain block: %s", addedBlock.Hash)
	}

	for _, acceptedBlock := range addedBlock.AcceptedBlocks {
		dbAcceptedBlock, err := dbaccess.BlockByHash(dbTx, acceptedBlock.Hash)
		if err != nil {
			return err
		}
		if dbAcceptedBlock == nil {
			return errors.Errorf("missing block for hash: %s", acceptedBlock.Hash)
		}
		if dbAcceptedBlock.AcceptingBlockID != nil && *dbAcceptedBlock.AcceptingBlockID == dbAddedBlock.ID {
			return errors.Errorf("block %s erroneously marked as accepted by %s", acceptedBlock.Hash, addedBlock.Hash)
		}

		transactionIDsIn := make([]string, len(acceptedBlock.AcceptedTxIDs))
		for i, acceptedTxID := range acceptedBlock.AcceptedTxIDs {
			transactionIDsIn[i] = acceptedTxID
		}

		// We filter transaction by dbAcceptedBlock.ID because transaction malleability
		// can create a situation with multiple transactions on different blocks with
		// same ID and different hashes.
		dbAcceptedTransactions, err := dbaccess.TransactionsByIDsAndBlockID(dbTx, transactionIDsIn, dbAcceptedBlock.ID,
			dbmodels.TransactionFieldNames.InputsPreviousTransactionOutputs)
		if err != nil {
			return err
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
				err = dbaccess.UpdateTransactionOutputIsSpent(dbTx, dbPreviousTransactionOutput.ID, true)
				if err != nil {
					return err
				}
			}

			err := dbaccess.UpdateTransactionAcceptingBlockID(dbTx, dbAcceptedTransaction.ID, &dbAddedBlock.ID)
			if err != nil {
				return err
			}
		}

		err = dbaccess.UpdateBlockAcceptingBlockID(dbTx, dbAcceptedBlock.ID, &dbAddedBlock.ID)
		if err != nil {
			return err
		}
	}

	err = dbaccess.UpdateBlockIsChainBlock(dbTx, dbAddedBlock.ID, true)
	if err != nil {
		return err
	}

	return nil
}

func handleBlockAddedMsg(client *kaspadrpc.Client, blockAdded *appmessage.BlockAddedNotificationMessage) error {
	blockHash := blockAdded.Block.Header.BlockHash()
	blockExists, err := dbaccess.DoesBlockExist(database.NoTx(), blockHash.String())
	if err != nil {
		return err
	}
	if blockExists {
		return nil
	}

	dbTx, err := database.NewTx()
	if err != nil {
		return err
	}

	defer dbTx.RollbackUnlessCommitted()

	addedBlockHashes, err := fetchAndAddBlock(client, dbTx, blockHash)
	if err != nil {
		return err
	}

	err = dbTx.Commit()
	if err != nil {
		return err
	}

	for _, hash := range addedBlockHashes {
		err := mqtt.PublishBlockAddedNotifications(hash)
		if err != nil {
			return err
		}
	}
	return nil
}

func fetchAndAddBlock(client *kaspadrpc.Client, dbTx *database.TxContext,
	blockHash *daghash.Hash) (addedBlockHashes []string, err error) {

	block, err := fetchBlock(client, blockHash)
	if err != nil {
		return nil, err
	}

	missingAncestors, err := fetchMissingAncestors(client, dbTx, block, nil)
	if err != nil {
		return nil, err
	}

	blocks := append([]*rawAndVerboseBlock{block}, missingAncestors...)
	err = bulkInsertBlocksData(client, dbTx, blocks)
	if err != nil {
		return nil, err
	}

	addedBlockHashes = make([]string, len(blocks))
	for i, block := range blocks {
		addedBlockHashes[i] = block.hash()
	}

	return addedBlockHashes, nil
}

func fetchMissingAncestors(client *kaspadrpc.Client, dbTx *database.TxContext, block *rawAndVerboseBlock,
	blockExistingInMemory map[string]*rawAndVerboseBlock) ([]*rawAndVerboseBlock, error) {

	pendingBlocks := []*rawAndVerboseBlock{block}
	missingAncestors := make([]*rawAndVerboseBlock, 0)
	missingAncestorsSet := make(map[string]struct{})
	for len(pendingBlocks) > 0 {
		var currentBlock *rawAndVerboseBlock
		currentBlock, pendingBlocks = pendingBlocks[0], pendingBlocks[1:]
		missingParentHashes, err := missingBlockHashes(dbTx, currentBlock.Verbose.ParentHashes, blockExistingInMemory)
		if err != nil {
			return nil, err
		}
		blocksToPrependToPending := make([]*rawAndVerboseBlock, 0, len(missingParentHashes))
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
				missingAncestorsSet[currentBlock.hash()] = struct{}{}
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
func missingBlockHashes(dbTx *database.TxContext, blockHashes []string,
	blocksExistingInMemory map[string]*rawAndVerboseBlock) ([]string, error) {

	// filter out all the hashes that exist in blocksExistingInMemory
	hashesNotInMemory := make([]string, 0)
	for _, hash := range blockHashes {
		if _, ok := blocksExistingInMemory[hash]; !ok {
			hashesNotInMemory = append(hashesNotInMemory, hash)
		}
	}

	// Check which of the hashes in hashesNotInMemory do
	// not exist in the database.
	dbBlocks, err := dbaccess.BlocksByHashes(dbTx, hashesNotInMemory)
	if err != nil {
		return nil, err
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
func enqueueChainChangedMsg(chainChanged *appmessage.ChainChangedNotificationMessage) {
	pendingChainChangedMsgs = append(pendingChainChangedMsgs, chainChanged)
}

// processChainChangedMsgs processes all pending onChainChanged messages.
// Messages that cannot yet be processed are re-enqueued.
func processChainChangedMsgs(client *kaspadrpc.Client) error {
	var unprocessedChainChangedMessages []*appmessage.ChainChangedNotificationMessage
	for _, chainChanged := range pendingChainChangedMsgs {
		canHandle, err := canHandleChainChangedMsg(chainChanged)
		if err != nil {
			return errors.Wrap(err, "Could not resolve if can handle ChainChangedMsg")
		}
		if !canHandle {
			unprocessedChainChangedMessages = append(unprocessedChainChangedMessages, chainChanged)
			continue
		}

		err = handleChainChangedMsg(client, chainChanged)
		if err != nil {
			return err
		}
	}
	pendingChainChangedMsgs = unprocessedChainChangedMessages
	return nil
}

func handleChainChangedMsg(client *kaspadrpc.Client, chainChanged *appmessage.ChainChangedNotificationMessage) error {
	removedHashes := chainChanged.RemovedChainBlockHashes
	addedBlocks := chainChanged.AddedChainBlocks
	err := updateSelectedParentChain(client, removedHashes, addedBlocks)
	if err != nil {
		return errors.Wrap(err, "Could not update selected parent chain")
	}
	log.Infof("Chain changed: removed %d blocks and added %d block",
		len(removedHashes), len(addedBlocks))

	return mqtt.PublishSelectedTipNotification(addedBlocks[len(addedBlocks)-1].Hash)
}

// canHandleChainChangedMsg checks whether we have all the necessary data
// to successfully handle a ChainChangedMsg.
func canHandleChainChangedMsg(chainChanged *appmessage.ChainChangedNotificationMessage) (bool, error) {
	// Collect all referenced block hashes
	hashesIn := make([]string, 0, len(chainChanged.AddedChainBlocks)+len(chainChanged.RemovedChainBlockHashes))
	for _, hash := range chainChanged.RemovedChainBlockHashes {
		hashesIn = append(hashesIn, hash)
	}
	for _, block := range chainChanged.AddedChainBlocks {
		hashesIn = append(hashesIn, block.Hash)
	}

	dbBlocks, err := dbaccess.BlocksByHashes(database.NoTx(), hashesIn)
	if err != nil {
		return false, err
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
		isDBBlockChainBlock := hashesToIsChainBlocks[hash]
		if !isDBBlockChainBlock {
			return false, nil
		}
		hashesToIsChainBlocks[hash] = false
	}
	for _, block := range chainChanged.AddedChainBlocks {
		isDBBlockChainBlock := hashesToIsChainBlocks[block.Hash]
		if isDBBlockChainBlock {
			return false, nil
		}
		hashesToIsChainBlocks[block.Hash] = true
	}

	return true, nil
}

// addBlocks inserts data in the given rawBlocks and verboseBlocks pairwise
// into the database.
func addBlocks(client *kaspadrpc.Client, rawBlocks []string, verboseBlocks []*appmessage.BlockVerboseData) error {
	dbTx, err := database.NewTx()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	blocks := make([]*rawAndVerboseBlock, 0)
	blockHashesToRawAndVerboseBlock := make(map[string]*rawAndVerboseBlock)
	for i, rawBlock := range rawBlocks {
		blockExists, err := dbaccess.DoesBlockExist(dbTx, verboseBlocks[i].Hash)
		if err != nil {
			return err
		}
		if blockExists {
			continue
		}

		block := &rawAndVerboseBlock{
			Raw:     rawBlock,
			Verbose: verboseBlocks[i],
		}
		missingAncestors, err := fetchMissingAncestors(client, dbTx, &rawAndVerboseBlock{
			Raw:     rawBlock,
			Verbose: verboseBlocks[i],
		}, blockHashesToRawAndVerboseBlock)
		if err != nil {
			return err
		}

		blocks = append(blocks, block)
		blockHashesToRawAndVerboseBlock[block.hash()] = block

		blocks = append(blocks, missingAncestors...)
		for _, block := range missingAncestors {
			blockHashesToRawAndVerboseBlock[block.hash()] = block
		}
	}
	err = bulkInsertBlocksData(client, dbTx, blocks)
	if err != nil {
		return err
	}
	return dbTx.Commit()
}

// bulkInsertBlocksData inserts the given blocks and their data (transactions
// and new subnetworks data) to the database in chunks.
func bulkInsertBlocksData(client *kaspadrpc.Client, dbTx *database.TxContext, blocks []*rawAndVerboseBlock) error {
	subnetworkIDToID, err := insertSubnetworks(client, dbTx, blocks)
	if err != nil {
		return err
	}

	transactionHashesToTxsWithMetadata, err := insertTransactions(dbTx, blocks, subnetworkIDToID)
	if err != nil {
		return err
	}

	err = insertTransactionOutputs(dbTx, transactionHashesToTxsWithMetadata)
	if err != nil {
		return err
	}

	err = insertTransactionInputs(dbTx, transactionHashesToTxsWithMetadata)
	if err != nil {
		return err
	}

	err = insertRawTransactions(dbTx, transactionHashesToTxsWithMetadata)
	if err != nil {
		return err
	}

	err = insertBlocks(dbTx, blocks, transactionHashesToTxsWithMetadata)
	if err != nil {
		return err
	}

	blockHashesToIDs, err := getBlocksWithTheirAcceptedBlocksAndParentIDs(dbTx, blocks)
	if err != nil {
		return err
	}

	err = insertBlockParents(dbTx, blocks, blockHashesToIDs)
	if err != nil {
		return err
	}

	err = insertAcceptedBlocks(dbTx, blocks, blockHashesToIDs)
	if err != nil {
		return err
	}

	err = insertRawBlocks(dbTx, blocks, blockHashesToIDs)
	if err != nil {
		return err
	}

	err = insertTransactionBlocks(dbTx, blocks, blockHashesToIDs, transactionHashesToTxsWithMetadata)
	if err != nil {
		return err
	}

	log.Infof("Added %d blocks", len(blocks))
	return nil
}
