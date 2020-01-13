package main

import (
	"bytes"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
	"github.com/kaspanet/kasparov/jsonrpc"
	"github.com/kaspanet/kasparov/kasparovsyncd/mqtt"

	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kaspad/blockdag"
	"github.com/kaspanet/kaspad/rpcmodel"
	"github.com/kaspanet/kaspad/util"
	"github.com/kaspanet/kaspad/util/daghash"
	"github.com/kaspanet/kaspad/util/subnetworkid"
	"github.com/kaspanet/kaspad/wire"
	"github.com/pkg/errors"
	"github.com/t-tiger/gorm-bulk-insert"
)

const insertChunkSize = 3000

// pendingChainChangedMsgs holds chainChangedMsgs in order of arrival
var pendingChainChangedMsgs []*jsonrpc.ChainChangedMsg

// startSync keeps the node and the database in sync. On start, it downloads
// all data that's missing from the dabase, and once it's done it keeps
// sync with the node via notifications.
func startSync(doneChan chan struct{}) error {
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
			log.Infof("startSync stopped")
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
	*rawAndVerboseBlock, error) {
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
	return &rawAndVerboseBlock{
		rawBlock:     rawBlock,
		verboseBlock: verboseBlock,
	}, nil
}

// addBlocks inserts data in the given rawBlocks and verboseBlocks pairwise
// into the database. See addBlock for further details.
func addBlocks(client *jsonrpc.Client, rawBlocks []string, verboseBlocks []rpcmodel.GetBlockVerboseResult) error {
	db, err := database.DB()
	if err != nil {
		return err
	}

	blocks := make([]*rawAndVerboseBlock, 0)
	blocksMap := make(map[string]*rawAndVerboseBlock)
	for i, rawBlock := range rawBlocks {
		blockExists, err := doesBlockExist(db, verboseBlocks[i].Hash)
		if err != nil {
			return err
		}
		if blockExists {
			continue
		}

		blockAndMissingAncestors, err := fetchBlockAndMissingAncestors(client, &rawAndVerboseBlock{
			rawBlock:     rawBlock,
			verboseBlock: &verboseBlocks[i],
		}, blocksMap)
		if err != nil {
			return err
		}

		blocks = append(blocks, blockAndMissingAncestors...)
		for _, block := range blockAndMissingAncestors {
			blocksMap[block.verboseBlock.Hash] = block
		}
	}
	return addBlocksAndTransactions(client, blocks)
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

func addBlocksAndTransactions(client *jsonrpc.Client, blocks []*rawAndVerboseBlock) error {
	db, err := database.DB()
	if err != nil {
		return err
	}
	dbTx := db.Begin()
	defer dbTx.RollbackUnlessCommitted()

	transactionIDtoTxWithMetaData, err := insertBlocksTransactions(dbTx, client, blocks)
	if err != nil {
		return err
	}

	err = insertBlocksTransactionOutputs(dbTx, transactionIDtoTxWithMetaData)
	if err != nil {
		return err
	}

	err = insertBlocksTransactionInputs(dbTx, transactionIDtoTxWithMetaData)
	if err != nil {
		return err
	}

	blockHashToID, err := insertBlocks(dbTx, blocks, transactionIDtoTxWithMetaData)
	if err != nil {
		return err
	}

	err = insertTransactionBlocks(dbTx, blocks, blockHashToID, transactionIDtoTxWithMetaData)
	if err != nil {
		return err
	}

	dbTx.Commit()
	log.Infof("Added %d blocks", len(blocks))
	return nil
}

func insertBlocks(dbTx *gorm.DB, blocks []*rawAndVerboseBlock, transactionIDtoTxWithMetaData map[string]*txWithMetaData) (map[string]uint64, error) {
	blocksToAdd := make([]interface{}, len(blocks))
	for i, block := range blocks {
		blockMass := uint64(0)
		for _, tx := range block.verboseBlock.RawTx {
			blockMass += transactionIDtoTxWithMetaData[tx.TxID].mass
		}
		var err error
		blocksToAdd[i], err = makeDBBlock(block.verboseBlock, blockMass)
		if err != nil {
			return nil, err
		}
	}
	err := bulkInsert(dbTx, blocksToAdd, insertChunkSize)
	if err != nil {
		return nil, err
	}

	blockHashToID, err := getBlocksAndParentsIDs(dbTx, blocks)
	if err != nil {
		return nil, err
	}

	dataToAdd := make([]interface{}, 0)
	for _, block := range blocks {
		blockID, ok := blockHashToID[block.verboseBlock.Hash]
		if !ok {
			return nil, errors.Errorf("couldn't find block ID for block %s", block.verboseBlock.Hash)
		}
		dbBlockParents, err := makeBlockParents(blockHashToID, block.verboseBlock)
		if err != nil {
			return nil, err
		}
		dbRawBlock, err := makeDBRawBlock(block.rawBlock, blockID)
		if err != nil {
			return nil, err
		}
		for _, dbBlockParent := range dbBlockParents {
			dataToAdd = append(dataToAdd, dbBlockParent)
		}
		dataToAdd = append(dataToAdd, dbRawBlock)
	}
	err = bulkInsert(dbTx, dataToAdd, insertChunkSize)
	if err != nil {
		return nil, err
	}
	return blockHashToID, nil
}

func getBlocksAndParentsIDs(dbTx *gorm.DB, blocks []*rawAndVerboseBlock) (map[string]uint64, error) {
	blockSet := make(map[string]struct{})
	for _, block := range blocks {
		blockSet[block.verboseBlock.Hash] = struct{}{}
		for _, parentHash := range block.verboseBlock.ParentHashes {
			blockSet[parentHash] = struct{}{}
		}
	}

	blockHashes := make([]string, len(blockSet))
	i := 0
	for hash := range blockSet {
		blockHashes[i] = hash
	}

	var dbBlocks []*dbmodels.Block
	dbResult := dbTx.
		Where("block_hash in (?)", blockHashes).
		Find(&dbBlocks)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find blocks: ", dbErrors)
	}

	blockHashToID := make(map[string]uint64)
	for _, dbBlock := range dbBlocks {
		blockHashToID[dbBlock.BlockHash] = dbBlock.ID
	}
	return blockHashToID, nil
}

func insertTransactionBlocks(dbTx *gorm.DB, blocks []*rawAndVerboseBlock, blockHashToID map[string]uint64, transactionIDtoTxWithMetaData map[string]*txWithMetaData) error {
	transactionBlocksToAdd := make([]interface{}, 0)
	for _, block := range blocks {
		blockID, ok := blockHashToID[block.verboseBlock.Hash]
		if !ok {
			return errors.Errorf("couldn't find block ID for block %s", block.verboseBlock.Hash)
		}
		for i, tx := range block.verboseBlock.RawTx {
			transactionBlocksToAdd = append(transactionBlocksToAdd, &dbmodels.TransactionBlock{
				TransactionID: transactionIDtoTxWithMetaData[tx.TxID].id,
				BlockID:       blockID,
				Index:         uint32(i),
			})
		}
	}
	return bulkInsert(dbTx, transactionBlocksToAdd, insertChunkSize)
}

type outpoint struct {
	transactionID string
	index         uint32
}

func outpointSetToSqlTuples(outpointToID map[outpoint]struct{}) [][]interface{} {
	outpoints := make([][]interface{}, len(outpointToID))
	i := 0
	for o := range outpointToID {
		outpoints[i] = []interface{}{o.transactionID, o.index}
		i++
	}
	return outpoints
}

func insertBlocksTransactionInputs(dbTx *gorm.DB, transactionIDtoTxWithMetaData map[string]*txWithMetaData) error {
	outpointsSet := make(map[outpoint]struct{})
	newNonCoinbaseTransactions := make(map[string]*txWithMetaData)
	inputsCount := 0
	for transactionID, transaction := range transactionIDtoTxWithMetaData {
		if !transaction.isNew {
			continue
		}
		isCoinbase, err := isTransactionCoinbase(transaction.verboseTx)
		if err != nil {
			return err
		}
		if isCoinbase {
			continue
		}

		newNonCoinbaseTransactions[transactionID] = transaction
		inputsCount += len(transaction.verboseTx.Vin)
		for _, txIn := range transaction.verboseTx.Vin {
			outpointsSet[outpoint{
				transactionID: txIn.TxID,
				index:         txIn.Vout,
			}] = struct{}{}
		}
	}

	if inputsCount == 0 {
		return nil
	}

	outpoints := outpointSetToSqlTuples(outpointsSet)

	var dbPreviousTransactionsOutputs []*dbmodels.TransactionOutput
	dbResult := dbTx.
		Joins("LEFT JOIN `transactions` ON `transactions`.`id` = `transaction_outputs`.`transaction_id`").
		Where("(`transactions`.`transaction_id`, `transaction_outputs`.`index`) IN (?)", outpoints).
		Preload("Transaction").
		Find(&dbPreviousTransactionsOutputs)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("failed to find previous transaction outputs: ", dbErrors)
	}

	if len(dbPreviousTransactionsOutputs) != len(outpoints) {
		return errors.New("couldn't fetch all of the requested outpoints")
	}

	outpointToID := make(map[outpoint]uint64)
	for _, dbTransactionOutput := range dbPreviousTransactionsOutputs {
		outpointToID[outpoint{
			transactionID: dbTransactionOutput.Transaction.TransactionID,
			index:         dbTransactionOutput.Index,
		}] = dbTransactionOutput.ID
	}

	inputsToAdd := make([]interface{}, inputsCount)
	inputIterator := 0
	for _, transaction := range newNonCoinbaseTransactions {
		for i, txIn := range transaction.verboseTx.Vin {
			scriptSig, err := hex.DecodeString(txIn.ScriptSig.Hex)
			if err != nil {
				return nil
			}
			prevOutputID, ok := outpointToID[outpoint{
				transactionID: txIn.TxID,
				index:         txIn.Vout,
			}]
			if !ok || prevOutputID == 0 {
				return errors.Errorf("couldn't find ID for outpoint (%s:%s)", txIn.TxID, txIn.Vout)
			}
			inputsToAdd[inputIterator] = &dbmodels.TransactionInput{
				TransactionID:               transaction.id,
				PreviousTransactionOutputID: prevOutputID,
				Index:                       uint32(i),
				SignatureScript:             scriptSig,
				Sequence:                    txIn.Sequence,
			}
			inputIterator++
		}
	}
	return bulkInsert(dbTx, inputsToAdd, insertChunkSize)
}

func insertBlocksTransactionOutputs(dbTx *gorm.DB, transactionIDtoTxWithMetaData map[string]*txWithMetaData) error {
	addressToAddressID, err := insertBlocksTransactionAddresses(dbTx, transactionIDtoTxWithMetaData)
	if err != nil {
		return err
	}

	outputsToAdd := make([]interface{}, 0)
	for _, transaction := range transactionIDtoTxWithMetaData {
		if !transaction.isNew {
			continue
		}
		for i, txOut := range transaction.verboseTx.Vout {
			scriptPubKey, err := hex.DecodeString(txOut.ScriptPubKey.Hex)
			if err != nil {
				return errors.WithStack(err)
			}
			var addressID *uint64
			if txOut.ScriptPubKey.Address != nil {
				addressID = rpcmodel.Uint64(addressToAddressID[*txOut.ScriptPubKey.Address])
			}
			outputsToAdd = append(outputsToAdd, &dbmodels.TransactionOutput{
				TransactionID: transaction.id,
				Index:         uint32(i),
				Value:         txOut.Value,
				IsSpent:       false, // This must be false for updateSelectedParentChain to work properly
				ScriptPubKey:  scriptPubKey,
				AddressID:     addressID,
			})
		}
	}

	return bulkInsert(dbTx, outputsToAdd, insertChunkSize)
}

func insertBlocksTransactionAddresses(dbTx *gorm.DB, transactionIDtoTxWithMetaData map[string]*txWithMetaData) (map[string]uint64, error) {
	addressSet := make(map[string]struct{})
	for _, transaction := range transactionIDtoTxWithMetaData {
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

	addressToAddressID := make(map[string]uint64)
	for _, dbAddress := range dbAddresses {
		addressToAddressID[dbAddress.Address] = dbAddress.ID
	}

	newAddresses := make([]string, 0)
	for address, id := range addressToAddressID {
		if id != 0 {
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

	err := bulkInsert(dbTx, addressesToAdd, insertChunkSize)
	if err != nil {
		return nil, err
	}

	var dbNewAddresses []*dbmodels.Address
	dbResult = dbTx.
		Where("address in (?)", newAddresses).
		Find(&dbNewAddresses)
	dbErrors = dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find blocks: ", dbErrors)
	}

	if len(dbNewAddresses) != len(newAddresses) {
		return nil, errors.New("couldn't add all new addresses")
	}

	for _, dbNewAddress := range dbNewAddresses {
		addressToAddressID[dbNewAddress.Address] = dbNewAddress.ID
	}
	return addressToAddressID, nil
}

type txWithMetaData struct {
	verboseTx *rpcmodel.TxRawResult
	id        uint64
	isNew     bool
	mass      uint64
}

func transactionIDtoTxWithMetaDataToTransactionIDs(transactionIDtoTxWithMetaData map[string]*txWithMetaData) []string {
	transactionIDs := make([]string, len(transactionIDtoTxWithMetaData))
	i := 0
	for txID := range transactionIDtoTxWithMetaData {
		transactionIDs[i] = txID
		i++
	}
	return transactionIDs
}

func insertBlocksTransactions(dbTx *gorm.DB, client *jsonrpc.Client, blocks []*rawAndVerboseBlock) (map[string]*txWithMetaData, error) {
	subnetworkIDToID, err := insertBlocksSubnetworks(dbTx, client, blocks)
	if err != nil {
		return nil, err
	}

	transactionIDtoTxWithMetaData := make(map[string]*txWithMetaData)
	for _, block := range blocks {
		for _, transaction := range block.verboseBlock.RawTx {
			transactionIDtoTxWithMetaData[transaction.TxID] = &txWithMetaData{
				verboseTx: &transaction,
			}
		}
	}

	transactionIDs := transactionIDtoTxWithMetaDataToTransactionIDs(transactionIDtoTxWithMetaData)

	var dbTransactions []*dbmodels.Transaction
	dbResult := dbTx.
		Where("transaction_id in (?)", transactionIDs).
		Find(&dbTransactions)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find transactions: ", dbErrors)
	}

	for _, dbTransaction := range dbTransactions {
		transactionIDtoTxWithMetaData[dbTransaction.TransactionID].id = dbTransaction.ID
		transactionIDtoTxWithMetaData[dbTransaction.TransactionID].mass = dbTransaction.Mass
	}

	newTransactions := make([]string, 0)
	for txID, verboseTx := range transactionIDtoTxWithMetaData {
		if verboseTx.id != 0 {
			continue
		}
		newTransactions = append(newTransactions, txID)
	}

	transactionsToAdd := make([]interface{}, len(newTransactions))
	for i, id := range newTransactions {
		verboseTx := transactionIDtoTxWithMetaData[id].verboseTx
		mass, err := calcTxMass(dbTx, verboseTx)
		if err != nil {
			return nil, err
		}
		transactionIDtoTxWithMetaData[id].mass = mass

		payload, err := hex.DecodeString(verboseTx.Payload)
		if err != nil {
			return nil, err
		}

		subnetworkID, ok := subnetworkIDToID[verboseTx.Subnetwork]
		if !ok {
			return nil, errors.Errorf("couldn't find ID for subnetwork %s", verboseTx.Subnetwork)
		}
		transactionsToAdd[i] = dbmodels.Transaction{
			TransactionHash: verboseTx.Hash,
			TransactionID:   verboseTx.TxID,
			LockTime:        verboseTx.LockTime,
			SubnetworkID:    subnetworkID,
			Gas:             verboseTx.Gas,
			PayloadHash:     verboseTx.PayloadHash,
			Payload:         payload,
			Mass:            mass,
		}
	}

	err = bulkInsert(dbTx, transactionsToAdd, insertChunkSize)
	if err != nil {
		return nil, err
	}

	var dbNewTransactions []*dbmodels.Transaction
	dbResult = dbTx.
		Where("transaction_id in (?)", newTransactions).
		Find(&dbNewTransactions)
	dbErrors = dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find blocks: ", dbErrors)
	}

	if len(dbNewTransactions) != len(newTransactions) {
		return nil, errors.New("couldn't add all new transactions")
	}

	for _, dbTransaction := range dbNewTransactions {
		transactionIDtoTxWithMetaData[dbTransaction.TransactionID].id = dbTransaction.ID
		transactionIDtoTxWithMetaData[dbTransaction.TransactionID].isNew = true
	}
	return transactionIDtoTxWithMetaData, nil
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

func insertBlocksSubnetworks(dbTx *gorm.DB, client *jsonrpc.Client, blocks []*rawAndVerboseBlock) (map[string]uint64, error) {
	subnetworkSet := make(map[string]struct{})
	for _, block := range blocks {
		for _, transaction := range block.verboseBlock.RawTx {
			subnetworkSet[transaction.Subnetwork] = struct{}{}
		}
	}

	subnetworkIDs := stringsSetToSlice(subnetworkSet)

	var dbSubnetworks []*dbmodels.Subnetwork
	dbResult := dbTx.
		Where("subnetwork_id in (?)", subnetworkIDs).
		Find(&dbSubnetworks)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find subnetworks: ", dbErrors)
	}

	subnetworkIDToID := make(map[string]uint64)
	for _, dbSubnetwork := range dbSubnetworks {
		subnetworkIDToID[dbSubnetwork.SubnetworkID] = dbSubnetwork.ID
	}

	newSubnetworks := make([]string, 0)
	for subnetworkID, id := range subnetworkIDToID {
		if id != 0 {
			continue
		}
		newSubnetworks = append(newSubnetworks, subnetworkID)
	}

	subnetworksToAdd := make([]interface{}, len(newSubnetworks))
	for i, subnetworkID := range newSubnetworks {
		subnetwork, err := client.GetSubnetwork(subnetworkID)
		if err != nil {
			return nil, err
		}
		subnetworksToAdd[i] = dbmodels.Subnetwork{
			SubnetworkID: subnetworkID,
			GasLimit:     subnetwork.GasLimit,
		}
	}

	err := bulkInsert(dbTx, subnetworksToAdd, insertChunkSize)
	if err != nil {
		return nil, err
	}

	var dbNewSubnetworks []*dbmodels.Subnetwork
	dbResult = dbTx.
		Where("subnetwork_id in (?)", newSubnetworks).
		Find(&dbNewSubnetworks)
	dbErrors = dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find blocks: ", dbErrors)
	}

	if len(dbNewSubnetworks) != len(newSubnetworks) {
		return nil, errors.New("couldn't add all new subnetworks")
	}

	for _, dbSubnetwork := range dbNewSubnetworks {
		subnetworkIDToID[dbSubnetwork.SubnetworkID] = dbSubnetwork.ID
	}
	return subnetworkIDToID, nil
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

func makeBlockParents(blockHashToID map[string]uint64, verboseBlock *rpcmodel.GetBlockVerboseResult) ([]*dbmodels.ParentBlock, error) {
	// Exit early if this is the genesis block
	if len(verboseBlock.ParentHashes) == 0 {
		return nil, nil
	}

	blockID, ok := blockHashToID[verboseBlock.Hash]
	if !ok {
		return nil, errors.Errorf("couldn't find block ID for block %s", verboseBlock.Hash)
	}
	dbParentBlocks := make([]*dbmodels.ParentBlock, len(verboseBlock.ParentHashes))
	for i, parentHash := range verboseBlock.ParentHashes {
		parentID, ok := blockHashToID[parentHash]
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

func calcTxMass(dbTx *gorm.DB, transaction *rpcmodel.TxRawResult) (uint64, error) {
	msgTx, err := convertTxRawResultToMsgTx(transaction)
	if err != nil {
		return 0, err
	}
	prevTxIDs := make([]string, len(transaction.Vin))
	for i, txIn := range transaction.Vin {
		prevTxIDs[i] = txIn.TxID
	}
	var prevDBTransactionsOutputs []dbmodels.TransactionOutput
	dbResult := dbTx.
		Joins("LEFT JOIN `transactions` ON `transactions`.`id` = `transaction_outputs`.`transaction_id`").
		Where("transactions.transaction_id in (?)", prevTxIDs).
		Preload("Transaction").
		Find(&prevDBTransactionsOutputs)
	dbErrors := dbResult.GetErrors()
	if len(dbErrors) > 0 {
		return 0, httpserverutils.NewErrorFromDBErrors("error fetching previous transactions: ", dbErrors)
	}
	prevScriptPubKeysMap := make(map[string]map[uint32][]byte)
	for _, prevDBTransactionsOutput := range prevDBTransactionsOutputs {
		txID := prevDBTransactionsOutput.Transaction.TransactionID
		if prevScriptPubKeysMap[txID] == nil {
			prevScriptPubKeysMap[txID] = make(map[uint32][]byte)
		}
		prevScriptPubKeysMap[txID][prevDBTransactionsOutput.Index] = prevDBTransactionsOutput.ScriptPubKey
	}
	orderedPrevScriptPubKeys := make([][]byte, len(transaction.Vin))
	for i, txIn := range transaction.Vin {
		orderedPrevScriptPubKeys[i] = prevScriptPubKeysMap[txIn.TxID][uint32(i)]
	}
	return blockdag.CalcTxMass(util.NewTx(msgTx), orderedPrevScriptPubKeys), nil
}

func convertTxRawResultToMsgTx(tx *rpcmodel.TxRawResult) (*wire.MsgTx, error) {
	txIns := make([]*wire.TxIn, len(tx.Vin))
	for i, txIn := range tx.Vin {
		prevTxID, err := daghash.NewTxIDFromStr(txIn.TxID)
		if err != nil {
			return nil, err
		}
		signatureScript, err := hex.DecodeString(txIn.ScriptSig.Hex)
		if err != nil {
			return nil, err
		}
		txIns[i] = &wire.TxIn{
			PreviousOutpoint: wire.Outpoint{
				TxID:  *prevTxID,
				Index: txIn.Vout,
			},
			SignatureScript: signatureScript,
			Sequence:        txIn.Sequence,
		}
	}
	txOuts := make([]*wire.TxOut, len(tx.Vout))
	for i, txOut := range tx.Vout {
		scriptPubKey, err := hex.DecodeString(txOut.ScriptPubKey.Hex)
		if err != nil {
			return nil, err
		}
		txOuts[i] = &wire.TxOut{
			Value:        txOut.Value,
			ScriptPubKey: scriptPubKey,
		}
	}
	subnetworkID, err := subnetworkid.NewFromStr(tx.Subnetwork)
	if err != nil {
		return nil, err
	}
	if subnetworkID.IsEqual(subnetworkid.SubnetworkIDNative) {
		return wire.NewNativeMsgTx(tx.Version, txIns, txOuts), nil
	}
	payload, err := hex.DecodeString(tx.Payload)
	if err != nil {
		return nil, err
	}
	return wire.NewSubnetworkMsgTx(tx.Version, txIns, txOuts, subnetworkID, tx.Gas, payload), nil
}

func isTransactionCoinbase(transaction *rpcmodel.TxRawResult) (bool, error) {
	subnetwork, err := subnetworkid.NewFromStr(transaction.Subnetwork)
	if err != nil {
		return false, err
	}
	return subnetwork.IsEqual(subnetworkid.SubnetworkIDCoinbase), nil
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

type rawAndVerboseBlock struct {
	rawBlock     string
	verboseBlock *rpcmodel.GetBlockVerboseResult
}

func (r *rawAndVerboseBlock) String() string {
	return r.verboseBlock.Hash
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

	blocks, err := fetchBlockAndMissingAncestors(client, block, nil)
	if err != nil {
		return err
	}

	err = addBlocksAndTransactions(client, blocks)
	if err != nil {
		return err
	}

	for _, block := range blocks {
		err := mqtt.PublishTransactionsNotifications(block.verboseBlock.RawTx)
		if err != nil {
			return err
		}
	}
	return nil
}

func fetchBlockAndMissingAncestors(client *jsonrpc.Client, block *rawAndVerboseBlock, blockExistingInMemory map[string]*rawAndVerboseBlock) ([]*rawAndVerboseBlock, error) {
	pendingBlocks := []*rawAndVerboseBlock{block}
	blocksToAdd := make([]*rawAndVerboseBlock, 0)
	blocksToAddSet := make(map[string]struct{})
	for len(pendingBlocks) > 0 {
		var currentBlock *rawAndVerboseBlock
		currentBlock, pendingBlocks = pendingBlocks[0], pendingBlocks[1:]
		missingHashes, err := missingParentHashes(currentBlock.verboseBlock.ParentHashes, blockExistingInMemory)
		if err != nil {
			return nil, err
		}
		blocksToPrependToPending := make([]*rawAndVerboseBlock, 0, len(missingHashes))
		for _, missingHash := range missingHashes {
			if _, ok := blocksToAddSet[missingHash]; ok {
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
			blocksToAddSet[currentBlock.verboseBlock.Hash] = struct{}{}
			blocksToAdd = append(blocksToAdd, currentBlock)
			continue
		}
		log.Debugf("Found %s missing parents for block %s and fetched them", blocksToPrependToPending, currentBlock)
		blocksToPrependToPending = append(blocksToPrependToPending, currentBlock)
		pendingBlocks = append(blocksToPrependToPending, pendingBlocks...)
	}
	return blocksToAdd, nil
}

func missingParentHashes(parentHashes []string, blockExistingInMemory map[string]*rawAndVerboseBlock) ([]string, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	parentsNotInMemory := make([]string, 0)
	for _, hash := range parentHashes {
		if _, ok := blockExistingInMemory[hash]; !ok {
			parentsNotInMemory = append(parentsNotInMemory, hash)
		}
	}

	// Make sure that all the parent hashes exist in the database
	var dbParentBlocks []dbmodels.Block
	dbResult := db.
		Model(&dbmodels.Block{}).
		Where("block_hash in (?)", parentsNotInMemory).
		Find(&dbParentBlocks)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find parent blocks: ", dbErrors)
	}
	if len(parentsNotInMemory) != len(dbParentBlocks) {
		// Some parent hashes are missing. Collect and return them
		var missingHashes []string
	outerLoop:
		for _, hash := range parentsNotInMemory {
			for _, dbParentBlock := range dbParentBlocks {
				if dbParentBlock.BlockHash == hash {
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

func bulkInsert(db *gorm.DB, objects []interface{}, chunkSize int, excludeColumns ...string) error {
	return errors.WithStack(gormbulk.BulkInsert(db, objects, chunkSize, excludeColumns...))
}
