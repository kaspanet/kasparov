package apimodels

import (
	"encoding/hex"
	"github.com/kaspanet/kaspad/domain/dagconfig"
	"github.com/kaspanet/kaspad/util/pointers"
	"github.com/kaspanet/kaspad/util/subnetworkid"
	"github.com/pkg/errors"
	"sort"

	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/serializer"
)

func confirmations(acceptingBlockBlueScore *uint64, selectedTipBlueScore uint64) uint64 {
	if acceptingBlockBlueScore == nil {
		return 0
	}
	return selectedTipBlueScore - *acceptingBlockBlueScore + 1
}

// ConvertTxModelToTxResponse converts a transaction database object to a TransactionResponse
func ConvertTxModelToTxResponse(tx *dbmodels.Transaction, selectedTipBlueScore uint64) *TransactionResponse {
	txRes := &TransactionResponse{
		TransactionHash: tx.TransactionHash,
		TransactionID:   tx.TransactionID,
		SubnetworkID:    tx.Subnetwork.SubnetworkID,
		LockTime:        serializer.BytesToUint64(tx.LockTime),
		Gas:             tx.Gas,
		PayloadHash:     tx.PayloadHash,
		Payload:         hex.EncodeToString(tx.Payload),
		Inputs:          make([]*TransactionInputResponse, len(tx.TransactionInputs)),
		Outputs:         make([]*TransactionOutputResponse, len(tx.TransactionOutputs)),
		Mass:            tx.Mass,
		Version:         tx.Version,
		Raw:             hex.EncodeToString(tx.RawTransaction.TransactionData),
	}
	if tx.AcceptingBlock != nil {
		txRes.AcceptingBlockHash = &tx.AcceptingBlock.BlockHash
		txRes.AcceptingBlockBlueScore = &tx.AcceptingBlock.BlueScore
	}

	txRes.Confirmations = pointers.Uint64(confirmations(txRes.AcceptingBlockBlueScore, selectedTipBlueScore))
	for i, txOut := range tx.TransactionOutputs {
		txRes.Outputs[i] = &TransactionOutputResponse{
			Value:        txOut.Value,
			ScriptPubKey: hex.EncodeToString(txOut.ScriptPubKey),
			Index:        txOut.Index,
			IsSpent:      txOut.IsSpent,
		}
		if txOut.Address != nil {
			txRes.Outputs[i].Address = txOut.Address.Address
		}
	}
	sort.Slice(txRes.Outputs, func(i, j int) bool {
		return txRes.Outputs[i].Index < txRes.Outputs[j].Index
	})

	for i, txIn := range tx.TransactionInputs {
		txRes.Inputs[i] = &TransactionInputResponse{
			PreviousTransactionID:          txIn.PreviousTransactionOutput.Transaction.TransactionID,
			PreviousTransactionOutputIndex: txIn.PreviousTransactionOutput.Index,
			SignatureScript:                hex.EncodeToString(txIn.SignatureScript),
			Sequence:                       serializer.BytesToUint64(txIn.Sequence),
			Index:                          txIn.Index,
		}
		if txIn.PreviousTransactionOutput.Address != nil {
			txRes.Inputs[i].Address = txIn.PreviousTransactionOutput.Address.Address
		}
	}
	sort.Slice(txRes.Inputs, func(i, j int) bool {
		return txRes.Inputs[i].Index < txRes.Inputs[j].Index
	})

	return txRes
}

// ConvertBlockModelToBlockResponse converts a block database object into a BlockResponse
func ConvertBlockModelToBlockResponse(block *dbmodels.Block, selectedTipBlueScore uint64) *BlockResponse {
	blockRes := &BlockResponse{
		BlockHash:            block.BlockHash,
		Version:              block.Version,
		HashMerkleRoot:       block.HashMerkleRoot,
		AcceptedIDMerkleRoot: block.AcceptedIDMerkleRoot,
		UTXOCommitment:       block.UTXOCommitment,
		Timestamp:            uint64(block.Timestamp.Unix()),
		Bits:                 block.Bits,
		Nonce:                serializer.BytesToUint64(block.Nonce),
		ParentBlockHashes:    make([]string, len(block.ParentBlocks)),
		AcceptedBlockHashes:  make([]string, len(block.AcceptedBlocks)),
		BlueScore:            block.BlueScore,
		IsChainBlock:         block.IsChainBlock,
		Mass:                 block.Mass,
	}
	if block.AcceptingBlock != nil {
		blockRes.AcceptingBlockHash = &block.AcceptingBlock.BlockHash
		blockRes.AcceptingBlockBlueScore = &block.AcceptingBlock.BlueScore
	}
	blockRes.Confirmations = pointers.Uint64(confirmations(blockRes.AcceptingBlockBlueScore, selectedTipBlueScore))
	for i, parent := range block.ParentBlocks {
		blockRes.ParentBlockHashes[i] = parent.BlockHash
	}
	for i, acceptedBlock := range block.AcceptedBlocks {
		blockRes.AcceptedBlockHashes[i] = acceptedBlock.BlockHash
	}
	return blockRes
}

// ConvertTransactionOutputModelToTransactionOutputResponse converts a transaction output
// database object into a TransactionOutputResponse
func ConvertTransactionOutputModelToTransactionOutputResponse(transactionOutput *dbmodels.TransactionOutput,
	selectedTipBlueScore uint64, activeNetParams *dagconfig.Params, isSpent bool) (*TransactionOutputResponse, error) {

	subnetworkID := &subnetworkid.SubnetworkID{}
	err := subnetworkid.Decode(subnetworkID, transactionOutput.Transaction.Subnetwork.SubnetworkID)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't decode subnetwork id %s", transactionOutput.Transaction.Subnetwork.SubnetworkID)
	}
	var acceptingBlockHash *string
	var acceptingBlockBlueScore *uint64
	if transactionOutput.Transaction.AcceptingBlock != nil {
		acceptingBlockHash = &transactionOutput.Transaction.AcceptingBlock.BlockHash
		acceptingBlockBlueScore = &transactionOutput.Transaction.AcceptingBlock.BlueScore
	}
	isCoinbase := subnetworkID.IsEqual(subnetworkid.SubnetworkIDCoinbase)
	utxoConfirmations := confirmations(acceptingBlockBlueScore, selectedTipBlueScore)

	isSpendable := false
	if !isSpent {
		isSpendable = (!isCoinbase && utxoConfirmations > 0) ||
			(isCoinbase && utxoConfirmations >= activeNetParams.BlockCoinbaseMaturity)
	}

	return &TransactionOutputResponse{
		TransactionID:           transactionOutput.Transaction.TransactionID,
		Value:                   transactionOutput.Value,
		ScriptPubKey:            hex.EncodeToString(transactionOutput.ScriptPubKey),
		AcceptingBlockHash:      acceptingBlockHash,
		AcceptingBlockBlueScore: acceptingBlockBlueScore,
		Index:                   transactionOutput.Index,
		IsCoinbase:              &isCoinbase,
		Confirmations:           &utxoConfirmations,
		IsSpendable:             &isSpendable,
	}, nil
}
