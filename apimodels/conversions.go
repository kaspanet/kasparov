package apimodels

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/kaspanet/kasparov/dbmodels"
)

// ConvertTxModelToTxResponse converts a transaction database object to a TransactionResponse
func ConvertTxModelToTxResponse(tx *dbmodels.Transaction) *TransactionResponse {
	txRes := &TransactionResponse{
		TransactionHash: tx.TransactionHash,
		TransactionID:   tx.TransactionID,
		SubnetworkID:    tx.Subnetwork.SubnetworkID,
		LockTime:        tx.LockTime,
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
	for i, txOut := range tx.TransactionOutputs {
		txRes.Outputs[i] = &TransactionOutputResponse{
			Value:        txOut.Value,
			ScriptPubKey: hex.EncodeToString(txOut.ScriptPubKey),
			Index:        txOut.Index,
		}
		if txOut.Address != nil {
			txRes.Outputs[i].Address = txOut.Address.Address
		}
	}
	for i, txIn := range tx.TransactionInputs {
		txRes.Inputs[i] = &TransactionInputResponse{
			PreviousTransactionID:          txIn.PreviousTransactionOutput.Transaction.TransactionID,
			PreviousTransactionOutputIndex: txIn.PreviousTransactionOutput.Index,
			SignatureScript:                hex.EncodeToString(txIn.SignatureScript),
			Sequence:                       txIn.Sequence,
		}
		if txIn.PreviousTransactionOutput.Address != nil {
			txRes.Inputs[i].Address = txIn.PreviousTransactionOutput.Address.Address
		}
	}
	return txRes
}

// ConvertBlockModelToBlockResponse converts a block database object into a BlockResponse
func ConvertBlockModelToBlockResponse(block *dbmodels.Block) *BlockResponse {
	blockRes := &BlockResponse{
		BlockHash:            block.BlockHash,
		Version:              block.Version,
		HashMerkleRoot:       block.HashMerkleRoot,
		AcceptedIDMerkleRoot: block.AcceptedIDMerkleRoot,
		UTXOCommitment:       block.UTXOCommitment,
		Timestamp:            uint64(block.Timestamp.Unix()),
		Bits:                 block.Bits,
		Nonce:                binary.LittleEndian.Uint64(block.Nonce),
		ParentBlockHashes:    make([]string, len(block.ParentBlocks)),
		BlueScore:            block.BlueScore,
		IsChainBlock:         block.IsChainBlock,
		Mass:                 block.Mass,
	}
	if block.AcceptingBlock != nil {
		blockRes.AcceptingBlockHash = &block.AcceptingBlock.BlockHash
	}
	for i, parent := range block.ParentBlocks {
		blockRes.ParentBlockHashes[i] = parent.BlockHash
	}
	return blockRes
}
