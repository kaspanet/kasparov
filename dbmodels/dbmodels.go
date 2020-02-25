package dbmodels

import (
	"time"
)

// FieldName is the string reprenetation for field names of database models.
// Used to specify which fields to preload
type FieldName string

// Block is the gorm model for the 'blocks' table
type Block struct {
	ID                   uint64 `gorm:"primary_key"`
	BlockHash            string
	AcceptingBlockID     *uint64
	AcceptingBlock       *Block
	Version              int32
	HashMerkleRoot       string
	AcceptedIDMerkleRoot string
	UTXOCommitment       string
	Timestamp            time.Time
	Bits                 uint32
	Nonce                uint64
	BlueScore            uint64
	IsChainBlock         bool
	Mass                 uint64
	ParentBlocks         []*Block       `gorm:"many2many:parent_blocks;association_jointable_foreignkey:parent_block_id;"`
	Transactions         []*Transaction `gorm:"many2many:transactions_to_blocks;association_jointable_foreignkey:transaction_id;"`
}

// BlockFieldNames is a list of FieldNames for the 'Block' object
var BlockFieldNames = struct {
	AcceptingBlock,
	ParentBlocks,
	Transactions FieldName
}{
	AcceptingBlock: "AcceptingBlock",
	ParentBlocks:   "ParentBlocks",
	Transactions:   "Transactions",
}

// BlockRecommendedPreloadedFields is a list of fields recommended to preload when getting blocks
var BlockRecommendedPreloadedFields = []FieldName{
	BlockFieldNames.AcceptingBlock,
	BlockFieldNames.ParentBlocks,
}

// ParentBlock is the gorm model for the 'parent_blocks' table
type ParentBlock struct {
	BlockID       uint64
	Block         Block
	ParentBlockID uint64
	ParentBlock   Block
}

// ParentBlockFieldNames is a list of FieldNames for the 'ParentBlock' object
var ParentBlockFieldNames = struct {
	Block       FieldName
	ParentBlock FieldName
}{
	Block:       "Block",
	ParentBlock: "ParentBlock",
}

// RawBlock is the gorm model for the 'raw_blocks' table
type RawBlock struct {
	BlockID   uint64
	Block     Block
	BlockData []byte
}

// RawBlockFieldNames is a list of FieldNames for the 'RawBlock' object
var RawBlockFieldNames = struct {
	Block FieldName
}{
	Block: "Block",
}

// Subnetwork is the gorm model for the 'subnetworks' table
type Subnetwork struct {
	ID           uint64 `gorm:"primary_key"`
	SubnetworkID string
	GasLimit     *uint64
}

// Transaction is the gorm model for the 'transactions' table
type Transaction struct {
	ID                 uint64 `gorm:"primary_key"`
	AcceptingBlockID   *uint64
	AcceptingBlock     *Block
	TransactionHash    string
	TransactionID      string
	LockTime           uint64
	SubnetworkID       uint64
	Subnetwork         Subnetwork
	Gas                uint64
	PayloadHash        string
	Payload            []byte
	Mass               uint64
	Version            int32
	RawTransaction     *RawTransaction
	Blocks             []Block `gorm:"many2many:transactions_to_blocks;"`
	TransactionOutputs []TransactionOutput
	TransactionInputs  []TransactionInput
}

// TransactionFieldNames is a list of FieldNames for the 'Transaction' object
var TransactionFieldNames = struct {
	AcceptingBlock                   FieldName
	Subnetwork                       FieldName
	RawTransaction                   FieldName
	Blocks                           FieldName
	TransactionOutputs               FieldName
	TransactionInputs                FieldName
	OutputsAddresses                 FieldName
	InputsPreviousTransactionOutputs FieldName
	InputsPreviousTransactions       FieldName
	InputsAddresses                  FieldName
}{
	AcceptingBlock:                   "AcceptingBlock",
	Subnetwork:                       "Subnetwork",
	RawTransaction:                   "RawTransaction",
	Blocks:                           "Blocks",
	TransactionOutputs:               "TransactionOutputs",
	TransactionInputs:                "TransactionInputs",
	OutputsAddresses:                 "TransactionOutputs.Address",
	InputsPreviousTransactionOutputs: "TransactionInputs.PreviousTransactionOutput",
	InputsPreviousTransactions:       "TransactionInputs.PreviousTransactionOutput.Transaction",
	InputsAddresses:                  "TransactionInputs.PreviousTransactionOutput.Address",
}

// TransactionRecommendedPreloadedFields is a list of fields recommended to preload when getting transactions
var TransactionRecommendedPreloadedFields = []FieldName{
	TransactionFieldNames.AcceptingBlock,
	TransactionFieldNames.Subnetwork,
	TransactionFieldNames.RawTransaction,
	TransactionFieldNames.TransactionOutputs,
	TransactionFieldNames.OutputsAddresses,
	TransactionFieldNames.InputsPreviousTransactions,
	TransactionFieldNames.InputsAddresses,
}

// TransactionBlock is the gorm model for the 'transactions_to_blocks' table
type TransactionBlock struct {
	TransactionID uint64
	Transaction   Transaction
	BlockID       uint64
	Block         Block
	Index         uint32
}

// TableName returns the table name associated to the
// TransactionBlock gorm model
func (TransactionBlock) TableName() string {
	return "transactions_to_blocks"
}

// TransactionBlockFieldNames  is a list of FieldNames for the 'TransactionBlock' object
var TransactionBlockFieldNames = struct {
	Transaction FieldName
	Block       FieldName
}{
	Transaction: "Transaction",
	Block:       "Block",
}

// TransactionOutput is the gorm model for the 'transaction_outputs' table
type TransactionOutput struct {
	ID            uint64 `gorm:"primary_key"`
	TransactionID uint64
	Transaction   Transaction
	Index         uint32
	Value         uint64
	ScriptPubKey  []byte
	IsSpent       bool
	AddressID     *uint64
	Address       *Address
}

// TransactionOutputFieldNames is a list of FieldNames for the 'TransactionOutput' object
var TransactionOutputFieldNames = struct {
	Transaction               FieldName
	Address                   FieldName
	TransactionAcceptingBlock FieldName
	TransactionSubnetwork     FieldName
}{
	Transaction:               "Transaction",
	Address:                   "Address",
	TransactionAcceptingBlock: "Transaction.AcceptingBlock",
	TransactionSubnetwork:     "Transaction.Subnetwork",
}

// TransactionInput is the gorm model for the 'transaction_inputs' table
type TransactionInput struct {
	ID                          uint64 `gorm:"primary_key"`
	TransactionID               uint64
	Transaction                 Transaction
	PreviousTransactionOutputID uint64
	PreviousTransactionOutput   TransactionOutput
	Index                       uint32
	SignatureScript             []byte
	Sequence                    uint64
}

// TransactionInputFieldNames is a list of FieldNames for the 'TransactionInput' object
var TransactionInputFieldNames = struct {
	Transaction               FieldName
	PreviousTransactionOutput FieldName
}{
	Transaction:               "Transaction",
	PreviousTransactionOutput: "PreviousTransactionOutput",
}

// Address is the gorm model for the 'addresses' table
type Address struct {
	ID      uint64 `gorm:"primary_key"`
	Address string
}

// RawTransaction is the gorm model for the 'raw_transactions' table
type RawTransaction struct {
	TransactionID   uint64
	Transaction     Transaction
	TransactionData []byte
}

// RawTransactionFieldNames is a list of FieldNames for the 'RawTransaction' object
var RawTransactionFieldNames = struct {
	Transaction FieldName
}{
	Transaction: "Transaction",
}
