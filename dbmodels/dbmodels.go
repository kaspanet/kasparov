package dbmodels

import (
	"time"
)

// FieldName is the string reprenetation for field names of database models.
// Used to specify which fields to preload
type FieldName string

// Block is the database model for the 'blocks' table
type Block struct {
	ID                   uint64 `pg:",pk"`
	BlockHash            string `pg:",use_zero"`
	AcceptingBlockID     *uint64
	AcceptingBlock       *Block
	Version              int32          `pg:",use_zero"`
	HashMerkleRoot       string         `pg:",use_zero"`
	AcceptedIDMerkleRoot string         `pg:",use_zero"`
	UTXOCommitment       string         `pg:",use_zero"`
	Timestamp            time.Time      `pg:",use_zero"`
	Bits                 uint32         `pg:",use_zero"`
	Nonce                []byte         `pg:",use_zero"`
	BlueScore            uint64         `pg:",use_zero"`
	IsChainBlock         bool           `pg:",use_zero"`
	Mass                 uint64         `pg:",use_zero"`
	ParentBlocks         []*Block       `pg:"many2many:parent_blocks,joinFK:parent_block_id"`
	AcceptedBlocks       []*Block       `pg:"many2many:accepted_blocks,joinFK:accepted_block_id"`
	Transactions         []*Transaction `pg:"many2many:transactions_to_blocks,joinFK:transaction_id"`
}

// BlockFieldNames is a list of FieldNames for the 'Block' object
var BlockFieldNames = struct {
	AcceptingBlock,
	ParentBlocks,
	AcceptedBlocks,
	Transactions FieldName
}{
	AcceptingBlock: "AcceptingBlock",
	ParentBlocks:   "ParentBlocks",
	AcceptedBlocks: "AcceptedBlocks",
	Transactions:   "Transactions",
}

// BlockRecommendedPreloadedFields is a list of fields recommended to preload when getting blocks
var BlockRecommendedPreloadedFields = []FieldName{
	BlockFieldNames.AcceptingBlock,
	BlockFieldNames.ParentBlocks,
	BlockFieldNames.AcceptedBlocks,
}

// ParentBlock is the database model for the 'parent_blocks' table
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

// AcceptedBlock is the database model for the 'accepted_blocks' table
type AcceptedBlock struct {
	BlockID         uint64
	Block           Block
	AcceptedBlockID uint64
	AcceptedBlock   Block
}

// AcceptedBlockFieldNames is a list of FieldNames for the 'AcceptedBlock' object
var AcceptedBlockFieldNames = struct {
	Block         FieldName
	AcceptedBlock FieldName
}{
	Block:         "Block",
	AcceptedBlock: "AcceptedBlock",
}

// RawBlock is the database model for the 'raw_blocks' table
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

// Subnetwork is the database model for the 'subnetworks' table
type Subnetwork struct {
	ID           uint64 `pg:",pk"`
	SubnetworkID string `pg:",use_zero"`
	GasLimit     *uint64
}

// Transaction is the database model for the 'transactions' table
type Transaction struct {
	ID                 uint64 `pg:",pk"`
	AcceptingBlockID   *uint64
	AcceptingBlock     *Block
	TransactionHash    string `pg:",use_zero"`
	TransactionID      string `pg:",use_zero"`
	LockTime           []byte `pg:",use_zero"`
	SubnetworkID       uint64 `pg:",use_zero"`
	Subnetwork         Subnetwork
	Gas                uint64 `pg:",use_zero"`
	PayloadHash        string `pg:",use_zero"`
	Payload            []byte `pg:",use_zero"`
	Mass               uint64 `pg:",use_zero"`
	Version            int32  `pg:",use_zero"`
	RawTransaction     *RawTransaction
	Blocks             []Block `pg:"many2many:transactions_to_blocks"`
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

// TransactionBlock is the database model for the 'transactions_to_blocks' table
type TransactionBlock struct {
	tableName     struct{} `pg:"transactions_to_blocks"`
	TransactionID uint64   `pg:",use_zero"`
	Transaction   Transaction
	BlockID       uint64 `pg:",use_zero"`
	Block         Block
	Index         uint32 `pg:",use_zero"`
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

// TransactionOutput is the database model for the 'transaction_outputs' table
type TransactionOutput struct {
	ID            uint64 `pg:",pk"`
	TransactionID uint64 `pg:",use_zero"`
	Transaction   Transaction
	Index         uint32 `pg:",use_zero"`
	Value         uint64 `pg:",use_zero"`
	ScriptPubKey  []byte `pg:",use_zero"`
	IsSpent       bool   `pg:",use_zero"`
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

// TransactionInput is the database model for the 'transaction_inputs' table
type TransactionInput struct {
	ID                          uint64 `pg:",pk"`
	TransactionID               uint64 `pg:",use_zero"`
	Transaction                 Transaction
	PreviousTransactionOutputID uint64 `pg:",use_zero"`
	PreviousTransactionOutput   TransactionOutput
	Index                       uint32 `pg:",use_zero"`
	SignatureScript             []byte `pg:",use_zero"`
	Sequence                    []byte `pg:",use_zero"`
}

// TransactionInputFieldNames is a list of FieldNames for the 'TransactionInput' object
var TransactionInputFieldNames = struct {
	Transaction               FieldName
	PreviousTransactionOutput FieldName
}{
	Transaction:               "Transaction",
	PreviousTransactionOutput: "PreviousTransactionOutput",
}

// Address is the database model for the 'addresses' table
type Address struct {
	ID      uint64 `pg:",pk"`
	Address string `pg:",use_zero"`
}

// RawTransaction is the database model for the 'raw_transactions' table
type RawTransaction struct {
	TransactionID   uint64 `pg:",use_zero"`
	Transaction     Transaction
	TransactionData []byte `pg:",use_zero"`
}

// RawTransactionFieldNames is a list of FieldNames for the 'RawTransaction' object
var RawTransactionFieldNames = struct {
	Transaction FieldName
}{
	Transaction: "Transaction",
}

// PrefixFieldNames returns the given fields prefixed
// with the given prefix and a dot.
func PrefixFieldNames(prefix FieldName, fields []FieldName) []FieldName {
	prefixedFields := make([]FieldName, len(fields))
	for i, fieldName := range fields {
		prefixedFields[i] = prefix + FieldName(".") + fieldName
	}
	return prefixedFields
}
