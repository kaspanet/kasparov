package apimodels

// TransactionResponse is a json representation of a transaction
type TransactionResponse struct {
	TransactionHash         string                       `json:"transactionHash"`
	TransactionID           string                       `json:"transactionId"`
	AcceptingBlockHash      *string                      `json:"acceptingBlockHash"`
	AcceptingBlockBlueScore *uint64                      `json:"acceptingBlockBlueScore,omitempty"`
	SubnetworkID            string                       `json:"subnetworkId"`
	LockTime                uint64                       `json:"lockTime"`
	Gas                     uint64                       `json:"gas,omitempty"`
	PayloadHash             string                       `json:"payloadHash,omitempty"`
	Payload                 string                       `json:"payload,omitempty"`
	Inputs                  []*TransactionInputResponse  `json:"inputs"`
	Outputs                 []*TransactionOutputResponse `json:"outputs"`
	Mass                    uint64                       `json:"mass"`
	Version                 int32                        `json:"version"`
	Raw                     string                       `json:"raw"`
	Confirmations           *uint64                      `json:"confirmations,omitempty"`
}

// TransactionOutputResponse is a json representation of a transaction output
type TransactionOutputResponse struct {
	TransactionID           string  `json:"transactionId,omitempty"`
	Value                   uint64  `json:"value"`
	ScriptPubKey            string  `json:"scriptPubKey"`
	Address                 string  `json:"address,omitempty"`
	AcceptingBlockHash      *string `json:"acceptingBlockHash,omitempty"`
	AcceptingBlockBlueScore *uint64 `json:"acceptingBlockBlueScore,omitempty"`
	Index                   uint32  `json:"index"`
	IsSpent                 bool    `json:"isSpent"`
	IsCoinbase              *bool   `json:"isCoinbase,omitempty"`
	IsSpendable             *bool   `json:"isSpendable,omitempty"`
	Confirmations           *uint64 `json:"confirmations,omitempty"`
}

// TransactionInputResponse is a json representation of a transaction input
type TransactionInputResponse struct {
	TransactionID                  string `json:"transactionId,omitempty"`
	PreviousTransactionID          string `json:"previousTransactionId"`
	PreviousTransactionOutputIndex uint32 `json:"previousTransactionOutputIndex"`
	SignatureScript                string `json:"signatureScript"`
	Sequence                       uint64 `json:"sequence"`
	Address                        string `json:"address"`
	Index                          uint32 `json:"index"`
}

// TransactionsResponse is a json representation of a transactions response
type TransactionsResponse struct {
	Transactions []*TransactionResponse `json:"transactions"`
}

// BlockResponse is a json representation of a block
type BlockResponse struct {
	BlockHash               string   `json:"blockHash"`
	Version                 int32    `json:"version"`
	HashMerkleRoot          string   `json:"hashMerkleRoot"`
	AcceptedIDMerkleRoot    string   `json:"acceptedIDMerkleRoot"`
	UTXOCommitment          string   `json:"utxoCommitment"`
	Timestamp               uint64   `json:"timestamp"`
	Bits                    uint32   `json:"bits"`
	Nonce                   uint64   `json:"nonce"`
	ParentBlockHashes       []string `json:"parentBlockHashes"`
	AcceptingBlockHash      *string  `json:"acceptingBlockHash"`
	AcceptedBlockHashes     []string `json:"acceptedBlockHashes"`
	AcceptingBlockBlueScore *uint64  `json:"acceptingBlockBlueScore"`
	BlueScore               uint64   `json:"blueScore"`
	IsChainBlock            bool     `json:"isChainBlock"`
	Mass                    uint64   `json:"mass"`
	Confirmations           *uint64  `json:"confirmations,omitempty"`
}

// FeeEstimateResponse is a json representation of a fee estimate
type FeeEstimateResponse struct {
	HighPriority   float64 `json:"highPriority"`
	NormalPriority float64 `json:"normalPriority"`
	LowPriority    float64 `json:"lowPriority"`
}

// TransactionDoubleSpendsResponse is a json representation of transaction doublespends response
type TransactionDoubleSpendsResponse struct {
	Transactions []*TransactionResponse `json:"transactions"`
}
