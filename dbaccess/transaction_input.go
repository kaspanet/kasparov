package dbaccess

// Outpoint represent an outpoint in a transaction input.
type Outpoint struct {
	TransactionID string
	Index         uint32
}
