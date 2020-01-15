package utils

import "github.com/kaspanet/kaspad/rpcmodel"

// RawAndVerboseBlock is a type that holds either
// the block hexadecimal raw representation and
// either its verbose representation.
type RawAndVerboseBlock struct {
	Raw     string
	Verbose *rpcmodel.GetBlockVerboseResult
}

func (r *RawAndVerboseBlock) String() string {
	return r.Hash()
}

// Hash returns the block hash
func (r *RawAndVerboseBlock) Hash() string {
	return r.Verbose.Hash
}
