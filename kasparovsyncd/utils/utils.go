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
	return r.Verbose.Hash
}
