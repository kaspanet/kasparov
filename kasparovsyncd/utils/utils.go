package utils

import "github.com/kaspanet/kaspad/rpcmodel"

type RawAndVerboseBlock struct {
	Raw     string
	Verbose *rpcmodel.GetBlockVerboseResult
}

func (r *RawAndVerboseBlock) String() string {
	return r.Verbose.Hash
}
