package sync

import (
	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kaspad/rpcmodel"
	"github.com/pkg/errors"
	gormbulk "github.com/t-tiger/gorm-bulk-insert"
)

const chunkSize = 3000

func bulkInsert(db *gorm.DB, objects []interface{}) error {
	return errors.WithStack(gormbulk.BulkInsert(db, objects, chunkSize))
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

// rawAndVerboseBlock is a type that holds either
// the block hexadecimal raw representation and
// either its verbose representation.
type rawAndVerboseBlock struct {
	Raw     string
	Verbose *rpcmodel.GetBlockVerboseResult
}

func (r *rawAndVerboseBlock) String() string {
	return r.hash()
}

// hash returns the block hash
func (r *rawAndVerboseBlock) hash() string {
	return r.Verbose.Hash
}
