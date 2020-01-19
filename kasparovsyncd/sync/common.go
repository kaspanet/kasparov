package sync

import (
	"github.com/jinzhu/gorm"
	"github.com/pkg/errors"
	gormbulk "github.com/t-tiger/gorm-bulk-insert"
)

const insertChunkSize = 3000

func bulkInsert(db *gorm.DB, objects []interface{}) error {
	return errors.WithStack(gormbulk.BulkInsert(db, objects, insertChunkSize))
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
