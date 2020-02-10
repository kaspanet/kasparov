package dbaccess

import (
	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
	"github.com/pkg/errors"
	gormbulk "github.com/t-tiger/gorm-bulk-insert"
)

func preloadFields(query *gorm.DB, preloadedFields []dbmodels.FieldName) *gorm.DB {
	for _, field := range preloadedFields {
		query = query.Preload(string(field))
	}
	return query
}

// Save updates a value in database, if the value's primary key is nil (or the type's default value), will insert it
func Save(ctx Context, value interface{}) error {
	db, err := ctx.db()
	if err != nil {
		return err
	}

	dbResult := db.Save(value)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("failed to save object: ", dbErrors)
	}

	return nil
}

const chunkSize = 3000

// BulkInsert inserts a long list of objects into the database.
// Utilizes bulk insertion for much faster times.
func BulkInsert(ctx Context, objects []interface{}) error {
	db, err := ctx.db()
	if err != nil {
		return err
	}

	return errors.WithStack(gormbulk.BulkInsert(db, objects, chunkSize))
}
