package dbaccess

import (
	"github.com/go-pg/pg/v9/orm"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/pkg/errors"
)

func preloadFields(query *orm.Query, columns []dbmodels.FieldName) *orm.Query {
	for _, field := range columns {
		query = query.Relation(string(field))
	}
	return query
}

// Save updates a value in database, if the value's primary key is nil (or the type's default value), will insert it
//
// Don't use this method - it saves all the object graph whether you want it or not. Kept until all usages
// of save are converted to updates.
func Save(ctx Context, value interface{}) error {
	db, err := ctx.db()
	if err != nil {
		return err
	}

	err = db.Insert(value)
	if err != nil {
		return errors.WithMessage(err, "failed to save object: ")
	}

	return nil
}

const chunkSize = 3000

// BulkInsert inserts a long list of objects into the database.
// Utilizes bulk insertion for much faster times.
func BulkInsert(ctx Context, objects []interface{}) error {
	if len(objects) == 0 {
		return nil
	}

	db, err := ctx.db()
	if err != nil {
		return err
	}
	return db.Insert(&objects)
}
