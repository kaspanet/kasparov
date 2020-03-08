package dbaccess

import (
	"github.com/go-pg/pg/v9/orm"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/dbmodels"
)

func preloadFields(query *orm.Query, columns []dbmodels.FieldName) *orm.Query {
	for _, field := range columns {
		query = query.Relation(string(field))
	}
	return query
}

const chunkSize = 3000

// BulkInsert inserts a long list of objects into the database.
// Utilizes bulk insertion for much faster times.
func BulkInsert(ctx database.Context, objects []interface{}) error {
	if len(objects) == 0 {
		return nil
	}

	db, err := ctx.DB()
	if err != nil {
		return err
	}
	return db.Insert(&objects)
}
