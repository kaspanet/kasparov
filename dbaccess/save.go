package dbaccess

import "github.com/kaspanet/kasparov/httpserverutils"

// Save update value in database, if the value doesn't have primary key, will insert it
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
