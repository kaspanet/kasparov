package dbaccess

import (
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
)

// AddressesByAddressStrings retrieves all addresss by their address strings.
// If preloadedFields was provided - preloads the requested fields
func AddressesByAddressStrings(ctx Context, addressStrings []string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Address, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	query := db.
		Where("`addresses`.`address` IN (?)", addressStrings)
	query = preloadFields(query, preloadedFields)

	var addresses []*dbmodels.Address
	dbResult := query.Find(&addresses)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading addresses from the database:", dbErrors)
	}

	return addresses, nil
}
