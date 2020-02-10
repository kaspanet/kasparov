package dbaccess

import (
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
)

// SubnetworksByIDs retrieves all subnetworks by their `subnetworkIDs`.
// If preloadedFields was provided - preloads the requested fields
func SubnetworksByIDs(ctx Context, subnetworkIDs []string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Subnetwork, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	query := db.
		Where("`subnetworks`.`subnetwork_id` IN (?)", subnetworkIDs)
	query = preloadFields(query, preloadedFields)

	var subnetworks []*dbmodels.Subnetwork
	dbResult := query.Find(&subnetworks)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("some errors were encountered when loading subnetworks from the database:", dbErrors)
	}

	return subnetworks, nil
}
