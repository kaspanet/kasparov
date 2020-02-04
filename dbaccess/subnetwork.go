package dbaccess

import (
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
)

// SubnetworksByIDs retrieves all subnetworks by their `subnetworkIDs`.
// If preloadedColumns was provided - preloads the requested columns
func SubnetworksByIDs(ctx Context, subnetworkIDs []string, preloadedColumns ...string) ([]*dbmodels.Subnetwork, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	query := db.
		Where("`subnetworks`.`subnetwork_id` IN (?)", subnetworkIDs)
	query = preloadColumns(query, preloadedColumns)

	var subnetworks []*dbmodels.Subnetwork
	dbResult := query.Find(&subnetworks)

	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("Some errors were encountered when loading subnetworks from the database:", dbErrors)
	}

	return subnetworks, nil
}
