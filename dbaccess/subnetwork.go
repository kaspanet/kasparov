package dbaccess

import (
	"github.com/go-pg/pg/v9"
	"github.com/kaspanet/kasparov/dbmodels"
)

// SubnetworksByIDs retrieves all subnetworks by their `subnetworkIDs`.
// If preloadedFields was provided - preloads the requested fields
func SubnetworksByIDs(ctx Context, subnetworkIDs []string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Subnetwork, error) {
	db, err := ctx.db()
	if err != nil {
		return nil, err
	}

	if len(subnetworkIDs) == 0 { // TODO REMOVE
		return nil, nil
	}

	var subnetworks []*dbmodels.Subnetwork
	query := db.Model(&subnetworks).
		Where("subnetwork_id IN (?)", pg.In(subnetworkIDs))
	query = preloadFields(query, preloadedFields)
	err = query.Select()
	if err != nil {
		return nil, err
	}

	return subnetworks, nil
}
