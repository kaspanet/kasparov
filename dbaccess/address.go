package dbaccess

import (
	"github.com/go-pg/pg/v9"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/dbmodels"
)

// AddressesByAddressStrings retrieves all addresss by their address strings.
// If preloadedFields was provided - preloads the requested fields
func AddressesByAddressStrings(ctx database.Context, addressStrings []string, preloadedFields ...dbmodels.FieldName) ([]*dbmodels.Address, error) {
	db, err := ctx.DB()
	if err != nil {
		return nil, err
	}
	if len(addressStrings) == 0 {
		return nil, nil
	}
	var addresses []*dbmodels.Address
	query := db.Model(&addresses).
		Where("address IN (?)", pg.In(addressStrings))
	query = preloadFields(query, preloadedFields)
	err = query.Select()
	if err != nil {
		return nil, err
	}

	return addresses, nil
}
