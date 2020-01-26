package sync

import (
	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
	"github.com/pkg/errors"
)

func insertAddresses(dbTx *gorm.DB, transactionIDsToTxsWithMetadata map[string]*txWithMetadata) (map[string]uint64, error) {
	addressSet := make(map[string]struct{})
	for _, transaction := range transactionIDsToTxsWithMetadata {
		if !transaction.isNew {
			continue
		}
		for _, txOut := range transaction.verboseTx.Vout {
			if txOut.ScriptPubKey.Address == nil {
				continue
			}
			addressSet[*txOut.ScriptPubKey.Address] = struct{}{}
		}
	}
	addresses := stringsSetToSlice(addressSet)

	var dbAddresses []*dbmodels.Address
	dbResult := dbTx.
		Where("address in (?)", addresses).
		Find(&dbAddresses)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find addresses: ", dbErrors)
	}

	addressesToAddressIDs := make(map[string]uint64)
	for _, dbAddress := range dbAddresses {
		addressesToAddressIDs[dbAddress.Address] = dbAddress.ID
	}

	newAddresses := make([]string, 0)
	for address := range addressSet {
		if _, exists := addressesToAddressIDs[address]; exists {
			continue
		}
		newAddresses = append(newAddresses, address)
	}

	addressesToAdd := make([]interface{}, len(newAddresses))
	for i, address := range newAddresses {
		addressesToAdd[i] = &dbmodels.Address{
			Address: address,
		}
	}

	err := bulkInsert(dbTx, addressesToAdd)
	if err != nil {
		return nil, err
	}

	var dbNewAddresses []*dbmodels.Address
	dbResult = dbTx.
		Where("address in (?)", newAddresses).
		Find(&dbNewAddresses)
	dbErrors = dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find addresses: ", dbErrors)
	}

	if len(dbNewAddresses) != len(newAddresses) {
		return nil, errors.New("couldn't add all new addresses")
	}

	for _, dbNewAddress := range dbNewAddresses {
		addressesToAddressIDs[dbNewAddress.Address] = dbNewAddress.ID
	}
	return addressesToAddressIDs, nil
}
