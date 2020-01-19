package sync

import (
	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/httpserverutils"
	"github.com/kaspanet/kasparov/jsonrpc"
	"github.com/kaspanet/kasparov/kasparovsyncd/utils"
	"github.com/pkg/errors"
)

func insertSubnetworks(dbTx *gorm.DB, client *jsonrpc.Client, blocks []*utils.RawAndVerboseBlock) (subnetworkIDsToIDs map[string]uint64, err error) {
	subnetworkSet := make(map[string]struct{})
	for _, block := range blocks {
		for _, transaction := range block.Verbose.RawTx {
			subnetworkSet[transaction.Subnetwork] = struct{}{}
		}
	}

	subnetworkIDs := stringsSetToSlice(subnetworkSet)

	var dbSubnetworks []*dbmodels.Subnetwork
	dbResult := dbTx.
		Where("subnetwork_id in (?)", subnetworkIDs).
		Find(&dbSubnetworks)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find subnetworks: ", dbErrors)
	}

	subnetworkIDsToIDs = make(map[string]uint64)
	for _, dbSubnetwork := range dbSubnetworks {
		subnetworkIDsToIDs[dbSubnetwork.SubnetworkID] = dbSubnetwork.ID
	}

	newSubnetworks := make([]string, 0)
	for subnetworkID := range subnetworkSet {
		if _, exists := subnetworkIDsToIDs[subnetworkID]; exists {
			continue
		}
		newSubnetworks = append(newSubnetworks, subnetworkID)
	}

	subnetworksToAdd := make([]interface{}, len(newSubnetworks))
	for i, subnetworkID := range newSubnetworks {
		subnetwork, err := client.GetSubnetwork(subnetworkID)
		if err != nil {
			return nil, err
		}
		subnetworksToAdd[i] = dbmodels.Subnetwork{
			SubnetworkID: subnetworkID,
			GasLimit:     subnetwork.GasLimit,
		}
	}

	err = bulkInsert(dbTx, subnetworksToAdd)
	if err != nil {
		return nil, err
	}

	var dbNewSubnetworks []*dbmodels.Subnetwork
	dbResult = dbTx.
		Where("subnetwork_id in (?)", newSubnetworks).
		Find(&dbNewSubnetworks)
	dbErrors = dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return nil, httpserverutils.NewErrorFromDBErrors("failed to find subnetworks: ", dbErrors)
	}

	if len(dbNewSubnetworks) != len(newSubnetworks) {
		return nil, errors.New("couldn't add all new subnetworks")
	}

	for _, dbSubnetwork := range dbNewSubnetworks {
		subnetworkIDsToIDs[dbSubnetwork.SubnetworkID] = dbSubnetwork.ID
	}
	return subnetworkIDsToIDs, nil
}
