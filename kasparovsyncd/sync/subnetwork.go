package sync

import (
	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/kaspanet/kasparov/jsonrpc"

	"github.com/pkg/errors"
)

func insertSubnetworks(dbTx *dbaccess.TxContext, client *jsonrpc.Client, blocks []*rawAndVerboseBlock) (
	subnetworkIDsToIDs map[string]uint64, err error) {

	subnetworkSet := make(map[string]struct{})
	for _, block := range blocks {
		for _, transaction := range block.Verbose.RawTx {
			subnetworkSet[transaction.Subnetwork] = struct{}{}
		}
	}

	subnetworkIDs := stringsSetToSlice(subnetworkSet)

	dbSubnetworks, err := dbaccess.SubnetworksByIDs(dbTx, subnetworkIDs)
	if err != nil {
		return nil, err
	}

	subnetworkIDsToIDs = make(map[string]uint64)
	for _, dbSubnetwork := range dbSubnetworks {
		subnetworkIDsToIDs[dbSubnetwork.SubnetworkID] = dbSubnetwork.ID
	}

	newSubnetworkIDs := make([]string, 0)
	for subnetworkID := range subnetworkSet {
		if _, exists := subnetworkIDsToIDs[subnetworkID]; exists {
			continue
		}
		newSubnetworkIDs = append(newSubnetworkIDs, subnetworkID)
	}

	subnetworksToAdd := make([]interface{}, len(newSubnetworkIDs))
	for i, subnetworkID := range newSubnetworkIDs {
		subnetwork, err := client.GetSubnetwork(subnetworkID)
		if err != nil {
			return nil, err
		}
		subnetworksToAdd[i] = dbmodels.Subnetwork{
			SubnetworkID: subnetworkID,
			GasLimit:     subnetwork.GasLimit,
		}
	}

	err = dbaccess.BulkInsert(dbTx, subnetworksToAdd)
	if err != nil {
		return nil, err
	}

	dbNewSubnetworks, err := dbaccess.SubnetworksByIDs(dbTx, newSubnetworkIDs)

	if len(dbNewSubnetworks) != len(newSubnetworkIDs) {
		return nil, errors.New("couldn't add all new subnetworks")
	}

	for _, dbSubnetwork := range dbNewSubnetworks {
		subnetworkIDsToIDs[dbSubnetwork.SubnetworkID] = dbSubnetwork.ID
	}
	return subnetworkIDsToIDs, nil
}
