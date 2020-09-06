package kaspadrpc

import (
	"github.com/kaspanet/kaspad/app/appmessage"
	"github.com/kaspanet/kaspad/infrastructure/network/rpcclient"
	"github.com/kaspanet/kasparov/config"
	"time"

	"github.com/pkg/errors"
)

const timeout = 10 * time.Second

// KasparovClient represents a connection to the JSON-RPC API of a full node
type KasparovClient struct {
	*rpcclient.RPCClient

	OnBlockAdded   chan *appmessage.BlockAddedNotificationMessage
	OnChainChanged chan *appmessage.ChainChangedNotificationMessage
}

var client *KasparovClient

// GetClient returns an instance of the RPC client, in case we have an active connection
func GetClient() (*KasparovClient, error) {
	if client == nil {
		return nil, errors.New("RPC is not connected")
	}

	return client, nil
}

func NewClient(cfg *config.KasparovFlags, subscribeToNotifications bool) (*KasparovClient, error) {
	rpcAddress, err := cfg.NetParams().NormalizeRPCServerAddress(cfg.RPCServer)
	if err != nil {
		return nil, err
	}
	rpcClient, err := rpcclient.NewRPCClient(rpcAddress)
	if err != nil {
		return nil, err
	}
	rpcClient.SetTimeout(timeout)

	kasparovClient := &KasparovClient{
		RPCClient:      rpcClient,
		OnBlockAdded:   make(chan *appmessage.BlockAddedNotificationMessage),
		OnChainChanged: make(chan *appmessage.ChainChangedNotificationMessage),
	}

	if subscribeToNotifications {
		err = rpcClient.RegisterForBlockAddedNotifications(func(notification *appmessage.BlockAddedNotificationMessage) {
			kasparovClient.OnBlockAdded <- notification
		})
		if err != nil {
			return nil, errors.Wrapf(err, "error requesting block-added notifications")
		}
		err = rpcClient.RegisterForChainChangedNotifications(func(notification *appmessage.ChainChangedNotificationMessage) {
			kasparovClient.OnChainChanged <- notification
		})
		if err != nil {
			return nil, errors.Wrapf(err, "error requesting chain-changed notifications")
		}
	}

	client = kasparovClient

	return kasparovClient, nil
}
