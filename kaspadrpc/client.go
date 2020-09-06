package kaspadrpc

import (
	"github.com/kaspanet/kaspad/app/appmessage"
	"github.com/kaspanet/kaspad/infrastructure/network/rpcclient"
	"github.com/kaspanet/kasparov/config"
	"time"

	"github.com/pkg/errors"
)

const timeout = 10 * time.Second

// Client represents a connection to the JSON-RPC API of a full node
type Client struct {
	*rpcclient.RPCClient

	OnBlockAdded   chan *appmessage.BlockAddedNotificationMessage
	OnChainChanged chan *appmessage.ChainChangedNotificationMessage
}

var client *Client

// GetClient returns an instance of the JSON-RPC client, in case we have an active connection
func GetClient() (*Client, error) {
	if client == nil {
		return nil, errors.New("JSON-RPC is not connected")
	}

	return client, nil
}

func NewClient(cfg *config.KasparovFlags, subscribeToNotifications bool) (*Client, error) {
	rpcAddress, err := cfg.NetParams().NormalizeRPCServerAddress(cfg.RPCServer)
	if err != nil {
		return nil, err
	}
	rpcClient, err := rpcclient.NewRPCClient(rpcAddress)
	if err != nil {
		return nil, err
	}
	rpcClient.SetTimeout(timeout)

	client := &Client{
		RPCClient:      rpcClient,
		OnBlockAdded:   make(chan *appmessage.BlockAddedNotificationMessage),
		OnChainChanged: make(chan *appmessage.ChainChangedNotificationMessage),
	}

	if subscribeToNotifications {
		err = rpcClient.RegisterForBlockAddedNotifications(func(notification *appmessage.BlockAddedNotificationMessage) {
			client.OnBlockAdded <- notification
		})
		if err != nil {
			return nil, errors.Wrapf(err, "error requesting block-added notifications")
		}
		err = rpcClient.RegisterForChainChangedNotifications(func(notification *appmessage.ChainChangedNotificationMessage) {
			client.OnChainChanged <- notification
		})
		if err != nil {
			return nil, errors.Wrapf(err, "error requesting chain-changed notifications")
		}

	}

	return client, nil
}
