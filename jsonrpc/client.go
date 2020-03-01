package jsonrpc

import (
	"io/ioutil"
	"time"

	"github.com/kaspanet/kasparov/config"
	"github.com/pkg/errors"

	"github.com/kaspanet/kaspad/util/daghash"

	"github.com/kaspanet/kaspad/rpcclient"
	"github.com/kaspanet/kaspad/util"
	"github.com/kaspanet/kaspad/wire"
)

// Client represents a connection to the JSON-RPC API of a full node
type Client struct {
	*rpcclient.Client
	OnBlockAdded   chan *BlockAddedMsg
	OnChainChanged chan *ChainChangedMsg
}

var client *Client

// GetClient returns an instance of the JSON-RPC client, in case we have an active connection
func GetClient() (*Client, error) {
	if client == nil {
		return nil, errors.New("JSON-RPC is not connected")
	}

	return client, nil
}

// BlockAddedMsg defines the message received in onBlockAdded
type BlockAddedMsg struct {
	ChainHeight uint64
	Header      *wire.BlockHeader
}

// ChainChangedMsg defines the message received in onChainChanged
type ChainChangedMsg struct {
	RemovedChainBlockHashes []*daghash.Hash
	AddedChainBlocks        []*rpcclient.ChainBlock
}

// Close closes the connection to the JSON-RPC API server
func Close() {
	if client == nil {
		return
	}

	client.Disconnect()
	client = nil
}

// Connect initiates a connection to the JSON-RPC API Server
func Connect(cfg *config.KasparovFlags) error {
	var cert []byte
	if !cfg.DisableTLS {
		var err error
		cert, err = ioutil.ReadFile(cfg.RPCCert)
		if err != nil {
			return errors.Errorf("Error reading certificates file: %s", err)
		}
	}

	rpcServerAddress, err := cfg.NetParams().NormalizeRPCServerAddress(cfg.RPCServer)
	if err != nil {
		return err
	}

	connCfg := &rpcclient.ConnConfig{
		Host:           rpcServerAddress,
		Endpoint:       "ws",
		User:           cfg.RPCUser,
		Pass:           cfg.RPCPassword,
		DisableTLS:     cfg.DisableTLS,
		RequestTimeout: time.Minute * 2,
	}

	if !cfg.DisableTLS {
		connCfg.Certificates = cert
	}

	client, err = newClient(connCfg)
	if err != nil {
		return errors.Errorf("Error connecting to address %s: %s", rpcServerAddress, err)
	}

	return nil
}

func newClient(connCfg *rpcclient.ConnConfig) (*Client, error) {
	client = &Client{
		OnBlockAdded:   make(chan *BlockAddedMsg),
		OnChainChanged: make(chan *ChainChangedMsg),
	}
	notificationHandlers := &rpcclient.NotificationHandlers{
		OnFilteredBlockAdded: func(height uint64, header *wire.BlockHeader,
			txs []*util.Tx) {
			client.OnBlockAdded <- &BlockAddedMsg{
				ChainHeight: height,
				Header:      header,
			}
		},
		OnChainChanged: func(removedChainBlockHashes []*daghash.Hash,
			addedChainBlocks []*rpcclient.ChainBlock) {
			client.OnChainChanged <- &ChainChangedMsg{
				RemovedChainBlockHashes: removedChainBlockHashes,
				AddedChainBlocks:        addedChainBlocks,
			}
		},
	}
	var err error
	client.Client, err = rpcclient.New(connCfg, notificationHandlers)
	if err != nil {
		return nil, errors.Errorf("Error connecting to address %s: %s", connCfg.Host, err)
	}

	if err = client.NotifyBlocks(); err != nil {
		return nil, errors.Errorf("Error while registering client %s for block notifications: %s", client.Host(), err)
	}
	if err = client.NotifyChainChanges(); err != nil {
		return nil, errors.Errorf("Error while registering client %s for chain changes notifications: %s", client.Host(), err)
	}

	return client, nil
}
