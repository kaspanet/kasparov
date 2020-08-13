package jsonrpc

import (
	"github.com/kaspanet/kaspad/infrastructure/logs"
	rpcclient "github.com/kaspanet/kaspad/network/rpc/client"
	"github.com/kaspanet/kasparov/logger"
)

func init() {
	rpcclient.UseLogger(logger.BackendLog, logs.LevelInfo)
}
