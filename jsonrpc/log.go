package jsonrpc

import (
	"github.com/kaspanet/kaspad/logs"
	rpcclient "github.com/kaspanet/kaspad/rpc/client"
	"github.com/kaspanet/kasparov/logger"
)

func init() {
	rpcclient.UseLogger(logger.BackendLog, logs.LevelInfo)
}
