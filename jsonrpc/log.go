package jsonrpc

import (
	kaspadlogger "github.com/kaspanet/kaspad/infrastructure/logger"
	rpcclient "github.com/kaspanet/kaspad/infrastructure/network/rpc/client"
	"github.com/kaspanet/kasparov/logger"
)

func init() {
	rpcclient.UseLogger(logger.BackendLog, kaspadlogger.LevelInfo)
}
