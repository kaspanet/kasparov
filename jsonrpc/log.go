package jsonrpc

import (
	"github.com/kaspanet/kaspad/logs"
	"github.com/kaspanet/kaspad/rpcclient"
	"github.com/kaspanet/kasparov/logger"
)

func init() {
	rpcclient.UseLogger(logger.BackendLog, logs.LevelTrace)
}
