package jsonrpc

import (
	"github.com/kaspanet/kaspad/logs"
	"github.com/kaspanet/kaspad/rpcclient"
)

var (
	backendLog = logs.NewBackend()
)

func init() {
	rpcclient.UseLogger(backendLog, logs.LevelTrace)
}
