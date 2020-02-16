package jsonrpc

import (
	"github.com/kaspanet/kaspad/rpcclient"
	"github.com/kaspanet/kasparov/logger"
)

var (
	log = logger.BackendLog.Logger("RPCC")
)

func init() {
	rpcclient.UseLogger(log)
}
