package jsonrpc

import (
	"github.com/kaspanet/kaspad/rpcclient"
	"github.com/kaspanet/kaspad/util/panics"
	"github.com/kaspanet/kasparov/logger"
)

var (
	log   = logger.BackendLog.Logger("RPCC")
	spawn = panics.GoroutineWrapperFunc(log)
)

func init() {
	rpcclient.UseLogger(log)
}
