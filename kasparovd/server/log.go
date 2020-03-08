package server

import (
	"github.com/kaspanet/kaspad/util/panics"
	"github.com/kaspanet/kasparov/logger"
)

var (
	log   = logger.Logger("REST")
	spawn = panics.GoroutineWrapperFunc(log)
)
