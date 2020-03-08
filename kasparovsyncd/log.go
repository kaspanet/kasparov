package main

import (
	"github.com/kaspanet/kaspad/util/panics"
	"github.com/kaspanet/kasparov/logger"
)

var (
	log   = logger.Logger("KVSD")
	spawn = panics.GoroutineWrapperFunc(log)
)
