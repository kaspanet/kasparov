package main

import (
	"github.com/kaspanet/kaspad/util/panics"
	"github.com/kaspanet/kasparov/logger"
)

var (
	log   = logger.Logger("KVSV")
	spawn = panics.GoroutineWrapperFunc(log)
)
