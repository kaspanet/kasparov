package profiling

import (
	"github.com/kaspanet/kaspad/util/panics"
	"github.com/kaspanet/kasparov/logger"
)

var (
	log   = logger.Logger("PROF")
	spawn = panics.GoroutineWrapperFunc(log)
)
