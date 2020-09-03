package main

import (
	"fmt"
	"os"

	"github.com/kaspanet/kaspad/util/profiling"

	"github.com/pkg/errors"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/kaspanet/kaspad/infrastructure/os/signal"
	"github.com/kaspanet/kaspad/util/panics"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/kaspadrpc"
	"github.com/kaspanet/kasparov/kasparovd/config"
	"github.com/kaspanet/kasparov/kasparovd/server"
	"github.com/kaspanet/kasparov/version"
)

func main() {
	defer panics.HandlePanic(log, "main", nil)
	interrupt := signal.InterruptListener()

	err := config.Parse()
	if err != nil {
		errString := fmt.Sprintf("Error parsing command-line arguments: %s\n", err)
		_, fErr := fmt.Fprint(os.Stderr, errString)
		if fErr != nil {
			panic(errString)
		}
		return
	}

	// Show version at startup.
	log.Infof("Version %s", version.Version())

	// Start the profiling server if required
	if config.ActiveConfig().Profile != "" {
		profiling.Start(config.ActiveConfig().Profile, log)
	}

	err = database.Connect(&config.ActiveConfig().KasparovFlags)
	if err != nil {
		panic(errors.Errorf("Error connecting to database: %s", err))
	}
	defer func() {
		err := database.Close()
		if err != nil {
			panic(errors.Errorf("Error closing the database: %s", err))
		}
	}()

	client, err := kaspadrpc.NewClient(&config.ActiveConfig().KasparovFlags, false)
	if err != nil {
		panic(errors.Errorf("Error connecting to servers: %s", err))
	}
	defer client.Close()

	shutdownServer := server.Start(config.ActiveConfig().HTTPListen)
	defer shutdownServer()

	<-interrupt
}
