package main

import (
	"fmt"
	"os"

	"github.com/kaspanet/kaspad/util/profiling"
	"github.com/kaspanet/kasparov/kasparovsyncd/sync"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/kaspanet/kaspad/infrastructure/os/signal"
	"github.com/kaspanet/kaspad/util/panics"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/kaspadrpc"
	"github.com/kaspanet/kasparov/kasparovsyncd/config"
	"github.com/kaspanet/kasparov/kasparovsyncd/mqtt"
	"github.com/kaspanet/kasparov/version"
	"github.com/pkg/errors"
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

	if config.ActiveConfig().Migrate {
		err := database.Migrate(&config.ActiveConfig().KasparovFlags)
		if err != nil {
			panic(errors.Errorf("Error migrating database: %s", err))
		}
		return
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

	err = mqtt.Connect()
	if err != nil {
		panic(errors.Errorf("Error connecting to MQTT: %s", err))
	}
	defer mqtt.Close()

	client, err := kaspadrpc.NewClient(&config.ActiveConfig().KasparovFlags, true)
	if err != nil {
		panic(errors.Errorf("Error connecting to servers: %s", err))
	}
	defer client.Close()

	doneChan := make(chan struct{}, 1)
	spawn("main-sync.StartSync", func() {
		err := sync.StartSync(doneChan)
		if err != nil {
			panic(err)
		}
	})

	<-interrupt

	// Gracefully stop syncing
	doneChan <- struct{}{}
}
