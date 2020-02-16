package main

import (
	"fmt"
	"github.com/kaspanet/kasparov/kasparovsyncd/sync"
	"os"

	_ "github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/kaspanet/kaspad/signal"
	"github.com/kaspanet/kaspad/util/panics"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/jsonrpc"
	"github.com/kaspanet/kasparov/kasparovsyncd/config"
	"github.com/kaspanet/kasparov/kasparovsyncd/mqtt"
	"github.com/kaspanet/kasparov/version"
	"github.com/pkg/errors"
)

func main() {
	defer panics.HandlePanic(log, nil, nil)
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

	err = jsonrpc.Connect(&config.ActiveConfig().KasparovFlags)
	if err != nil {
		panic(errors.Errorf("Error connecting to servers: %s", err))
	}
	defer jsonrpc.Close()

	doneChan := make(chan struct{}, 1)
	spawn(func() {
		err := sync.StartSync(doneChan)
		if err != nil {
			panic(err)
		}
	})

	<-interrupt

	// Gracefully stop syncing
	doneChan <- struct{}{}
}
