package main

import (
	"fmt"
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
	"github.com/pkg/errors"
)

func main() {
	defer panics.HandlePanic(log, nil, nil)
	interrupt := signal.InterruptListener()

	err := config.Parse()
	if err != nil {
		errString := fmt.Sprintf("Error parsing command-line arguments: %s", err)
		_, fErr := fmt.Fprintf(os.Stderr, errString)
		if fErr != nil {
			panic(errString)
		}
		return
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

	err = jsonrpc.Connect(&config.ActiveConfig().KasparovFlags)
	if err != nil {
		panic(errors.Errorf("Error connecting to servers: %s", err))
	}
	defer jsonrpc.Close()

	doneChan := make(chan struct{}, 1)
	spawn(func() {
		err := startSync(doneChan)
		if err != nil {
			panic(err)
		}
	})

	<-interrupt

	// Gracefully stop syncing
	doneChan <- struct{}{}
}
