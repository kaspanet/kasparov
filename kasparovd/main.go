package main

import (
	"fmt"
	"os"

	"github.com/pkg/errors"

	_ "github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/kaspanet/kaspad/signal"
	"github.com/kaspanet/kaspad/util/panics"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/jsonrpc"
	"github.com/kaspanet/kasparov/kasparovd/config"
	"github.com/kaspanet/kasparov/kasparovd/server"
	"github.com/kaspanet/kasparov/version"
)

func main() {
	defer panics.HandlePanic(log, nil, nil)
	interrupt := signal.InterruptListener()

	err := config.Parse()
	if err != nil {
		errString := fmt.Sprintf("Error parsing command-line arguments: %s\n", err)
		_, fErr := fmt.Fprintf(os.Stderr, errString)
		if fErr != nil {
			panic(errString)
		}
		return
	}

	// Show version at startup.
	log.Infof("Version %s", version.Version())

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

	err = jsonrpc.Connect(&config.ActiveConfig().KasparovFlags)
	if err != nil {
		panic(errors.Errorf("Error connecting to servers: %s", err))
	}
	defer jsonrpc.Close()

	shutdownServer := server.Start(config.ActiveConfig().HTTPListen)
	defer shutdownServer()

	<-interrupt
}
