package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jessevdk/go-flags"
	"github.com/kaspanet/kaspad/util"
	"github.com/kaspanet/kasparov/config"
	"github.com/kaspanet/kasparov/version"
	"github.com/pkg/errors"
)

const (
	logFilename    = "kasparov_syncd.log"
	errLogFilename = "kasparov_syncd_err.log"
)

var (
	// Default configuration options
	defaultLogDir = util.AppDataDir("kasparov_syncd", false)
	activeConfig  *Config
)

// ActiveConfig returns the active configuration struct
func ActiveConfig() *Config {
	return activeConfig
}

// Config defines the configuration options for the sync daemon.
type Config struct {
	Migrate           bool   `long:"migrate" description:"Migrate the database to the latest version. The daemon will not start when using this flag."`
	MQTTBrokerAddress string `long:"mqttaddress" description:"MQTT broker address" required:"false"`
	MQTTUser          string `long:"mqttuser" description:"MQTT server user" required:"false"`
	MQTTPassword      string `long:"mqttpass" description:"MQTT server password" required:"false"`
	config.KasparovFlags
}

// Parse parses the CLI arguments and returns a config struct.
func Parse() error {
	activeConfig = &Config{}
	parser := flags.NewParser(activeConfig, flags.HelpFlag)
	_, err := parser.Parse()
	// Show the version and exit if the version flag was specified.

	if activeConfig.ShowVersion {
		appName := filepath.Base(os.Args[0])
		appName = strings.TrimSuffix(appName, filepath.Ext(appName))
		fmt.Println(appName, "version", version.Version())
		os.Exit(0)
	}

	if err != nil {
		return err
	}

	err = activeConfig.ResolveKasparovFlags(parser, defaultLogDir, logFilename, errLogFilename)
	if err != nil {
		return err
	}

	if (activeConfig.MQTTBrokerAddress != "" || activeConfig.MQTTUser != "" || activeConfig.MQTTPassword != "") &&
		(activeConfig.MQTTBrokerAddress == "" || activeConfig.MQTTUser == "" || activeConfig.MQTTPassword == "") {
		return errors.New("--mqttaddress, --mqttuser, and --mqttpass must be passed all together")
	}

	return nil
}
