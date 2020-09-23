package config

import (
	"path/filepath"
	"strconv"

	"github.com/jessevdk/go-flags"
	"github.com/kaspanet/kaspad/infrastructure/config"
	"github.com/kaspanet/kasparov/logger"
	"github.com/pkg/errors"
)

// KasparovFlags holds configuration common to both the Kasparov server and the Kasparov daemon.
type KasparovFlags struct {
	ShowVersion bool   `short:"V" long:"version" description:"Display version information and exit"`
	LogDir      string `long:"logdir" description:"Directory to log output."`
	DebugLevel  string `short:"d" long:"debuglevel" description:"Set log level {trace, debug, info, warn, error, critical}"`
	DBAddress   string `long:"dbaddress" description:"Database address" default:"localhost:5432"`
	DBSSLMode   string `long:"dbsslmode" description:"Database SSL mode" choice:"disable" choice:"allow" choice:"prefer" choice:"require" choice:"verify-ca" choice:"verify-full" default:"disable"`
	DBUser      string `long:"dbuser" description:"Database user" required:"true"`
	DBPassword  string `long:"dbpass" description:"Database password" required:"true"`
	DBName      string `long:"dbname" description:"Database name" required:"true"`
	RPCServer   string `short:"s" long:"rpcserver" description:"RPC server to connect to"`
	Profile     string `long:"profile" description:"Enable HTTP profiling on the given port"`
	config.NetworkFlags
}

// ResolveKasparovFlags parses command line arguments and sets KasparovFlags accordingly.
func (kasparovFlags *KasparovFlags) ResolveKasparovFlags(parser *flags.Parser,
	defaultLogDir, logFilename, errLogFilename string, isMigrate bool) error {
	if kasparovFlags.LogDir == "" {
		kasparovFlags.LogDir = defaultLogDir
	}
	logFile := filepath.Join(kasparovFlags.LogDir, logFilename)
	errLogFile := filepath.Join(kasparovFlags.LogDir, errLogFilename)
	logger.InitLog(logFile, errLogFile)

	if kasparovFlags.DebugLevel != "" {
		err := logger.SetLogLevels(kasparovFlags.DebugLevel)
		if err != nil {
			return err
		}
	}

	if kasparovFlags.RPCServer == "" && !isMigrate {
		return errors.New("--rpcserver is required")
	}

	if isMigrate {
		return nil
	}

	if kasparovFlags.Profile != "" {
		profilePort, err := strconv.Atoi(kasparovFlags.Profile)
		if err != nil || profilePort < 1024 || profilePort > 65535 {
			return errors.New("The profile port must be between 1024 and 65535")
		}
	}

	return kasparovFlags.ResolveNetwork(parser)
}
