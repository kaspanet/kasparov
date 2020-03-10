package config

import (
	"path/filepath"

	"github.com/jessevdk/go-flags"
	"github.com/kaspanet/kaspad/config"
	"github.com/kaspanet/kasparov/logger"
	"github.com/pkg/errors"
)

var (
	// Default configuration options
	defaultDBHost    = "localhost"
	defaultDBPort    = "5432"
	defaultDBSSLMode = "disable"
)

// KasparovFlags holds configuration common to both the Kasparov server and the Kasparov daemon.
type KasparovFlags struct {
	ShowVersion bool   `short:"V" long:"version" description:"Display version information and exit"`
	LogDir      string `long:"logdir" description:"Directory to log output."`
	DebugLevel  string `short:"d" long:"debuglevel" description:"Set log level {trace, debug, info, warn, error, critical}"`
	DBHost      string `long:"dbhost" description:"Database host" default:"localhost"`
	DBPort      string `long:"dbport" description:"Database port" default:"5432"`
	DBSSLMode   string `long:"dbsslmode" description:"Database SSL mode" choice:"disable" choice:"allow" choice:"prefer" choice:"require" choice:"verify-ca" choice:"verify-full" default:"disable"`
	DBUser      string `long:"dbuser" description:"Database user" required:"true"`
	DBPassword  string `long:"dbpass" description:"Database password" required:"true"`
	DBName      string `long:"dbname" description:"Database name" required:"true"`
	RPCUser     string `short:"u" long:"rpcuser" description:"RPC username"`
	RPCPassword string `short:"P" long:"rpcpass" default-mask:"-" description:"RPC password"`
	RPCServer   string `short:"s" long:"rpcserver" description:"RPC server to connect to"`
	RPCCert     string `short:"c" long:"rpccert" description:"RPC server certificate chain for validation"`
	DisableTLS  bool   `long:"notls" description:"Disable TLS"`
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

	if kasparovFlags.DBHost == "" {
		kasparovFlags.DBHost = defaultDBHost
	}

	if kasparovFlags.DBPort == "" {
		kasparovFlags.DBPort = defaultDBPort
	}

	if kasparovFlags.DBSSLMode == "" {
		kasparovFlags.DBSSLMode = defaultDBSSLMode
	}
	if kasparovFlags.RPCUser == "" && !isMigrate {
		return errors.New("--rpcuser is required")
	}
	if kasparovFlags.RPCPassword == "" && !isMigrate {
		return errors.New("--rpcpass is required")
	}
	if kasparovFlags.RPCServer == "" && !isMigrate {
		return errors.New("--rpcserver is required")
	}

	if kasparovFlags.RPCCert == "" && !kasparovFlags.DisableTLS && !isMigrate {
		return errors.New("--notls has to be disabled if --cert is used")
	}
	if kasparovFlags.RPCCert != "" && kasparovFlags.DisableTLS && !isMigrate {
		return errors.New("--cert should be omitted if --notls is used")
	}

	if isMigrate {
		return nil
	}
	return kasparovFlags.ResolveNetwork(parser)
}
