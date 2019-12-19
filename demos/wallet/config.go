package main

import (
	"os"

	"github.com/jessevdk/go-flags"
)

const (
	createSubCmd  = "create"
	balanceSubCmd = "balance"
	sendSubCmd    = "send"
)

type createConfig struct {
}

type balanceConfig struct {
	APIAddress string `long:"api-address" short:"a" description:"Address of API-Server" required:"true"`
	Address    string `long:"address" short:"d" description:"Address whose balance to check" required:"true"`
}

type sendConfig struct {
	APIAddress string  `long:"api-address" short:"a" description:"Address of API-Server" required:"true"`
	PrivateKey string  `long:"private-key" short:"k" description:"Signing private key in hex" required:"true"`
	ToAddress  string  `long:"to-address" short:"t" description:"Address to which to send funds" required:"true"`
	SendAmount float64 `long:"send-amount" short:"v" description:"Amount of coins to send" required:"true"`
}

func parseCommandLine() (subCommand string, config interface{}) {
	cfg := &struct{}{}
	parser := flags.NewParser(cfg, flags.PrintErrors|flags.HelpFlag)

	createConf := &createConfig{}
	parser.AddCommand(createSubCmd, "Creates a new wallet",
		"Creates a new wallet and prints it's private key as well as addresses to all networks", createConf)

	balanceConf := &balanceConfig{}
	parser.AddCommand(balanceSubCmd, "Shows the balance for a given address",
		"Shows the balance for a given address", balanceConf)

	sendConf := &sendConfig{}
	parser.AddCommand(sendSubCmd, "Sends a transaction to given address",
		"Sends a transaction to given address", sendConf)

	_, err := parser.Parse()

	if err != nil {
		if err, ok := err.(*flags.Error); ok && err.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
		return "", nil
	}

	switch parser.Command.Active.Name {
	case createSubCmd:
		config = createConf
	case balanceSubCmd:
		config = balanceConf
	case sendSubCmd:
		config = sendConf
	}

	return parser.Command.Active.Name, config
}
