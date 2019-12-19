package main

import (
	"fmt"
	"os"
)

func main() {
	subCommand, config := parseCommandLine()

	switch subCommand {
	case createSubCmd:
		create(config.(*createConfig))
	case balanceSubCmd:
		err := balance(config.(*balanceConfig))
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err)
			os.Exit(1)
		}
	case sendSubCmd:
		err := send(config.(*sendConfig))
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown sub-command '%s'\n", subCommand)
		os.Exit(1)
	}
}
