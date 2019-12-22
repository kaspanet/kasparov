package main

import (
	"fmt"
	"os"

	"github.com/kaspanet/kaspad/dagconfig"
	"github.com/kaspanet/kaspad/ecc"
	"github.com/kaspanet/kaspad/util"
)

func create(*createConfig) {
	privateKey, err := ecc.NewPrivateKey(ecc.S256())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to generate private key: %s", err)
		os.Exit(1)
	}

	fmt.Println("This is your private key, granting access to all wallet funds. Keep it safe.")
	fmt.Printf("\nPrivate key (hex): %x\n", privateKey.Serialize())

	fmt.Println("These are your public addresses for each network, where money is to be sent.")
	for _, netParams := range []*dagconfig.Params{&dagconfig.MainNetParams, &dagconfig.TestNetParams, &dagconfig.DevNetParams} {
		addr, err := util.NewAddressPubKeyHashFromPublicKey(privateKey.PubKey().SerializeCompressed(), netParams.Prefix)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to generate p2pkh address: %s", err)
			os.Exit(1)
		}
		fmt.Printf("Address (%s): %s\n", netParams.Name, addr)
	}
}
